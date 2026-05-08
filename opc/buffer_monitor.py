import asyncio
import logging
from datetime import datetime

from asyncua import Client, Node, ua

from config.settings import PUBLISHING_INTERVAL_MS, SAMPLING_INTERVAL_MS, QUEUE_MAXSIZE, buffer_cache, ciclo_cache
from config.ws import ConnectionManager
from opc.browser import find_objects_by_name, read_node_tree
from opc.handler import DataChangeHandler
from config.bdd import create_ciclo_sync, close_ciclo_sync, get_open_ciclo_sync, create_fallo_captura_sync

# Contador global de pausas acumuladas (segundos) y momento de inicio de la pausa activa
pausa_counter: int = 0
pausa_start: datetime | None = None


def get_tiempo_pausa_actual() -> int:
    """Retorna pausa_counter + los segundos de la pausa actualmente en curso (si la hay).
    Usar justo antes de cerrar un ciclo para no perder el tramo activo.
    """
    extra = 0
    if pausa_start is not None:
        extra = int((datetime.now() - pausa_start).total_seconds())
    return pausa_counter + extra


# ============== MONITOR DE CICLOS (BD) ==============

async def run_cycle_monitor(
    client: Client,
    buf_cfgs: dict,
    obj_map: dict[str, Node],
):
    """Suscripción dedicada a inicioCiclo y finCiclo.
    Crea/cierra ciclos en BD con receta y rack actual.
    
    buf_cfgs: dict de todos los buffers (para extraer config de ciclos)
    """
    global pausa_counter, pausa_start
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    node_labels: dict[str, str] = {}
    node_to_queues: dict[str, list[asyncio.Queue]] = {}
    nodes_list: list[Node] = []

    # Hardcodeados: los nodos de ciclo son globales y siempre los mismos
    # inicioCiclo y finCiclo son SOLO para BD, no para WebSocket
    all_triggers: dict[str, str] = {}  # label → nombre_nodo
    for buf_path in buf_cfgs.keys():
        all_triggers[f"{buf_path}__inicio"] = "inicioCiclo"
        all_triggers[f"{buf_path}__fin"] = "finCiclo"

    # Resolver nodos
    for key, name in all_triggers.items():
        node = obj_map.get(name)
        if node:
            nid = node.nodeid.to_string()
            node_labels[nid] = key
            node_to_queues[nid] = [queue]
            nodes_list.append(node)
        else:
            logging.warning("Ciclo trigger no encontrado: %s", name)

    if not nodes_list:
        logging.error("No se encontraron nodos de ciclos")
        return

    # Resolvamos nodos de receta y rack
    receta_node = obj_map.get("recetaActual")
    rack_node = obj_map.get("rackActual")

    if not receta_node or not rack_node:
        logging.error("No se encontraron nodos recetaActual o rackActual")
        return

    prev_values: dict[str, bool] = {}

    # Crear suscripción
    handler = DataChangeHandler(node_labels, node_to_queues)
    subscription = await client.create_subscription(PUBLISHING_INTERVAL_MS, handler)
    await subscription.subscribe_data_change(
        nodes_list, queuesize=1, sampling_interval=SAMPLING_INTERVAL_MS
    )
    logging.info("Monitor de ciclos activo: %s triggers", len(nodes_list))

    try:
        while True:
            item = await queue.get()
            try:
                key = item["tag"]  # "buf_path__inicio" o "buf_path__fin"
                val = bool(item["value"])
                prev = prev_values.get(key, False)
                prev_values[key] = val

                if val and not prev:  # Flanco ascendente
                    parts = key.split("__")
                    if len(parts) == 2:
                        buf_path, event_type = parts

                        if event_type == "inicio":
                            # ── Lectura de receta y rack ──
                            try:
                                id_receta = await receta_node.read_value()
                                id_rack = await rack_node.read_value()
                            except Exception as e:
                                logging.warning("Error leyendo receta/rack: %s", e)
                                id_receta = None
                                id_rack = None

                            # ── Si hay ciclo anterior abierto, ciérralo ──
                            old_ciclo_id = ciclo_cache.get(buf_path)
                            if old_ciclo_id:
                                logging.warning(
                                    "Ciclo anterior abierto sin cerrar (id=%s). Cerrando ahora.",
                                    old_ciclo_id,
                                )
                                close_ciclo_sync(old_ciclo_id, datetime.now(), get_tiempo_pausa_actual())

                            # ── Resetear contador de pausas para el nuevo ciclo ──
                            pausa_counter = 0
                            pausa_start = None

                            # ── Si estadoEquipo ya es 2, arrancar el contador de pausa ahora ──
                            estado_node = obj_map.get("estadoEquipo")
                            if estado_node:
                                try:
                                    estado_actual = int(await estado_node.read_value())
                                    if estado_actual == 2:
                                        pausa_start = datetime.now()
                                        logging.info("InicioCiclo: estadoEquipo ya es 2, pausa iniciada desde el arranque")
                                except Exception as e:
                                    logging.warning("No se pudo leer estadoEquipo al iniciar ciclo: %s", e)

                            # ── Crear nuevo ciclo en BD ──
                            try:
                                new_ciclo_id = create_ciclo_sync(
                                    datetime.now(), id_receta, id_rack
                                )
                                ciclo_cache[buf_path] = new_ciclo_id
                                logging.info(
                                    "InicioCiclo %s: ciclo_id=%s, receta=%s, rack=%s",
                                    buf_path, new_ciclo_id, id_receta, id_rack,
                                )
                            except Exception as e:
                                logging.error("Error creando ciclo: %s", e)

                        elif event_type == "fin":
                            # ── Cerrar ciclo actual ──
                            ciclo_id = ciclo_cache.get(buf_path)
                            if ciclo_id:
                                try:
                                    tiempo_pausa_final = get_tiempo_pausa_actual()
                                    close_ciclo_sync(ciclo_id, datetime.now(), tiempo_pausa_final)
                                    logging.info("FinCiclo %s: ciclo_id=%s, tiempo_pausa=%ss", buf_path, ciclo_id, tiempo_pausa_final)
                                    ciclo_cache[buf_path] = None
                                    pausa_counter = 0
                                    pausa_start = None
                                except Exception as e:
                                    logging.error("Error cerrando ciclo: %s", e)
                            else:
                                logging.warning("FinCiclo detectado pero no hay ciclo abierto: %s", buf_path)

            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Error en cycle monitor")
            finally:
                queue.task_done()

    finally:
        try:
            await subscription.delete()
        except Exception:
            pass


# ============== MONITOR DE BUFFERS (Búsqueda) ==============

async def run_simple_buffer_monitor(
    client: Client,
    buf_path: str,
    buf_cfg: dict,
    obj_map: dict[str, Node],
):
    """Monitorea el nodo "buscar" del buffer (dentro del árbol).
    Cuando detecta flanco true: lee árbol completo, envía por WS y escribe false.
    """

    manager: ConnectionManager = buf_cfg["manager"]
    buffer_root_name = buf_cfg["buffer_root"]
    buffer_root_node = obj_map.get(buffer_root_name)
    
    # Extraer número de buffer (buffer1 → "1", buffer2 → "2")
    # El nodo buscar sigue el patrón: buscarBuffer{num}
    buffer_num = buffer_root_name.replace("buffer", "")
    buscar_name = f"buscarBuffer{buffer_num}"

    if buffer_root_node is None:
        logging.error("Buffer %s: nodo raíz '%s' no encontrado", buf_path, buffer_root_name)
        return

    if not buscar_name:
        logging.warning("Buffer %s: no hay nodo 'buscar' configurado", buf_path)
        return

    # ── BUSCAR el nodo "buscar" DENTRO del árbol del buffer ──
    logging.info("Buffer %s: buscando nodo '%s' dentro de %s…", buf_path, buscar_name, buffer_root_name)
    
    inner_nodes = await find_objects_by_name(
        buffer_root_node,
        {buscar_name},  # Buscar específicamente este nodo
        max_depth=10,
    )
    
    buscar_node = inner_nodes.get(buscar_name)
    
    if not buscar_node:
        logging.error("Buffer %s: nodo 'buscar' '%s' no encontrado dentro de %s", 
                     buf_path, buscar_name, buffer_root_name)
        return

    # Cola dedicada para el nodo de buscar
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    node_labels: dict[str, str] = {buscar_node.nodeid.to_string(): buscar_name}
    node_to_queues: dict[str, list[asyncio.Queue]] = {
        buscar_node.nodeid.to_string(): [queue]
    }

    prev_buscar_value = False

    # Handler y suscripción (SOLO el nodo de buscar)
    handler = DataChangeHandler(node_labels, node_to_queues)
    subscription = await client.create_subscription(PUBLISHING_INTERVAL_MS, handler)
    await subscription.subscribe_data_change(
        [buscar_node], queuesize=1, sampling_interval=SAMPLING_INTERVAL_MS
    )
    logging.info("Monitor de búsqueda activo: %s (monitoreando %s)", buf_path, buscar_name)

    try:
        while True:
            item = await queue.get()
            try:
                val = bool(item.get("value"))
                logging.debug("Buffer %s: Recibido item en cola: %s = %s (prev: %s)", 
                             buf_path, buscar_name, val, prev_buscar_value)
                
                # Detectar flanco ascendente (false → true)
                if val and not prev_buscar_value:
                    logging.info("Buffer %s: Flanco %s = true → leyendo y enviando…", buf_path, buscar_name)
                    
                    try:
                        # Leer árbol completo del buffer
                        logging.debug("Buffer %s: Intentando leer árbol desde %s…", buf_path, buffer_root_name)
                        buffer_data = await read_node_tree(buffer_root_node)
                        logging.debug("Buffer %s: Árbol leído correctamente. Keys: %s", buf_path, list(buffer_data.keys()))
                        
                        # ── FILTRAR: excluir el nodo buscar y los nodos de ciclo que no deben salir por WS ──
                        excludes = {buscar_name, "recetaActual", "rackActual"}
                        filtered_data = {k: v for k, v in buffer_data.items() if k not in excludes}
                        
                        # Añadir nodos extra configurados para este buffer (solo numeroEquipo aquí)
                        for extra_node in buf_cfg.get("extra_nodes", []):
                            if extra_node in ("recetaActual", "rackActual"):
                                continue
                            if extra_node in obj_map:
                                try:
                                    filtered_data[extra_node] = await obj_map[extra_node].read_value()
                                except Exception as e:
                                    logging.warning(
                                        "Buffer %s: no se pudo leer nodo extra %s: %s",
                                        buf_path,
                                        extra_node,
                                        e,
                                    )
                        
                        logging.debug("Buffer %s: Datos filtrados (excluído: %s). Keys después filtro: %s", 
                                     buf_path, buscar_name, list(filtered_data.keys()))
                        buffer_cache[buf_path] = filtered_data
                        
                        # Enviar por WebSocket
                        logging.info("Buffer %s: Enviando por WebSocket a través de manager…", buf_path)
                        await manager.broadcast_json(filtered_data)
                        logging.info("Buffer %s: Enviado por WebSocket - %d campos", buf_path, len(filtered_data))
                        
                    except Exception as e:
                        logging.error("Buffer %s: Error leyendo o enviando árbol: %s", buf_path, e, exc_info=True)
                    
                    # Escribir false en el nodo de buscar
                    try:
                        false_dv = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
                        await buscar_node.write_attribute(ua.AttributeIds.Value, false_dv)
                        logging.info("Buffer %s: %s ← false", buf_path, buscar_name)
                    except Exception as e:
                        logging.error("Buffer %s: Error escribiendo false: %s", buf_path, e)
                else:
                    logging.debug("Buffer %s: Cambio detectado pero no es flanco (val=%s, prev=%s)", 
                                 buf_path, val, prev_buscar_value)
                
                prev_buscar_value = val

            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Error en buffer monitor %s", buf_path)
            finally:
                queue.task_done()

    finally:
        try:
            await subscription.delete()
        except Exception:
            pass


# ============== RETROCOMPATIBILIDAD ==============

async def run_buffer_monitor(
    client: Client,
    buf_path: str,
    buf_cfg: dict,
    obj_map: dict[str, Node],
):
    """Función reemplazada por run_simple_buffer_monitor.
    Mantiene firma compatible."""
    await run_simple_buffer_monitor(client, buf_path, buf_cfg, obj_map)


# ============== MONITOR DE FALLOS DE LECTURA ==============

async def run_fallo_monitor(
    client: Client,
    obj_map: dict[str, Node],
):
    """Suscripción dedicada al nodo booleano 'falloCiclos'.
    Cuando detecta flanco ascendente (false → true), registra un fallo en BD.
    """
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    
    # Buscar el nodo falloCiclos
    fallo_node = obj_map.get("falloCiclos")
    if not fallo_node:
        logging.error("Monitor de fallos: nodo 'falloCiclos' no encontrado")
        return
    
    # Configurar suscripción
    node_labels: dict[str, str] = {fallo_node.nodeid.to_string(): "falloCiclos"}
    node_to_queues: dict[str, list[asyncio.Queue]] = {
        fallo_node.nodeid.to_string(): [queue]
    }
    
    prev_fallo_value = False
    
    handler = DataChangeHandler(node_labels, node_to_queues)
    subscription = await client.create_subscription(PUBLISHING_INTERVAL_MS, handler)
    await subscription.subscribe_data_change(
        [fallo_node], queuesize=1, sampling_interval=SAMPLING_INTERVAL_MS
    )
    logging.info("Monitor de fallos activo: falloCiclos")
    
    try:
        while True:
            item = await queue.get()
            try:
                val = bool(item.get("value"))
                
                # Detectar flanco ascendente (false → true)
                if val and not prev_fallo_value:
                    logging.warning("Flanco ascendente falloCiclos detectado: registrando fallo en BD")
                    try:
                        create_fallo_captura_sync(datetime.now())
                        logging.info("✓ Fallo de lectura registrado correctamente")
                    except Exception as e:
                        logging.error("✗ Error registrando fallo: %s", e)
                
                prev_fallo_value = val
                
            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Error en fallo monitor")
            finally:
                queue.task_done()
    
    finally:
        try:
            await subscription.delete()
        except Exception:
            pass


async def run_pausa_monitor(
    client: Client,
    obj_map: dict[str, Node],
):
    """Acumula los segundos que estadoEquipo == 2 (pausa activa).
    Cuando el valor cambia a 2 registra el inicio; cuando sale de 2 suma
    los segundos transcurridos a pausa_counter.
    """
    global pausa_counter, pausa_start

    estado_node = obj_map.get("estadoEquipo")
    if not estado_node:
        logging.error("Monitor de pausas: nodo 'estadoEquipo' no encontrado")
        return

    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    node_labels: dict[str, str] = {estado_node.nodeid.to_string(): "estadoEquipo"}
    node_to_queues: dict[str, list[asyncio.Queue]] = {
        estado_node.nodeid.to_string(): [queue]
    }

    prev_estado_value: int = -1

    handler = DataChangeHandler(node_labels, node_to_queues)
    subscription = await client.create_subscription(PUBLISHING_INTERVAL_MS, handler)
    await subscription.subscribe_data_change(
        [estado_node], queuesize=1, sampling_interval=SAMPLING_INTERVAL_MS
    )
    logging.info("Monitor de pausas activo: estadoEquipo")

    try:
        while True:
            item = await queue.get()
            try:
                try:
                    val_int = int(item.get("value", -1))
                except (TypeError, ValueError):
                    val_int = -1

                if val_int == 2 and prev_estado_value != 2:
                    # Entrada en pausa: registrar momento de inicio
                    pausa_start = datetime.now()
                    logging.info("Pausa iniciada: estadoEquipo=2")

                elif val_int != 2 and prev_estado_value == 2 and pausa_start is not None:
                    # Salida de pausa: acumular segundos transcurridos
                    segundos = int((datetime.now() - pausa_start).total_seconds())
                    pausa_counter += segundos
                    logging.info(
                        "Pausa finalizada: +%ds (total en ciclo: %ds)", segundos, pausa_counter
                    )
                    pausa_start = None  # ya acumulado, limpiar referencia global
                    # Sincronizar la variable global para que get_tiempo_pausa_actual() sea consistente

                prev_estado_value = val_int

            except asyncio.CancelledError:
                raise
            except Exception:
                logging.exception("Error en pausa monitor")
            finally:
                queue.task_done()

    finally:
        try:
            await subscription.delete()
        except Exception:
            pass
