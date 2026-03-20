import asyncio
import logging
from datetime import datetime

from asyncua import Client, Node, ua

from config.settings import PUBLISHING_INTERVAL_MS, SAMPLING_INTERVAL_MS, QUEUE_MAXSIZE, buffer_cache
from config.ws import ConnectionManager
from opc.browser import find_objects_by_name, read_node_tree
from opc.handler import DataChangeHandler


async def run_buffer_monitor(
    client: Client,
    buf_path: str,
    buf_cfg: dict,
    obj_map: dict[str, Node],
):
    """Suscripción dedicada a inicioCiclo, finCiclo y buscarBuffer.
    Al detectar flancos ascendentes, captura datetimes y lee datos del buffer."""
    trigger = buf_cfg["trigger_nodes"]
    manager: ConnectionManager = buf_cfg["manager"]
    buffer_root_name = buf_cfg["buffer_root"]
    extra_node_names: list[str] = buf_cfg.get("extra_nodes", [])

    # Resolver nodos extra (hermanos del buffer_root en el árbol OPC)
    extra_nodes: dict[str, Node] = {}
    for name in extra_node_names:
        node = obj_map.get(name)
        if node:
            extra_nodes[name] = node
        else:
            logging.warning("Buffer %s: nodo extra '%s' no encontrado", buf_path, name)

    # Estado del ciclo
    inicio_dt = None
    fin_dt = None
    buffer_data = None
    prev_values: dict[str, bool] = {}

    # Nodos requeridos
    inicio_node = obj_map.get(trigger["inicio_ciclo"])
    fin_node = obj_map.get(trigger["fin_ciclo"])
    buffer_root_node = obj_map.get(buffer_root_name)

    # buscar y confirmacion son hijos de buffer_root
    buscar_node = None
    confirm_node = None
    if buffer_root_node:
        inner = await find_objects_by_name(
            buffer_root_node,
            {trigger["buscar"], trigger["confirmacion"]},
            max_depth=2,
        )
        buscar_node = inner.get(trigger["buscar"])
        confirm_node = inner.get(trigger["confirmacion"])

    subscribe_map = {
        trigger["inicio_ciclo"]: inicio_node,
        trigger["fin_ciclo"]: fin_node,
        trigger["buscar"]: buscar_node,
    }
    missing = [name for name, node in subscribe_map.items() if node is None]
    if missing:
        logging.error("Buffer %s: nodos trigger no encontrados: %s", buf_path, missing)
        return
    if buffer_root_node is None:
        logging.error("Buffer %s: nodo raíz '%s' no encontrado", buf_path, buffer_root_name)
        return

    # Cola y suscripción dedicada
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
    node_labels: dict[str, str] = {}
    node_to_queues: dict[str, list[asyncio.Queue]] = {}
    nodes_list: list[Node] = []

    for name, node in subscribe_map.items():
        nid = node.nodeid.to_string()
        node_labels[nid] = name
        node_to_queues[nid] = [queue]
        nodes_list.append(node)
        val = await node.read_value()
        prev_values[name] = bool(val)

    handler = DataChangeHandler(node_labels, node_to_queues)
    subscription = await client.create_subscription(PUBLISHING_INTERVAL_MS, handler)
    await subscription.subscribe_data_change(
        nodes_list, queuesize=1, sampling_interval=SAMPLING_INTERVAL_MS
    )
    logging.info(
        "Buffer monitor activo: %s (suscripto a %s)", buf_path, list(subscribe_map.keys())
    )

    async def try_send():
        nonlocal inicio_dt, fin_dt, buffer_data
        if inicio_dt and fin_dt and buffer_data is not None:
            # Leer nodos extra (hermanos, no hijos del buffer)
            extra_values = {}
            for name, node in extra_nodes.items():
                try:
                    extra_values[name] = await node.read_value()
                except Exception:
                    logging.warning("No se pudo leer nodo extra: %s", name)

            payload = {
                "inicioCiclo": inicio_dt.isoformat(),
                "finCiclo": fin_dt.isoformat(),
                **extra_values,
                **buffer_data,
            }
            buffer_cache[buf_path] = payload
            await manager.broadcast_json(payload)
            logging.info("Buffer %s enviado por WebSocket", buf_path)
            inicio_dt = None
            fin_dt = None
            buffer_data = None

    try:
        while True:
            item = await queue.get()
            try:
                tag = item["tag"]
                val = bool(item["value"])
                prev = prev_values.get(tag, False)
                prev_values[tag] = val

                # Solo flanco ascendente (false → true)
                if val and not prev:
                    if tag == trigger["inicio_ciclo"]:
                        inicio_dt = datetime.now()
                        logging.info("InicioCiclo detectado: %s", inicio_dt.isoformat())
                        await try_send()

                    elif tag == trigger["fin_ciclo"]:
                        fin_dt = datetime.now()
                        logging.info("FinCiclo detectado: %s", fin_dt.isoformat())
                        await try_send()

                    elif tag == trigger["buscar"]:
                        logging.info("buscarBuffer activado → leyendo datos de %s…", buffer_root_name)
                        buffer_data = await read_node_tree(buffer_root_node)
                        logging.info("Datos buffer leídos: %s", list(buffer_data.keys()))

                        # Escribir buscar y confirmacion en false
                        false_dv = ua.DataValue(ua.Variant(False, ua.VariantType.Boolean))
                        await buscar_node.write_attribute(ua.AttributeIds.Value, false_dv)
                        await confirm_node.write_attribute(ua.AttributeIds.Value, false_dv)
                        logging.info("buscar y confirmacion → false")

                        await try_send()
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
