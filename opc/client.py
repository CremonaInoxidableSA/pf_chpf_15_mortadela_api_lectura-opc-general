import asyncio
import logging

from asyncua import Client, Node

from config.settings import (
    OPC_ENDPOINT,
    PUBLISHING_INTERVAL_MS,
    SAMPLING_INTERVAL_MS,
    QUEUE_MAXSIZE,
    BROWSE_DEPTH,
    ALL_OBJECT_NAMES,
    endpoint_configs,
    endpoint_cache,
    buffer_configs,
)
from config.ws import ConnectionManager
from opc.browser import find_objects_by_name
from opc.handler import DataChangeHandler
from opc.buffer_monitor import run_buffer_monitor, run_cycle_monitor
from opc.source_ws import run_source_ws


async def queue_worker(queue: asyncio.Queue, mgr: ConnectionManager, cache: dict):
    """Consume payloads de la cola y los despacha por WebSocket."""
    while True:
        item = await queue.get()
        try:
            logging.info("Cambio | %s | valor=%s", item["tag"], item["value"])
            cache[item["tag"]] = item
            await mgr.broadcast_json(item)
        finally:
            queue.task_done()


async def run_client():
    """Cliente OPC-UA con reconexión automática.
    Busca objetos, crea suscripciones y despacha cambios a cada endpoint."""
    while True:
        worker_tasks: list[asyncio.Task] = []
        subscription = None
        try:
            logging.info("Conectando a %s …", OPC_ENDPOINT)

            async with Client(url=OPC_ENDPOINT, timeout=4) as client:
                # ── Buscar objetos declarados en endpoints.json ──────
                obj_map = await find_objects_by_name(
                    client.nodes.objects, ALL_OBJECT_NAMES, BROWSE_DEPTH,
                )
                missing = ALL_OBJECT_NAMES - obj_map.keys()
                if missing:
                    logging.warning("Objetos NO encontrados: %s", missing)
                if not obj_map:
                    raise RuntimeError("Ningún objeto de endpoints.json encontrado")

                # ── Crear colas y mapear nodeids → queues por endpoint
                all_nodes: list[Node] = []
                all_labels: dict[str, str] = {}
                node_to_queues: dict[str, list[asyncio.Queue]] = {}
                ep_queues: dict[str, asyncio.Queue] = {}

                for path, ep_cfg in endpoint_configs.items():
                    q = asyncio.Queue(maxsize=QUEUE_MAXSIZE)
                    ep_queues[path] = q
                    for obj_name in ep_cfg["opc_objects"]:
                        if obj_name in obj_map:
                            n = obj_map[obj_name]
                            nid = n.nodeid.to_string()
                            if nid not in all_labels:
                                all_nodes.append(n)
                                all_labels[nid] = obj_name
                            node_to_queues.setdefault(nid, []).append(q)

                if not all_nodes:
                    raise RuntimeError("No se encontraron variables en ningún objeto")
                logging.info("Objetos suscritos en total: %d", len(all_nodes))

                # ── Lectura inicial ──────────────────────────────────
                values = await client.read_values(all_nodes)
                for n, v in zip(all_nodes, values):
                    logging.info("  %s = %s", all_labels[n.nodeid.to_string()], v)

                # ── Workers por endpoint ─────────────────────────────
                for path, ep_cfg in endpoint_configs.items():
                    task = asyncio.create_task(
                        queue_worker(ep_queues[path], ep_cfg["manager"], endpoint_cache[path])
                    )
                    worker_tasks.append(task)

                # ── Fuentes WebSocket externas ───────────────────────
                for path, ep_cfg in endpoint_configs.items():
                    source_url = ep_cfg.get("source_ws")
                    if source_url:
                        task = asyncio.create_task(
                            run_source_ws(source_url, client, obj_map)
                        )
                        worker_tasks.append(task)
                        logging.info("Fuente WS iniciada: %s → %s", source_url, path)

                # ── Buffer monitors ──────────────────────────────────
                # Ciclos (BD): monitor único para todos
                if buffer_configs:
                    task = asyncio.create_task(
                        run_cycle_monitor(client, buffer_configs, obj_map)
                    )
                    worker_tasks.append(task)
                    logging.info("Monitor de ciclos iniciado")

                # Buffers (tiempo real): monitor por cada buffer
                for buf_path, buf_cfg in buffer_configs.items():
                    task = asyncio.create_task(
                        run_buffer_monitor(client, buf_path, buf_cfg, obj_map)
                    )
                    worker_tasks.append(task)
                    logging.info("Buffer monitor iniciado: %s", buf_path)

                # ── Suscripción principal ─────────────────────────────
                handler = DataChangeHandler(all_labels, node_to_queues)
                subscription = await client.create_subscription(
                    PUBLISHING_INTERVAL_MS, handler
                )
                await subscription.subscribe_data_change(
                    all_nodes,
                    queuesize=1,
                    sampling_interval=SAMPLING_INTERVAL_MS,
                )
                logging.info(
                    "Suscripción activa: %d variables → %d endpoints",
                    len(all_nodes), len(endpoint_configs),
                )

                # ── Keep-alive ───────────────────────────────────────
                while True:
                    await client.check_connection()
                    await asyncio.sleep(2)

        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.exception("Desconectado/error: %s", exc)
            logging.info("Reintentando en 3 s…")
            await asyncio.sleep(3)
        finally:
            if subscription:
                try:
                    await subscription.delete()
                except Exception:
                    pass
            for t in worker_tasks:
                t.cancel()
                try:
                    await t
                except asyncio.CancelledError:
                    pass
