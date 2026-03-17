import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path
import websockets
from asyncua import Client, Node, ua
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from config.ws import ConnectionManager
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)

# ── Cargar configuración desde endpoints.json ───────────────────────
CONFIG_PATH = Path(__file__).parent / "config" / "endpoints.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    CONFIG = json.load(f)

ENDPOINT = CONFIG["opc_endpoint"]
PUBLISHING_INTERVAL_MS = CONFIG.get("publishing_interval_ms", 100)
SAMPLING_INTERVAL_MS = CONFIG.get("sampling_interval_ms", 0.0)
QUEUE_MAXSIZE = CONFIG.get("queue_maxsize", 500)

BROWSE_DEPTH = CONFIG.get("browse_depth", 3)

# Estructura por endpoint: path → {manager, opc_objects, source_ws?}
endpoint_configs: dict[str, dict] = {}
for ep in CONFIG["endpoints"]:
    endpoint_configs[ep["path"]] = {
        "manager": ConnectionManager(),
        "opc_objects": ep["opc_objects"],
        "source_ws": ep.get("source_ws"),
    }

# Nombres únicos de objetos OPC necesarios
ALL_OBJECT_NAMES: set[str] = {
    name
    for ep in CONFIG["endpoints"]
    for name in ep["opc_objects"]
}

# Cache global: endpoint_path → {obj_name: último payload}
endpoint_cache: dict[str, dict[str, dict]] = {path: {} for path in endpoint_configs}


# ── Handler de suscripción ──────────────────────────────────────────
class DataChangeHandler:

    def __init__(self, node_labels: dict[str, str],
                 node_to_queues: dict[str, list[asyncio.Queue]]):
        self.node_labels = node_labels
        self.node_to_queues = node_to_queues

    def datachange_notification(self, node: Node, val, data):
        try:
            nodeid = node.nodeid.to_string()
            tag = self.node_labels.get(nodeid, nodeid)

            source_ts = server_ts = status = None
            try:
                mv = data.monitored_item.Value
                source_ts = mv.SourceTimestamp
                server_ts = mv.ServerTimestamp
                status = mv.StatusCode
            except Exception:
                pass

            payload = {
                "nodeid": nodeid,
                "tag": tag,
                "value": val,
            }

            for q in self.node_to_queues.get(nodeid, []):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    try:
                        q.put_nowait(payload)
                    except asyncio.QueueFull:
                        pass
        except Exception:
            logging.exception("Error en datachange_notification")

    def status_change_notification(self, status):
        logging.warning("Estado de suscripción: %s", status)


# ── Búsqueda recursiva de objetos por nombre ────────────────────────
async def find_objects_by_name(
    root: Node,
    target_names: set[str],
    max_depth: int = 3,
) -> dict[str, Node]:
    """Recorre el árbol OPC hasta max_depth buscando nodos cuyo
    browse-name o display-name coincida con target_names.
    Retorna dict name → Node."""
    found: dict[str, Node] = {}
    remaining = set(target_names)

    async def _browse(node: Node, depth: int):
        if depth > max_depth or not remaining:
            return
        try:
            children = await node.get_children()
        except Exception:
            return
        for child in children:
            if not remaining:
                return
            try:
                bn = await child.read_browse_name()
                dn = await child.read_display_name()
                names = {bn.Name, dn.Text} - {None, ""}
                matched = names & remaining
                if matched:
                    name = matched.pop()
                    found[name] = child
                    remaining.discard(name)
                    logging.info("Objeto encontrado: %s (%s)", name, child.nodeid)
                else:
                    await _browse(child, depth + 1)
            except Exception:
                logging.warning("No se pudo inspeccionar %s", child.nodeid)

    await _browse(root, 1)
    return found


# ── Worker que despacha cambios por WebSocket ───────────────────────
async def queue_worker(queue: asyncio.Queue, mgr: ConnectionManager, cache: dict):
    while True:
        item = await queue.get()
        try:
            logging.info("Cambio | %s | valor=%s", item["tag"], item["value"])
            cache[item["tag"]] = item
            await mgr.broadcast_json(item)
        finally:
            queue.task_done()


# ── Escucha de WebSocket externo y escritura al OPC ─────────────────
async def run_source_ws(source_url: str, opc_client: Client, obj_map: dict[str, Node]):
    """Se suscribe a un WS externo; escribe los datos recibidos al OPC."""
    # Cachear VariantType real de cada nodo para escribir con el tipo correcto
    node_vtypes: dict[str, ua.VariantType] = {}
    for name, node in obj_map.items():
        try:
            dv = await node.read_data_value()
            node_vtypes[name] = dv.Value.VariantType
        except Exception:
            node_vtypes[name] = None

    # Mapeo VariantType → conversión Python
    _CAST = {
        ua.VariantType.Boolean: lambda v: bool(int(v)),
        ua.VariantType.SByte: lambda v: int(v),
        ua.VariantType.Byte: lambda v: int(v),
        ua.VariantType.Int16: lambda v: int(v),
        ua.VariantType.UInt16: lambda v: int(v),
        ua.VariantType.Int32: lambda v: int(v),
        ua.VariantType.UInt32: lambda v: int(v),
        ua.VariantType.Int64: lambda v: int(v),
        ua.VariantType.UInt64: lambda v: int(v),
        ua.VariantType.Float: lambda v: float(v),
        ua.VariantType.Double: lambda v: float(v),
        ua.VariantType.String: lambda v: str(v),
    }

    while True:
        try:
            async with websockets.connect(source_url) as ws:
                logging.info("Conectado a fuente externa: %s", source_url)
                async for message in ws:
                    try:
                        data = json.loads(message)
                        if isinstance(data, dict):
                            for obj_name, value in data.items():
                                node = obj_map.get(obj_name)
                                if node is not None:
                                    vtype = node_vtypes.get(obj_name)
                                    if vtype is not None:
                                        cast_fn = _CAST.get(vtype)
                                        if cast_fn:
                                            value = cast_fn(value)
                                        variant = ua.Variant(value, vtype)
                                        dv = ua.DataValue(variant)
                                        await node.write_attribute(ua.AttributeIds.Value, dv)
                                    else:
                                        await node.write_value(value)
                                    logging.info("OPC escrito desde fuente: %s = %s", obj_name, value)
                    except Exception:
                        logging.exception("Error procesando mensaje de fuente WS")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logging.warning("Fuente WS %s desconectada: %s. Reintentando en 3 s…", source_url, exc)
            await asyncio.sleep(3)


# ── Cliente OPC-UA con reconnect automático ─────────────────────────
async def run_client():
    while True:
        worker_tasks: list[asyncio.Task] = []
        subscription = None
        try:
            logging.info("Conectando a %s …", ENDPOINT)

            async with Client(url=ENDPOINT, timeout=4) as client:
                # Buscar todos los objetos declarados en endpoints.json
                obj_map = await find_objects_by_name(
                    client.nodes.objects, ALL_OBJECT_NAMES, BROWSE_DEPTH,
                )
                missing = ALL_OBJECT_NAMES - obj_map.keys()
                if missing:
                    logging.warning("Objetos NO encontrados: %s", missing)
                if not obj_map:
                    raise RuntimeError("Ningún objeto de endpoints.json encontrado")

                # Crear queues y mapear nodeids → queues por endpoint
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

                # Lectura inicial
                values = await client.read_values(all_nodes)
                for n, v in zip(all_nodes, values):
                    logging.info("  %s = %s", all_labels[n.nodeid.to_string()], v)

                # Iniciar un worker por endpoint
                for path, ep_cfg in endpoint_configs.items():
                    task = asyncio.create_task(
                        queue_worker(ep_queues[path], ep_cfg["manager"], endpoint_cache[path])
                    )
                    worker_tasks.append(task)

                # Iniciar listeners de fuentes WS externas
                for path, ep_cfg in endpoint_configs.items():
                    source_url = ep_cfg.get("source_ws")
                    if source_url:
                        task = asyncio.create_task(
                            run_source_ws(source_url, client, obj_map)
                        )
                        worker_tasks.append(task)
                        logging.info("Fuente WS iniciada: %s → %s", source_url, path)

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


# ── FastAPI ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(run_client())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)


# Registrar rutas WebSocket dinámicamente desde endpoints.json
def _make_ws_handler(manager: ConnectionManager, cache: dict):
    async def websocket_handler(ws: WebSocket):
        await manager.connect(ws)
        # Enviar último estado conocido al conectarse
        for payload in cache.values():
            try:
                await ws.send_json(payload)
            except Exception:
                manager.disconnect(ws)
                return
        try:
            while True:
                await ws.receive_text()
        except (WebSocketDisconnect, Exception):
            manager.disconnect(ws)
    return websocket_handler


for _ws_path, _ep_cfg in endpoint_configs.items():
    app.websocket(_ws_path)(_make_ws_handler(_ep_cfg["manager"], endpoint_cache[_ws_path]))
    logging.info("WebSocket registrado: %s → objetos %s", _ws_path, _ep_cfg["opc_objects"])

