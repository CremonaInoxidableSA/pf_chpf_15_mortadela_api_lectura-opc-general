import asyncio
import logging
from datetime import datetime
from asyncua import Client, Node
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from config.ws import ConnectionManager
from contextlib import asynccontextmanager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)

# ── Configuración OPC-UA ────────────────────────────────────────────
ENDPOINT = "opc.tcp://localhost:4840/general/simulador/"
NODE_NAME = "Nodo1"
PUBLISHING_INTERVAL_MS = 100
SAMPLING_INTERVAL_MS = 0.0
QUEUE_MAXSIZE = 500

ws_manager = ConnectionManager()


# ── Handler de suscripción ──────────────────────────────────────────
class DataChangeHandler:

    def __init__(self, queue: asyncio.Queue, node_labels: dict[str, str]):
        self.queue = queue
        self.node_labels = node_labels

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
                "recv_ts": datetime.now().isoformat(timespec="milliseconds"),
                "nodeid": nodeid,
                "tag": tag,
                "value": val,
                "source_ts": source_ts.isoformat() if source_ts else None,
                "server_ts": server_ts.isoformat() if server_ts else None,
                "status": str(status) if status else None,
            }

            try:
                self.queue.put_nowait(payload)
            except asyncio.QueueFull:
                try:
                    self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass
        except Exception:
            logging.exception("Error en datachange_notification")

    def status_change_notification(self, status):
        logging.warning("Estado de suscripción: %s", status)


# ── Búsqueda del nodo carpeta y descubrimiento de hijos ─────────────
async def find_folder_node(client: Client, name: str) -> Node:
    """Busca un nodo hijo directo de Objects por browse-name."""
    for child in await client.nodes.objects.get_children():
        bn = await child.read_browse_name()
        if bn.Name == name:
            return child
    raise RuntimeError(f"Nodo '{name}' no encontrado en Objects")


async def discover_children(folder: Node) -> tuple[list[Node], dict[str, str]]:
    """Devuelve las variables hijas de un nodo carpeta y un dict nodeid->label."""
    children = await folder.get_children()
    nodes: list[Node] = []
    labels: dict[str, str] = {}
    for child in children:
        try:
            dn = await child.read_display_name()
            bn = await child.read_browse_name()
            label = dn.Text if dn and dn.Text else bn.Name
            labels[child.nodeid.to_string()] = label
            nodes.append(child)
        except Exception:
            logging.warning("No se pudo inspeccionar hijo %s", child.nodeid)
    return nodes, labels


# ── Worker que despacha cambios por WebSocket ───────────────────────
async def queue_worker(queue: asyncio.Queue, mgr: ConnectionManager):
    while True:
        item = await queue.get()
        try:
            logging.info("Cambio | %s | valor=%s", item["tag"], item["value"])
            await mgr.broadcast_json(item)
        finally:
            queue.task_done()


# ── Cliente OPC-UA con reconnect automático ─────────────────────────
async def run_client(mgr: ConnectionManager):
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    while True:
        worker_task = subscription = None
        try:
            logging.info("Conectando a %s …", ENDPOINT)

            async with Client(url=ENDPOINT, timeout=4) as client:
                folder = await find_folder_node(client, NODE_NAME)
                logging.info("Carpeta encontrada: %s (%s)", NODE_NAME, folder.nodeid)

                nodes, node_labels = await discover_children(folder)
                if not nodes:
                    raise RuntimeError(f"No se encontraron variables dentro de '{NODE_NAME}'")
                logging.info("Variables descubiertas en '%s': %d", NODE_NAME, len(nodes))

                # Lectura inicial
                values = await client.read_values(nodes)
                for n, v in zip(nodes, values):
                    logging.info("  %s = %s", node_labels[n.nodeid.to_string()], v)

                worker_task = asyncio.create_task(queue_worker(queue, mgr))

                handler = DataChangeHandler(queue, node_labels)
                subscription = await client.create_subscription(
                    PUBLISHING_INTERVAL_MS, handler
                )
                await subscription.subscribe_data_change(
                    nodes,
                    queuesize=1,
                    sampling_interval=SAMPLING_INTERVAL_MS,
                )
                logging.info("Suscripción activa en '%s'. Esperando cambios…", NODE_NAME)

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
            if worker_task:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass


# ── FastAPI ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(run_client(ws_manager))
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)


@app.websocket("/ws/alarmas")
async def websocket_alarmas(ws: WebSocket):
    await ws_manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except (WebSocketDisconnect, Exception):
        ws_manager.disconnect(ws)

