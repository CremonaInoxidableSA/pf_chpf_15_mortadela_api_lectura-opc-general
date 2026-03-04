import asyncio
import logging
from datetime import datetime
from asyncua import Client, ua, Node
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from config.ws import ConnectionManager

from contextlib import asynccontextmanager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

# Silenciar logs internos de asyncua
logging.getLogger("asyncua").setLevel(logging.WARNING)
logging.getLogger("asyncua.common.subscription").setLevel(logging.WARNING)
logging.getLogger("asyncua.client.ua_client.UaClient").setLevel(logging.WARNING)


URL = "opc.tcp://127.0.0.1:4840/alarma/simulador/"
NAMESPACE_URI = "urn:simulador:opcua:alarma"
PUBLISHING_INTERVAL_MS = 100
SAMPLING_INTERVAL_MS = 0.0
QUEUE_MAXSIZE = 500

# Instancia compartida del gestor de conexiones WebSocket
ws_manager = ConnectionManager()


class UaExpertStyleHandler:

    def __init__(self, queue: asyncio.Queue, node_labels: dict[str, str]):
        self.queue = queue
        self.node_labels = node_labels

    def datachange_notification(self, node: Node, val, data):
        try:
            nodeid = node.nodeid.to_string()
            tag = self.node_labels.get(nodeid, nodeid)

            source_ts = None
            server_ts = None
            status = None

            try:
                mi_val = data.monitored_item.Value
                source_ts = mi_val.SourceTimestamp
                server_ts = mi_val.ServerTimestamp
                status = mi_val.StatusCode
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
                # descarta el más viejo y conserva el último
                try:
                    _ = self.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self.queue.put_nowait(payload)
                except asyncio.QueueFull:
                    pass

        except Exception:
            logging.exception("Error en datachange_notification")

    def status_change_notification(self, status):
        logging.warning("Estado de subscription: %s", status)


async def find_alarma_folder(client, ns_idx: int):
    return client.get_node(ua.NodeId(1005, ns_idx))


async def discover_alarm_nodes(client: Client):
    ns_idx = await client.get_namespace_index(NAMESPACE_URI)
    alarma_folder = await find_alarma_folder(client, ns_idx)

    children = await alarma_folder.get_children()

    node_labels = {}
    nodes = []

    for child in children:
        try:
            browse_name = await child.read_browse_name()
            display_name = await child.read_display_name()

            # Ejemplo esperado: [0], [1], [2], ...
            label = display_name.Text if display_name and display_name.Text else browse_name.Name
            node_labels[child.nodeid.to_string()] = label
            nodes.append(child)
        except Exception:
            logging.exception("No se pudo inspeccionar un hijo de Alarma")

    # Ordenamos por label si parecen [0], [1], [2]...
    def sort_key(node: Node):
        label = node_labels.get(node.nodeid.to_string(), "")
        if label.startswith("[") and label.endswith("]"):
            try:
                return int(label[1:-1])
            except ValueError:
                pass
        return label

    nodes.sort(key=sort_key)

    return ns_idx, alarma_folder, nodes, node_labels


async def snapshot_read(client: Client, nodes: list[Node], node_labels: dict[str, str]):
    values = await client.read_values(nodes)
    snapshot = []
    for node, value in zip(nodes, values):
        snapshot.append({
            "tag": node_labels.get(node.nodeid.to_string(), node.nodeid.to_string()),
            "nodeid": node.nodeid.to_string(),
            "value": value,
        })
    return snapshot


async def queue_worker(queue: asyncio.Queue, ws_manager: ConnectionManager):
    while True:
        item = await queue.get()
        try:
            logging.info(
                "Cambio | %s | valor=%s",
                item["tag"],
                item["value"],
            )
            await ws_manager.broadcast_json(item)
        finally:
            queue.task_done()


async def run_client(ws_manager: ConnectionManager):
    queue = asyncio.Queue(maxsize=QUEUE_MAXSIZE)

    while True:
        worker_task = None
        subscription = None

        try:
            logging.info("Conectando a %s", URL)

            async with Client(url=URL, timeout=4) as client:
                ns_idx, alarma_folder, nodes, node_labels = await discover_alarm_nodes(client)

                logging.info("Namespace index: %s", ns_idx)
                logging.info("Nodo Alarma: %s", alarma_folder.nodeid.to_string())
                logging.info("Nodos descubiertos en Alarma: %s", len(nodes))

                if not nodes:
                    raise RuntimeError("No se encontraron nodos debajo de Alarma")

                # Si el server soporta RegisterNodes, puede optimizar accesos repetidos.
                try:
                    nodes = await client.register_nodes(nodes)
                    logging.info("RegisterNodes OK")
                except Exception as e:
                    logging.warning("RegisterNodes no disponible o falló: %s", e)

                # Snapshot inicial
                snapshot = await snapshot_read(client, nodes, node_labels)
                logging.info("Snapshot inicial:")
                for item in snapshot:
                    logging.info("  %s = %s", item["tag"], item["value"])

                #worker_task = asyncio.create_task(queue_worker(queue))
                worker_task = asyncio.create_task(queue_worker(queue, ws_manager))

                handler = UaExpertStyleHandler(queue, node_labels)
                subscription = await client.create_subscription(
                    PUBLISHING_INTERVAL_MS,
                    handler
                )

                handles = await subscription.subscribe_data_change(
                    nodes,
                    queuesize=1,
                    sampling_interval=SAMPLING_INTERVAL_MS
                )

                logging.info("Subscription creada. MonitoredItems: %s", len(handles))
                logging.info("Esperando cambios...")

                while True:
                    await client.check_connection()
                    await asyncio.sleep(2)

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.exception("Cliente desconectado o con error: %s", e)
            logging.info("Reintentando en 3 segundos...")
            await asyncio.sleep(3)
        finally:
            if subscription is not None:
                try:
                    await subscription.delete()
                except Exception:
                    pass

            if worker_task is not None:
                worker_task.cancel()
                try:
                    await worker_task
                except asyncio.CancelledError:
                    pass



@asynccontextmanager
async def lifespan(app: FastAPI):
    opc_task = asyncio.create_task(run_client(ws_manager))
    try:
        yield
    finally:
        opc_task.cancel()
        try:
            await opc_task
        except asyncio.CancelledError:
            pass

app = FastAPI(lifespan=lifespan)

@app.websocket("/ws/alarmas")
async def websocket_alarmas(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            # si querés, podés leer mensajes del cliente
            await websocket.receive_text()
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)
    except Exception:
        ws_manager.disconnect(websocket)

"""
if __name__ == "__main__":
    try:
        asyncio.run(run_client())
    except KeyboardInterrupt:
        print("Cliente finalizado por usuario.")
"""

