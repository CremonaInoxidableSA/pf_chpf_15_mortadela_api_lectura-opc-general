import asyncio
import logging

from asyncua import Node


class DataChangeHandler:
    """Recibe notificaciones de cambio de datos OPC-UA y las enruta
    a las colas correspondientes."""

    def __init__(
        self,
        node_labels: dict[str, str],
        node_to_queues: dict[str, list[asyncio.Queue]],
    ):
        self.node_labels = node_labels
        self.node_to_queues = node_to_queues

    def datachange_notification(self, node: Node, val, data):
        try:
            nodeid = node.nodeid.to_string()
            tag = self.node_labels.get(nodeid, nodeid)

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
