import logging

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from config.settings import endpoint_configs, endpoint_cache, buffer_configs, buffer_cache
from config.ws import ConnectionManager


def register_endpoint_routes(app: FastAPI):
    """Registra un WebSocket por cada endpoint definido en endpoints.json."""

    def _make_ws_handler(manager: ConnectionManager, cache: dict):
        async def websocket_handler(ws: WebSocket):
            await manager.connect(ws)
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

    for ws_path, ep_cfg in endpoint_configs.items():
        app.websocket(ws_path)(_make_ws_handler(ep_cfg["manager"], endpoint_cache[ws_path]))
        logging.info("WebSocket registrado: %s → objetos %s", ws_path, ep_cfg["opc_objects"])


def register_buffer_routes(app: FastAPI):
    """Registra un WebSocket por cada buffer endpoint definido en endpoints.json."""

    def _make_buffer_ws_handler(manager: ConnectionManager, cache: dict, path: str):
        async def websocket_handler(ws: WebSocket):
            await manager.connect(ws)
            cached = cache.get(path)
            if cached:
                try:
                    await ws.send_json(cached)
                except Exception:
                    manager.disconnect(ws)
                    return
            try:
                while True:
                    await ws.receive_text()
            except (WebSocketDisconnect, Exception):
                manager.disconnect(ws)
        return websocket_handler

    for buf_path, buf_cfg in buffer_configs.items():
        app.websocket(buf_path)(
            _make_buffer_ws_handler(buf_cfg["manager"], buffer_cache, buf_path)
        )
        logging.info("WebSocket buffer registrado: %s", buf_path)
