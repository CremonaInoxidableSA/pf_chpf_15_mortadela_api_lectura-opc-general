import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI

from opc.client import run_client
from routes.websockets import register_endpoint_routes, register_buffer_routes


# ── Ciclo de vida de la aplicación ──────────────────────────────────
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


# ── Aplicación FastAPI ──────────────────────────────────────────────
app = FastAPI(lifespan=lifespan)

register_endpoint_routes(app)
register_buffer_routes(app)

