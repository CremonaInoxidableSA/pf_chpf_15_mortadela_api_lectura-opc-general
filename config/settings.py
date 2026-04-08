import json
import logging
from pathlib import Path

from config.ws import ConnectionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)

# ── Cargar configuración desde endpoints.json ───────────────────────
CONFIG_PATH = Path(__file__).parent / "endpoints.json"
with open(CONFIG_PATH, encoding="utf-8") as f:
    CONFIG = json.load(f)

OPC_ENDPOINT = CONFIG["opc_endpoint"]
PUBLISHING_INTERVAL_MS = CONFIG.get("publishing_interval_ms", 100)
SAMPLING_INTERVAL_MS = CONFIG.get("sampling_interval_ms", 0.0)
QUEUE_MAXSIZE = CONFIG.get("queue_maxsize", 500)
BROWSE_DEPTH = CONFIG.get("browse_depth", 3)

# ── Estructura por endpoint: path → {manager, opc_objects, source_ws?} ──
endpoint_configs: dict[str, dict] = {}
for _ep in CONFIG["endpoints"]:
    endpoint_configs[_ep["path"]] = {
        "manager": ConnectionManager(),
        "opc_objects": _ep["opc_objects"],
        "source_ws": _ep.get("source_ws"),
    }

# ── Nombres únicos de objetos OPC necesarios ────────────────────────
ALL_OBJECT_NAMES: set[str] = {
    name
    for ep in CONFIG["endpoints"]
    for name in ep["opc_objects"]
}

# ── Cache global: endpoint_path → {obj_name: último payload} ────────
endpoint_cache: dict[str, dict[str, dict]] = {path: {} for path in endpoint_configs}

# ── Configuración de buffer endpoints ────────────────────────────────
buffer_configs: dict[str, dict] = {}
for _buf in CONFIG.get("buffer_endpoints", []):
    buffer_configs[_buf["path"]] = {
        "manager": ConnectionManager(),
        "buffer_root": _buf["buffer_root"],
        "extra_nodes": _buf.get("extra_nodes", []),
    }

# ── Agregar triggers y buffers a ALL_OBJECT_NAMES ────────────────────
# Los triggers son HARDCODEADOS: inicioCiclo, finCiclo, buscarBuffer{N}
ALL_OBJECT_NAMES.update(["inicioCiclo", "finCiclo"])  # Triggers de ciclos (globales)

for _buf_cfg in buffer_configs.values():
    buffer_root = _buf_cfg["buffer_root"]
    ALL_OBJECT_NAMES.add(buffer_root)
    ALL_OBJECT_NAMES.update(_buf_cfg["extra_nodes"])
    
    # Extraer número de buffer y agregar su nodo buscar y limpiar
    buffer_num = buffer_root.replace("buffer", "")
    buscar_node = f"buscarBuffer{buffer_num}"
    clear_node = _buf_cfg.get("clear_node")
    
    ALL_OBJECT_NAMES.add(buscar_node)
    if clear_node:
        ALL_OBJECT_NAMES.add(clear_node)

# ── Agregar recetaActual y torreActual para ciclos ────────────────────
ALL_OBJECT_NAMES.update(["recetaActual", "torreActual", "falloCiclos"])

buffer_cache: dict[str, dict] = {path: {} for path in buffer_configs}

# ── Cache de ciclos abiertos: buf_path → ciclo_id ──────────────────
# Almacena el ID del ciclo actualmente abierto por cada buffer_path
# Se actualiza cuando se abre un nuevo ciclo y se limpia cuando se cierra
ciclo_cache: dict[str, int | None] = {path: None for path in buffer_configs}
