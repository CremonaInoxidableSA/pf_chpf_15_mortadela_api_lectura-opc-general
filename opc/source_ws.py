import asyncio
import json
import logging

import websockets
from asyncua import Client, Node, ua


async def run_source_ws(
    source_url: str,
    opc_client: Client,
    obj_map: dict[str, Node],
):
    """Se suscribe a un WebSocket externo y escribe los datos recibidos al OPC."""
    # Cachear VariantType real de cada nodo para escribir con el tipo correcto
    node_vtypes: dict[str, ua.VariantType] = {}
    for name, node in obj_map.items():
        try:
            dv = await node.read_data_value()
            node_vtypes[name] = dv.Value.VariantType
        except Exception:
            node_vtypes[name] = None

    _CAST = {
        ua.VariantType.Boolean: lambda v: bool(int(v)),
        ua.VariantType.SByte:   lambda v: int(v),
        ua.VariantType.Byte:    lambda v: int(v),
        ua.VariantType.Int16:   lambda v: int(v),
        ua.VariantType.UInt16:  lambda v: int(v),
        ua.VariantType.Int32:   lambda v: int(v),
        ua.VariantType.UInt32:  lambda v: int(v),
        ua.VariantType.Int64:   lambda v: int(v),
        ua.VariantType.UInt64:  lambda v: int(v),
        ua.VariantType.Float:   lambda v: float(v),
        ua.VariantType.Double:  lambda v: float(v),
        ua.VariantType.String:  lambda v: str(v),
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
