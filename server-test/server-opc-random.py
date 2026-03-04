# pip install asyncua

import asyncio
from datetime import datetime, timezone

from asyncua import Server, ua
from asyncua.ua.uaerrors._auto import BadNodeIdExists


ENDPOINT = "opc.tcp://0.0.0.0:4840/alarma/simulador/"
NAMESPACE_URI = "urn:simulador:opcua:alarma"

# En la captura no se ve completo el final del árbol.
# Dejé 40 variables: [0] ... [39].
# Si en tu caso real son menos o más, cambiá este valor.
ALARM_COUNT = 800

UPDATE_SECONDS = 1.0


def make_datavalue(value: bool) -> ua.DataValue:
    """Crea un DataValue booleano con timestamps actualizados."""
    now = datetime.now(timezone.utc)
    dv = ua.DataValue(ua.Variant(value, ua.VariantType.Boolean))
    dv.SourceTimestamp = now
    dv.ServerTimestamp = now
    return dv


def generate_cycle_values(cycle: int, count: int) -> list[bool]:
    """
    Genera un patrón simple de simulación:
    - una alarma principal va rotando
    - una secundaria aparece cada 2 ciclos
    - una tercera aparece cada 3 ciclos
    """
    values = [False] * count

    main_idx = cycle % count
    second_idx = (cycle + 5) % count
    third_idx = (cycle + 10) % count

    values[main_idx] = True

    if cycle % 2 == 0:
        values[second_idx] = True

    if cycle % 3 == 0:
        values[third_idx] = True

    return values


async def build_structure(server: Server) -> tuple[int, list]:
    """
    Replica la estructura visible de la captura:

    Objects
    ├── DeviceSet
    ├── PLC_1
    ├── Server        <- este ya existe como nodo estándar del servidor OPC UA
    └── ServerInterfaces
        └── Server interface_1
            └── DATOS OPC A ENVIAR
                └── Alarma
                    ├── [0]
                    ├── [1]
                    ├── [2]
                    └── ...
    """
    idx = await server.register_namespace(NAMESPACE_URI)
    objects = server.nodes.objects

    # Helper: devuelve el nodo existente si puede leerse, o None si no existe
    async def get_existing_node(nodeid: ua.NodeId):
        node = server.get_node(nodeid)
        try:
            await node.read_display_name()
            return node
        except Exception:
            return None

    # Nodos de nivel superior visibles en tu captura
    node = await get_existing_node(ua.NodeId(1000, idx))
    if node is None:
        await objects.add_object(ua.NodeId(1000, idx), "DeviceSet")

    node = await get_existing_node(ua.NodeId(1001, idx))
    if node is None:
        await objects.add_object(ua.NodeId(1001, idx), "PLC_1")

    server_interfaces = await get_existing_node(ua.NodeId(1002, idx))
    if server_interfaces is None:
        server_interfaces = await objects.add_folder(
            ua.NodeId(1002, idx), "ServerInterfaces"
        )

    server_interface_1 = await get_existing_node(ua.NodeId(1003, idx))
    if server_interface_1 is None:
        server_interface_1 = await server_interfaces.add_object(
            ua.NodeId(1003, idx), "Server interface_1"
        )

    datos_opc = await get_existing_node(ua.NodeId(1004, idx))
    if datos_opc is None:
        datos_opc = await server_interface_1.add_folder(
            ua.NodeId(1004, idx), "DATOS OPC A ENVIAR"
        )

    alarma = await get_existing_node(ua.NodeId(1005, idx))
    if alarma is None:
        alarma = await datos_opc.add_folder(
            ua.NodeId(1005, idx), "Alarma"
        )

    # Variables de Alarma
    # En tu captura se ven variables booleanas con display name tipo [0], [1], [2], etc.
    # También se observan NodeIds numéricos; por eso acá las creo explícitamente.
    alarm_nodes = []

    for i in range(ALARM_COUNT):
        # Replica el patrón visible del PLC: variables con NodeIds numéricos consecutivos
        # Ejemplo: [0] -> i=4, [1] -> i=5, etc.
        node_id = ua.NodeId(4 + i, idx)
        # Si ya existe el nodo con ese NodeId, recupéralo; si no, créalo.
        var = await get_existing_node(node_id)
        if var is None:
            var = await alarma.add_variable(
                node_id,
                f"[{i}]",
                False,
                ua.VariantType.Boolean,
            )
        await var.set_writable()
        alarm_nodes.append(var)

    return idx, alarm_nodes


async def simulation_loop(alarm_nodes: list) -> None:
    cycle = 0

    while True:
        values = generate_cycle_values(cycle, len(alarm_nodes))

        # Escribimos todas las variables con timestamps actualizados
        for node, value in zip(alarm_nodes, values):
            # Escribe el valor booleano directamente. Evita construir un ua.DataValue
            # que en esta versión de asyncua lanza error al asignar timestamps.
            await node.write_value(value)

        print(
            f"[CICLO {cycle:05d}] "
            f"Activas: {[i for i, v in enumerate(values) if v]}"
        )

        cycle += 1
        await asyncio.sleep(UPDATE_SECONDS)


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)

    idx, alarm_nodes = await build_structure(server)

    print("Servidor OPC UA iniciado")
    print(f"Endpoint: {ENDPOINT}")
    print(f"Namespace index asignado: {idx}")
    print(f"Nodos de alarma creados: {len(alarm_nodes)}")

    async with server:
        await simulation_loop(alarm_nodes)


if __name__ == "__main__":
    asyncio.run(main())