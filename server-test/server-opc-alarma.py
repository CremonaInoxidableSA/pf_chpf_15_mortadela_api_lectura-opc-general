# pip install asyncua
import asyncio
from datetime import datetime, timezone

from asyncua import Server, ua

ENDPOINT = "opc.tcp://0.0.0.0:4840/alarma/simulador/"
NAMESPACE_URI = "urn:simulador:opcua:alarma"

ALARM_COUNT = 100


def make_datavalue(value: bool) -> ua.DataValue:
    now = datetime.now(timezone.utc)
    return ua.DataValue(
        ua.Variant(value, ua.VariantType.Boolean),
        SourceTimestamp=now,
        ServerTimestamp=now,
        StatusCode_=ua.StatusCode(ua.StatusCodes.Good),
    )


def parse_bool(text: str) -> bool:
    value = text.strip().lower()

    if value in ("1", "true", "on", "yes", "si", "sí"):
        return True
    if value in ("0", "false", "off", "no"):
        return False

    raise ValueError("Valor inválido. Usá 1/0, true/false, on/off.")


async def build_structure(server: Server) -> tuple[int, list]:
    idx = await server.register_namespace(NAMESPACE_URI)
    objects = server.nodes.objects

    await objects.add_object(ua.NodeId(1000, idx), "DeviceSet")
    await objects.add_object(ua.NodeId(1001, idx), "PLC_1")

    server_interfaces = await objects.add_folder(
        ua.NodeId(1002, idx), "ServerInterfaces"
    )
    server_interface_1 = await server_interfaces.add_object(
        ua.NodeId(1003, idx), "Server interface_1"
    )
    datos_opc = await server_interface_1.add_folder(
        ua.NodeId(1004, idx), "DATOS OPC A ENVIAR"
    )
    alarma = await datos_opc.add_folder(
        ua.NodeId(1005, idx), "Alarma"
    )

    alarm_nodes = []

    for i in range(ALARM_COUNT):
        node_id = ua.NodeId(4 + i, idx)

        var = await alarma.add_variable(
            node_id,
            f"[{i}]",
            False,
            ua.VariantType.Boolean,
        )
        await var.set_writable()
        alarm_nodes.append(var)

    return idx, alarm_nodes


async def write_single_node(alarm_nodes: list, index: int, value: bool) -> None:
    await alarm_nodes[index].write_value(make_datavalue(value))


async def write_all_nodes(alarm_nodes: list, values: list[bool]) -> None:
    for i, value in enumerate(values):
        await write_single_node(alarm_nodes, i, value)


async def manual_command_loop(alarm_nodes: list, state: dict, stop_event: asyncio.Event) -> None:
    print("\nComandos disponibles:")
    print("  set <indice> <valor>   -> Ej: set 0 1")
    print("  setall <valor>         -> Ej: setall 1 | setall 0")
    print("  show                   -> Muestra estado actual")
    print("  resetall               -> Pone todas las alarmas en False")
    print("  exit                   -> Cierra el servidor\n")

    while not stop_event.is_set():
        try:
            raw = await asyncio.to_thread(input, "cmd> ")
        except EOFError:
            stop_event.set()
            break

        cmd = raw.strip()
        if not cmd:
            continue

        parts = cmd.split()
        action = parts[0].lower()

        try:
            if action == "set":
                if len(parts) != 3:
                    print("Uso: set <indice> <valor>")
                    continue

                index = int(parts[1])
                value = parse_bool(parts[2])

                if not 0 <= index < len(alarm_nodes):
                    print(f"Índice fuera de rango. Debe estar entre 0 y {len(alarm_nodes) - 1}")
                    continue

                async with state["lock"]:
                    state["current_values"][index] = value

                await write_single_node(alarm_nodes, index, value)
                print(f"Valor aplicado manualmente: [{index}] = {value}")

            elif action == "show":
                async with state["lock"]:
                    current_values = state["current_values"].copy()

                active = [i for i, v in enumerate(current_values) if v]
                print(f"Estado actual: {current_values}")
                print(f"Alarmas activas: {active}")

            elif action == "resetall":
                async with state["lock"]:
                    state["current_values"] = [False] * len(alarm_nodes)
                    values = state["current_values"].copy()

                await write_all_nodes(alarm_nodes, values)
                print("Todas las alarmas fueron puestas en False")
            
            elif action == "setall":
                if len(parts) != 2:
                    print("Uso: setall <valor>")
                    continue

                value = parse_bool(parts[1])

                async with state["lock"]:
                    state["current_values"] = [value] * len(alarm_nodes)
                    values = state["current_values"].copy()

                await write_all_nodes(alarm_nodes, values)
                print(f"Todas las alarmas fueron puestas en {value}")

            elif action == "exit":
                print("Cerrando servidor...")
                stop_event.set()

            else:
                print("Comando no reconocido. Usá: set, show, resetall, exit")

        except ValueError as e:
            print(f"Error: {e}")
        except Exception as e:
            print(f"Error inesperado: {e}")


async def main() -> None:
    server = Server()
    await server.init()
    server.set_endpoint(ENDPOINT)

    # Para pruebas simples con UaExpert / cliente local
    server.set_security_policy([ua.SecurityPolicyType.NoSecurity])
    server.set_security_IDs(["Anonymous"])

    idx, alarm_nodes = await build_structure(server)

    state = {
        "lock": asyncio.Lock(),
        "current_values": [False] * ALARM_COUNT,
    }

    # Inicializa todos los nodos en False
    await write_all_nodes(alarm_nodes, state["current_values"])

    stop_event = asyncio.Event()

    print("Servidor OPC UA iniciado")
    print(f"Endpoint: {ENDPOINT}")
    print(f"Namespace index asignado: {idx}")
    print(f"Nodos de alarma creados: {len(alarm_nodes)}")
    print("Modo manual habilitado")

    async with server:
        await manual_command_loop(alarm_nodes, state, stop_event)


if __name__ == "__main__":
    asyncio.run(main())