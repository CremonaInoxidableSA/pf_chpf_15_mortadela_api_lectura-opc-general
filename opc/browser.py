import logging
from datetime import datetime

from asyncua import Node


async def find_objects_by_name(
    root: Node,
    target_names: set[str],
    max_depth: int = 3,
) -> dict[str, Node]:
    """Recorre el árbol OPC hasta *max_depth* buscando nodos cuyo
    browse-name o display-name coincida con *target_names*.
    Retorna ``{name: Node}``."""
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


async def read_node_tree(node: Node) -> dict:
    """Lee recursivamente todos los hijos de un nodo y retorna un dict anidado."""
    result = {}
    try:
        children = await node.get_children()
    except Exception:
        return result
    for child in children:
        try:
            bn = await child.read_browse_name()
            name = bn.Name
            sub_children = await child.get_children()
            if sub_children:
                result[name] = await read_node_tree(child)
            else:
                val = await child.read_value()
                if isinstance(val, datetime):
                    val = val.isoformat()
                result[name] = val
        except Exception:
            logging.warning("No se pudo leer nodo hijo: %s", child.nodeid)
    return result
