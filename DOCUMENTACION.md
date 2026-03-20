# Documentación del Proyecto — API Lectura OPC General

API FastAPI que se conecta a un servidor OPC-UA, suscribe variables configuradas dinámicamente en `endpoints.json` y las expone en tiempo real a través de WebSockets.

---

## Estructura del Proyecto

```
main.py                     Punto de entrada de la aplicación
requirements.txt            Dependencias del proyecto
config/
    endpoints.json          Configuración de endpoints OPC y WebSocket
    settings.py             Carga de configuración y estado global compartido
    ws.py                   Clase ConnectionManager para WebSockets
    bdd.py                  Configuración de base de datos (MySQL/SQLAlchemy)
opc/
    browser.py              Funciones de búsqueda y lectura del árbol OPC
    handler.py              Handler de suscripción a cambios de datos OPC
    client.py               Cliente OPC-UA principal con reconexión automática
    buffer_monitor.py       Monitor de buffers con detección de flancos
    source_ws.py            Escucha de WebSockets externos y escritura al OPC
routes/
    websockets.py           Registro dinámico de rutas WebSocket en FastAPI
```

---

## Descripción de Archivos

### `main.py`
Punto de entrada. Crea la app FastAPI, configura el ciclo de vida (arranca el cliente OPC al iniciar, lo cancela al cerrar) y registra las rutas WebSocket.

### `config/endpoints.json`
Archivo JSON con toda la configuración:
- `opc_endpoint`: URL del servidor OPC-UA.
- `publishing_interval_ms`, `sampling_interval_ms`, `queue_maxsize`, `browse_depth`: parámetros de suscripción.
- `endpoints`: lista de endpoints WebSocket, cada uno con su `path`, objetos OPC a suscribir (`opc_objects`) y opcionalmente una fuente WS externa (`source_ws`).
- `buffer_endpoints`: endpoints de buffer con nodos trigger (`inicio_ciclo`, `fin_ciclo`, `buscar`, `confirmacion`) y nodo raíz del buffer.

### `config/settings.py`
Lee `endpoints.json` y expone las constantes y estructuras compartidas:
- `OPC_ENDPOINT`, `PUBLISHING_INTERVAL_MS`, `SAMPLING_INTERVAL_MS`, `QUEUE_MAXSIZE`, `BROWSE_DEPTH`
- `endpoint_configs`: diccionario `{path: {manager, opc_objects, source_ws}}` por cada endpoint.
- `buffer_configs`: diccionario `{path: {manager, trigger_nodes, buffer_root}}` por cada buffer.
- `ALL_OBJECT_NAMES`: nombres únicos de todos los objetos OPC a buscar.
- `endpoint_cache`, `buffer_cache`: caches globales con el último payload por endpoint.

### `config/ws.py`
Clase `ConnectionManager`: gestiona conexiones WebSocket activas (connect, disconnect, broadcast_json). Limpia automáticamente conexiones caídas.

### `config/bdd.py`
Configuración de conexión a MySQL con SQLAlchemy. Lee credenciales desde variables de entorno (`.env`). Incluye reintentos automáticos de conexión.

---

### `opc/browser.py`
- `find_objects_by_name(root, target_names, max_depth)`: recorre recursivamente el árbol OPC buscando nodos cuyo browse-name o display-name coincida con los nombres solicitados.
- `read_node_tree(node)`: lee recursivamente todos los hijos de un nodo y devuelve un diccionario anidado con sus valores.

### `opc/handler.py`
- `DataChangeHandler`: clase que recibe notificaciones de cambio de datos OPC-UA y las enruta a las colas (`asyncio.Queue`) correspondientes. Si una cola está llena, descarta el elemento más viejo.

### `opc/client.py`
- `queue_worker(queue, mgr, cache)`: consume payloads de una cola y los envía por WebSocket a los clientes conectados.
- `run_client()`: bucle principal del cliente OPC-UA. Se conecta, busca los objetos, crea suscripciones, arranca workers, fuentes WS externas y monitores de buffer. Reconecta automáticamente ante desconexiones.

### `opc/buffer_monitor.py`
- `run_buffer_monitor(client, buf_path, buf_cfg, obj_map)`: suscripción dedicada a nodos trigger (inicioCiclo, finCiclo, buscarBuffer). Detecta flancos ascendentes (false→true), captura timestamps, lee datos del buffer y los envía por WebSocket cuando las tres condiciones se cumplen.

### `opc/source_ws.py`
- `run_source_ws(source_url, opc_client, obj_map)`: se conecta a un WebSocket externo y escribe los datos JSON recibidos en los nodos OPC correspondientes, respetando el tipo de dato (VariantType) original del nodo. Reconecta automáticamente.

---

### `routes/websockets.py`
- `register_endpoint_routes(app)`: registra dinámicamente un WebSocket por cada endpoint definido en `endpoints.json`. Al conectarse un cliente, le envía el último estado conocido (cache).
- `register_buffer_routes(app)`: registra un WebSocket por cada buffer endpoint. Mismo comportamiento de cache al conectar.