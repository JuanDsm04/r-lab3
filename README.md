# Laboratorio 3: algoritmos de enrutamiento

Simulador distribuido en el que cada nodo corre como un proceso independiente,
mantiene conexiones TCP persistentes con sus vecinos e intercambia paquetes
JSON delimitados por saltos de línea (NDJSON). Implementa los modos **Flooding**,
**Dijkstra** y **LSR**.

## Requisitos

- Python 3.10 o posterior.
- No hay dependencias de ejecución fuera de la biblioteca estándar.

Desde la raíz del repositorio:

```bash
python -m pip install -r requirements.txt
```

Para ejecutar las pruebas también se necesita `pytest`:

```bash
python -m pip install pytest
python -m pytest -q
```

## Ejecución rápida

Abra tres terminales en la raíz del repositorio y use en todas el mismo modo.
Por ejemplo, para LSR:

```bash
# Terminal 1
python run.py config/node_A_lsr.json

# Terminal 2
python run.py config/node_B_lsr.json

# Terminal 3
python run.py config/node_C_lsr.json
```

También hay configuraciones equivalentes con sufijo `_flooding.json` y
`_dijkstra.json`. Los procesos pueden iniciarse en cualquier orden: cada nodo
reintenta las conexiones que todavía no estén disponibles.

## CLI interactivo

Una vez iniciado un nodo, acepta estos comandos:

```text
send <destino> <mensaje>  Envía texto a un ID lógico, por ejemplo: send C hola
table                     Muestra destino, siguiente salto y costo
neighbors                 Muestra estado, costo y RTT de cada vecino
quit                      Cierra sockets e hilos y termina el proceso
help                      Muestra la ayuda
```

En Flooding, `table` indica que no existe una tabla global. Los mensajes
recibidos se muestran como `[C] mensaje de A: hola`.

## Configuración de un nodo

Cada proceso recibe un archivo JSON con esta estructura:

```json
{
  "node_id": "A",
  "listen": {"host": "127.0.0.1", "port": 5000},
  "mode": "lsr",
  "neighbors": [
    {"node_id": "B", "host": "127.0.0.1", "port": 5001, "cost": 4}
  ],
  "params": {
    "initial_ttl": 10,
    "hello_interval_sec": 5,
    "hello_timeout_sec": 3,
    "hello_max_failures": 3,
    "dedup_cache_ttl_sec": 60,
    "log_level": "INFO"
  }
}
```

Todos los nodos de una ejecución deben usar el mismo `mode`. Cada vecino debe
apuntar al `listen.host` y `listen.port` del proceso correspondiente. Los IDs
son lógicos y deben ser únicos.

El modo `dijkstra` requiere además una topología completa:

```json
"topology_file": "topology.json"
```

La ruta es relativa al archivo de configuración. El archivo contiene un mapa
`nodo -> vecino -> costo`, por ejemplo:

```json
{
  "A": {"B": 4, "C": 1},
  "B": {"A": 4, "C": 2},
  "C": {"A": 1, "B": 2}
}
```

## Modos de enrutamiento

- **Flooding:** reenvía cada mensaje nuevo a todos los vecinos activos excepto
  al emisor inmediato. TTL y una caché de IDs evitan ciclos y duplicados.
- **Dijkstra:** carga la topología estática al arrancar y calcula una sola tabla
  de caminos mínimos. Cada mensaje sale por un único siguiente salto.
- **LSR:** cada nodo anuncia sus enlaces activos mediante LSPs, reconstruye una
  base distribuida de topología y aplica Dijkstra. Un cambio de estado provoca
  un nuevo anuncio.

En los tres modos, `hello`/`echo` miden RTT y detectan caída o recuperación de
vecinos. Los `hello` siguen enviándose a vecinos marcados como caídos.

## Protocolo y transporte

- TCP con conexiones persistentes y reconexión automática.
- Un objeto JSON compacto por línea (`\n`).
- Envelope v1 con `id`, `proto`, `type`, `from`, `to`, `ttl`, `headers` y
  `payload`.
- Tipos de paquete: `hello`, `echo`, `message` e `info`.
- `from` identifica al emisor inmediato; `to` conserva el destino final.
- Los LSP usan `to: "*"`; los mensajes de usuario llevan texto plano.

Los paquetes inválidos se descartan sin cerrar el proceso ni la conexión.

## Estructura principal

```text
run.py                  Entrada del programa
src/node.py             Sockets TCP, reconexión, ensamblaje y CLI
src/routing.py          Health-check y selección de rutas
src/forwarding.py       Procesamiento y reenvío de paquetes
src/algorithms/         Flooding, Dijkstra y LSR
config/                 Ejemplos para A, B y C
tests/                  Pruebas unitarias y de integración
```
