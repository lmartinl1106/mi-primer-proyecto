# Clase 1 · Ingesta y capa Bronze

**Alumno:** martin_lin
**Catálogo / esquema:** `workspace.bigdata_martin_lin`
**Escala:** small

## Formato de entrega

Este README acompaña al notebook `01_ingesta_bronze.ipynb`, que queda ejecutado de punta a punta con las salidas de las cuatro tablas Bronze (`bronze_customers`, `bronze_products`, `bronze_transactions`, `bronze_events`) y del diagnóstico de calidad. Acá van las dos respuestas escritas que pide la consigna.

---

## 1. Tres observaciones sobre CSV/JSON, Parquet y Delta

**a) CSV no trae tipos — Parquet sí, y eso se nota en todo el pipeline.**
Al leer `customers_csv` y `transactions_csv` sin `inferSchema`, las ocho columnas de `transactions` (incluido `amount`) quedaron como `string`, porque el CSV es texto plano sin metadata de tipos y Spark, sin pedirle inferencia, asigna `string` por default. En cambio `products_parquet` llegó con `price: decimal(12,2)` ya definido, porque Parquet guarda el esquema físico junto con los datos en el footer del archivo. Probar `inferSchema=True` sobre transacciones (experimento del notebook) confirmó que la inferencia sí puede recuperar tipos razonables (`transaction_id`, `customer_id`, `product_id` → `integer`; `event_ts` → `timestamp`), pero **no resolvió `amount`**, que siguió como `string`: basta un único valor `N/A` en la columna para que el inferidor descarte el tipo numérico para todas las filas. Esto tiene costo doble: un pase de lectura extra (3.5s en esta muestra chica) y un resultado que depende de qué datos estén presentes ese día, no de un contrato estable.

**b) JSON trae estructura anidada nativa; CSV y Parquet (tal como los generamos) no.**
`events_json` llegó con un `struct` (`context.platform`, `context.session_id`) directamente utilizable con notación de punto, mientras que CSV y Parquet en este dataset son estrictamente tabulares. Eso hace que JSON sea cómodo para datos semiestructurados (eventos, logs, payloads de API) pero más caro de leer/parsear a gran escala, y obliga a decidir temprano si esa anidación se aplana en Bronze o se preserva para resolverla en Silver.

**c) Delta agrega una capa de metadata y gobierno que ni CSV ni Parquet "pelados" tienen.**
Un directorio Parquet es solo archivos binarios: no versiona, no registra quién escribió qué ni cuándo. Al materializar las tablas Bronze como Delta, `DESCRIBE DETAIL` mostró que por debajo sigue habiendo un único archivo Parquet (`numFiles: 1`, compresión `zstd`), pero Delta le suma un log transaccional: `DESCRIBE HISTORY` registró la operación completa (`CREATE OR REPLACE TABLE AS SELECT`, versión 0, usuario, cluster, `numOutputRows: 50011`, `isolationLevel: WriteSerializable`), y `EXPLAIN FORMATTED` mostró que el motor (Photon) lee la tabla a través del `PreparedDeltaFileIndex`, no de un path Parquet plano. Esa trazabilidad — versión, historial de escrituras, estadísticas por archivo — es justamente lo que permite time travel, ACID y auditoría, algo que un CSV o un Parquet suelto no ofrecen por sí solos.

---

## 2. Las 5V en este caso

- **Volumen:** las cuatro fuentes ya en escala "small" suman ~255.000 filas (5.000 clientes, 500 productos, 50.011 transacciones, 200.000 eventos). `events` es el orden de magnitud mayor porque cada interacción de cliente genera un registro, a diferencia de las entidades maestras (`customers`, `products`) que crecen mucho más despacio.

- **Velocidad:** se manifiesta en la diferencia entre las tablas de referencia (`customers`, `products`), que cambian con poca frecuencia, y los flujos transaccionales/de eventos (`transactions`, `events`), que llegan de forma continua y son candidatos naturales a ingesta incremental (streaming o micro-batch) en vez de full-reload como hicimos acá con `overwrite`.

- **Variedad:** está explícita en la mezcla de formatos de origen — CSV tabular plano (`customers`, `transactions`), Parquet columnar tipado (`products`) y JSON semiestructurado con anidación (`events`, con su `struct context`). Cada uno exige una estrategia de lectura distinta, lo cual es justamente el motivo del ejercicio de inspección de esquemas.

- **Veracidad:** quedó cuantificada en el diagnóstico de calidad de Bronze: sobre 50.011 filas de `transactions` hay solo 50.000 `transaction_id` distintos (11 duplicados) y 52 valores de `amount` que no castean a `DECIMAL(12,2)` (el `N/A` que vimos en el experimento de inferencia es un ejemplo). Bronze deliberadamente no corrige nada de esto — solo lo deja medido — para que la limpieza sea explícita y trazable en Silver.

- **Valor:** aparece recién cuando estas cuatro fuentes se integran — por ejemplo, cruzar `transactions` con `products` para tener el precio de catálogo junto al importe cobrado, o con `events` para contextualizar una transacción con el `device_id`/`session_id` que la originó. Ninguna fuente aislada responde una pregunta de negocio (o de riesgo/fraude) por sí sola; el valor está en el modelo integrado que se construye en las capas siguientes (Silver/Gold), no en Bronze.
