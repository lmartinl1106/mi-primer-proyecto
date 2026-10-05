# **Análisis de las tablas**

1. ¿Cuántas filas físicas recibió cada lote en bronze_transactions_incremental? Escribí una consulta que muestre el resultado por source_batch_id

**RTA:** Cada lote en bronze_transactions_incremental recibió 204 filas físicas

SELECT source_batch_id, COUNT(*) AS filas_fisicas
FROM bronze_transactions_incremental
GROUP BY source_batch_id
ORDER BY source_batch_id;

2. Para cada lote, ¿cuántas transacciones fueron aceptadas y cuántas quedaron en silver_transactions_quarantine? Reconciliá tus resultados con gold_batch_summary.

**RTA:** 
- batch_002: 200 aceptadas y 2 rechazadas
- batch_003: 201 aceptadas y 2 rechazadas
- initial: 49948 aceptadas y 51 rechazadas

WITH a AS (
  SELECT source_batch_id, COUNT(*) AS aceptadas
  FROM silver_transactions GROUP BY source_batch_id
), r AS (
  SELECT source_batch_id, COUNT(*) AS rechazadas
  FROM silver_transactions_quarantine GROUP BY source_batch_id
), calc AS (
  SELECT COALESCE(a.source_batch_id, r.source_batch_id) AS source_batch_id,
         COALESCE(a.aceptadas, 0) AS aceptadas,
         COALESCE(r.rechazadas, 0) AS rechazadas
  FROM a FULL OUTER JOIN r ON a.source_batch_id = r.source_batch_id
)
SELECT c.source_batch_id, c.aceptadas, c.rechazadas,
       g.accepted_transactions AS gold_aceptadas,
       g.rejected_transactions AS gold_rechazadas,
       (c.aceptadas = g.accepted_transactions
        AND c.rechazadas = g.rejected_transactions) AS reconcilia
FROM calc c
JOIN gold_batch_summary g ON c.source_batch_id = g.source_batch_id
ORDER BY c.source_batch_id;

3. ¿Qué motivos de rechazo aparecen en la cuarentena y cuántos registros tiene cada uno por lote? ¿Los rechazos observados coinciden con los casos introducidos por el generador?

**RTA:** Por cada lote el generador inyecta dos errores controlados: un importe N/A (que cae como INVALID_AMOUNT) y un customer_id inexistente (que cae como UNKNOWN_CUSTOMER). El duplicado exacto no aparece en la cuarentena porque se elimina antes con dropDuplicates, y la corrección de la 42 es una versión válida. Los 2 rechazos por lote coinciden con expected_invalid_rows = 2 que declara el generador.

SELECT source_batch_id, quality_reason, COUNT(*) AS registros
FROM silver_transactions_quarantine
GROUP BY source_batch_id, quality_reason
ORDER BY source_batch_id, quality_reason;

4. Seguí la transacción 42 desde bronze_transactions_all hasta silver_transactions. ¿Cuántas versiones existen en Bronze y cuál quedó vigente en Silver? Mostrá las columnas que justifican la elección.

**RTA:** Bronze conserva todas las versiones: la original (source_batch_id = 'initial', con updated_at = event_ts), la de batch_002 y la de batch_003, es decir, 3 versiones.
Silver conserva una sola, la de batch_003: amount = 1999.99, device_id = device_corrected_42, is_fraud = 1.
Las columnas que justifican la elección son updated_at (la de batch_003 es la más reciente) y source_batch_id (criterio de desempate). Silver ordena por updated_at descendente y luego por source_batch_id descendente, y el MERGE solo actualiza si s.updated_at > t.updated_at.

-- Versiones en Bronze
SELECT source_batch_id, transaction_id, customer_id, product_id,
       event_ts, amount, device_id, is_fraud, updated_at
FROM bronze_transactions_all
WHERE transaction_id = '42'
ORDER BY updated_at;

-- Versión vigente en Silver
SELECT transaction_id, amount, device_id, is_fraud, source_batch_id, updated_at
FROM silver_transactions
WHERE transaction_id = 42;

5. Comprobá mediante una consulta que silver_transactions tiene una sola fila por transaction_id. ¿Qué resultado indicaría que la deduplicación falló?

**RTA**: Silver tiene una sola fila por transaction_id cuando filas = ids_distintos y la opción B no devuelve filas. El control silver_no_duplicate_ids del notebook 04_validate_pipeline ya dio true en la última corrida.

La deduplicación habría fallado si filas > ids_distintos, o si la opción B devolviera alguna fila (un id repetido). En el Job, eso haría fallar el assert de validación.

-- Opción A: total vs distintos
SELECT COUNT(*) AS filas, COUNT(DISTINCT transaction_id) AS ids_distintos
FROM silver_transactions;

-- Opción B: ids repetidos (debe devolver 0 filas)
SELECT transaction_id, COUNT(*) AS veces
FROM silver_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

6. Calculá la tasa de rechazo de cada lote como rechazadas / (aceptadas + rechazadas) en gold_batch_summary. ¿Es correcto comparar solamente las cantidades absolutas si los lotes tienen tamaños diferentes?

**RTA**: no es correcto comparar solo las cantidades absolutas cuando los lotes tienen tamaños distintos. La carga inicial tiene 51 rechazos contra 2 de cada lote nuevo, pero su tasa es de aproximadamente 0,10 %, unas diez veces menor que la de los lotes nuevos (≈ 0,99 %), porque el lote inicial es mucho más grande (≈ 50.000 registros contra ≈ 200). La tasa normaliza por el tamaño del lote y permite compararlos en igualdad de condiciones.

SELECT source_batch_id,
       accepted_transactions AS aceptadas,
       rejected_transactions AS rechazadas,
       ROUND(rejected_transactions /
             (accepted_transactions + rejected_transactions), 4) AS tasa_rechazo
FROM gold_batch_summary
ORDER BY source_batch_id;

7. ¿Qué día presenta el mayor monto total y cuál presenta la mayor cantidad de transacciones? Consultá gold_daily_sales y explicá si ambos máximos coinciden.

**RTA**: El 2026-03-01 presenta tanto el mayor volumen y monto de transacciones

-- Mayor monto
SELECT sale_date, SUM(total_amount) AS monto_total, SUM(transaction_count) AS transacciones
FROM gold_daily_sales
GROUP BY sale_date
ORDER BY monto_total DESC
LIMIT 5;

-- Mayor cantidad de transacciones
SELECT sale_date, SUM(total_amount) AS monto_total, SUM(transaction_count) AS transacciones
FROM gold_daily_sales
GROUP BY sale_date
ORDER BY transacciones DESC
LIMIT 5;

8. ¿Qué canal de pago tiene la mayor tasa global de fraude? Calculala como SUM(fraud_transactions) / SUM(transaction_count) y explicá por qué no corresponde promediar directamente fraud_rate.

**RTA**: No corresponde porque cada fila de gold_daily_sales ya es una tasa calculada sobre un grupo (día, país, categoría, canal) de tamaño distinto. Promediar tasas da el mismo peso a un grupo de 2 transacciones que a uno de 200, y el resultado se distorsiona. La tasa global correcta es el cociente de las sumas, SUM(fraud_transactions) / SUM(transaction_count), que pondera cada grupo por su cantidad de transacciones.

SELECT payment_channel,
       SUM(fraud_transactions) AS fraudes,
       SUM(transaction_count)  AS transacciones,
       ROUND(SUM(fraud_transactions) / SUM(transaction_count), 4) AS tasa_fraude_global,
       ROUND(AVG(fraud_rate), 4) AS promedio_simple_fraud_rate
FROM gold_daily_sales
GROUP BY payment_channel
ORDER BY tasa_fraude_global DESC;

9. ¿Qué combinación de país y categoría concentra el mayor monto vendido? Mostrá también la combinación líder dentro de cada país.

**RTA**: BR-home es la combinación de mayor monto vendido.

-- Combinación líder global
SELECT country, category, SUM(total_amount) AS monto_total
FROM gold_daily_sales
GROUP BY country, category
ORDER BY monto_total DESC
LIMIT 1;

-- Combinación líder dentro de cada país
WITH t AS (
  SELECT country, category, SUM(total_amount) AS monto_total
  FROM gold_daily_sales
  GROUP BY country, category
), ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY monto_total DESC) AS rn
  FROM t
)
SELECT country, category, monto_total
FROM ranked
WHERE rn = 1
ORDER BY monto_total DESC;

10. Compará las dos primeras filas de pipeline_run_audit correspondientes a la reejecución de batch_002. ¿Qué métricas permanecen iguales y qué columna demuestra que se realizó la comparación de idempotencia?

**RTA**: entre la primera ejecución y la reejecución de batch_002 permanecen iguales todas las métricas: silver_rows (50.149), quarantine_rows (53), gold_rows (598) y gold_total_amount (50.286.183,33). Cambian solo job_run_id y recorded_at.

La columna que demuestra que se realizó la comparación de idempotencia es idempotence_compared: vale false en la primera corrida (no había una corrida previa del mismo lote contra la cual comparar) y true en la reejecución. Ese valor es true solo cuando la corrida anterior corresponde al mismo expected_batch_id, y en ese caso el notebook 04_validate_pipeline ejecuta un assert que exige que las métricas sean idénticas.

Estas métricas son totales de toda la tabla y no solo del lote, por eso incluyen la carga inicial.

SELECT job_run_id, expected_batch_id, silver_rows, quarantine_rows,
       gold_rows, gold_total_amount, recorded_at, idempotence_compared
FROM pipeline_run_audit
WHERE expected_batch_id = 'batch_002'
ORDER BY recorded_at;

# **Interpretación del código y del pipeline**

11. En 01_ingest_bronze_incremental.ipynb, ¿qué problema resuelve COPY INTO y qué información utiliza para evitar cargar dos veces el mismo archivo físico?

**RTA**: COPY INTO resuelve la ingesta incremental de archivos: carga los CSV de incoming/transactions/ en la tabla Delta bronze_transactions_incremental, y solo procesa los archivos que todavía no cargó. Para no cargar dos veces el mismo archivo físico, la tabla destino guarda un registro de los archivos ya ingeridos (identificados por su ruta). Al reejecutar, los archivos ya registrados se omiten. Esto se ve en la corrida de batch_003: num_inserted_rows = 204, que corresponde solo al archivo nuevo, y no se volvieron a insertar las filas de batch_002.

12. ¿Por qué la vista bronze_transactions_all usa UNION ALL en lugar de eliminar duplicados? ¿En qué capa se resuelven los duplicados de negocio y por qué?

**RTA**: La vista usa UNION ALL porque Bronze debe conservar todos los registros tal como llegaron, incluidas las distintas versiones de una misma transacción. UNION (sin ALL) eliminaría filas idénticas, perdería trazabilidad y obligaría a un costoso proceso de comparación en la capa que debe ser más fiel a la fuente.

Los duplicados de negocio se resuelven en Silver (02_build_silver, celda donde se arma latest). Primero se eliminan los duplicados exactos con dropDuplicates(raw_columns), y después se conserva una sola versión por transacción con row_number(). Silver es la capa de tipado, validación y deduplicación; Bronze queda como historial reprocesable.

13. ¿Por qué las transacciones iniciales reciben source_batch_id='initial' y usan event_ts como updated_at? ¿Cómo afecta eso a la corrección de la transacción 42?

**RTA**: La tabla Bronze inicial no tiene las columnas source_batch_id ni updated_at, que sí traen los lotes nuevos. Para unificar los esquemas, la vista las completa: 'initial' identifica el origen y event_ts sirve como fecha de última modificación.

Para la transacción 42, la versión original queda con updated_at igual a su fecha de evento, que es anterior a la de cualquier corrección. Al ordenar por updated_at descendente en Silver, la versión corregida gana, y en el MERGE la condición s.updated_at > t.updated_at permite actualizarla. Si las filas iniciales no tuvieran un updated_at comparable, la corrección no podría imponerse sobre ellas.

14. En quality_rules.py, ¿qué ventaja ofrece try_cast frente a un cast convencional cuando llega un importe como N/A?

**RTA**: Con un cast convencional, un importe como N/A provoca un error de conversión (en Databricks, con ANSI activado) y tira abajo toda la tarea. try_cast devuelve NULL en lugar de fallar, de modo que el registro sigue en el flujo, amount_typed queda nulo y la regla lo envía a cuarentena con INVALID_AMOUNT. El pipeline no se cae por un dato malo y el registro queda auditable.

15. Las reglas de calidad asignan una única quality_reason. ¿Qué sucede si un registro viola más de una regla y por qué importa el orden de las condiciones?

**RTA**: La regla es una cadena when(...).when(...) y Spark asigna la primera condición que se cumple. Si un registro viola varias reglas, solo se registra la primera y las demás quedan ocultas.

El orden importa por dos motivos:

Determina qué motivo cuenta en la cuarentena. Un registro con amount = N/A y cliente desconocido se reporta como INVALID_AMOUNT, no como UNKNOWN_CUSTOMER.
Los chequeos de formato (INVALID_*) van antes que los de integridad referencial (UNKNOWN_CUSTOMER, UNKNOWN_PRODUCT). Si un customer_id no se pudo convertir, known_customer también queda nulo; si UNKNOWN_CUSTOMER se evaluara primero, el error se clasificaría mal.

16. Explicá cómo se construye _record_key y cómo se usa junto con row_number. ¿Qué caso cubre el hash cuando transaction_id no puede convertirse a un número?

**RTA**: _record_key es transaction_id_typed convertido a texto; si ese valor es nulo, se usa un hash SHA-256 de todas las columnas raw, donde cada nulo se reemplaza por '<NULL>'. Luego row_number() particiona por _record_key y ordena por updated_at_typed descendente (nulos al final) y source_batch_id descendente. Se conserva solo la fila con _rn = 1, es decir, la versión más reciente.

El hash cubre el caso en que transaction_id no se puede convertir a número. Sin él, todos esos registros tendrían la misma clave nula y row_number dejaría uno solo, perdiendo registros inválidos distintos. Con el hash, cada registro inválido distinto conserva su propia clave y llega a la cuarentena, mientras que los idénticos se colapsan en uno.

17. Interpretá las dos cláusulas principales del MERGE de silver_transactions. ¿Cuándo se actualiza una fila existente y cuándo se inserta una nueva?

**RTA**:

- Se actualiza una fila existente cuando el transaction_id ya está en Silver y la versión entrante tiene un updated_at estrictamente mayor. Así se aplica la corrección de la transacción 42.
- Se inserta una fila cuando el transaction_id no existe en Silver.
- Si la versión entrante es igual o más antigua, no se hace nada, y por eso reejecutar un lote no cambia los datos. No hay cláusula de borrado.

18. ¿Por qué las tablas Gold se reconstruyen completamente en esta práctica mientras Silver se actualiza con MERGE? Mencioná una ventaja y una limitación de cada estrategia.

**RTA**:

Gold, reconstrucción completa
- Ventaja: es simple y determinista. Cada ejecución refleja exactamente el estado actual de Silver, incluidas las correcciones, y reejecutar da el mismo resultado.
- Limitación: recalcula todo cada vez, por lo que el costo crece con el volumen de datos.

Silver, MERGE
- Ventaja: aplica altas y correcciones sobre la tabla existente, conserva la historia en Delta y mantiene la identidad por clave de negocio.
- Limitación: es más compleja y depende de que la clave y el criterio de orden (updated_at) sean correctos. Además, no elimina registros que desaparezcan de la fuente.

19. ¿Por qué expected_batch_id no participa en la detección del archivo nuevo? Indicá qué parte del pipeline descubre batch_003 y qué parte utiliza el parámetro.

**RTA**: batch_003 lo descubre COPY INTO (tarea ingest_bronze), que revisa la carpeta incoming/transactions/ y carga los archivos que no están registrados. El generador (00_generate_new_batch) deja el CSV ahí, fuera del Job.

expected_batch_id no participa en esa detección. Se usa para verificar:
- En 01, un assert comprueba que el lote esperado llegó a Bronze (found > 0).
- En 04, define el lote contra el cual se evalúan los controles y se registra en pipeline_run_audit.

20. Si la tarea build_silver falla, ¿qué ocurre con build_gold y validate en el Job? Explicá cómo las dependencias del DAG evitan publicar o validar resultados incompletos.
**RTA**: El DAG es lineal: ingest_bronze → build_silver → build_gold → validate, con run_if: ALL_SUCCESS en todas las tareas. Si build_silver falla, build_gold y validate no se ejecutan (quedan como Upstream failed). Esto evita:
- Publicar Gold sobre un Silver incompleto o desactualizado (Gold sigue mostrando el último resultado válido).
- Que validate registre en pipeline_run_audit métricas de una corrida inconsistente.