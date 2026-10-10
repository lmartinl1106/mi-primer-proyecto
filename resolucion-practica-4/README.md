# Entrega práctica 4 — Streaming

- **Nombre:** Lucas Martin Lin
- **student_id:** martin_lin
- **Escala:** small

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

## Log y offsets

**1. En el ejemplo en Python, ¿cuántos mensajes quedaron en la cola y cuántos en el log después de leer tres? ¿Qué diferencia hay entre una cola tradicional y un log como Kafka?**

En la cola tradicional quedan 2, mientras que en el log quedan 5, porque en el caso del log, leer solo avanza el offset del consumidor a 3, no elimina. En la cola el mensaje se borra al consumirse, así que no se puede releer ni lo pueden leer varios consumidores independientes. En el log el mensaje se conserva según la retención, lo que permite fan-out y reprocesamiento, a costa de más almacenamiento y de que el consumidor tenga que gestionar su offset.

**2. Después de la llegada 2, ¿cuántos mensajes había leído el consumidor `antifraude` y cuántos `reportes`? ¿El atraso de `reportes` afectó a `antifraude`? ¿Por qué?**

Llegada 2: `antifraude` leyó 120 mensajes nuevos; `reportes` leyó 0. El atraso de `reportes` no afectó a `antifraude` porque cada consumidor tiene su propio checkpoint, lo que permite el fan-out sin molestarse entre ambos. 

**3. ¿Qué es un offset y dónde lo guarda cada consumidor en esta práctica?**

Un offset es la posición hasta donde un consumidor leyó el log; en Kafka es el número de mensaje dentro de la partición (1, 2, 3, etc.). En esta práctica, cada consumidor lo guarda en el checkpoint de su propia consulta de streaming (la carpeta checkpoints/<nombre>, por ejemplo antifraude, reportes y replay_v2), donde se registra la versión de la tabla Delta (el topic) hasta la que leyó, es decir, la próxima versión a leer.

**4. ¿Cuántos mensajes leyó el replay? ¿Qué propone la arquitectura Kappa para reprocesar datos y en qué se diferencia de Lambda?**

El replay leyó 280 mensajes (el topic tiene 280). Kappa usa un solo código de streaming: para reprocesar con lógica nueva se lanza una consulta nueva, con otro checkpoint, que lee el log completo desde el principio (el replay con replay_v2). Lambda mantiene dos sistemas, un batch preciso y una capa de streaming rápida, con la misma lógica escrita dos veces. La ventaja de Kappa es que hay una sola base de código por mantener, sin riesgo de que ambas versiones diverjan. Su desventaja es que depende de que el log conserve todo el historial necesario (más almacenamiento) y de que reprocesar lo completo puede ser lento o costoso.

## Ingesta y enriquecimiento

**5. ¿Cuántas filas nuevas procesó Bronze en cada llegada y cuántas al reejecutar sin archivos nuevos? ¿Qué componente evita leer dos veces el mismo archivo?**

Bronce procesó la misma cantidad de filas nuevas a medida que fueron llegando los mensajes stream_001: 60 filas, strem_002: 120 filas, stream_003: 40 filas stream_004: 40 filas, stream_005: 20 filas. Al reejecutar sin archivos nuevos no procesó nada. La componente **Auto Loader** (`format("cloudFiles")`) recuerda en su checkpoint qué archivos ya leyó: cada ejecución procesa **sólo los archivos nuevos**. Es el equivalente a un offset, pero para archivos.

**6. ¿Qué motivos de rechazo aparecieron en la cuarentena y en qué llegada? ¿Por qué Bronze guarda el texto original en lugar de descartar los registros con errores?**

Los rechazos aparecieron en el stream_002, los motivos de rechazo son INVALID_AMOUNT y UNKNOWN_CUSTOMER. Bronze guarda el texto original (raw_json) porque, si cambia el contrato o se corrige una regla, el dato original sigue disponible para reprocesar, es la definición de la capa Bronze.

**7. ¿Qué columnas agregó el join con `silver_customers` y `silver_products`? ¿Qué tipo de join es (stream-stream, stream-tabla o tabla-tabla) según la teoría?**

El join con `silver_customers` y `silver_products` agregó las variables `country` y `category`. El tipo de join es stream-tabla: cada evento del stream se enriquece con tablas estáticas de la clase 2.

## Tiempo y ventanas

**8. ¿Qué diferencia hay entre event time y processing time? Usá `late_ok` como ejemplo.**

Event time:cuándo ocurrió la compra.
Processing time: cuándo llega a nuestro sistema. En el mundo ideal Processing time debería ser lo más cercado al event time. Pero puede definirse un umbral (Processing time - Event time > x), en donde a partir de cierto tiempo se diferencia entre un dato `late_ok` y `late_bad`, en donde `late_bad` lo descarto por haber llegado demasiado tarde a nuestro sistema.

**9. ¿Cuánto valía el watermark al terminar cada llegada? ¿Cómo se calcula a partir del máximo event time visto?**

El watermark es el máximo event time visto menos 10 minutos, y al terminar cada llegada valió 11:54, 11:58, 12:16, 12:17 y 12:35 (con máximos de 12:04, 12:08, 12:26, 12:27 y 12:45). Se calcula así porque el sistema solo espera datos de hasta 10 minutos antes del evento más reciente que vio.

**10. ¿En qué llegada aparecieron las primeras ventanas tumbling en modo append y por qué no antes?**

Las primeras ventanas tumbling en modo append aparecieron en la llegada 3, cuando el watermark saltó a 12:16 y superó el fin de las ventanas de 12:00 a 12:10, y el total emitido pasó de 0 a 4. No aparecieron antes porque en las llegadas 1 y 2 el watermark (11:54 y 11:58) todavía estaba por debajo del fin de cualquier ventana, y en append solo se emite una ventana cuando ya está cerrada.

**11. ¿Qué pasó con `late_bad`? ¿Por qué se aceptó `late_ok` y se descartó `late_bad`?**

`late_bad` (12:02) llegó en la llegada 4, cuando el watermark ya estaba en 12:16, así que era más viejo que el watermark y se descartó sin sumar en ninguna ventana (la tabla muestra 20 eventos más viejos que el watermark en esa llegada). `late_ok` (12:03) llegó en la llegada 2, cuando el watermark todavía estaba en 11:54, así que cayó dentro del margen y se aceptó.

**12. Mirando el gráfico de ventanas, ¿en qué se diferencian tumbling, hopping y session? Da un ejemplo de negocio para cada una.**

Tumbling usa ventanas fijas de 5 minutos sin solaparse (en el gráfico, 80, 40 y 60 compras), útil para el monto total de pagos cada 5 minutos en un reporte. Hopping usa ventanas de 10 minutos que avanzan cada 5 y se solapan (12:00-12:10 suma 120), útil para monitorear picos de pagos con actualización frecuente, pero una misma compra se cuenta en dos ventanas. Session se cierra tras 5 minutos sin compras del cliente (el cliente 0 tiene sesiones de 4 y 3 compras), útil para agrupar la ráfaga de operaciones de una visita al home banking, aunque es más costosa porque su tamaño no es fijo.

**13. Para la ventana [12:00, 12:05) del canal card, ¿cuántas veces y con qué valores se emitió en modo update y en modo append? ¿Qué ventaja y qué desventaja tiene cada modo?**

En update, la ventana [12:00, 12:05) del canal card se emitió dos veces: en la llegada 1 con 40 compras y monto 6000.00, y en la llegada 2 con 60 compras y 8400.00. En append se emitió una sola vez, en la llegada 3, con 60 compras y 8400.00. Update da resultados rápidos pero provisorios que el consumidor debe saber corregir; append da un valor final y simple, pero con demora.

**14. ¿Qué se gana y qué se pierde si el watermark fuera de 1 minuto en lugar de 10?**

Con un watermark de 1 minuto las ventanas cierran antes y se usa menos memoria. A cambio se descartan más eventos tardíos: `late_ok` (12:03) se habría perdido y la ventana de card habría quedado con 40 compras en lugar de 60.

## Garantías y fallas

**15. ¿Cuántas filas quedaron en el destino sin deduplicar y cuántas deduplicando por `event_id`? ¿Por qué el productor puede enviar el mismo evento dos veces?**

Sin deduplicar quedaron 220 filas en el destino, y deduplicando por event_id quedaron 200 (220 filas, 200 event_id distintos, 20 duplicados). El productor puede enviar el mismo evento dos veces porque ante la duda reintenta (at-least-once): e002_000 aparece en stream_001 y en stream_002 con el mismo timestamp y el mismo monto de 200.00.

**16. ¿Qué pasó al reiniciar con el mismo checkpoint y qué pasó al perderlo? ¿Qué garantía de entrega se observa en cada caso?**

Al reiniciar con el mismo checkpoint el conteo se quedó en 200, porque el checkpoint sabe hasta dónde procesó y no vuelve a escribir nada; ahí se observa exactly-once dentro de Spark. Al perder el checkpoint el conteo subió a 400, porque la consulta empezó de nuevo y escribió todo otra vez en un destino append; ahí se observa at-least-once (duplicados, pero sin pérdida).

**17. ¿Por qué con `MERGE` el resultado no cambió aunque se perdiera el checkpoint? ¿Qué significa que una escritura sea idempotente?**

Con MERGE el destino quedó en 200 tanto en la primera ejecución (5a) como con el checkpoint perdido (5b), porque cada microbatch solo inserta los event_id que todavía no están en la tabla. Una escritura es idempotente cuando aplicarla dos veces tiene el mismo efecto que aplicarla una (como MERGE por clave o SET x = 5 en lugar de INCREMENT x), y su desventaja es el costo extra de hacer el MERGE en cada microbatch.

## CDC

**18. ¿Cuántos eventos de cambio generó el log y de qué tipos? ¿Por qué un `UPDATE` genera dos eventos?**

El log generó 13 eventos de cambio, de cuatro tipos: insert (6: los 5 clientes iniciales más el alta del cliente 100), update_preimage (3), update_postimage (3) y delete (1, el cliente 3). Un UPDATE genera dos eventos porque el log registra la fila antes del cambio (update_preimage) y después (update_postimage), lo que permite saber qué cambió y no solo el valor final.

**19. ¿Se pudo reconstruir la tabla a partir del log? ¿Qué información tiene el log que la tabla actual no tiene?**

Sí, la tabla reconstruida desde el log resultó igual a la original (la celda imprime “Sí”, 0 diferencias). Se logró quedándose con el último cambio por customer_id (ignorando los update_preimage y descartando los que terminan en delete), que es la idea de log compaction. El log conserva información que la tabla actual perdió: por ejemplo, que el cliente 2 pasó de UY a CL (versión 3) y luego a BR (versión 8), y que existió el cliente 3, que fue borrado. La ventaja es que el log tiene la historia completa, y su desventaja es que ocupa más almacenamiento que guardar solo el estado actual.

## Cierre

**20.**
