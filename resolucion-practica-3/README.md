# Entrega práctica 3 — Bases de datos NoSQL

- **Nombre:** Martin Lin
- **student_id:** martin_lin
- **Escala:** small

Respondé cada pregunta en una a tres oraciones. Cuando se pide un dato, copiá el valor que te dio el notebook.

## CAP y PACELC

**1. Con la red partida, ¿qué respondió la réplica `C` en modo CP y qué respondió en modo AP? ¿Qué propiedad de CAP sacrifica cada modo?**

En modo CP, la réplica `C` aislada respondió `ERROR: no disponible` al leer, porque no tiene quórum; en modo AP respondió `100`, un saldo viejo (ya se había escrito `80` en `A`). Por lo tanto CP sacrifica disponibilidad y AP sacrifica consistencia.

**2. En modo AP, ¿qué saldo quedó en las tres réplicas después de la reparación y qué escritura se perdió?**

Después de la reparación las tres réplicas quedaron con saldo `50`. Se perdió la escritura `saldo=80` hecha en `A`, porque con last-write-wins gana la escritura más nueva (`50`, hecha en `C`) aunque las dos fueran concurrentes.

**3. Según el gráfico de latencia, ¿cuál es la p99 con `W=1` y con `W=3`? ¿Qué se gana a cambio de esperar más réplicas? (PACELC)**

La p99 es 9,6 ms con `W=1` y 55,6 ms con `W=3`. Esperar más réplicas da más consistencia (la escritura ya está en todas las copias y la lectura no ve datos viejos), pero se paga con latencia, porque hay que esperar a la réplica remota. Es el trade-off latencia/consistencia de PACELC, que se da incluso sin partición.

**4. Con cinco réplicas partidas en `ABC | DE`, ¿qué lado pudo seguir escribiendo en modo CP y por qué?**

Pudo seguir escribiendo el lado `ABC` (escribió `saldo=80` con éxito). Tiene 3 de 5 réplicas, que es mayoría (quórum). El lado `DE` tiene solo 2 y recibió `ERROR: sin quórum` al escribir y `ERROR: no disponible` al leer.

## Clave-valor

**5. ¿Cuántas veces más rápido fue el GET en memoria que el GET con Spark? ¿Por qué Redis guarda los datos en memoria?**

El GET en memoria (0,0003 ms) fue unas 2.296.238 veces más rápido que el GET con Spark (618 ms). Redis guarda los datos en memoria porque el acceso a RAM es órdenes de magnitud más rápido que ir a disco o lanzar una consulta, y una tabla hash permite encontrar una clave en O(1).

**6. ¿Por qué para contar los clientes de AR hubo que leer y parsear todos los valores? Mencioná una ventaja y una desventaja del modelo clave-valor.**

El país está dentro del valor, que para la base es un JSON opaco, y no existe un `WHERE country = 'AR'`. Hubo que parsear los 5.000 valores para encontrar los 992 clientes de AR. Ventaja: acceso por clave muy rápido (O(1)) y fácil de repartir entre nodos. Desventaja: no se puede consultar por el contenido del valor sin armar índices a mano.

**7. Al pasar de 4 a 5 nodos, ¿qué porcentaje de claves se movió con `hash mod N` y con hashing consistente? ¿Por qué conviene el segundo?**

Con `hash mod N` se movió el 79,6 % de las claves y con hashing consistente el 21,2 % (con 100 nodos virtuales). Conviene el hashing consistente porque al agregar un nodo solo se mueven las claves de su tramo del anillo (≈1/N), en vez de casi todos los datos por la red.

## Documental

**8. ¿Cuántas filas devolvió la consulta relacional del cliente 42 y cuántos documentos la documental? ¿Qué datos quedaron embebidos dentro del documento?**

La consulta relacional del cliente 42 devolvió 11 filas (con 2 joins y el dato del cliente repetido en cada una) y la documental devolvió 1 documento. Quedaron embebidos el perfil (`country`, `segment`, `email`), las estadísticas (`stats`) y todas sus transacciones, cada una con un snapshot del producto (`product_id`, `category`, `price`).

**9. Al leer documentos con campos distintos, ¿qué hizo Spark con los campos que faltaban en algunos? ¿Por qué se dice que el modelo documental tiene esquema flexible?**

Spark armó un esquema que es la unión de todos los campos y completó con `null` los que faltaban en cada documento. El esquema es flexible porque cada documento puede tener campos distintos (`loyalty`, `phones`, `preferences`, `schema_version`) sin migrar una estructura fija, y el esquema se infiere al leer (schema-on-read).

**10. ¿Cuánto pesa el documento más grande? ¿Qué problema aparece si un documento crece sin límite?**

El documento más grande pesa 5,2 KB (≈225 bytes por transacción). Si un documento crece sin límite puede chocar con el máximo del motor (16 MB en MongoDB, unas 74.599 transacciones a ese ritmo), y se vuelve lento de leer y reescribir completo.

**11. Para calcular el monto por categoría hubo que usar `explode`. ¿Qué tipo de consultas resuelve bien el modelo documental y cuáles le cuestan más?**

Resuelve bien las consultas centradas en una entidad (leer un cliente con todo su historial, o filtrar dentro del documento) sin joins. Le cuestan las consultas que cruzan documentos, como el monto por categoría, que obligan a desarmar los arrays con `explode` y volver a filas. También le cuestan los datos duplicados (como el snapshot del producto), que hay que actualizar en muchos lugares.

## Grafos

**12. ¿Cuántos nodos `Customer`, nodos `Device` y aristas `USES` tiene el grafo?**

El grafo tiene 5.000 nodos `Customer`, 2.361 nodos `Device` y 5.400 aristas `USES`.

**13. ¿Cuántos joins necesitó SQL para el patrón de 2 saltos y para el de 4? ¿Por qué Neo4j recorre relaciones más eficientemente que una base relacional?**

SQL necesitó 3 joins para el patrón de 2 saltos y 5 joins para el de 4 saltos (1.389 y 254 clientes alcanzados). Neo4j recorre relaciones con más eficiencia porque cada nodo guarda punteros directos a sus vecinos (index-free adjacency). El costo depende de los vecinos recorridos y no de buscar en toda la tabla de aristas con un join.

**14. ¿Cuántas iteraciones tardó en converger la búsqueda de componentes conexos y cuántos nodos tiene el componente más grande?**

La propagación de etiquetas tardó 14 iteraciones en converger (en la 14 ya no cambió ningún nodo). El componente más grande tiene 23 nodos.

**15. ¿Qué porcentaje de aristas quedó entre máquinas distintas al repartir los nodos con `hash(id) mod 4`? ¿Por qué es difícil distribuir una base de grafos?**

El 74,4 % de las aristas quedó entre máquinas distintas con `hash(id) mod 4`. Es difícil distribuir un grafo porque los datos están muy conectados: cualquier reparto corta aristas, y cada arista cortada obliga a un salto por la red en cada recorrido. El hash ignora la estructura del grafo, y agrupar por componente (0 % de aristas cortadas) no siempre es posible ni queda balanceado en grafos reales.

## Vectorial

**16. ¿Qué representa el vector de cada cliente y qué mide la similitud coseno?**

El vector de cada cliente tiene 10 números que resumen su comportamiento: proporción del gasto en cada una de 5 categorías, proporción de pagos por cada uno de 3 canales, el ticket promedio (dividido por 2.000) y la proporción de compras de más de 1.500. La similitud coseno mide el ángulo entre dos vectores (1 = misma dirección), o sea qué tan parecido es el perfil de comportamiento sin importar la magnitud.

**17. ¿Qué porcentaje de clientes marcados hay entre los vecinos de clientes marcados y cuál es la tasa base? ¿Para qué sirve buscar "vecinos parecidos"?**

Entre los 10 vecinos de clientes marcados, el 31,6 % está marcado, contra una tasa base del 14,8 %. Buscar vecinos parecidos sirve para encontrar clientes con comportamiento similar a uno sospechoso, para clasificar con k-NN y para priorizar revisiones. Aun así, el clasificador por voto de 10 vecinos acertó 85,9 %, sin superar a predecir siempre "no marcado" (86,3 %), porque solo el 14,8 % de los clientes está marcado.

**18. En la curva del índice IVF, ¿qué recall y qué porcentaje de vectores escaneados se obtienen con `nprobe=4`? ¿Qué se gana y qué se pierde con la búsqueda aproximada (ANN)?**

Con `nprobe=4` el recall@10 es 0,872 (87,2 %) y se escanea el 5,985 % de los vectores. Con la búsqueda aproximada se gana velocidad y menor costo, porque se compara contra una fracción chica del total. Se pierde exactitud: puede faltar algún vecino verdadero (≈13 % en este caso).

## Columnar

**19. ¿Cuánto ocupan los datos en CSV, JSON y Parquet? ¿Qué columnas leyó Spark para la consulta de `amount` sobre Parquet (`ReadSchema`)? ¿Cuántos archivos se pueden saltear con los datos ordenados?**

Los datos ocupan 50,0 MB en CSV, 121,1 MB en JSON y 5,3 MB en Parquet. Para la consulta de `amount` sobre Parquet, Spark leyó solo esa columna (`ReadSchema: struct<amount:decimal(12,2)>`). Con los datos ordenados por `amount` se pueden saltear 7 de 8 archivos (con datos al azar, 0 de 8).

**20. ¿Por qué el formato columnar es bueno para analítica y poco conveniente para modificar una fila por vez?**

Es bueno para analítica porque las consultas suelen usar pocas columnas de muchas filas. Al guardar cada columna junta se lee solo lo necesario, se comprime mejor (valores del mismo tipo) y se saltean bloques con estadísticas min/max. Es poco conveniente para modificar una fila por vez porque los valores de esa fila están repartidos en bloques de columnas distintos, comprimidos y en archivos inmutables, así que un cambio implica reescribir bloques o archivos enteros. Para eso conviene un almacenamiento por filas (OLTP).
