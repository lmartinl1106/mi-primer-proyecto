# Entrega práctica 5 — MLOps con MLflow

- **Nombre:** Martin Lin
- **student_id:** martin_lin
- **Escala:** small

## Mis runs

Tabla impresa por `03_comparar_y_elegir` (sección 5):

| run | modelo | features | class_weight | umbral | hiperparametros | train_pr_auc | pr_auc | accuracy | precision | recall | f1 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| run_ejercicio_5.2 | gradient_boosting | completas | ninguno | 0.2 | learning_rate=0.1, max_depth=8 | 0.66 | 0.581 | 0.881 | 0.45 | 0.674 | 0.54 |
| run_ejercicio_5.1 | gradient_boosting | completas | ninguno | 0.3 | learning_rate=0.1, max_depth=8 | 0.66 | 0.581 | 0.907 | 0.551 | 0.574 | 0.563 |
| run_ejercicio_3.2 | gradient_boosting | completas | ninguno | 0.5 | learning_rate=0.1, max_depth=8 | 0.66 | 0.581 | 0.917 | 0.696 | 0.362 | 0.476 |
| run_personalizado7 | gradient_boosting | completas | balanced | 0.4 | learning_rate=0.1, max_depth=sin_limite | 0.62 | 0.578 | 0.764 | 0.284 | 0.839 | 0.425 |
| run_ejercicio_4 | gradient_boosting | completas | balanced | 0.5 | learning_rate=0.1, max_depth=8 | 0.617 | 0.577 | 0.81 | 0.328 | 0.793 | 0.464 |
| run_personalizado8 | gradient_boosting | completas | balanced | 0.2 | learning_rate=0.03, max_depth=sin_limite | 0.612 | 0.577 | 0.617 | 0.205 | 0.934 | 0.336 |
| max_depth=8 | random_forest | completas | ninguno | 0.2 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.874 | 0.432 | 0.68 | 0.528 |
| run_ejercicio_3.1 | random_forest | completas | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.914 | 0.799 | 0.226 | 0.352 |
| run_ejercicio_2.3 | random_forest | completas | ninguno | 0.5 | n_estimators=100, max_depth=8 | 0.643 | 0.568 | 0.914 | 0.799 | 0.226 | 0.352 |
| run_personalizado1 | random_forest | completas | balanced | 0.5 | n_estimators=200, max_depth=8 | 0.632 | 0.553 | 0.808 | 0.325 | 0.792 | 0.461 |
| max_depth=8 | random_forest | completas | balanced | 0.2 | n_estimators=100, max_depth=8 | 0.629 | 0.55 | 0.527 | 0.175 | 0.955 | 0.296 |
| run_ejercicio_2.3 | logistic_regression | completas | ninguno | 0.5 | C=1.0 | 0.531 | 0.545 | 0.917 | 0.758 | 0.288 | 0.417 |
| max_depth=5 | random_forest | completas | ninguno | 0.2 | n_estimators=100, max_depth=5 | 0.561 | 0.541 | 0.88 | 0.443 | 0.631 | 0.521 |
| max_depth=5 | random_forest | completas | ninguno | 0.2 | n_estimators=100, max_depth=5 | 0.561 | 0.541 | 0.88 | 0.443 | 0.631 | 0.521 |
| max_depth=5 | random_forest | completas | balanced | 0.2 | n_estimators=100, max_depth=5 | 0.557 | 0.539 | 0.306 | 0.129 | 0.985 | 0.227 |

**Run registrado:** `run_ejercicio_5.2` (gradient boosting, completas, sin class_weight, umbral 0.2), registrado como `workspace.bigdata_martin_lin.fraud_model` versión 1 con alias `champion`.

## Datos y tracking

**1.** Train tiene 35.654 filas (11,05 % de contracargos), test 7.129 (10,38 %) y nuevas 7.165 (11,05 %). Se separa por fecha porque en la realidad el modelo se entrena con datos pasados y después tiene que predecir días que todavía no vio. Si se mezcla al azar, el modelo "ve" información de los mismos días que después evalúa y las métricas salen mejores de lo que serían.

**2.** Siempre diciendo que no hay contracargo se llega a 89,6 % de accuracy, pero sin detectar ninguno. Por eso se usa PR-AUC, que mira qué tan bien quedan ordenados los contracargos y no se infla por la cantidad de casos negativos (al azar da más o menos 0,104).

**3.** Quedaron como parámetros el modelo (logistic_regression), las features (basicas), C=1.0, class_weight=ninguno, el umbral 0.5, la tabla y la versión de los datos (0). Las métricas fueron test_pr_auc 0,339, roc_auc 0,794, accuracy 0,896, precision 0,493, recall 0,045, f1 0,082, 67 alertas y train_pr_auc 0,341, y como artefactos la matriz de confusión y el modelo (que en MLflow 3 aparece en Logged models). El experimento es la carpeta donde se juntan todos los entrenamientos de este problema, y cada run es uno de esos entrenamientos.

**4.** Autolog guardó 46 parámetros (todos los del pipeline, incluso los que yo no toqué) y métricas de entrenamiento: accuracy 0,91, f1 0,887, log_loss 0,229, precision 0,901, recall 0,91, roc_auc 0,899 y score 0,91. Lo que no registra es lo que tiene que ver con nuestro problema: las métricas en test, el PR-AUC, el umbral, el conjunto de features y la versión de los datos.

**5.** Sí, los dos dieron exactamente lo mismo (PR-AUC 0,5099 y 212 alertas en ambos). Para que eso pase hay que dejar fijos los datos, el código, la semilla y el entorno (las versiones de las librerías).

**6.** Tiró un error ("Failed to enforce schema of data") y no llegó a predecir nada. La firma dice qué columnas y de qué tipo espera el modelo (amount double y payment_channel string), así que si le falta algo lo frena en vez de devolver un resultado incorrecto.

## Tus experimentos

**7.** El mejor fue run_ejercicio_5.2: gradient boosting con las features completas, sin class_weight, learning_rate 0.1, max_depth 8 y umbral 0.2, con un PR-AUC de 0,581. Usando solo la regla daba 0,170, así que mejoró más de tres veces.

**8.** Con la logística el PR-AUC pasó de 0,339 con básicas a 0,448 con cliente y a 0,545 con completas, así que cada grupo de variables sumó alrededor de 0,1. En el mejor run lo que más pesó fue amount (0,337), después payment_channel (0,181) y hour (0,149).

**9.** Con completas anduvo mejor el boosting (0,581), después el bosque (0,568) y por último la logística (0,545). Los árboles van partiendo los datos de a una variable, y dentro de cada parte pueden volver a cortar por otra, entonces pueden encontrar algo como "de noche y por transferencia". La logística suma el efecto de cada variable por separado y esa combinación no la ve salvo que uno la arme a mano.

**10.** Con balanced la precision bajó de 0,696 a 0,328, el recall subió de 0,362 a 0,793 y las alertas pasaron de 385 a 1.790. El PR-AUC casi no se movió (0,581 contra 0,577), porque el modelo sigue ordenando las transacciones más o menos igual y lo que cambia es dónde caen las probabilidades.

**11.** Al bajar el umbral de 0.5 a 0.3 y 0.2 la precision fue bajando (0,696, 0,551, 0,45) y el recall subiendo (0,362, 0,574, 0,674), con más alertas (1.109 con 0.2). El PR-AUC quedó igual en 0,581 porque se calcula con todos los umbrales posibles, mientras que el F1 depende del punto de corte que elijas (dio 0,476, 0,563 y 0,54).

**12.** Barrí max_depth en un random forest con completas. El mejor PR-AUC de test fue con max_depth 8 (0,550), y con 3 y 5 dio 0,522 y 0,539. Sin límite de profundidad el train_pr_auc llegó a 1,0 pero en test bajó a 0,533, y eso es sobreajuste: el modelo se aprende de memoria los datos de entrenamiento.

## Comparar y elegir

**13.** No coincidieron. El mejor por PR-AUC fue run_ejercicio_5.2 (0,581) y el mejor por accuracy fue la logística run_ejercicio_2.3 (0,917), que tiene un recall de apenas 0,288. Como casi todas las compras son legítimas, un modelo que alerta poco tiene buena accuracy aunque se le escapen la mayoría de los contracargos.

**14.** Los más alejados son los bosques sin límite de profundidad, que tienen 1,0 en train y alrededor de 0,53 en test. Eso muestra que sobreajustan; los demás quedan bastante cerca de la diagonal.

**15.** Con search_runs los runs quedan en una tabla que se puede filtrar y ordenar desde código, y eso sirve para comparar muchos de una vez o para automatizar la elección del modelo. El filtro sería `"tags.student_id = 'martin_lin' AND params.model_type = 'random_forest' AND metrics.test_pr_auc > 0.55"`.

**16.** Normalmente cuesta más un contracargo que no se detecta, porque se pierde la plata de la compra, mientras que una alerta falsa es más que nada tiempo de revisión. Si el equipo puede revisar una cantidad fija por día, ordenaría las transacciones por score y pondría el umbral justo en el score de la última que entra en esa cantidad.

## Registro, uso y monitoreo

**17.** Sí, quedó como `workspace.bigdata_martin_lin.fraud_model`, versión 1, con alias champion. Usar el alias tiene la ventaja de que el código que consume el modelo no cambia: si mañana hay un modelo mejor, se le pasa el alias a la nueva versión y listo, sin andar buscando run_ids.

**18.** Se puntuaron 7.165 transacciones y 1.156 dieron alerta (16,1 %). Para auditar cada decisión están model_name, model_version, run_id, threshold y scored_at, además del score. El umbral queda afuera del modelo porque es una decisión del negocio y se puede querer cambiar sin tener que reentrenar.

**19.** Sí, dio parecido: 0,563 en nuevas contra 0,581 en test. Con la campaña simulada las alertas subieron del 16,1 % al 30,2 %, porque los importes y las transferencias aumentaron. El drift es cuando los datos que llegan dejan de parecerse a los de entrenamiento, y se puede ver antes de tener las etiquetas mirando cómo cambian las variables, los scores o el porcentaje de alertas.

## Cierre

**20.** En el 00 preparé los datos, en el 01 y 02 entrené y registré los runs, en el 03 comparé y elegí, y en el 04 registré el modelo, lo usé para puntuar las transacciones nuevas (el despliegue, en este caso batch) y lo monitoreé. Para usarlo en producción de verdad faltaría que el scoring corra solo como un job programado, tener monitoreo automático que avise si hay drift, y un proceso para reentrenar y comparar contra el champion antes de cambiarlo.
