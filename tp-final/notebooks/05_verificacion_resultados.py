# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Verificación de resultados
# MAGIC
# MAGIC Se ejecuta **después** de una actualización completa del pipeline. No modifica tablas: consulta las
# MAGIC salidas, el event log del pipeline y dibuja los gráficos que responden las preguntas del caso.
# MAGIC
# MAGIC 1. ¿El pipeline concilia entre capas? 2. ¿Qué rechazó Silver y por qué? 3. ¿La ingesta es idempotente?
# MAGIC 4. ¿Cuánto riesgo tiene hoy la cartera y dónde está? 5. ¿El modelo de VaR funcionó en el tiempo?

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "martin_lin_tp_fx", "02 - Esquema")

# COMMAND ----------

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()


def T(nombre):
    return f"{catalog}.{schema}.{nombre}"


# Paleta: un color por función. Identidad (series), estado (semáforo) y texto en tinta neutra.
AZUL, NARANJA = "#2a78d6", "#eb6834"
VERDE, AMARILLO, ROJO = "#0ca30c", "#fab219", "#d03b3b"
TINTA, TINTA_2, MUTED, GRILLA = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
plt.rcParams.update({
    "axes.edgecolor": "#c3c2b7", "axes.labelcolor": TINTA_2, "xtick.color": MUTED, "ytick.color": MUTED,
    "axes.spines.top": False, "axes.spines.right": False, "axes.titlelocation": "left",
    "axes.titlesize": 12, "axes.titlecolor": TINTA, "font.size": 10,
})

tablas = ["bronze_cotizaciones", "silver_cotizaciones", "silver_cotizaciones_cuarentena", "silver_cartera",
          "gold_retornos", "gold_pnl_diario", "gold_var_diario", "gold_backtesting_anual", "pipeline_validacion"]
faltan = [t for t in tablas if not spark.catalog.tableExists(T(t))]
assert not faltan, f"Faltan tablas: {faltan}. Ejecutá primero el pipeline completo."

# COMMAND ----------

# MAGIC %md ## 1. Conciliación entre capas
# MAGIC Una fila con las métricas y el resultado de cada control. Si alguno hubiera fallado, la
# MAGIC actualización del pipeline habría terminado en error (`expect_all_or_fail`).

# COMMAND ----------

v = spark.table(T("pipeline_validacion")).drop("validado_en").first().asDict()
display(spark.createDataFrame([(k, str(val)) for k, val in v.items()], ["métrica / control", "valor"]))

# COMMAND ----------

display(spark.createDataFrame(
    [(t, spark.table(T(t)).count()) for t in tablas if t != "pipeline_validacion"], ["tabla", "filas"]))

# COMMAND ----------

# MAGIC %md ## 2. Calidad de datos: qué rechazó Silver y qué sólo se avisó

# COMMAND ----------

display(spark.table(T("silver_cotizaciones_cuarentena"))
        .groupBy("quality_reason", "fecha_original")
        .agg(F.count("*").alias("filas"), F.countDistinct("moneda_pedida").alias("monedas"))
        .orderBy(F.desc("filas")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Serie USD contra REF (Com. A 3500)
# MAGIC Días en que el dólar de la serie USD se aparta más de 3 % de REF. En días de crisis es esperable
# MAGIC (USD es un valor puntual y REF un promedio del día), y por eso no se descartan. Un desvío grande con REF
# MAGIC quieto indica un error de publicación, como el 07/12/2023. Por esto los precios en pesos se valúan con REF.

# COMMAND ----------

usd_raw = (spark.table(T("silver_cotizaciones")).where("moneda = 'USD'")
           .select("fecha", F.col("tipo_cotizacion").cast("double").alias("usd_serie")))
ref = (spark.table(T("silver_cotizaciones")).where("moneda = 'REF'")
       .select("fecha", F.col("precio_ars").cast("double").alias("ref")))
from pyspark.sql.window import Window  # noqa: E402
w_ref = Window.orderBy("fecha")
display(usd_raw.join(ref.withColumn("ref_variacion_dia_pct", F.round((F.col("ref") / F.lag("ref").over(w_ref) - 1) * 100, 2)), "fecha")
        .withColumn("desvio_usd_vs_ref_pct", F.round((F.col("usd_serie") / F.col("ref") - 1) * 100, 2))
        .where(F.abs(F.col("desvio_usd_vs_ref_pct")) > 3)
        .orderBy("fecha"))

# COMMAND ----------

# Métricas de cada expectation en la última actualización (event log del pipeline).
event_log = f"event_log(TABLE({T('silver_cotizaciones')}))"
try:
    calidad = spark.sql(f"""
    WITH ultima AS (
      SELECT origin.update_id FROM {event_log}
      WHERE event_type = 'create_update' ORDER BY timestamp DESC LIMIT 1
    ), exp AS (
      SELECT explode(from_json(details:flow_progress.data_quality.expectations,
               'array<struct<name:string,dataset:string,passed_records:bigint,failed_records:bigint>>')) e
      FROM {event_log}
      WHERE event_type = 'flow_progress' AND origin.update_id = (SELECT update_id FROM ultima)
        AND details:flow_progress.data_quality.expectations IS NOT NULL
    )
    SELECT e.dataset, e.name AS expectation, sum(e.passed_records) AS pasan, sum(e.failed_records) AS fallan
    FROM exp GROUP BY ALL ORDER BY e.dataset, fallan DESC""")
    display(calidad)
except Exception as e:
    print("No se pudo leer el event log desde este cómputo:", str(e)[:200])
    print("Las mismas métricas están en la interfaz del pipeline → tabla → pestaña Data quality.")

# COMMAND ----------

# MAGIC %md ## 3. Idempotencia de la ingesta
# MAGIC Filas escritas en `bronze_cotizaciones` por actualización. Una actualización sin archivos nuevos
# MAGIC debe escribir 0: Auto Loader no vuelve a leer lo ya procesado.

# COMMAND ----------

try:
    display(spark.sql(f"""
    SELECT origin.update_id, min(timestamp) AS inicio,
           coalesce(sum(cast(details:flow_progress.metrics.num_output_rows AS BIGINT)), 0) AS filas_escritas
    FROM {event_log}
    WHERE event_type = 'flow_progress' AND origin.flow_name LIKE '%bronze_cotizaciones%'
    GROUP BY ALL ORDER BY inicio"""))
except Exception as e:
    print("No se pudo leer el event log:", str(e)[:200])

# COMMAND ----------

# MAGIC %md ## 4. Contexto: el dólar de referencia (REF, Com. A 3500) y sus saltos
# MAGIC **Pregunta:** ¿qué episodios dominan la historia del tipo de cambio? Escala logarítmica para que una
# MAGIC suba del 10 % se vea igual en 2003 que en 2026.

# COMMAND ----------

usd = (spark.table(T("silver_cotizaciones")).where("moneda = 'USD'")
       .select("fecha", F.col("precio_ars").cast("double").alias("precio"), "salto_anomalo", "variacion_ars")
       .orderBy("fecha").toPandas())
saltos = usd[usd["salto_anomalo"]]

fig, ax = plt.subplots(figsize=(11, 4))
ax.plot(usd["fecha"], usd["precio"], color=AZUL, linewidth=1.6)
ax.scatter(saltos["fecha"], saltos["precio"], s=40, color=ROJO, zorder=3, edgecolor="white", linewidth=1.5)
for _, r in saltos.iterrows():
    ax.annotate(f"{r.fecha:%d/%m/%Y}  {float(r.variacion_ars):+.0%}", (r.fecha, r.precio),
                xytext=(6, -12), textcoords="offset points", fontsize=8, color=TINTA_2)
ax.set_yscale("log")
ax.set_ylabel("Pesos por dólar (escala log)")
ax.grid(axis="y", color=GRILLA, linewidth=0.8)
ax.set_title("Dólar de referencia A3500 (BCRA) y días con variación mayor al 10 %")
ax.legend(handles=[Line2D([], [], color=AZUL, lw=1.6, label="USD/ARS (REF)"),
                   Line2D([], [], marker="o", ls="", color=ROJO, label="Salto > 10 %")],
          frameon=False, loc="upper left")
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md ## 5. ¿Cuánto riesgo tiene hoy la cartera y dónde está?
# MAGIC VaR 99 % del último día por mesa, y la suma de las mesas contra la cartera: la diferencia es el
# MAGIC beneficio de diversificación. Las dos perspectivas tienen escalas distintas, por eso van en paneles separados.

# COMMAND ----------

ultima = spark.table(T("gold_var_diario")).agg(F.max("fecha")).first()[0]
hoy = (spark.table(T("gold_var_diario")).where(F.col("fecha") == ultima)
       .where("nivel IN ('mesa', 'cartera')")
       .select("perspectiva", "nivel", "segmento", F.col("var_99_usd_mm").alias("var")).toPandas())

fig, axes = plt.subplots(1, 2, figsize=(12, 3.8))
for ax, persp in zip(axes, ["ARS", "USD"]):
    d = hoy[hoy["perspectiva"] == persp]
    mesas = d[d["nivel"] == "mesa"].sort_values("var")
    total = float(d[d["nivel"] == "cartera"]["var"].iloc[0])
    suma = float(mesas["var"].sum())
    etiquetas = list(mesas["segmento"]) + ["Suma de mesas", "Cartera"]
    valores = list(mesas["var"]) + [suma, total]
    colores = [AZUL] * len(mesas) + ["#86b6ef", "#1c5cab"]
    ax.barh(etiquetas, valores, color=colores, height=0.6)
    for i, val in enumerate(valores):
        ax.text(val, i, f"  {val:.2f}", va="center", fontsize=9, color=TINTA)
    ax.set_xlim(0, max(valores) * 1.25)
    ax.grid(axis="x", color=GRILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_xlabel("VaR 99 % a 1 día (millones de USD eq.)")
    ax.set_title(f"Perspectiva {persp} — diversificación {1 - total / suma:.0%}")
fig.suptitle(f"VaR por mesa al {ultima:%d/%m/%Y}", x=0.01, ha="left", fontsize=13, color=TINTA)
fig.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md ## 6. ¿El modelo funcionó? Resultado diario contra VaR
# MAGIC Cada barra es el resultado de un día; la línea es la pérdida que el VaR 99 % no esperaba superar.
# MAGIC Un punto rojo es una excepción: la pérdida real fue mayor que el VaR.

# COMMAND ----------

def grafico_pnl_vs_var(perspectiva, desde, hasta, titulo):
    d = (spark.table(T("gold_var_diario"))
         .where(f"nivel = 'cartera' AND perspectiva = '{perspectiva}' AND var_99_usd_mm IS NOT NULL")
         .where(F.col("fecha").between(desde, hasta))
         .select("fecha", "pnl_usd_mm", "var_99_usd_mm", "excepcion").orderBy("fecha").toPandas())
    exc = d[d["excepcion"]]
    # Las devaluaciones generan ganancias enormes que aplastarían la escala: se recorta el eje y
    # se señalan los días fuera de escala con su valor.
    lim = max(d["pnl_usd_mm"].abs().quantile(0.995), 2 * d["var_99_usd_mm"].max()) * 1.15
    fuera = d[d["pnl_usd_mm"].abs() > lim]
    fig, ax = plt.subplots(figsize=(11, 4))
    ax.bar(d["fecha"], d["pnl_usd_mm"].clip(-lim, lim), width=1.0, color=AZUL, alpha=0.55)
    ax.plot(d["fecha"], -d["var_99_usd_mm"], color=NARANJA, linewidth=2)
    ax.scatter(exc["fecha"], exc["pnl_usd_mm"].clip(-lim, lim), s=36, color=ROJO, zorder=3,
               edgecolor="white", linewidth=1.5)
    # Etiquetas escalonadas en tres alturas para que no se superpongan días cercanos.
    for k, (_, r) in enumerate(fuera.sort_values("fecha").iterrows()):
        y = lim if r.pnl_usd_mm > 0 else -lim
        desplazamiento = -10 - 12 * (k % 3) if y > 0 else 4 + 12 * (k % 3)
        ax.annotate(f"{r.pnl_usd_mm:+.1f} ({r.fecha:%d/%m/%Y})", (r.fecha, y), xytext=(4, desplazamiento),
                    textcoords="offset points", fontsize=8, color=TINTA_2)
    ax.set_ylim(-lim * 1.05, lim * 1.05)
    ax.axhline(0, color="#c3c2b7", linewidth=1)
    ax.grid(axis="y", color=GRILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_ylabel("Millones de USD eq.")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%m/%Y"))
    ax.set_title(f"{titulo} — {len(exc)} excepciones en {len(d)} días")
    ax.legend(handles=[Patch(color=AZUL, alpha=0.55, label="Resultado diario"),
                       Line2D([], [], color=NARANJA, lw=2, label="−VaR 99 %"),
                       Line2D([], [], marker="o", ls="", color=ROJO, label="Excepción")],
              frameon=False, loc="upper left", bbox_to_anchor=(0, -0.12), ncol=3)
    fig.tight_layout()
    plt.show()


grafico_pnl_vs_var("ARS", "2023-06-01", "2026-12-31", "Perspectiva ARS: del crawling peg a la salida del cepo")

# COMMAND ----------

grafico_pnl_vs_var("ARS", "2003-01-01", "2026-12-31", "Perspectiva ARS: historia completa")

# COMMAND ----------

grafico_pnl_vs_var("USD", "2003-01-01", "2026-12-31", "Perspectiva USD: historia completa")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Detalle de las excepciones en pesos desde 2015
# MAGIC Cada excepción con la moneda que más perdió ese día: permite distinguir movimientos de mercado
# MAGIC (apreciación del peso, que afecta a todas las posiciones) de problemas de datos en una sola moneda.

# COMMAND ----------

display(spark.sql(f"""
WITH exc AS (
  SELECT fecha, pnl_usd_mm, var_99_usd_mm
  FROM {T('gold_var_diario')}
  WHERE perspectiva = 'ARS' AND nivel = 'cartera' AND excepcion AND fecha >= '2015-01-01'
), aporte AS (
  SELECT p.fecha, p.moneda, sum(p.pnl_usd_mm) AS pnl_moneda,
         row_number() OVER (PARTITION BY p.fecha ORDER BY sum(p.pnl_usd_mm)) AS rn
  FROM {T('gold_pnl_diario')} p JOIN exc USING (fecha)
  WHERE p.perspectiva = 'ARS'
  GROUP BY p.fecha, p.moneda
)
SELECT e.fecha, round(e.pnl_usd_mm, 2) AS pnl, round(e.var_99_usd_mm, 2) AS var_99,
       a.moneda AS moneda_mayor_perdida, round(a.pnl_moneda, 2) AS pnl_moneda
FROM exc e JOIN aporte a ON a.fecha = e.fecha AND a.rn = 1
ORDER BY e.fecha"""))

# COMMAND ----------

# MAGIC %md ## 7. Backtesting por año
# MAGIC Excepciones por año con el color de la zona de Basilea (verde 0–4, amarillo 5–9, rojo 10 o más).
# MAGIC La línea punteada son las excepciones esperadas (1 % de los días). El triángulo marca los años en que
# MAGIC el test de Kupiec rechaza el modelo: por exceso de excepciones **o por falta** (modelo demasiado conservador).

# COMMAND ----------

bt = (spark.table(T("gold_backtesting_anual")).where("nivel = 'cartera'")
      .select("perspectiva", "anio", "dias", "excepciones", "excepciones_esperadas", "kupiec_rechaza_modelo")
      .orderBy("perspectiva", "anio").toPandas())
zona = lambda x: VERDE if x <= 4 else (AMARILLO if x <= 9 else ROJO)  # noqa: E731

fig, axes = plt.subplots(2, 1, figsize=(11, 6.5), sharex=True)
for ax, persp in zip(axes, ["ARS", "USD"]):
    d = bt[bt["perspectiva"] == persp]
    ax.bar(d["anio"], d["excepciones"], color=[zona(x) for x in d["excepciones"]], width=0.7)
    ax.step(d["anio"], d["excepciones_esperadas"], where="mid", color=TINTA_2, linestyle="--", linewidth=1)
    rech = d[d["kupiec_rechaza_modelo"]]
    ax.scatter(rech["anio"], rech["excepciones"] + 1.6, marker="v", s=30, color=TINTA)
    for _, r in d.iterrows():
        ax.text(r.anio, r.excepciones + 0.15, str(int(r.excepciones)), ha="center", va="bottom", fontsize=8, color=TINTA)
    ax.set_ylim(0, max(bt["excepciones"]) + 4)
    ax.grid(axis="y", color=GRILLA, linewidth=0.8)
    ax.set_axisbelow(True)
    ax.set_ylabel("Excepciones")
    ax.set_title(f"Perspectiva {persp}")
axes[1].set_xticks(sorted(bt["anio"].unique()))
axes[1].tick_params(axis="x", rotation=90)
axes[0].legend(handles=[Patch(color=VERDE, label="Verde (0–4)"), Patch(color=AMARILLO, label="Amarillo (5–9)"),
                        Patch(color=ROJO, label="Rojo (10+)"),
                        Line2D([], [], color=TINTA_2, ls="--", label="Esperadas"),
                        Line2D([], [], marker="v", ls="", color=TINTA, label="Kupiec rechaza")],
               frameon=False, ncol=5, loc="upper left", fontsize=8)
fig.suptitle("Backtesting del VaR 99 % de la cartera", x=0.01, ha="left", fontsize=13, color=TINTA)
fig.tight_layout()
plt.show()
display(spark.createDataFrame(bt))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusiones
# MAGIC
# MAGIC **1. El pipeline concilia y es idempotente.** Los 8 controles de `pipeline_validacion` dan `true`: Silver
# MAGIC tiene una fila por moneda y fecha, todas las monedas de la cartera tienen precio, la grilla de retornos está
# MAGIC completa y el resultado de la cartera es igual a la suma por mesa y por grupo. Al reejecutar sin archivos
# MAGIC nuevos, `bronze_cotizaciones` no escribe filas.
# MAGIC
# MAGIC **2. Silver encontró problemas reales en los datos del BCRA.** La API repite fechas dentro de una misma
# MAGIC respuesta. En 7 fechas las copias traen valores distintos (el 06/09/2017 viene 5 veces y una copia duplica el
# MAGIC valor): esas 435 filas van a cuarentena, porque no hay forma de saber cuál es la correcta. Las copias idénticas
# MAGIC (como las 13 del 09/11/2012) quedan una sola vez. También van a cuarentena el sábado 16/06/2012 (72 filas) y el
# MAGIC oro publicado en 0 (2 filas). De 156.799 cotizaciones quedan 155.487 únicas, con 509 en cuarentena. Además hay 38 avisos: 27 de inconsistencia entre la cotización
# MAGIC en pesos y pase × dólar (entre ellos XDR en 2013, con 72,7 % de desvío) y 11 días en que la serie USD se
# MAGIC aparta más de 3 % de REF.
# MAGIC
# MAGIC **3. El cruce USD contra REF detectó un error de publicación.** En 10 de esos 11 días el desvío se explica
# MAGIC por la volatilidad (crisis de 2014, 2018, 2019 y 2025), porque USD es un valor puntual y REF un promedio del
# MAGIC día. El 07/12/2023 no: REF se movió 0,24 % y el USD saltó 5,7 % (385,00) para volver al día siguiente, y el
# MAGIC BCRA calculó con ese valor todas las cotizaciones en pesos. Valuar en pesos con REF corrige el error sin
# MAGIC descartar los días de crisis. Además bajó las excepciones en pesos de 74 a 67.
# MAGIC
# MAGIC **4. Riesgo actual (23/10/2026).** El VaR 99 % a un día es 1,38 millones de USD eq. en pesos y 0,30 en
# MAGIC dólares. En pesos, Tesorería concentra el riesgo (1,07) por su posición larga en dólares, y la
# MAGIC diversificación entre mesas es del 19 %. El ES 97,5 % en pesos (2,50) es 82 % mayor que el VaR: la ventana
# MAGIC incluye dos pérdidas extremas (13/10/2025 y 27/10/2025) que el VaR, como 3.ª peor pérdida, no refleja. En
# MAGIC dólares la diversificación sube al 30 %, y los metales (mesa Inversiones) están en zona amarilla.
# MAGIC
# MAGIC **5. Backtesting 2004–2026.** En pesos hubo 67 excepciones contra 56 esperadas (1,20 %): Kupiec no rechaza
# MAGIC el modelo en el agregado (LR 2,09). En dólares hubo 88 (1,57 %) y sí lo rechaza (LR 15,84). Los años en zona
# MAGIC amarilla o roja coinciden con crisis: 2008, 2015, 2018 y 2025, el único en zona roja (10). Kupiec también
# MAGIC rechaza los años sin excepciones (2006, 2012, 2020): ahí el modelo fue demasiado conservador.
# MAGIC
# MAGIC **6. El modelo falla igual en las dos salidas del cepo.** Con el dólar administrado, la ventana muestra un VaR
# MAGIC cercano a cero (0,21 en diciembre de 2015, 0,18 en abril de 2025). Cuando el peso empieza a flotar, cualquier
# MAGIC apreciación lo supera: hubo 6 excepciones entre el 18/12/2015 y el 22/03/2016, y 5 entre el 16/04/2025 y el
# MAGIC 08/05/2025, todas por la posición en USD. Después el modelo se recalibra solo: en 2026 no hubo excepciones.
# MAGIC Es la debilidad típica de la simulación histórica ante un cambio de régimen. Un *stressed VaR* o una
# MAGIC ponderación por volatilidad la mitigarían.
# MAGIC
# MAGIC **7. Las devaluaciones son ganancias para esta cartera.** Al ser larga en moneda extranjera, los saltos del
# MAGIC dólar aparecen como barras positivas fuera de escala (por ejemplo, +97,5 el 13/12/2023), no como excepciones.
# MAGIC El riesgo de pérdida es la apreciación del peso. Con el dólar administrado (2021–2024), las únicas excepciones
# MAGIC vinieron de monedas cruzadas (yuan, oro, real), igual que en agosto de 2015 con la devaluación del yuan.
