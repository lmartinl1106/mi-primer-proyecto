"""
Capa Gold — riesgo de mercado de la cartera hipotética en moneda extranjera.

Dos perspectivas, con la misma lógica:
  ARS: cuánto cambia el valor de cada posición frente al peso (riesgo de la posición en moneda
       extranjera, lo que mira el regulador local). Dominada por el tipo de cambio USD/ARS.
  USD: cuánto cambia frente al dólar (riesgo cruzado entre monedas; el peso queda afuera).

Productos:
  gold_retornos           fecha × moneda: precio (con relleno hacia adelante) y retornos diarios
  gold_pnl_diario         fecha × perspectiva × posición (moneda y mesa): P&L hipotético
  gold_var_diario         fecha × perspectiva × nivel (cartera, mesa, grupo): VaR 99 %, ES 97,5 %,
                          excepción del backtesting y semáforo de Basilea
  gold_backtesting_anual  perspectiva × nivel × año: excepciones y test de Kupiec

Metodología: simulación histórica con cartera estática. Las posiciones están expresadas en
millones de USD equivalentes y se mantienen constantes (P&L hipotético), así que el P&L de cada
día es posición × retorno simple de la moneda. El VaR del día t usa los 250 P&L anteriores a t
(sin incluir t), y el P&L de t es el que se compara contra ese VaR en el backtesting.
"""
import math
from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

VENTANA = 250                                   # días hábiles de historia para el VaR
CONF_VAR, CONF_ES = 0.99, 0.975
K_VAR = math.ceil(VENTANA * (1 - CONF_VAR))     # 3: VaR 99 % = 3.ª peor pérdida de 250
K_ES = math.ceil(VENTANA * (1 - CONF_ES))       # 7: ES 97,5 % = promedio de las 7 peores
                                                #    (y VaR 97,5 % = 7.ª peor, para comparar a igual nivel)
KUPIEC_CRITICO = 3.841                          # chi-cuadrado 1 g.l., 95 %


@dp.materialized_view(
    name="gold_retornos",
    comment="Precio diario por moneda de la cartera sobre el calendario del USD, con relleno hacia adelante, y retornos simples.",
    table_properties={"quality": "gold"},
)
def gold_retornos():
    cot = (spark.read.table("silver_cotizaciones").where("NOT es_referencia")
           .select("moneda", "fecha", "precio_usd", "precio_ars"))
    calendario = cot.where("moneda = 'USD'").select("fecha").distinct()
    monedas = spark.read.table("silver_cartera").select("moneda").distinct()
    grilla = calendario.crossJoin(monedas).join(cot, ["fecha", "moneda"], "left")

    w = Window.partitionBy("moneda").orderBy("fecha")
    w_acum = w.rowsBetween(Window.unboundedPreceding, 0)
    con_relleno = (
        grilla
        .withColumn("precio_imputado", F.col("precio_ars").isNull())
        .withColumn("precio_usd", F.last("precio_usd", ignorenulls=True).over(w_acum))
        .withColumn("precio_ars", F.last("precio_ars", ignorenulls=True).over(w_acum))
    )
    retorno = lambda c: (F.try_divide(F.col(c), F.lag(c).over(w)) - 1).cast("double")  # noqa: E731
    return (
        con_relleno
        .withColumn("retorno_ars", retorno("precio_ars"))
        .withColumn("retorno_usd", retorno("precio_usd"))
        # Antes de que el BCRA publique una moneda no hay precio: el retorno queda nulo.
        .withColumn("sin_historia", F.col("precio_ars").isNull())
    )


@dp.materialized_view(
    name="gold_pnl_diario",
    comment="P&L hipotético diario por posición (moneda × mesa) y perspectiva, en millones de USD equivalentes.",
    table_properties={"quality": "gold"},
)
def gold_pnl_diario():
    ret = spark.read.table("gold_retornos")
    cartera = spark.read.table("silver_cartera").select("moneda", "mesa", "grupo", "posicion_usd_mm")
    base = ret.join(cartera, "moneda")
    perspectivas = [
        base.select("fecha", F.lit("ARS").alias("perspectiva"), "moneda", "mesa", "grupo",
                    "posicion_usd_mm", F.col("retorno_ars").alias("retorno")),
        base.select("fecha", F.lit("USD").alias("perspectiva"), "moneda", "mesa", "grupo",
                    "posicion_usd_mm", F.col("retorno_usd").alias("retorno")),
    ]
    return (
        reduce(lambda a, b: a.unionByName(b), perspectivas)
        .withColumn("retorno_disponible", F.col("retorno").isNotNull())
        .withColumn("pnl_usd_mm", F.col("posicion_usd_mm").cast("double") * F.coalesce("retorno", F.lit(0.0)))
    )


@dp.materialized_view(
    name="gold_var_diario",
    comment="VaR 99 % y ES 97,5 % por simulación histórica (250 días), con backtesting, por cartera, mesa y grupo.",
    table_properties={"quality": "gold"},
)
def gold_var_diario():
    pnl = spark.read.table("gold_pnl_diario")
    niveles = [("cartera", F.lit("Total")), ("mesa", F.col("mesa")), ("grupo", F.col("grupo"))]
    series = reduce(lambda a, b: a.unionByName(b), [
        pnl.groupBy("fecha", "perspectiva", seg.alias("segmento"))
        .agg(F.sum("pnl_usd_mm").alias("pnl_usd_mm"))
        .withColumn("nivel", F.lit(nivel))
        for nivel, seg in niveles
    ])

    clave = ["perspectiva", "nivel", "segmento"]
    w_hist = Window.partitionBy(*clave).orderBy("fecha").rowsBetween(-VENTANA, -1)
    w_bt = Window.partitionBy(*clave).orderBy("fecha").rowsBetween(-(VENTANA - 1), 0)

    hist = F.array_sort(F.collect_list("pnl_usd_mm").over(w_hist))   # de la peor a la mejor
    completo = F.size("_hist") == VENTANA
    suma_peores = F.aggregate(F.slice("_hist", 1, K_ES), F.lit(0.0), lambda acc, x: acc + x)

    v = (
        series.withColumn("_hist", hist)
        .withColumn("observaciones", F.size("_hist"))
        .withColumn("var_99_usd_mm", F.when(completo, -F.element_at("_hist", K_VAR)))
        .withColumn("var_975_usd_mm", F.when(completo, -F.element_at("_hist", K_ES)))
        .withColumn("es_975_usd_mm", F.when(completo, -suma_peores / K_ES))
        .withColumn("excepcion", F.when(completo, F.col("pnl_usd_mm") < -F.col("var_99_usd_mm")))
        .drop("_hist")
    )
    excepciones = F.sum(F.col("excepcion").cast("int")).over(w_bt)
    dias = F.count("var_99_usd_mm").over(w_bt)
    return (
        v.withColumn("excepciones_250d", excepciones)
        .withColumn("dias_backtesting_250d", dias)
        .withColumn(
            "semaforo_basilea",
            F.when(F.col("dias_backtesting_250d") < VENTANA, F.lit(None).cast("string"))
            .when(F.col("excepciones_250d") <= 4, "verde")
            .when(F.col("excepciones_250d") <= 9, "amarillo")
            .otherwise("rojo"),
        )
        .select("fecha", "perspectiva", "nivel", "segmento", "pnl_usd_mm", "observaciones",
                "var_99_usd_mm", "var_975_usd_mm", "es_975_usd_mm", "excepcion", "excepciones_250d",
                "dias_backtesting_250d", "semaforo_basilea")
    )


@dp.materialized_view(
    name="gold_backtesting_anual",
    comment="Backtesting por año: excepciones del VaR 99 % y test de cobertura incondicional de Kupiec.",
    table_properties={"quality": "gold"},
)
def gold_backtesting_anual():
    p = 1 - CONF_VAR
    anual = (
        spark.read.table("gold_var_diario").where("var_99_usd_mm IS NOT NULL")
        .groupBy("perspectiva", "nivel", "segmento", F.year("fecha").alias("anio"))
        .agg(F.count("*").alias("dias"),
             F.sum(F.col("excepcion").cast("int")).alias("excepciones"),
             F.avg("var_99_usd_mm").alias("var_99_promedio"),
             F.min("pnl_usd_mm").alias("peor_pnl"))
    )
    n, x = F.col("dias").cast("double"), F.col("excepciones").cast("double")
    phat = x / n
    # Convención 0 · ln(0) = 0 para los casos sin excepciones o con todas excepciones.
    xlog = lambda k, q: F.when(k == 0, F.lit(0.0)).otherwise(k * F.log(q))  # noqa: E731
    lr = -2 * ((n - x) * math.log(1 - p) + x * math.log(p)
               - (xlog(n - x, 1 - phat) + xlog(x, phat)))
    return (
        anual.withColumn("tasa_excepciones", phat)
        .withColumn("excepciones_esperadas", n * p)
        .withColumn("kupiec_lr", lr)
        .withColumn("kupiec_rechaza_modelo", F.col("kupiec_lr") > KUPIEC_CRITICO)
    )
