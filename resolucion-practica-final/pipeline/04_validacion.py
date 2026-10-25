"""
Validación end-to-end. pipeline_validacion calcula en una fila las métricas de conciliación entre
capas; cada control es expect_or_fail, así que si alguno no se cumple la actualización termina en
error en lugar de publicar resultados inconsistentes.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TOL = 1e-6

CONTROLES = {
    "bronze_tiene_archivos": "archivos_bronze > 0",
    "silver_sin_duplicados": "filas_silver = claves_silver",
    "cartera_con_cotizaciones": "monedas_cartera_sin_precio = 0",
    "retornos_grilla_completa": "filas_retornos = fechas_calendario * monedas_cartera",
    "pnl_cartera_igual_suma_mesas": f"max_dif_cartera_mesas <= {TOL}",
    "pnl_cartera_igual_suma_grupos": f"max_dif_cartera_grupos <= {TOL}",
    "var_calculado_en_ambas_perspectivas": "perspectivas_con_var = 2",
    # ES 97,5 % ≥ VaR 97,5 % siempre (promedio de las k peores ≤ k-ésima peor). Frente al VaR 99 %
    # no está garantizado, por eso se compara al mismo nivel de confianza.
    "es_mayor_o_igual_que_var_mismo_nivel": "filas_es_menor_que_var = 0",
}


@dp.materialized_view(
    name="pipeline_validacion",
    comment="Controles de conciliación entre Bronze, Silver y Gold.",
    table_properties={"quality": "gold"},
)
@dp.expect_all_or_fail({nombre: f"coalesce({nombre}, false)" for nombre in CONTROLES})
def pipeline_validacion():
    bronze = spark.read.table("bronze_cotizaciones").agg(F.count("*").alias("archivos_bronze"))
    silver = spark.read.table("silver_cotizaciones").agg(
        F.count("*").alias("filas_silver"),
        F.countDistinct("moneda", "fecha").alias("claves_silver"),
    )
    con_precio = spark.read.table("silver_cotizaciones").select("moneda").distinct()
    cartera = spark.read.table("silver_cartera").select("moneda").distinct()
    sin_precio = cartera.join(con_precio, "moneda", "left_anti").agg(F.count("*").alias("monedas_cartera_sin_precio"))

    ret = spark.read.table("gold_retornos").agg(
        F.count("*").alias("filas_retornos"),
        F.countDistinct("fecha").alias("fechas_calendario"),
        F.countDistinct("moneda").alias("monedas_cartera"),
    )

    var = spark.read.table("gold_var_diario")
    total = var.where("nivel = 'cartera'").select("fecha", "perspectiva", F.col("pnl_usd_mm").alias("pnl_total"))
    dif = lambda nivel: (  # noqa: E731
        var.where(f"nivel = '{nivel}'").groupBy("fecha", "perspectiva").agg(F.sum("pnl_usd_mm").alias("suma"))
        .join(total, ["fecha", "perspectiva"])
        .agg(F.coalesce(F.max(F.abs(F.col("suma") - F.col("pnl_total"))), F.lit(0.0)).alias(f"max_dif_cartera_{nivel}s"))
    )
    var_ok = var.agg(
        F.countDistinct(F.when(F.col("var_99_usd_mm").isNotNull(), F.col("perspectiva"))).alias("perspectivas_con_var"),
        F.sum(F.when(F.col("es_975_usd_mm") < F.col("var_975_usd_mm") - TOL, 1).otherwise(0)).alias("filas_es_menor_que_var"),
    )

    metricas = (bronze.crossJoin(silver).crossJoin(sin_precio).crossJoin(ret)
                .crossJoin(dif("mesa")).crossJoin(dif("grupo")).crossJoin(var_ok))
    for nombre, expresion in CONTROLES.items():
        metricas = metricas.withColumn(nombre, F.expr(expresion))
    return metricas.withColumn("validado_en", F.current_timestamp())
