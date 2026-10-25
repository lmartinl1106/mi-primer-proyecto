"""
Capa Silver — cotizaciones limpias, una fila por moneda y fecha.

Flujo:
  bronze_cotizaciones
    -> silver_cotizaciones_detalle  (vista: aplana páginas, resultados y detalle; tipa)
    -> silver_cotizaciones_chequeadas (vista: deriva precios y calcula quality_reason)
    -> silver_cotizaciones_validas  (privada: expect_all_or_drop + última versión por moneda y fecha)
    -> silver_cotizaciones          (publicada: precios, variación diaria y marca de salto)
    -> silver_cotizaciones_cuarentena (publicada: filas rechazadas con su motivo)

Cómo se leen los campos de la API (verificado con la API):
  tipoPase       = dólares por unidad de la moneda (para USD y REF viene 0)
  tipoCotizacion = pesos por unidad de la moneda  = tipoPase × cotización del USD
                   (para oro y plata viene 0 y hay que derivarlo)

Valuación en pesos: se usa REF (tipo de cambio de referencia Com. A 3500, el que usa la normativa
del BCRA). Como el BCRA calcula tipoCotizacion con su serie USD, se reescala:
  precio_ars = tipoCotizacion × REF / USD      (para USD da exactamente REF)
  precio_ars = tipoPase × REF                  (oro y plata, que traen tipoCotizacion = 0)
Si un día falta REF se usa la serie USD. Esto corrige además el error del 07/12/2023, cuando la
serie USD trae 385,00 con REF en 364,41 y todas las cotizaciones del día se calcularon con 385.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Tolerancia entre tipoCotizacion y tipoPase × USD. Con 0,5 % el guaraní (PYG) disparaba avisos
# por redondeo: el BCRA publica el pase con 6 decimales y 0,000155 USD tiene error relativo ~0,6 %.
TOLERANCIA_CONSISTENCIA = 0.01
# Aviso (no descarta) cuando la serie USD se aparta de REF más que esto. En días de crisis es
# normal (USD es un valor puntual y REF un promedio del día); en un día tranquilo indica un error.
# Descartar esos días borraría los movimientos más grandes de la historia y subestimaría el riesgo.
TOLERANCIA_USD_VS_REF = 0.03
UMBRAL_SALTO = 0.10               # variación diaria que se marca como salto (no se descarta)

# Orden = prioridad del motivo informado en la cuarentena. Se envuelven en coalesce(…, false)
# para que una regla que evalúa NULL cuente como incumplida y no pase inadvertida.
QUALITY_RULES = {nombre: f"coalesce(({regla}), false)" for nombre, regla in {
    "fecha_valida": "fecha IS NOT NULL",
    "moneda_coincide_con_pedido": "codigo_moneda = moneda_pedida",
    "fecha_dentro_del_tramo": "fecha BETWEEN tramo_desde AND tramo_hasta",
    "dia_habil": "dayofweek(fecha) NOT IN (1, 7)",
    "valores_numericos": "tipo_pase IS NOT NULL AND tipo_cotizacion IS NOT NULL",
    # La API repite fechas dentro de una misma respuesta y a veces con valores distintos (el
    # 06/09/2017 viene 5 veces y una copia duplica el valor). Si las copias no coinciden no hay forma
    # de saber cuál es la correcta: van a cuarentena y Gold usa el día anterior.
    "copias_de_la_fecha_coinciden": "copias_distintas = 1",
    "precio_ars_calculable": "precio_ars > 0",   # falla si es 0, o si hay que derivarlo y falta USD/REF del día
}.items()}

# Avisos: se registran en el event log pero no descartan filas.
AVISOS = {
    # Sólo aplica cuando vienen los dos valores (no a USD/REF, con pase 0, ni a oro/plata, con
    # cotización 0). try_divide evita el error por división por cero del modo ANSI.
    "cotizacion_consistente_con_pase": (
        f"coalesce(CASE WHEN tipo_pase > 0 AND tipo_cotizacion > 0 THEN "
        f"try_divide(abs(tipo_cotizacion - tipo_pase * usd_ars), tipo_cotizacion) <= {TOLERANCIA_CONSISTENCIA} "
        f"ELSE true END, true)"
    ),
    # Se evalúa sólo en la fila del USD para contar una vez por día.
    "dolar_consistente_con_ref": (
        f"coalesce(codigo_moneda <> 'USD' OR abs(try_divide(usd_ars, ref_ars) - 1) <= {TOLERANCIA_USD_VS_REF}, true)"
    ),
}


# ---------------------------------------------------------------- dimensiones
@dp.materialized_view(
    name="silver_divisas",
    comment="Divisas del BCRA según el maestro más reciente; una fila por código.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_drop("codigo_valido", "codigo RLIKE '^[A-Z]{3}$'")
def silver_divisas():
    ultimo = spark.read.table("bronze_divisas").agg(F.max("run_id").alias("run_id"))
    divisas = (
        spark.read.table("bronze_divisas").join(ultimo, "run_id")
        .select(F.explode("payload.results").alias("d"))
        .select(F.upper(F.trim("d.codigo")).alias("codigo"), F.trim("d.denominacion").alias("denominacion"))
    )
    # El maestro trae códigos repetidos (por ejemplo NIO dos veces): se conserva uno por código.
    w = Window.partitionBy("codigo").orderBy("denominacion")
    return divisas.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


@dp.materialized_view(
    name="silver_cartera",
    comment="Posiciones de la cartera hipotética tipadas (millones de USD equivalentes).",
    table_properties={"quality": "silver"},
)
@dp.expect_all_or_drop({
    "moneda_informada": "moneda IS NOT NULL",
    "posicion_numerica": "posicion_usd_mm IS NOT NULL",
    "moneda_existe_en_maestro": "moneda_en_maestro",
})
def silver_cartera():
    divisas = spark.read.table("silver_divisas").select(F.col("codigo").alias("moneda"), F.lit(True).alias("moneda_en_maestro"))
    return (
        spark.read.table("bronze_cartera")
        .select(
            F.upper(F.trim("moneda")).alias("moneda"),
            F.trim("grupo").alias("grupo"),
            F.trim("mesa").alias("mesa"),
            F.expr("try_cast(posicion_base_usd_mm AS DECIMAL(18,4))").alias("posicion_usd_mm"),
            F.trim("comentario").alias("comentario"),
        )
        .join(divisas, "moneda", "left")
        .withColumn("moneda_en_maestro", F.coalesce("moneda_en_maestro", F.lit(False)))
    )


# ---------------------------------------------------------------- cotizaciones
@dp.temporary_view(
    name="silver_cotizaciones_detalle",
    comment="Cotizaciones aplanadas y tipadas: una fila por moneda, fecha y archivo de origen.",
)
def silver_cotizaciones_detalle():
    return (
        spark.read.table("bronze_cotizaciones")
        .select("moneda", "fechadesde", "fechahasta", "run_id", "extracted_at", "_source_file",
                F.explode_outer("paginas").alias("p"))
        .select("*", F.explode_outer("p.results").alias("r"))
        .select("*", F.explode_outer("r.detalle").alias("d"))
        .where(F.col("r").isNotNull())            # tramos sin datos: no generan filas
        .select(
            F.col("moneda").alias("moneda_pedida"),
            F.expr("try_cast(fechadesde AS DATE)").alias("tramo_desde"),
            F.expr("try_cast(fechahasta AS DATE)").alias("tramo_hasta"),
            F.expr("try_cast(r.fecha AS DATE)").alias("fecha"),
            F.upper(F.trim("d.codigoMoneda")).alias("codigo_moneda"),
            F.trim("d.descripcion").alias("descripcion"),
            F.expr("try_cast(d.tipoPase AS DECIMAL(28,12))").alias("tipo_pase"),
            F.expr("try_cast(d.tipoCotizacion AS DECIMAL(28,12))").alias("tipo_cotizacion"),
            F.col("r.fecha").alias("fecha_original"),
            F.col("d.tipoPase").alias("tipo_pase_original"),
            F.col("d.tipoCotizacion").alias("tipo_cotizacion_original"),
            "run_id",
            F.expr("try_cast(extracted_at AS TIMESTAMP)").alias("extracted_at"),
            "_source_file",
        )
    )


@dp.temporary_view(
    name="silver_cotizaciones_chequeadas",
    comment="Agrega precios en USD y en pesos y el motivo de rechazo, si lo hay.",
)
def silver_cotizaciones_chequeadas():
    # Cuántos valores distintos trae la misma moneda y fecha dentro de una misma corrida.
    w_copias = Window.partitionBy("codigo_moneda", "fecha", "run_id")
    det = (
        spark.read.table("silver_cotizaciones_detalle")
        .withColumn("copias_distintas",
                    F.size(F.collect_set(F.struct("tipo_pase", "tipo_cotizacion")).over(w_copias)))
    )
    # Dólar y REF del día (última versión descargada, sin copias contradictorias), para valuar con REF.
    w_dia = Window.partitionBy("fecha").orderBy(F.col("extracted_at").desc())

    def serie_del_dia(codigo, alias):
        return (
            det.where(f"codigo_moneda = '{codigo}' AND moneda_pedida = '{codigo}' AND tipo_cotizacion > 0 "
                      f"AND copias_distintas = 1")
            .withColumn("_rn", F.row_number().over(w_dia)).where("_rn = 1")
            .select("fecha", F.col("tipo_cotizacion").alias(alias))
        )

    con_usd = det.join(serie_del_dia("USD", "usd_ars"), "fecha", "left").join(serie_del_dia("REF", "ref_ars"), "fecha", "left")
    precio_usd = (
        F.when(F.col("codigo_moneda") == "USD", F.lit(1).cast("decimal(28,12)"))
        .when(F.col("codigo_moneda") == "REF", F.lit(None).cast("decimal(28,12)"))
        .when(F.col("tipo_pase") > 0, F.col("tipo_pase"))
    )
    hay_ref = F.col("ref_ars").isNotNull() & F.col("usd_ars").isNotNull()
    precio_ars = (
        F.when(F.col("codigo_moneda") == "REF", F.col("tipo_cotizacion"))
        .when((F.col("tipo_cotizacion") > 0) & hay_ref,
              F.try_divide(F.col("tipo_cotizacion") * F.col("ref_ars"), F.col("usd_ars")))
        .when(F.col("tipo_cotizacion") > 0, F.col("tipo_cotizacion"))
        .when(F.col("tipo_pase") > 0, F.col("tipo_pase") * F.coalesce("ref_ars", "usd_ars"))
        .cast("decimal(28,12)")
    )
    origen = (
        F.when(F.col("codigo_moneda") == "REF", F.lit("ref"))
        .when((F.col("tipo_cotizacion") > 0) & hay_ref, F.lit("cotizacion_x_ref_sobre_usd"))
        .when(F.col("tipo_cotizacion") > 0, F.lit("cotizacion_sin_ref"))
        .when((F.col("tipo_pase") > 0) & F.col("ref_ars").isNotNull(), F.lit("pase_x_ref"))
        .when((F.col("tipo_pase") > 0) & F.col("usd_ars").isNotNull(), F.lit("pase_x_usd"))
    )
    reason = F.coalesce(*[F.when(~F.expr(regla), F.lit(nombre)) for nombre, regla in QUALITY_RULES.items()])
    return (
        con_usd.withColumn("precio_usd", precio_usd)
        .withColumn("precio_ars", precio_ars)
        .withColumn("origen_precio_ars", origen)
        .withColumn("quality_reason", reason)
    )


@dp.materialized_view(
    name="silver_cotizaciones_validas",
    comment="Paso interno: aplica las reglas (con métricas) y conserva la última versión de cada moneda y fecha.",
    private=True,
)
@dp.expect_all_or_drop(QUALITY_RULES)
@dp.expect_all(AVISOS)
def silver_cotizaciones_validas():
    chequeadas = spark.read.table("silver_cotizaciones_chequeadas")
    # Una misma fecha puede venir en varias corridas (el año en curso se relee siempre): gana la versión
    # descargada más recientemente. Las copias repetidas dentro de una corrida ya pasaron la regla
    # copias_de_la_fecha_coinciden, así que son idénticas y da igual cuál se conserva.
    w = Window.partitionBy("codigo_moneda", "fecha").orderBy(
        F.col("extracted_at").desc(), F.col("run_id").desc(),
        F.col("tipo_cotizacion").desc_nulls_last(), F.col("tipo_pase").desc_nulls_last())
    validas = (
        chequeadas.where("quality_reason IS NULL")
        .withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    )
    # Las inválidas pasan para que las expectations las cuenten y las descarten.
    return validas.unionByName(chequeadas.where("quality_reason IS NOT NULL"))


@dp.materialized_view(
    name="silver_cotizaciones",
    comment="Cotización diaria por moneda: precio en USD y en pesos, variación diaria y marca de salto.",
    table_properties={"quality": "silver"},
)
def silver_cotizaciones():
    v = spark.read.table("silver_cotizaciones_validas")
    w = Window.partitionBy("moneda").orderBy("fecha")
    return (
        v.select(
            F.col("codigo_moneda").alias("moneda"),
            "fecha",
            "descripcion",
            "tipo_pase",
            "tipo_cotizacion",
            "precio_usd",
            "precio_ars",
            "origen_precio_ars",
            (F.col("codigo_moneda") == "REF").alias("es_referencia"),
            "run_id",
            "extracted_at",
        )
        .withColumn("fecha_anterior", F.lag("fecha").over(w))
        .withColumn(
            "variacion_ars",
            (F.try_divide(F.col("precio_ars"), F.lag("precio_ars").over(w)) - 1).cast("decimal(18,8)"),
        )
        .withColumn("salto_anomalo", F.coalesce(F.abs("variacion_ars") > UMBRAL_SALTO, F.lit(False)))
        .withColumn("processed_at", F.current_timestamp())
    )


@dp.materialized_view(
    name="silver_cotizaciones_cuarentena",
    comment="Cotizaciones rechazadas con los valores originales y la primera regla que fallaron.",
    table_properties={"quality": "silver"},
)
def silver_cotizaciones_cuarentena():
    return (
        spark.read.table("silver_cotizaciones_chequeadas")
        .where("quality_reason IS NOT NULL")
        .select(
            "moneda_pedida", "codigo_moneda", "fecha_original", "tipo_pase_original",
            "tipo_cotizacion_original", "tramo_desde", "tramo_hasta", "run_id", "_source_file",
            "quality_reason", F.current_timestamp().alias("evaluated_at"),
        )
    )
