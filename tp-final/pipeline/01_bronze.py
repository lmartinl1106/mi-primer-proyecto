"""
Capa Bronze — copia fiel de lo que llegó a landing.

- bronze_cotizaciones: un registro por archivo JSON de la extracción (moneda × tramo × corrida),
  con la respuesta de la API anidada tal cual. Auto Loader recuerda qué archivos ya leyó, así que
  cada archivo entra una sola vez aunque el pipeline se ejecute muchas veces.
- bronze_divisas: maestro de divisas de cada corrida.
- bronze_cartera: posiciones de la cartera hipotética (CSV de referencia).

Los valores numéricos se guardan como texto: el tipado y la validación son trabajo de Silver.
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

LANDING = spark.conf.get("tp.landing_path").rstrip("/")      # …/landing/bcra
CARTERA = spark.conf.get("tp.cartera_path")                   # …/landing/referencia/cartera_base.csv


def _s(*nombres):
    return [StructField(n, StringType(), True) for n in nombres]


DETALLE = ArrayType(StructType(_s("codigoMoneda", "descripcion", "tipoPase", "tipoCotizacion")))
PAGINA = StructType(_s("status") + [
    StructField("metadata", StructType([StructField("resultset", StructType(_s("count", "offset", "limit")))])),
    StructField("results", ArrayType(StructType(_s("fecha") + [StructField("detalle", DETALLE)]))),
])
ESQUEMA_COTIZACIONES = StructType(
    _s("moneda", "fechadesde", "fechahasta", "run_id", "extracted_at", "url")
    + [StructField("paginas", ArrayType(PAGINA))]
)
ESQUEMA_MAESTRO = StructType(_s("run_id", "extracted_at") + [
    StructField("payload", StructType(_s("status") + [
        StructField("results", ArrayType(StructType(_s("codigo", "denominacion"))))
    ]))
])


def _auto_loader(carpeta, esquema):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.partitionColumns", "")      # ignorar las carpetas moneda=XXX
        .option("rescuedDataColumn", "_rescued_data")
        .schema(esquema)
        .load(f"{LANDING}/{carpeta}")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_cotizaciones",
    comment="Respuestas crudas de /Cotizaciones/{moneda}: un registro por archivo (moneda × año × corrida).",
    table_properties={"quality": "bronze"},
)
def bronze_cotizaciones():
    return _auto_loader("cotizaciones", ESQUEMA_COTIZACIONES)


@dp.table(
    name="bronze_divisas",
    comment="Maestro de divisas del BCRA, una versión por corrida de extracción.",
    table_properties={"quality": "bronze"},
)
def bronze_divisas():
    return _auto_loader("maestro", ESQUEMA_MAESTRO)


@dp.materialized_view(
    name="bronze_cartera",
    comment="Cartera hipotética: posición por moneda y mesa, en millones de USD equivalentes.",
    table_properties={"quality": "bronze"},
)
def bronze_cartera():
    return (
        spark.read.option("header", "true").option("inferSchema", "false").csv(CARTERA)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )
