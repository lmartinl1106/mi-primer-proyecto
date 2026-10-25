"""
Prueba local del pipeline con PySpark 4.x y un módulo `pyspark.pipelines` simulado.

Uso:  pip install pyspark==4.0.1  &&  python tests/prueba_local.py
Genera archivos con el mismo formato que deja 00_extraccion_bcra (dos corridas) y siembra
problemas conocidos para verificar cada regla. Auto Loader se reemplaza por una lectura batch.
No reemplaza la ejecución en Databricks.
"""
import json
import os
import shutil
import sys
import tempfile
import types
from datetime import date, timedelta

from pyspark.sql import SparkSession, functions as F

PIPE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "pipeline")
spark = (SparkSession.builder.master("local[2]").appName("tp-fx")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.ansi.enabled", "true")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ------------------------------------------------------------------ simulador de pyspark.pipelines
REG, ORDER = {}, []


def _register(kind):
    def factory(name=None, comment=None, private=False, table_properties=None, **_):
        def deco(fn):
            REG[name or fn.__name__] = dict(fn=fn, exps=getattr(fn, "_exps", []), kind=kind)
            ORDER.append(name or fn.__name__)
            return fn
        return deco
    return factory


def _exp(action, many):
    def factory(*args):
        rules = args[0] if many else {args[0]: args[1]}
        def deco(fn):
            fn._exps = getattr(fn, "_exps", []) + [(action, rules)]
            return fn
        return deco
    return factory


dp = types.ModuleType("pyspark.pipelines")
dp.table, dp.materialized_view, dp.temporary_view = _register("st"), _register("mv"), _register("view")
for action in ["warn", "drop", "fail"]:
    suf = "" if action == "warn" else f"_or_{action}"
    setattr(dp, f"expect{suf}", _exp(action, False))
    setattr(dp, f"expect_all{suf}", _exp(action, True))
sys.modules["pyspark.pipelines"] = dp
import pyspark  # noqa: E402
pyspark.pipelines = dp

# ------------------------------------------------------------------ datos de prueba
BASE = tempfile.mkdtemp()
LANDING = os.path.join(BASE, "bcra")


def dias_habiles(a, b):
    return [a + timedelta(i) for i in range((b - a).days + 1) if (a + timedelta(i)).weekday() < 5]


import random  # noqa: E402

random.seed(7)
DIAS = dias_habiles(date(2024, 1, 1), date(2026, 4, 3))
_usd, _pase, nivel = {}, {}, 1000.0
for d in DIAS:                                            # USD/ARS: ruido diario + devaluación del 40 %
    nivel *= (1.4 if d == date(2025, 4, 14) else 1 + random.gauss(0.0005, 0.006))
    _usd[d] = round(nivel, 2)
for m, p0, vol in [("EUR", 1.10, 0.006), ("CNY", 0.14, 0.003), ("XAU", 2000.0, 0.01), ("JPY", 0.0065, 0.007)]:
    p = p0
    for d in DIAS:
        p *= 1 + random.gauss(0, vol)
        _pase[(m, d)] = round(p, 8)


def usd(d):
    return _usd.get(d, 1000.0)


def pase(m, d):
    return _pase.get((m, d), 1.0)


DIA_USD_ERRONEO = date(2025, 7, 7)   # como el 07/12/2023 real: USD +6 % y todas las cotizaciones calculadas con él


def detalle(m, d):
    u = usd(d) * (1.06 if d == DIA_USD_ERRONEO else 1)
    if m == "USD":
        return {"codigoMoneda": "USD", "descripcion": "DOLAR", "tipoPase": 0.0, "tipoCotizacion": round(u, 2)}
    if m == "REF":
        return {"codigoMoneda": "REF", "descripcion": "REF A3500", "tipoPase": 0.0, "tipoCotizacion": round(usd(d) - 2, 4)}
    if m == "XAU":
        return {"codigoMoneda": "XAU", "descripcion": "ORO", "tipoPase": pase(m, d), "tipoCotizacion": 0.0}
    return {"codigoMoneda": m, "descripcion": m, "tipoPase": pase(m, d), "tipoCotizacion": round(pase(m, d) * u, 6)}


def escribir(run, m, a, b, filas):
    carpeta = os.path.join(LANDING, "cotizaciones", f"moneda={m}")
    os.makedirs(carpeta, exist_ok=True)
    pagina = {"status": 200, "metadata": {"resultset": {"count": len(filas), "offset": 0, "limit": 1000}},
              "results": [{"fecha": f, "detalle": [d]} for f, d in sorted(filas, key=lambda x: x[0], reverse=True)]}
    reg = {"moneda": m, "fechadesde": a.isoformat(), "fechahasta": b.isoformat(), "run_id": run,
           "extracted_at": f"{run[:4]}-{run[4:6]}-{run[6:8]}T{run[9:11]}:{run[11:13]}:00+00:00",
           "url": "x", "paginas": [pagina]}
    with open(os.path.join(carpeta, f"{m}_{a}_{b}__{run}.json"), "w") as f:
        json.dump(reg, f)


R1, R2 = "20261024T233916Z", "20261025T120000Z"
A25, B25, A26, B26a, B26b = date(2025, 1, 1), date(2025, 12, 31), date(2026, 1, 1), date(2026, 3, 31), date(2026, 4, 3)
for m in ["USD", "EUR", "CNY", "XAU", "JPY", "REF"]:
    for run, a, b in [(R1, date(2024, 1, 1), date(2024, 12, 31)), (R1, A25, B25), (R1, A26, B26a), (R2, A26, B26b)]:
        filas = [(d.isoformat(), detalle(m, d)) for d in dias_habiles(a, b)]
        if m == "EUR" and run == R1 and a == A25:
            filas.append(("2025-03-08", detalle("EUR", date(2025, 3, 8))))           # sábado
            filas.append((filas[20][0], {**filas[20][1], "tipoCotizacion": filas[20][1]["tipoCotizacion"] + 0.5}))  # fecha repetida en la misma respuesta
            filas[10][1]["tipoCotizacion"] = filas[10][1]["tipoCotizacion"] * 1.2   # inconsistente con pase
        if m == "CNY" and run == R1 and a == A25:
            filas[5] = (filas[5][0], {**filas[5][1], "codigoMoneda": "CNH"})        # moneda distinta a la pedida
            filas.append(filas[30])                                                     # fecha repetida, copias idénticas
            filas.append(("2024-12-31", detalle("CNY", date(2024, 12, 31))))       # fuera del tramo
        if m == "JPY" and run == R1 and a == A25:
            filas[7] = (filas[7][0], {**filas[7][1], "tipoCotizacion": None})       # valor faltante
            filas[200] = (filas[200][0], {**filas[200][1], "tipoCotizacion": filas[200][1]["tipoCotizacion"] * 1.5,
                                          "tipoPase": filas[200][1]["tipoPase"] * 1.5})  # salto del 50 %
        if m == "USD" and run == R1 and a == A25:
            filas = [f for f in filas if f[0] != "2025-06-02"]                     # USD falta → XAU no se puede derivar
        if m == "USD" and run == R2:
            filas = [(f, {**dd, "tipoCotizacion": dd["tipoCotizacion"] + 0.5}) if f == "2026-03-31" else (f, dd)
                     for f, dd in filas]                                              # revisión en la 2.ª corrida
        escribir(run, m, a, b, filas)
escribir(R1, "CNY", date(2003, 1, 1), date(2003, 12, 31), [])                         # tramo vacío

os.makedirs(os.path.join(LANDING, "maestro"))
with open(os.path.join(LANDING, "maestro", f"divisas__{R1}.json"), "w") as f:
    json.dump({"run_id": R1, "extracted_at": "x", "payload": {"status": 200, "results": [
        {"codigo": c, "denominacion": c} for c in ["USD", "EUR", "CNY", "XAU", "JPY", "REF", "NIO", "NIO"]]}}, f)
CARTERA = os.path.join(BASE, "cartera_base.csv")
with open(CARTERA, "w") as f:
    f.write("moneda,grupo,mesa,posicion_base_usd_mm,comentario\n"
            "USD,Principal,Tesoreria,45.0,\nEUR,Principal,Tesoreria,6.0,\nCNY,Principal,Tesoreria,8.0,\n"
            "XAU,Metales,Inversiones,3.0,\nJPY,G10,Tesoreria,-2.0,\nZZZ,Otros,Tesoreria,1.0,moneda inexistente\n"
            "EUR,Principal,Comercio Exterior,abc,posicion invalida\n")

spark.conf.set("tp.landing_path", LANDING)
spark.conf.set("tp.cartera_path", CARTERA)

# ------------------------------------------------------------------ carga de los archivos del pipeline
for nombre in sorted(os.listdir(PIPE)):
    if nombre.endswith(".py"):
        exec(compile(open(os.path.join(PIPE, nombre)).read(), nombre, "exec"), {"spark": spark, "__name__": nombre})


def falso_auto_loader(carpeta, esquema):
    def leer():
        return (spark.read.option("recursiveFileLookup", "true").schema(esquema).json(os.path.join(LANDING, carpeta))
                .withColumn("_rescued_data", F.lit(None).cast("string"))
                .withColumn("_source_file", F.col("_metadata.file_path"))
                .withColumn("_ingested_at", F.current_timestamp()))
    return leer


_bronze_globals = REG["bronze_cotizaciones"]["fn"].__globals__
REG["bronze_cotizaciones"]["fn"] = falso_auto_loader("cotizaciones", _bronze_globals["ESQUEMA_COTIZACIONES"])
REG["bronze_divisas"]["fn"] = falso_auto_loader("maestro", _bronze_globals["ESQUEMA_MAESTRO"])

# ------------------------------------------------------------------ ejecución
METRICAS = []


def ejecutar():
    METRICAS.clear()
    for nombre in ORDER:
        spec = REG[nombre]
        df = spec["fn"]()
        for action, rules in spec["exps"]:
            for rnombre, expr in rules.items():
                fallan = df.where(f"NOT coalesce(({expr}), false)").count()
                METRICAS.append((nombre, rnombre, action, df.count() - fallan, fallan))
                assert not (action == "fail" and fallan), f"expect_or_fail {nombre}.{rnombre}"
            if action == "drop":
                df = df.where(" AND ".join(f"coalesce(({e}), false)" for e in rules.values()))
        df = df.localCheckpoint()
        df.createOrReplaceTempView(nombre)
    return {n: spark.table(n) for n in ORDER}


if __name__ == "__main__":
    t = ejecutar()
    print("Orden:", " → ".join(ORDER))
    for n in ORDER:
        print(f"  {n:34s} {t[n].count():>6}")
    print("\nExpectations con fallas (dataset, regla, acción, pasan, fallan):")
    for m in METRICAS:
        if m[4]:
            print("  ", m)
    print("\nCuarentena:")
    t["silver_cotizaciones_cuarentena"].groupBy("quality_reason", "moneda_pedida").count().orderBy("quality_reason").show(truncate=False)
    s = t["silver_cotizaciones"]
    dup = s.groupBy("moneda", "fecha").count().where("count > 1").count()
    print("Duplicados moneda×fecha en Silver:", dup)
    print("Revisión de la 2.ª corrida (USD 2026-03-31):")
    s.where("moneda = 'USD' AND fecha = '2026-03-31'").select("precio_ars", "run_id").show()
    print("Oro derivado (primeras filas):")
    s.where("moneda = 'XAU'").orderBy("fecha").select("fecha", "tipo_pase", "tipo_cotizacion", "precio_usd", "precio_ars", "origen_precio_ars").show(3)
    print("Saltos marcados:")
    s.where("salto_anomalo").select("moneda", "fecha", "variacion_ars").show()
    print("Fechas por moneda en Silver:")
    s.groupBy("moneda").agg(F.count("*").alias("fechas"), F.min("fecha"), F.max("fecha")).orderBy("moneda").show()
    t["silver_cartera"].show()
    t["silver_divisas"].orderBy("codigo").show()

    print("Día con USD erróneo: precios en pesos reescalados a REF y retornos sin salto:")
    t["gold_retornos"].where(F.col("fecha").between("2025-07-04", "2025-07-08")).orderBy("moneda", "fecha").select(
        "moneda", "fecha", F.round("precio_ars", 4).alias("precio_ars"), F.round("retorno_ars", 5).alias("retorno_ars")).show(20)
    s.where("fecha = '2025-07-07'").select("moneda", "origen_precio_ars").show()

    print("Gold — retornos imputados o sin historia:")
    t["gold_retornos"].groupBy("moneda").agg(F.sum(F.col("precio_imputado").cast("int")).alias("imputados"),
                                             F.sum(F.col("sin_historia").cast("int")).alias("sin_historia")).orderBy("moneda").show()
    print("Gold — VaR de la cartera, últimos días:")
    v = t["gold_var_diario"]
    v.where("nivel = 'cartera'").orderBy(F.desc("fecha"), "perspectiva").select(
        "fecha", "perspectiva", F.round("pnl_usd_mm", 4).alias("pnl"), F.round("var_99_usd_mm", 4).alias("var99"),
        F.round("es_975_usd_mm", 4).alias("es975"), "excepcion", "excepciones_250d", "semaforo_basilea").show(4)
    print("Gold — días de la devaluación (2025-04-14) en la cartera:")
    v.where("nivel = 'cartera' AND fecha = '2025-04-14'").select("perspectiva", F.round("pnl_usd_mm", 3).alias("pnl"),
                                                                 "var_99_usd_mm", "excepcion").show()
    print("Gold — backtesting anual (cartera):")
    t["gold_backtesting_anual"].where("nivel = 'cartera'").orderBy("perspectiva", "anio").select(
        "perspectiva", "anio", "dias", "excepciones", F.round("excepciones_esperadas", 2).alias("esperadas"),
        F.round("kupiec_lr", 3).alias("kupiec_lr"), "kupiec_rechaza_modelo").show()

    # Comprobación independiente del VaR: recalcular con Python puro para una fecha.
    import pandas as pd  # noqa: E402
    serie = v.where("nivel = 'cartera' AND perspectiva = 'ARS'").orderBy("fecha").select("fecha", "pnl_usd_mm", "var_99_usd_mm", "es_975_usd_mm").toPandas()
    i = len(serie) - 1
    previos = sorted(serie["pnl_usd_mm"].iloc[i - 250:i])
    print(f"Chequeo VaR ARS {serie['fecha'].iloc[i]}: pipeline={serie['var_99_usd_mm'].iloc[i]:.6f}  python={-previos[2]:.6f} | "
          f"ES pipeline={serie['es_975_usd_mm'].iloc[i]:.6f}  python={-sum(previos[:7]) / 7:.6f}")

    print("\nValidación:")
    t["pipeline_validacion"].drop("validado_en").show(vertical=True, truncate=False)
    shutil.rmtree(BASE, ignore_errors=True)
