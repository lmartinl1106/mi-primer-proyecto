# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Extracción de cotizaciones del BCRA a la zona de landing
# MAGIC
# MAGIC Primera tarea del Job. Llama a la API pública de Estadísticas Cambiarias v1.0 del BCRA y
# MAGIC guarda **la respuesta cruda** de cada llamada como un JSON en el volume de landing. No limpia
# MAGIC ni transforma: eso es trabajo de Silver.
# MAGIC
# MAGIC - Una llamada por moneda y por año calendario (`/Cotizaciones/{moneda}?fechadesde&fechahasta`),
# MAGIC   basado en el script `bcra_data.py` (sesión con reintentos y descarga en paralelo).
# MAGIC - **Incremental:** un año ya cerrado y descargado completo no se vuelve a pedir. El año en curso
# MAGIC   se relee entero en cada corrida, para capturar publicaciones tardías y revisiones; Silver
# MAGIC   conserva la versión más reciente de cada fecha y moneda.
# MAGIC - **Append-only:** cada archivo lleva el id de la corrida en el nombre, así que nunca se pisa
# MAGIC   un archivo y Auto Loader procesa cada uno una sola vez.
# MAGIC - **Falla visible:** si alguna llamada falla después de los reintentos, la tarea termina en error
# MAGIC   (el Job no sigue con datos incompletos). Lo descargado se conserva y la próxima corrida
# MAGIC   pide sólo lo que falta.

# COMMAND ----------

MONEDAS_CARTERA = [
    "USD", "EUR", "CNY", "CNH", "BRL",                          # principales
    "CLP", "UYU", "PYG", "BOB", "PEN", "COP", "MXP",            # Latam
    "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "SEK", "NOK", "DKK",  # G10
    "HKD", "SGD", "INR", "ZAR", "TRY",                          # Asia y otros
    "XAU", "XAG", "XDR",                                        # metales y DEG
    "REF",                                                      # control: dólar Com. A 3500
]

dbutils.widgets.text("catalog", "workspace", "01 - Catálogo")
dbutils.widgets.text("schema", "martin_lin_tp_fx", "02 - Esquema")
dbutils.widgets.text("volume", "landing", "03 - Volume de landing")
dbutils.widgets.text("monedas", ",".join(MONEDAS_CARTERA), "04 - Monedas")
dbutils.widgets.text("fecha_inicio", "2003-01-01", "05 - Fecha inicio")
dbutils.widgets.text("fecha_fin", "", "06 - Fecha fin (vacío = ayer)")
dbutils.widgets.text("verificar_ssl", "true", "07 - Verificar SSL (true / false / ruta a .pem)")
dbutils.widgets.text("max_workers", "4", "08 - Llamadas en paralelo")
dbutils.widgets.text("minutos_max", "20", "09 - Tiempo máximo (minutos)")

# COMMAND ----------

import json
import os
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

URL_BASE = "https://api.bcra.gob.ar/estadisticascambiarias/v1.0"
HEADERS = {"Accept-Language": "es-AR"}   # igual que bcra_data.py (User-Agent por defecto de requests)
PATRON_ARCHIVO = re.compile(r"^(?P<moneda>[A-Z]{3})_(?P<desde>\d{4}-\d{2}-\d{2})_(?P<hasta>\d{4}-\d{2}-\d{2})__(?P<run>[\w-]+)\.json$")

catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
volume = dbutils.widgets.get("volume").strip()
monedas = sorted({m.strip().upper() for m in dbutils.widgets.get("monedas").split(",") if m.strip()})
fecha_inicio = date.fromisoformat(dbutils.widgets.get("fecha_inicio").strip())
fecha_fin_txt = dbutils.widgets.get("fecha_fin").strip()
fecha_fin = date.fromisoformat(fecha_fin_txt) if fecha_fin_txt else date.today() - timedelta(days=1)
max_workers = int(dbutils.widgets.get("max_workers"))
minutos_max = float(dbutils.widgets.get("minutos_max"))
TIMEOUT = (10, 30)   # segundos para conectar y para recibir la respuesta

ssl_txt = dbutils.widgets.get("verificar_ssl").strip()
VERIFY = {"true": True, "false": False}.get(ssl_txt.lower(), ssl_txt)   # True, False o ruta a un bundle
if VERIFY is False:
    from urllib3.exceptions import InsecureRequestWarning
    warnings.simplefilter("ignore", InsecureRequestWarning)
    print("ATENCIÓN: verificación SSL desactivada. Documentarlo como limitación.")

assert fecha_inicio <= fecha_fin, "fecha_inicio debe ser anterior o igual a fecha_fin"
for nombre in (catalog, schema, volume):
    assert re.fullmatch(r"[A-Za-z0-9_]+", nombre), f"Nombre inválido: {nombre}"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")
LANDING = f"/Volumes/{catalog}/{schema}/{volume}/bcra"
RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
print(f"Landing: {LANDING} | corrida {RUN_ID} | {len(monedas)} monedas | {fecha_inicio} a {fecha_fin}")

# COMMAND ----------

# MAGIC %md ## Funciones (adaptadas de `bcra_data.py`)

# COMMAND ----------

def make_session(total_retries=6, backoff_factor=2.0, pool_maxsize=16):
    """Sesión con reintentos ante errores transitorios (igual que bcra_data.py, con más espera).

    Con estos valores se reintenta hasta 6 veces esperando 2, 4, 8, 16, 32 y 64 segundos:
    la API del BCRA a veces devuelve 502 durante uno o dos minutos y después se recupera.
    Los errores que no se arreglan esperando (por ejemplo, de certificado SSL) no se reintentan.
    """
    sess = requests.Session()
    retries = Retry(
        total=total_retries, connect=total_retries, read=total_retries, status=total_retries,
        other=0,                                  # SSL y otros errores: fallar enseguida
        respect_retry_after_header=False,         # usar siempre la espera propia, acotada
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries, pool_maxsize=pool_maxsize, pool_block=True)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess


def tramos_anuales(inicio: date, fin: date):
    """Años calendario entre inicio y fin, recortados a los extremos."""
    return [(max(inicio, date(y, 1, 1)), min(fin, date(y, 12, 31))) for y in range(inicio.year, fin.year + 1)]


def tramos_descargados(moneda: str) -> dict:
    """{desde: hasta máximo ya descargado} según los archivos presentes en landing."""
    carpeta = f"{LANDING}/cotizaciones/moneda={moneda}"
    cubiertos = {}
    if os.path.isdir(carpeta):
        for nombre in os.listdir(carpeta):
            m = PATRON_ARCHIVO.match(nombre)
            if m:
                desde, hasta = date.fromisoformat(m["desde"]), date.fromisoformat(m["hasta"])
                cubiertos[desde] = max(hasta, cubiertos.get(desde, hasta))
    return cubiertos


def descargar_tramo(session, moneda: str, desde: date, hasta: date) -> dict:
    """Pide un tramo, pagina si hiciera falta y guarda la respuesta cruda. Lanza excepción si falla."""
    url = f"{URL_BASE}/Cotizaciones/{moneda}"
    paginas, offset = [], 0
    while True:
        params = {"fechadesde": desde.isoformat(), "fechahasta": hasta.isoformat()}
        if offset:
            params["offset"] = offset
        r = session.get(url, params=params, timeout=TIMEOUT, verify=VERIFY)
        if r.status_code != 200:
            raise RuntimeError(f"{moneda} {desde}..{hasta}: HTTP {r.status_code} — {r.text[:300]}")
        cuerpo = r.json()
        paginas.append(cuerpo)
        resultset = (cuerpo.get("metadata") or {}).get("resultset") or {}
        recibidos = offset + len(cuerpo.get("results") or [])
        if not cuerpo.get("results") or recibidos >= int(resultset.get("count", 0)):
            break
        offset = recibidos

    registro = {
        "moneda": moneda,
        "fechadesde": desde.isoformat(),
        "fechahasta": hasta.isoformat(),
        "run_id": RUN_ID,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "url": url,
        "paginas": paginas,                       # respuesta cruda de la API, sin tocar
    }
    carpeta = f"{LANDING}/cotizaciones/moneda={moneda}"
    os.makedirs(carpeta, exist_ok=True)
    ruta = f"{carpeta}/{moneda}_{desde}_{hasta}__{RUN_ID}.json"
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(registro, f, ensure_ascii=False)
    filas = sum(len(p.get("results") or []) for p in paginas)
    return {"moneda": moneda, "desde": desde.isoformat(), "hasta": hasta.isoformat(), "fechas": filas, "archivo": ruta}

# COMMAND ----------

# MAGIC %md ## Plan de descarga: sólo lo que falta

# COMMAND ----------

plan = []
for moneda in monedas:
    cubiertos = tramos_descargados(moneda)
    for desde, hasta in tramos_anuales(fecha_inicio, fecha_fin):
        if cubiertos.get(desde, date.min) < hasta:
            plan.append((moneda, desde, hasta))
print(f"Tramos a descargar: {len(plan)}")

# COMMAND ----------

# MAGIC %md ## Chequeo previo: una sola llamada antes de lanzar todo
# MAGIC Si la API sigue sin responder después de los reintentos (unos 2 minutos) o hay un problema
# MAGIC de certificado, se detiene acá con un mensaje claro, en lugar de lanzar cientos de llamadas
# MAGIC que van a fallar igual. Un 502 aislado de Cloudflare se reintenta y no frena la corrida.

# COMMAND ----------

session = make_session(pool_maxsize=max_workers * 2)
hasta_prueba = fecha_fin
t0 = time.time()
try:
    r = session.get(f"{URL_BASE}/Cotizaciones/USD",
                    params={"fechadesde": (hasta_prueba - timedelta(days=10)).isoformat(),
                            "fechahasta": hasta_prueba.isoformat()},
                    timeout=TIMEOUT, verify=VERIFY)
except requests.exceptions.SSLError as e:
    raise RuntimeError(
        "Error de certificado SSL al conectar con la API del BCRA. Cambiá el widget "
        "'verificar_ssl' a false (y anotalo como limitación en el README).\n" + str(e)[:300]) from None
except requests.exceptions.RequestException as e:
    raise RuntimeError(f"La API del BCRA no responde ({type(e).__name__}). Probá más tarde.\n{str(e)[:300]}") from None
latencia = time.time() - t0
if r.status_code != 200:
    raise RuntimeError(f"La API del BCRA respondió HTTP {r.status_code} después de ~2 minutos de reintentos. "
                       f"Probá más tarde.\n{' '.join(r.text.split())[:200]}")
estimado = len(plan) / max_workers * max(latencia, 0.5) / 60
print(f"API OK: {latencia:.1f} s por llamada → estimado para {len(plan)} tramos: ~{estimado:.0f} min")

# COMMAND ----------

# MAGIC %md ## Descarga

# COMMAND ----------

ok, errores, pendientes = [], [], []
inicio = time.time()
limite = inicio + minutos_max * 60


def registrar(fut, tramo):
    moneda, desde, hasta = tramo
    try:
        ok.append(fut.result())
    except Exception as e:                        # se registra, no se silencia
        errores.append({"moneda": moneda, "desde": desde.isoformat(), "hasta": hasta.isoformat(), "error": str(e)[:500]})
        print(f"  ERROR {moneda} {desde.year}: {str(e)[:150]}")


with ThreadPoolExecutor(max_workers=max_workers) as ex:
    futuros = {ex.submit(descargar_tramo, session, *tramo): tramo for tramo in plan}
    for i, fut in enumerate(as_completed(futuros), 1):
        registrar(fut, futuros[fut])
        futuros[fut] = None                       # marcado como registrado
        if i % 25 == 0 or i == len(plan):
            transcurrido = time.time() - inicio
            restante = transcurrido / i * (len(plan) - i)
            print(f"  {i}/{len(plan)} tramos | {len(errores)} errores | "
                  f"{transcurrido / 60:.1f} min | faltan ~{restante / 60:.1f} min")
        if time.time() > limite:
            for f, tramo in futuros.items():
                if tramo is not None and f.cancel():   # sólo se cancelan los que no empezaron
                    pendientes.append(tramo)
                    futuros[f] = None
            print(f"Se alcanzó el tiempo máximo ({minutos_max:.0f} min). "
                  f"Quedan {len(pendientes)} tramos para la próxima corrida.")
            break
# Al salir del bloque terminan los tramos que estaban en curso: se registran también.
for f, tramo in futuros.items():
    if tramo is not None:
        registrar(f, tramo)

# Maestro de divisas: es información de referencia, no bloquea la corrida.
# Si falla se avisa y se usa el maestro de una corrida anterior.
maestro_ok = False
try:
    r = session.get(f"{URL_BASE}/Maestros/Divisas", timeout=TIMEOUT, verify=VERIFY)
    r.raise_for_status()
    os.makedirs(f"{LANDING}/maestro", exist_ok=True)
    with open(f"{LANDING}/maestro/divisas__{RUN_ID}.json", "w", encoding="utf-8") as f:
        json.dump({"run_id": RUN_ID, "extracted_at": datetime.now(timezone.utc).isoformat(),
                   "payload": r.json()}, f, ensure_ascii=False)
    maestro_ok = True
except Exception as e:
    print(f"AVISO: no se pudo descargar el maestro de divisas ({str(e)[:200]}). Se usará el de una corrida anterior.")

# COMMAND ----------

# MAGIC %md ## Resumen de la corrida

# COMMAND ----------

resumen = {
    "run_id": RUN_ID,
    "monedas": len(monedas),
    "tramos_planificados": len(plan),
    "tramos_ok": len(ok),
    "tramos_con_error": len(errores),
    "tramos_pendientes": len(pendientes),
    "minutos": round((time.time() - inicio) / 60, 1),
    "fechas_moneda_descargadas": sum(x["fechas"] for x in ok),
    "tramos_vacios": sum(1 for x in ok if x["fechas"] == 0),
    "maestro_descargado": maestro_ok,
}
os.makedirs(f"{LANDING}/_corridas", exist_ok=True)
with open(f"{LANDING}/_corridas/corrida__{RUN_ID}.json", "w", encoding="utf-8") as f:
    json.dump({**resumen, "errores": errores}, f, ensure_ascii=False, indent=1)

print(json.dumps(resumen, indent=1))
if ok:
    display(spark.createDataFrame(ok).groupBy("moneda").sum("fechas").orderBy("moneda"))
if errores:
    display(spark.createDataFrame(errores))
if errores or pendientes:
    raise RuntimeError(f"Corrida incompleta: {len(errores)} tramos con error y {len(pendientes)} pendientes. "
                       "Volvé a ejecutar: sólo se pedirán los faltantes.")
