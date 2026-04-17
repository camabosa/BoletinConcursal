import os
import time
from datetime import datetime, timedelta
import pandas as pd
import oracledb
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import chromedriver_autoinstaller
oracledb.init_oracle_client() 

# -------------------------
# Variables
# -------------------------
path_descarga = os.path.join(os.environ["USERPROFILE"], "Downloads", "registro_publicaciones_full.csv")
batch_size = 50000
procesar = 'S'
fecha_ayer = datetime.now() - timedelta(days=12)
ayer = datetime.strptime(fecha_ayer.strftime('%d-%m-%Y'), '%d-%m-%Y')

# --------------------------------------------------------------------------
# ChromeDriver automático  CBS 03-11-2025
# debe ejecutar en su estacion previamente import chromedriver_autoinstaller
# --------------------------------------------------------------------------
chromedriver_path = chromedriver_autoinstaller.install()  # devuelve el path instalado

# Configurar opciones de Chrome
chrome_options = Options()
chrome_options.add_argument("--start-maximized")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": os.path.join(os.environ["USERPROFILE"], "Downloads"),
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
})

# Configuración Selenium silenciosa
chrome_options.add_argument("--disable-logging")
chrome_options.add_argument("--log-level=3")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-infobars")
chrome_options.add_argument("--disable-extensions")
# chrome_options.add_argument("--headless")  # si quieres que no abra ventana

# Crear el servicio de Chrome con el path correcto
service = Service(chromedriver_path)
service.creationflags = 0x08000000  # Oculta consola negra de ChromeDriver en Windows

# Crear instancia del navegador
driver = webdriver.Chrome(service=service, options=chrome_options)

# Inicio
inicio = datetime.today()
print("Inicio:", inicio)

# -------------------------
# Borrar archivo previo
# -------------------------
try:
    os.remove(path_descarga)
    print("Archivo eliminado de carpeta de descarga...")
except FileNotFoundError:
    print("Archivo no existente en la carpeta descarga")

# -------------------------
# Descargar CSV desde la web
# -------------------------
url = 'http://www.boletinconcursal.cl/boletin/procedimientos'
driver.get(url)
time.sleep(2)
driver.find_element("xpath", '//*[@id="btnRegistroCsv"]').click()
time.sleep(15)
driver.quit()
print("Archivo descargado:", path_descarga)

# -------------------------
# Cargar CSV en DataFrame
# -------------------------
df = pd.read_csv(path_descarga, sep=',', encoding='UTF-8')
df["Fecha Publicación"] = pd.to_datetime(df['Fecha Publicación'], format='%d/%m/%Y')
print("Última fecha:", df[['Fecha Publicación']].max())

if df.shape[0] <= 0:
    procesar = 'N'

# -------------------------
# Filtrado y carga a Oracle
# -------------------------
if procesar == 'S':
    df_carga = pd.concat([
        df[df['Nombre Publicación'] == 'Resolución de Liquidación'],
        df[df['Nombre Publicación'] == 'Publicación resolución de Reorganización'],
        df[df['Nombre Publicación'] == 'Resolución de Término del Procedimiento']
    ])
    df_carga.index = range(df_carga.shape[0])
    df_carga.fillna('', inplace=True)

    # Conexión Oracle
    cnxn = oracledb.connect(user='cobranzas', password='report', dsn='BODEGA')
    cursor = cnxn.cursor()
    cursor.execute("TRUNCATE TABLE cobranzas.insol_carga_boletin")

    # Inserción por lotes
    ciclos = -(-df_carga.shape[0] // batch_size)
    for x in range(ciclos):
        print(x+1, "-", ciclos)
        ini = x * batch_size
        fin = ini + batch_size
        carga = df_carga.iloc[ini:fin].fillna('')
        rows = [tuple(row) for row in carga.values]
        SQL = ('INSERT INTO cobranzas.insol_carga_boletin '
               '(rol, procedimiento_concursal, deudor, rut, veedor_liquidador_titular, nombre_publicacion, tribunal, fecha_boletin) '
               'VALUES (:0, :1, :2, :3, :4, :5, :6, :7)')
        cursor.executemany(SQL, rows)
        print('Registros insertados en insol_carga_boletin:', cursor.rowcount)
        cnxn.commit()
        time.sleep(1)

    # Actualización tabla final
    cursor.execute("""
        DELETE FROM cobranzas.insol_boletin_concursal
        WHERE EXISTS (
            SELECT 1 FROM cobranzas.insol_carga_boletin
            WHERE insol_boletin_concursal.fecha_boletin = insol_carga_boletin.fecha_boletin
        )
    """)
    cnxn.commit()

    SQL_final = """
        INSERT INTO cobranzas.insol_boletin_concursal
        SELECT DISTINCT a.fecha_boletin, a.tribunal, a.rol, a.procedimiento_concursal,
               a.deudor, a.rut, a.veedor_liquidador_titular, a.nombre_publicacion,
               TO_NUMBER(SUBSTR(a.rut,1,INSTR(a.rut,'-')-1)) AS rut_deudor,
               SUBSTR(a.rut,INSTR(a.rut,'-')+1,1) AS dv_deudor
        FROM cobranzas.insol_carga_boletin a
        WHERE LOWER(a.nombre_publicacion) IN
              ('resolución de liquidación','publicación resolución de reorganización','resolución de término del procedimiento')
        AND NOT EXISTS (
            SELECT 1 FROM cobranzas.insol_boletin_concursal b
            WHERE a.fecha_boletin=b.fecha_boletin
              AND a.tribunal=b.tribunal
              AND a.rol=b.rol
              AND a.procedimiento_concursal=b.procedimiento_concursal
              AND a.deudor=b.deudor
              AND a.rut=b.rut
              AND a.veedor_liquidador_titular=b.veedor_liquidador_titular
              AND a.nombre_publicacion=b.nombre_publicacion
        )
    """
    cursor.execute(SQL_final)
    print('Registros insertados en insol_boletin_concursal:', cursor.rowcount)
    cnxn.commit()
    cnxn.close()
else:
    print("No se procesó el archivo, nro. registros en DF:", df.shape[0])

# Fin del proceso
final = datetime.today()
print("Final:", final)
diff = final - inicio
print("Tiempo transcurrido:", diff.seconds / 60, "minutos")
