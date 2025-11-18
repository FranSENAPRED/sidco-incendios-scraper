from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import os
import re

SIDCO_BASE = "https://sidco.conaf.cl"

# >>> USAR VARIABLES DE ENTORNO (más seguro para GitHub)
USUARIO = os.getenv("SIDCO_USER")
PASSWORD = os.getenv("SIDCO_PASS")

# ---------------------------------------------------------------------
# LOGIN Y NAVEGACIÓN PRINCIPAL
# ---------------------------------------------------------------------
def scrapear_incendios_y_fichas() -> pd.DataFrame:
    """
    Inicia sesión en SIDCO, lee la tabla de incendios vigentes
    y enriquece cada fila con datos de la ficha detallada.
    """
    if not USUARIO or not PASSWORD:
        raise RuntimeError("Faltan variables de entorno SIDCO_USER y/o SIDCO_PASS")

    with sync_playwright() as p:
        # En GitHub/servidor SIEMPRE headless=True
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # 1) Ir al login
        page.goto(f"{SIDCO_BASE}/login/index.php", timeout=120_000)
        page.wait_for_timeout(3000)  # 3 segundos de espera “pasiva”

        # 2) Esperar campos y llenar credenciales
        page.wait_for_selector('input[name="username"]')
        page.fill('input[name="username"]', USUARIO)
        page.fill('input[name="password"]', PASSWORD)

        # 3) Hacer click en el botón "Iniciar sesión"
        page.click('#div_btn_0-button')

        # 4) Esperar que se cargue la sesión
        page.wait_for_load_state("networkidle")

        # 5) Ir a la página principal de incendios vigentes
        page.goto(f"{SIDCO_BASE}/principal.php", wait_until="networkidle")

        # (Opcional) Screenshot de debug
        # page.screenshot(path="sidco_principal.png", full_page=True)

        # 6) Parsear tabla principal
        html_main = page.content()
        df = parsear_tabla_incendios(html_main)

        # 7) Recorrer fichas individuales y enriquecer el DataFrame
        for idx, row in df.iterrows():
            url_ficha = row.get("url_ficha")
            if not url_ficha:
                continue

            print(f"Procesando ficha {idx+1}/{len(df)}: {url_ficha}")
            page.goto(url_ficha, wait_until="networkidle")
            ficha_html = page.content()
            datos_ficha = parsear_ficha_incendio(ficha_html)

            # Añadir/actualizar columnas en el DF
            for col, val in datos_ficha.items():
                df.at[idx, col] = val

        browser.close()

    return df


# ---------------------------------------------------------------------
# PARSEO TABLA PRINCIPAL "Incendios forestales vigentes"
# ---------------------------------------------------------------------
def parsear_tabla_incendios(html_page: str) -> pd.DataFrame:
    soup = BeautifulSoup(html_page, "lxml")

    # Buscar el título
    h1 = soup.find("h1", string=lambda t: t and "Incendios forestales vigentes" in t)
    if not h1:
        raise ValueError("No se encontró el título 'Incendios forestales vigentes'")

    # Hay más de una tabla .tabla; buscamos la que tenga encabezados 'Fecha' y 'Región'
    tablas = h1.find_all_next("table", class_="tabla")

    tabla_incendios = None
    for t in tablas:
        thead = t.find("thead")
        if not thead:
            continue

        header_cells = [c.get_text(strip=True) for c in thead.find_all(["td", "th"])]
        if "Fecha" in header_cells and "Región" in header_cells:
            tabla_incendios = t
            break

    if tabla_incendios is None:
        raise ValueError("No se encontró la tabla de incendios (encabezados Fecha/Región).")

    tbody = tabla_incendios.find("tbody")
    if not tbody:
        raise ValueError("La tabla de incendios no tiene <tbody>.")

    filas = []

    for tr in tbody.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 12:
            continue

        # === ALERTA (columna 0) ===
        alerta_span = tds[0].find("span")
        if alerta_span:
            alerta_titulo = alerta_span.get("title", "")
            alerta_codigo = next(
                (
                    clase
                    for clase in alerta_span.get("class", [])
                    if clase.startswith("incendio-estado-alerta-")
                ),
                None,
            )
        else:
            alerta_titulo = ""
            alerta_codigo = ""

        # === Datos principales ===
        fecha_raw = tds[1].get_text(strip=True)
        region = tds[2].get_text(strip=True)
        nombre = tds[3].get_text(strip=True)
        ambito = tds[4].get_text(strip=True)
        comuna = tds[5].get_text(strip=True)
        estado = tds[6].get_text(" ", strip=True)

        superficie_txt = (
            tds[7]
            .get_text(strip=True)
            .replace(".", "")   # separador de miles
            .replace(",", ".")  # coma decimal
        )
        superficie = float(superficie_txt) if superficie_txt else None

        # Links
        td_poligono = tds[8]
        a_descargar = td_poligono.find("a")
        url_poligono = (
            SIDCO_BASE + a_descargar["href"]
            if a_descargar and "href" in a_descargar.attrs
            else None
        )

        td_ficha = tds[9]
        a_ficha = td_ficha.find("a")
        url_ficha = (
            SIDCO_BASE + a_ficha["href"]
            if a_ficha and "href" in a_ficha.attrs
            else None
        )

        td_archivos = tds[11]
        a_archivos = td_archivos.find("a")
        url_archivos = (
            SIDCO_BASE + a_archivos["href"]
            if a_archivos and "href" in a_archivos.attrs
            else None
        )

        # Parseo de fecha "16-nov-2025 18:36"
        fecha_dt = None
        if fecha_raw:
            try:
                fecha_dt = datetime.strptime(fecha_raw, "%d-%b-%Y %H:%M")
            except ValueError:
                fecha_dt = None

        filas.append(
            {
                "alerta_titulo": alerta_titulo,
                "alerta_codigo": alerta_codigo,
                "fecha_raw": fecha_raw,
                "fecha": fecha_dt,
                "region": region,
                "nombre": nombre,
                "ambito": ambito,
                "comuna": comuna,
                "estado": estado,
                "superficie_ha": superficie,
                "url_poligono": url_poligono,
                "url_ficha": url_ficha,
                "url_archivos": url_archivos,
            }
        )

    df = pd.DataFrame(filas)
    return df


# ---------------------------------------------------------------------
# PARSEO DE CADA FICHA INDIVIDUAL
# ---------------------------------------------------------------------
def parsear_ficha_incendio(html_ficha: str) -> dict:
    """
    Extrae información de la ficha de un incendio:
    - Coordenadas UTM y geográficas (operativas/investigadas)
    - Condiciones meteorológicas y topográficas iniciales
    Devuelve un diccionario con columnas a agregar al DataFrame.
    """
    soup = BeautifulSoup(html_ficha, "lxml")
    datos = {}

    # ---------- COORDENADAS OPERATIVAS / INVESTIGADAS ----------
    td_coord_op = soup.find("td", string=lambda t: t and "Coordenadas operativas" in t)
    if td_coord_op:
        tabla_coord = td_coord_op.find_parent("table")
        tbody = tabla_coord.find("tbody")
        if tbody:
            for tr in tbody.find_all("tr"):
                celdas = tr.find_all("td")
                if len(celdas) < 4:
                    continue
                etiqueta = celdas[0].get_text(strip=True)
                valor_op = celdas[1].get_text(strip=True)
                valor_inv = celdas[3].get_text(strip=True)

                # Primer <a> para obtener lat/lon del onclick
                lat = lon = None
                a_tag = celdas[1].find("a")
                if a_tag:
                    onclick = a_tag.get("onclick", "")
                    m = re.search(
                        r"linkMapa\(this,\s*([\-0-9\.]+),\s*([\-0-9\.]+)\)",
                        onclick,
                    )
                    if m:
                        lat = float(m.group(1))
                        lon = float(m.group(2))

                if "Coordenadas UTM" in etiqueta:
                    datos["utm_operativas"] = valor_op
                    datos["utm_investigadas"] = valor_inv
                elif "Coordenadas geográficas" in etiqueta:
                    datos["geo_operativas"] = valor_op
                    datos["geo_investigadas"] = valor_inv
                    if lat is not None and lon is not None:
                        datos["lat_operativa"] = lat
                        datos["lon_operativa"] = lon

    # ---------- CONDICIONES METEOROLÓGICAS Y TOPOGRÁFICAS ----------
    td_cond = soup.find(
        "td",
        string=lambda t: t and "CONDICIONES METEOROLOGICAS Y TOPOGRAFICAS INICIALES" in t,
    )
    if td_cond:
        tabla_cond = td_cond.find_parent("table")
        tbody = tabla_cond.find("tbody")
        if tbody:
            mapeo = {
                "Temperatura:": "meteo_temperatura",
                "Nubosidad:": "meteo_nubosidad",
                "Hum. relativa:": "meteo_hum_relativa",
                "Velocidad viento:": "meteo_vel_viento",
                "Pendiente:": "meteo_pendiente",
                "Exposición:": "meteo_exposicion",
                "Direccion viento:": "meteo_dir_viento",
                "Topografia:": "meteo_topografia",
                "Estacion Meteorológica:": "meteo_estacion",
                "Fecha:": "meteo_fecha",
            }

            for tr in tbody.find_all("tr"):
                celdas = tr.find_all("td")
                if len(celdas) < 2:
                    continue
                etiqueta = celdas[0].get_text(strip=True)
                valor = celdas[1].get_text(strip=True)

                if etiqueta in mapeo:
                    datos[mapeo[etiqueta]] = valor

    return datos


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    df = scrapear_incendios_y_fichas()

    # --- limpiar nombres de columnas para ArcGIS ---
    # (saca caracteres raros/BOM y espacios al inicio/fin)
    df.columns = (
        df.columns.astype(str)
        .str.replace(r"[^\x00-\x7F]", "", regex=True)  # quita caracteres no ASCII (incluye BOM)
        .str.strip()
    )

    # --- guardar CSV sin BOM y separado por comas ---
    out_path = Path("incendios_vigentes_sidco_enriquecido.csv")
    df.to_csv(
        out_path,
        index=False,
        encoding="utf-8",  # << OJO: sin "-sig" para no agregar BOM
        sep=",",           # separador coma estándar
    )

    print(df.head())
    print(f"\nSe guardó {len(df)} registros en {out_path.resolve()}")


if __name__ == "__main__":
    main()
