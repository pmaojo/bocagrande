import pytest
from playwright.sync_api import Page, expect
import os
import time
import requests # Añadir import para requests

# Asegúrate de que la API key de Gemini esté disponible para el entorno de pruebas
os.environ["GEMINI_API_KEY"] = os.getenv("GEMINI_API_KEY", "TEST_API_KEY_FALLBACK") # Usar una fallback para tests

# La URL de la aplicación Streamlit dentro del entorno Docker Compose
STREAMLIT_APP_URL = "http://localhost:8501" # Usar localhost ya que e2e-tests usa la red del host

# Función para esperar que la aplicación Streamlit esté lista
def wait_for_streamlit_app(url, timeout=120):
    print(f"Intentando conectar a la aplicación Streamlit en: {url} (Timeout: {timeout}s)")
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(url, timeout=10) # Aumentar timeout para la solicitud de requests
            if response.status_code == 200:
                print(f"Streamlit app está lista y respondió 200 OK en {url}")
                return True
            else:
                print(f"La aplicación Streamlit en {url} respondió con estado {response.status_code}. Reintentando en 1s...")
        except requests.exceptions.ConnectionError as e:
            print(f"Error de conexión a {url}: {e}. Reintentando en 1s...")
        except requests.exceptions.Timeout as e:
            print(f"Timeout al conectar a {url}: {e}. Reintentando en 1s...")
        except Exception as e:
            print(f"Error inesperado al verificar {url}: {e}. Reintentando en 1s...")
        time.sleep(1) # Esperar 1 segundo antes de reintentar
    raise TimeoutError(f"La aplicación Streamlit no estuvo lista en {timeout} segundos en {url}")

@pytest.mark.e2e
def test_app_loads_and_generates_transformations(page: Page):
    # Aumentar el timeout de navegación para toda la página
    page.set_default_navigation_timeout(90000) # 90 segundos
    page.set_default_timeout(90000) # 90 segundos para otras operaciones

    # Esperar explícitamente a que la aplicación Streamlit esté lista
    wait_for_streamlit_app(STREAMLIT_APP_URL)

    # Asegurarse de que Playwright navega a la URL
    page.goto(STREAMLIT_APP_URL)
    page.wait_for_load_state('networkidle') # Esperar a que la red esté inactiva

    # Añadir captura de pantalla para depuración después de la navegación
    page.screenshot(path="screenshot_after_goto.png")

    # Esperar explícitamente a que el h1 esté visible antes de verificar su texto
    expect(page.locator("h1")).to_be_visible(timeout=30000) # Aumentar timeout para visibilidad

    # Verificar que el título principal está visible
    expect(page.locator("h1")).to_have_text("Conversor Semántico Universal ")

    # 1. Subir archivo de muestra para comparación (CSV)
    # El nth(1) es porque el primer input type='file' es para la sección de validación
    page.locator("input[type='file']").nth(1).set_input_files("raw/CLIENTES_sample.csv")

    # Esperar a que Streamlit procese la subida (puede variar)
    time.sleep(2)

    # 2. Seleccionar esquema YAML de destino
    # Primero, hacer clic en el combobox para abrirlo. Usamos nth(1) para la sección de comparación.
    page.locator("div[data-testid='stSelectbox']").nth(1).click()
    # Luego, seleccionar una opción. Usamos 'CLIENTES.yaml' como ejemplo del archivo existente.
    page.locator("li").filter(has_text="CLIENTES.yaml").click()

    # Esperar a que Streamlit procese la selección
    time.sleep(2)

    # 3. Hacer clic en el botón "Generar Transformaciones Sugeridas con IA"
    page.get_by_role("button", name="Generar Transformaciones Sugeridas con IA").click()

    # Esperar a que Gemini procese y muestre las transformaciones
    # Puede que necesites ajustar este tiempo si Gemini tarda más
    time.sleep(15) # Aumentado a 15 segundos por si Gemini tarda

    # 4. Verificar que las transformaciones sugeridas se muestran (buscando el subencabezado o la tabla)
    expect(page.locator("h2").filter(has_text="Transformaciones Sugeridas:")).to_be_visible()
    expect(page.locator("div[data-testid='stDataFrame']")).to_be_visible()

    # Opcional: Aceptar transformaciones para probar el siguiente paso
    # if page.get_by_role("button", name="Aceptar Transformaciones Sugeridas").is_visible():
    #     page.get_by_role("button", name="Aceptar Transformaciones Sugeridas").click()
    #     expect(page.text_content("body")).to_contain("¡Transformaciones aceptadas!") 