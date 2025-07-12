# Extracción de Especificación de Formato CSV desde PDF

Este proyecto extrae la definición de campos, claves y metadatos de un PDF de especificación de formato CSV y los convierte a YAML estructurado.

## Instalación

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Uso

Coloca el PDF en `data/input/` y ejecuta:

```bash
python src/extrae_tablas_pdf_a_yaml.py data/input/NombreDelPDF.pdf data/output/salida.yaml
```

## Tests

```bash
pytest
```

## Reglas y metadatos globales de los CSV

- **El orden de los campos (columnas) es el mismo que en el CSV de salida.**
- **El sistema es case sensitive:** respeta mayúsculas/minúsculas y la semántica original.
- **Separador de columnas:** `,` (coma, por defecto en CSV estándar).
- **Símbolo decimal:** `.` (punto).
- **Campos de tipo CLOB:** Si contienen caracteres especiales, se envuelven entre comillas dobles `"`.
- **Formatos especiales:**
  - Fechas: `YYYY-MM-DD` (u otro especificado en la tabla de campos).
  - Mediciones oftalmológicas: respetar el formato indicado en la especificación.

Estos metadatos se incluyen automáticamente en el YAML generado para cada fichero extraído del PDF. 