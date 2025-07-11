# Componentes de Procesamiento de Datos

Este directorio contiene los componentes para el procesamiento de datos en MagicETL.

## Arquitectura Modular para Procesamiento de Datos

MagicETL utiliza una arquitectura modular para el procesamiento de datos, siguiendo los principios SOLID:

1. **Separación de responsabilidades**: Cada componente tiene una única responsabilidad
2. **Extensibilidad**: Fácil adición de nuevos formatos y conectores
3. **Desacoplamiento**: Los componentes pueden usarse de forma independiente

### Estructura de la Arquitectura

- **`app/adapters/universal_io.py`**: Punto de entrada principal para operaciones de I/O
- **`app/connectors/`**: Conectores específicos para diferentes fuentes de datos
  - `file_handler.py`: Manejo de archivos en múltiples formatos
  - `db_connector.py`: Conexiones a bases de datos

## Uso de la Arquitectura Universal I/O

Para operaciones de lectura/escritura de datos, use el módulo `universal_io` que proporciona una interfaz unificada:

```python
from app.adapters.universal_io import universal_extract_to_df, universal_write_df
import pandas as pd

# Leer datos de un archivo CSV
config_csv = {
    "file_path": "/ruta/a/datos.csv",
    "source_type": "csv"
}
df = universal_extract_to_df(config_csv)

# Escribir datos a un archivo JSON
config_json = {
    "file_path": "/ruta/a/salida.json",
    "target_type": "json",
    "orient": "records"
}
universal_write_df(df, config_json)
```

### Formatos Soportados

El sistema soporta múltiples formatos de entrada y salida:

- **Archivos**: CSV, TSV, JSON, Excel, Parquet, SQLite, XML, YAML, texto plano
- **Bases de datos**: PostgreSQL, MySQL, SQLite, MSSQL, Oracle, MongoDB

## Servicios de Transformación con IA

`llm_transform_service.py` proporciona servicios para transformaciones de datos utilizando modelos de lenguaje (LLM):

```python
from app.processing.llm_transform_service import LLMTransformService

# Inicializar servicio
llm_service = LLMTransformService(api_key='tu_api_key')

# Generar transformación
result = llm_service.generate_transformation_script(
    source_data=data,
    transformation_description="Convertir fechas a formato ISO"
)

# Analizar flujo
analysis = llm_service.analyze_flow(flow_data)
```

## Integración con la API

Estos componentes están expuestos a través de endpoints en `app.api.v1.ai_transform`, permitiendo:

- Transformaciones de datos vía API REST
- Análisis de flujos ETL
- Comparación de archivos de origen y destino
- Descarga de resultados transformados

Para más detalles, consulte la documentación de la API en `/api/v1/ai_transform.py`.

## Ventajas de la Arquitectura Modular

- **Mantenibilidad**: Código más limpio y fácil de mantener
- **Testabilidad**: Pruebas unitarias específicas para cada componente
- **Extensibilidad**: Fácil adición de nuevos formatos y conectores
- **Reusabilidad**: Componentes independientes que pueden usarse en diferentes contextos
