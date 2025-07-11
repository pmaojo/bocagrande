"""
Adaptador para transformaciones de datos utilizando modelos de lenguaje (LLM) a través de Groq.

Este módulo proporciona funcionalidades para generar scripts de transformación de datos
utilizando modelos de lenguaje de gran tamaño (LLM). Permite analizar datos de origen y destino,
generar código Python para transformar los datos, y proporcionar explicaciones detalladas
sobre las transformaciones realizadas.

Características principales:
- Generación de scripts de transformación basados en datos de entrada y salida
- Análisis de flujos ETL completos
- Soporte para diferentes niveles de complejidad (básico, avanzado, semántico)
- Integración con la API de Groq para modelos de lenguaje
"""

import pandas as pd
import json
import re
import traceback
import time
from typing import Dict, List, Any, Tuple, Optional

from groq import Groq, APIError # Ensure Groq is imported

from app.interfaces.llm_service_port import LLMServicePort
from app.logger import get_logger # Standardized logger
from app.config import settings

logger = get_logger(__name__)

class GroqLLMAdapter(LLMServicePort): # Renamed class and added inheritance
    """
    Adaptador para transformar datos utilizando modelos de lenguaje (LLM) a través de Groq,
    implementando LLMServicePort.
    
    Esta clase proporciona una interfaz para interactuar con modelos de lenguaje como Llama
    a través de la API de Groq, con el fin de generar scripts de transformación de datos,
    analizar flujos ETL y proporcionar explicaciones detalladas sobre las transformaciones.
    
    Atributos:
        api_key (str): Clave API para el servicio LLM (Groq)
        model_name (str): Nombre del modelo a utilizar
    """
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        model_name: Optional[str] = None,
        api_base_url: Optional[str] = None,
    ):
        """
        Inicializa el servicio con la clave API y el modelo a utilizar.
        
        Args:
            api_key: Clave API para el servicio LLM (si es None, se usa la de configuración)
            model_name: Nombre del modelo a utilizar (si es None, usa un default)
        """
        self.api_key = api_key or settings.groq_api_key
        self.api_base_url = api_base_url
        # Default model can be specified here or taken from settings if available
        self.model_name = model_name or "mixtral-8x7b-32768"  # Example default
        
        if not self.api_key:
            logger.warning("No se ha proporcionado una clave API para el servicio LLM (Groq)")
    
    def _get_representative_sample(self, df: pd.DataFrame, max_rows: int = 13) -> str:
        """
        Obtiene una muestra representativa de un DataFrame.
        
        Args:
            df: DataFrame del que obtener la muestra
            max_rows: Número máximo de filas a incluir
            
        Returns:
            Representación en string de la muestra
        """
        # Aseguramos que siempre incluimos la cabecera (nombres de columnas)
        sample_size = min(max_rows, len(df))
        if len(df) <= sample_size:
            return df.to_string(index=False)
        else:
            # Tomamos algunas filas del principio, medio y final
            head_rows = min(5, sample_size // 3)
            tail_rows = min(5, sample_size // 3)
            middle_rows = sample_size - head_rows - tail_rows
            
            # Índices para filas del medio
            middle_start = len(df) // 2 - middle_rows // 2
            middle_end = middle_start + middle_rows
            
            # Concatenamos las muestras
            sample_df = pd.concat([
                df.head(head_rows),
                df.iloc[middle_start:middle_end],
                df.tail(tail_rows)
            ])
            
            return sample_df.to_string(index=False)
    
    def _extract_array(self, text: str, columns: List[str]) -> List[List[Any]]:
        """
        Extrae un array de un texto utilizando expresiones regulares.
        
        Args:
            text: Texto del que extraer el array
            columns: Nombres de las columnas
            
        Returns:
            Array extraído o mensaje de error
        """
        # Usar expresiones regulares para encontrar el array deseado
        pattern = r'\[.*\]'
        match = re.search(pattern, text, re.DOTALL)

        if match:
            extracted_array = match.group()
            return eval(extracted_array) # eval can be risky, consider safer alternatives if input is not trusted
        else:
            return "Array not found in the text." # Consider raising an error
    
    # Renamed from analyze_columns to match port
    def analyze_columns_for_script(self, raw_df: pd.DataFrame, target_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Analiza las columnas de un DataFrame y genera transformaciones para que coincidan con un DataFrame objetivo.
        
        Args:
            raw_df: DataFrame de origen
            target_df: DataFrame objetivo
            
        Returns:
            Tuple con el DataFrame transformado y estadísticas de uso
        """
        # Obtenemos muestras representativas
        raw_df_sample = self._get_representative_sample(raw_df)
        target_df_sample = self._get_representative_sample(target_df)
        
        logger.info(f"Tamaño de la muestra de datos de entrada: {len(raw_df_sample)} caracteres")
        logger.info(f"Tamaño de la muestra de datos de salida: {len(target_df_sample)} caracteres")
        
        try:
            # Inicialización del cliente Groq
            client = Groq(api_key=self.api_key, base_url=self.api_base_url) if self.api_base_url else Groq(api_key=self.api_key)
            logger.info(f"Cliente Groq inicializado correctamente con el modelo {self.model_name}")
        except Exception as e:
            logger.error(f"Error al inicializar cliente Groq: {str(e)}")
            logger.error(f"Tipo de error: {type(e).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(f"Error al inicializar cliente Groq: {str(e)}")

        # Prompt del sistema
        system_prompt = """Eres un experto en transformaciones de datos con Pandas. Tu tarea es generar un script de Python conciso y eficiente que transforme un DataFrame llamado 'df_raw' en un formato que coincida con 'df_target'.  

Sigue estas reglas:
1. Usa solo pandas y funciones estándar de Python
2. Asume que 'df_raw' ya está cargado
3. El resultado debe asignarse a 'df_transformed'
4. Enfócate en transformaciones esenciales (selección, renombrado, limpieza, conversión de tipos)
5. Incluye comentarios explicativos
6. No incluyas código para cargar o guardar datos
7. Genera código que sea robusto ante valores nulos o inesperados

Analiza cuidadosamente los datos de entrada y salida para identificar:
- Qué columnas seleccionar o descartar
- Cómo renombrar columnas
- Qué transformaciones aplicar a cada columna
- Cómo manejar valores nulos o incorrectos
"""

        # Prompt del usuario
        user_prompt = f"""
# Datos de entrada (df_raw):
```
{raw_df_sample}
```

# Datos de salida deseados (df_target):
```
{target_df_sample}
```

Genera un script de Python que transforme df_raw en df_transformed siguiendo el formato de df_target.
Incluye comentarios explicativos para cada paso importante.
"""

        try:
            # Realizar la llamada a la API
            start_time = time.time()
            response = client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                max_tokens=2000,
                top_p=1,
                stream=False
            )
            end_time = time.time()
            
            # Extraer la respuesta
            transformation_code = response.choices[0].message.content
            
            # Calcular estadísticas de uso
            usage_stats = {
                "total_tokens": response.usage.total_tokens,
                "prompt_tokens": response.usage.prompt_tokens,
                "completion_tokens": response.usage.completion_tokens,
                "processing_time": end_time - start_time
            }
            
            logger.info(f"Transformación generada correctamente. Tokens totales: {usage_stats['total_tokens']}")
            
            # Ejecutar el código generado para obtener el DataFrame transformado
            try:
                # Preparar el entorno de ejecución
                local_vars = {"df_raw": raw_df.copy(), "pd": pd}
                
                # Ejecutar el código
                exec(transformation_code, {}, local_vars)
                
                # Obtener el DataFrame transformado
                if "df_transformed" in local_vars:
                    transformed_df = local_vars["df_transformed"]
                    return transformed_df, usage_stats
                else:
                    logger.error("El código generado no produjo un DataFrame 'df_transformed'")
                    # Consider returning the original raw_df or an empty one based on desired error handling
                    return pd.DataFrame(), usage_stats
            except Exception as e:
                logger.error(f"Error al ejecutar el código generado: {str(e)}")
                logger.error(f"Código generado: {transformation_code}")
                return pd.DataFrame(), usage_stats # Or re-raise
            
        except APIError as e:
            logger.error(f"Error de API Groq: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    # Name matches port
    def generate_transformation_script(self, 
                                      source_data: Dict[str, Any], 
                                      target_schema: Optional[Dict[str, str]] = None,
                                      transformation_description: str = "",
                                      node_id: str = "",
                                      node_name: str = "") -> Dict[str, Any]:
        """
        Genera un script de transformación para datos de origen basado en un esquema objetivo.
        
        Args:
            source_data: Datos de origen (puede ser un diccionario, lista, etc.)
            target_schema: Esquema objetivo (opcional)
            transformation_description: Descripción de la transformación
            node_id: ID del nodo
            node_name: Nombre del nodo
            
        Returns:
            Diccionario con el script generado, explicación y estadísticas de uso
        """
        try:
            # Inicialización del cliente Groq
            client = Groq(api_key=self.api_key, base_url=self.api_base_url) if self.api_base_url else Groq(api_key=self.api_key)
            logger.info(f"Cliente Groq inicializado correctamente con el modelo {self.model_name}")
        except Exception as e:
            logger.error(f"Error al inicializar cliente Groq: {str(e)}")
            logger.error(f"Tipo de error: {type(e).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(f"Error al inicializar cliente Groq: {str(e)}")
        
        # Convertir datos de origen a DataFrame si es posible
        source_df = None
        if isinstance(source_data, dict):
            try:
                if "data" in source_data and isinstance(source_data["data"], list):
                    source_df = pd.DataFrame(source_data["data"])
                else:
                    # Intentar convertir diferentes estructuras
                    if all(isinstance(v, list) for v in source_data.values()):
                        # Formato {column1: [values], column2: [values]}
                        source_df = pd.DataFrame(source_data)
                    else:
                        # Formato [{col1: val1, col2: val2}, {...}]
                        source_df = pd.DataFrame([source_data]) # Wrap in list if single dict
            except Exception as e:
                logger.warning(f"No se pudo convertir los datos de origen a DataFrame: {str(e)}")
        elif isinstance(source_data, list): # Handle if source_data is a list of records
            try:
                source_df = pd.DataFrame(source_data)
            except Exception as e:
                logger.warning(f"No se pudo convertir la lista de datos de origen a DataFrame: {str(e)}")

        # Preparar la representación de los datos de origen
        source_data_repr = ""
        if source_df is not None:
            source_data_repr = self._get_representative_sample(source_df)
        else:
            # Truncate if too long, as it might be a large non-tabular structure
            json_source_data = json.dumps(source_data, indent=2)
            source_data_repr = (json_source_data[:1000] + '...') if len(json_source_data) > 1000 else json_source_data

        # Preparar la representación del esquema objetivo
        target_schema_repr = ""
        if target_schema:
            target_schema_repr = json.dumps(target_schema, indent=2)
        
        # Prompt del sistema
        system_prompt = """Eres un experto en transformaciones de datos con Python. Tu tarea es generar un script de Python que transforme los datos de origen según la descripción proporcionada y, si está disponible, el esquema objetivo.

Sigue estas reglas:
1. Genera una función llamada 'transform_data' que tome los datos de origen (pueden ser DataFrame, list of dicts, o dict) y devuelva los datos transformados (preferiblemente DataFrame).
2. Usa pandas y funciones estándar de Python. Si los datos de entrada no son un DataFrame, conviértelos a uno si es apropiado para la transformación.
3. Incluye manejo de errores y validación de datos.
4. Añade comentarios explicativos para cada paso importante.
5. El código debe ser eficiente y seguir las mejores prácticas.
6. Incluye docstrings explicativos para la función 'transform_data'.
7. La función debe ser autocontenida y no depender de variables globales excepto importaciones.
"""

        # Prompt del usuario
        user_prompt = f"""
# Nodo: {node_name} (ID: {node_id})
# Descripción de la transformación: {transformation_description}

# Datos de origen (muestra o estructura):
```
{source_data_repr}
```

"""
        
        # Añadir esquema objetivo si está disponible
        if target_schema:
            user_prompt += f"""
# Esquema objetivo:
```
{target_schema_repr}
```
"""
        
        user_prompt += """
Genera una función de transformación completa en Python llamada 'transform_data' que procese estos datos según la descripción.
Incluye una explicación detallada de la transformación fuera del bloque de código.
"""

        try:
            # Realizar la llamada a la API
            start_time = time.time()
            response = client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                max_tokens=3000, # Increased max_tokens slightly
                top_p=1,
                stream=False
            )
            end_time = time.time()
            
            # Extraer la respuesta
            full_response = response.choices[0].message.content
            
            # Intentar extraer el código y la explicación
            code_pattern = r'```python(.*?)```'
            code_matches = re.findall(code_pattern, full_response, re.DOTALL)
            
            transformation_script = ""
            if code_matches:
                transformation_script = max(code_matches, key=len).strip() # Use the longest code block
            else:
                # Fallback: if no ```python ``` blocks, assume the whole response might be code (less ideal)
                transformation_script = full_response.strip()
            
            explanation = re.sub(code_pattern, '', full_response, flags=re.DOTALL).strip()
            if not explanation and not code_matches: # If script took full_response, no explanation left
                 explanation = "No explanation provided separately from the script."
            elif not explanation and code_matches: # If code was extracted, but no text outside
                 explanation = "Script generated. Explanation might be embedded as comments within the script."


            token_usage = response.usage # Access usage object directly
            
            return {
                "transformationScript": transformation_script,
                "explanation": explanation,
                "tokenUsage": {
                    "total_tokens": token_usage.total_tokens,
                    "prompt_tokens": token_usage.prompt_tokens,
                    "completion_tokens": token_usage.completion_tokens,
                    "cost": 0.0  # Placeholder for actual cost calculation
                },
                "processingTime": end_time - start_time
            }
            
        except APIError as e:
            logger.error(f"Error de API Groq: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    # Renamed from analyze_flow to match port
    def analyze_etl_flow(self, flow_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Analiza un flujo ETL completo y proporciona sugerencias, optimizaciones y advertencias.
        
        Args:
            flow_data: Datos del flujo ETL (nodos y conexiones)
            
        Returns:
            Diccionario con sugerencias, optimizaciones, advertencias y estadísticas
        """
        try:
            # Inicialización del cliente Groq
            client = Groq(api_key=self.api_key, base_url=self.api_base_url) if self.api_base_url else Groq(api_key=self.api_key)
            logger.info(f"Cliente Groq inicializado correctamente con el modelo {self.model_name}")
        except Exception as e:
            logger.error(f"Error al inicializar cliente Groq: {str(e)}")
            logger.error(f"Tipo de error: {type(e).__name__}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise ValueError(f"Error al inicializar cliente Groq: {str(e)}")
        
        # Preparar la representación del flujo
        flow_data_repr = json.dumps(flow_data, indent=2)
        # Truncate if too long to avoid excessive token usage
        flow_data_repr = (flow_data_repr[:2000] + "\n... (flow data truncated)") if len(flow_data_repr) > 2000 else flow_data_repr

        # Prompt del sistema
        system_prompt = """Eres un experto en flujos ETL (Extract, Transform, Load) y optimización de datos. Tu tarea es analizar un flujo ETL y proporcionar sugerencias, optimizaciones y advertencias.

Sigue estas reglas:
1. Analiza la estructura del flujo (nodos y conexiones)
2. Identifica posibles cuellos de botella o ineficiencias
3. Sugiere mejoras en la estructura del flujo
4. Identifica posibles problemas de calidad de datos
5. Recomienda optimizaciones para mejorar el rendimiento
6. Advierte sobre posibles problemas o riesgos

Tu respuesta debe incluir:
1. Una lista de sugerencias concretas
2. Una lista de optimizaciones posibles
3. Una lista de advertencias importantes
4. Una explicación general del análisis
Formatea cada lista con items numerados o con viñetas.
"""

        # Prompt del usuario
        user_prompt = f"""
# Datos del flujo ETL:
```
{flow_data_repr}
```

Analiza este flujo ETL y proporciona:
1. Sugerencias para mejorar la estructura y funcionalidad
2. Optimizaciones para mejorar el rendimiento
3. Advertencias sobre posibles problemas o riesgos
4. Una explicación general de tu análisis
"""

        try:
            # Realizar la llamada a la API
            start_time = time.time()
            response = client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.2,
                max_tokens=2500, # Increased max_tokens
                top_p=1,
                stream=False
            )
            end_time = time.time()
            
            full_response = response.choices[0].message.content
            
            suggestions = []
            optimizations = []
            warnings_list = [] # Renamed to avoid conflict with 'warnings' module
            explanation = ""
            
            # Enhanced extraction logic
            current_section_keyword = None
            current_section_content = []

            for line in full_response.splitlines():
                stripped_line = line.strip()
                new_section = None
                if re.match(r'^(?:1\.\s+)?(?:Sugerencias?|Suggestions?)(?:\s*para.+)?[:\s]*$', stripped_line, re.IGNORECASE):
                    new_section = "suggestions"
                elif re.match(r'^(?:2\.\s+)?(?:Optimizaciones?|Optimizations?)(?:\s*para.+)?[:\s]*$', stripped_line, re.IGNORECASE):
                    new_section = "optimizations"
                elif re.match(r'^(?:3\.\s+)?(?:Advertencias?|Warnings?)(?:\s*sobre.+)?[:\s]*$', stripped_line, re.IGNORECASE):
                    new_section = "warnings"
                elif re.match(r'^(?:4\.\s+)?(?:Explicación|Explanation|Análisis General)(?:\s*general.+)?[:\s]*$', stripped_line, re.IGNORECASE):
                    new_section = "explanation"

                if new_section:
                    if current_section_keyword and current_section_content:
                        # Store previous section's content
                        content_text = "\n".join(current_section_content).strip()
                        if current_section_keyword == "suggestions":
                            suggestions.append(content_text)
                        elif current_section_keyword == "optimizations":
                            optimizations.append(content_text)
                        elif current_section_keyword == "warnings":
                            warnings_list.append(content_text)
                        elif current_section_keyword == "explanation":
                            explanation = content_text
                    current_section_keyword = new_section
                    current_section_content = []
                    # Check if the line itself that matched the section is also a point
                    item_match = re.match(r'^(?:\d+\.\s*|\-\s*|\*\s*)(.+)', stripped_line, re.IGNORECASE)
                    if item_match and new_section != "explanation":
                        # Remove the section keyword part from the item
                        item_text = re.sub(
                            r'^(?:Sugerencias?|Suggestions?|Optimizaciones?|Optimizations?|Advertencias?|Warnings?|Explicación|Explanation|Análisis General)(?:\s*para.+)?[:\s]*',
                            '',
                            item_match.group(1),
                            flags=re.IGNORECASE,
                        ).strip()
                        if item_text:
                            current_section_content.append(item_text)
                    
                elif current_section_keyword:
                    item_match = re.match(r'^(?:\d+\.\s*|\-\s*|\*\s*)(.+)', stripped_line)
                    if item_match:
                        current_section_content.append(item_match.group(1).strip())
                    elif stripped_line and current_section_keyword == "explanation": # For multi-line explanation text
                        current_section_content.append(stripped_line)
            
            # Store the last section's content
            if current_section_keyword and current_section_content:
                content_text = "\n".join(current_section_content).strip()
                if current_section_keyword == "suggestions":
                    suggestions.append(content_text)  # Append as one block
                elif current_section_keyword == "optimizations":
                    optimizations.append(content_text)
                elif current_section_keyword == "warnings":
                    warnings_list.append(content_text)
                elif current_section_keyword == "explanation":
                    explanation = content_text

            # If specific sections were not found, use the full response as explanation
            if not suggestions and not optimizations and not warnings_list and not explanation:
                explanation = full_response

            # Flatten if sections were captured as single multi-line strings
            suggestions = [s.strip() for item in suggestions for s in item.splitlines() if s.strip()]
            optimizations = [s.strip() for item in optimizations for s in item.splitlines() if s.strip()]
            warnings_list = [s.strip() for item in warnings_list for s in item.splitlines() if s.strip()]


            token_usage = response.usage # Access usage object directly
            
            return {
                "suggestions": suggestions,
                "optimizations": optimizations,
                "warnings": warnings_list,
                "explanation": explanation.strip(),
                "tokenUsage": {
                    "total_tokens": token_usage.total_tokens,
                    "prompt_tokens": token_usage.prompt_tokens,
                    "completion_tokens": token_usage.completion_tokens,
                    "cost": 0.0  # Placeholder
                },
                "processingTime": end_time - start_time
            }
            
        except APIError as e:
            logger.error(f"Error de API Groq: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado: {str(e)}")
            logger.error(traceback.format_exc())
            raise
