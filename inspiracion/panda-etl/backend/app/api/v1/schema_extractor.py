"""
API para extracción de esquemas de múltiples fuentes de datos.

Este módulo proporciona endpoints para extraer esquemas de múltiples fuentes de datos
(APIs, bases de datos, archivos, etc.) y convertirlos a formato JSON para ser procesados
por modelos LLM como DeepSeek-V3 con ventanas de contexto grandes.
"""

from fastapi import APIRouter, Depends, HTTPException, Body
from typing import List, Dict, Any, Optional
from app.processing.schema_extractor import extract_all_schemas
# from app.adapters.groq_llm_adapter import GroqLLMAdapter # Replaced by Port and DI
from app.interfaces.llm_service_port import LLMServicePort # Import Port
from app.config import settings # Keep for direct Groq API call if not removed from method
from app.api.dependencies import get_groq_llm_adapter # Import new provider
import logging
# import json # For direct Groq call in transform_multi_source
# from groq import Groq # For direct Groq call in transform_multi_source

# Configuración de logging
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/schema", tags=["Schema Extractor"])

@router.post("/extract")
async def extract_schemas(
    sources: List[Dict[str, Any]] = Body(..., 
                                       description="Lista de configuraciones de fuentes de datos"),
    token_limit: Optional[int] = Body(100000, 
                                    description="Límite de tokens para el contexto")
):
    """
    Extrae esquemas de múltiples fuentes de datos y los convierte a formato JSON.
    
    Args:
        sources: Lista de configuraciones de fuentes. Cada fuente debe tener un campo 'type'
                con valor 'api', 'database' o 'file' y los campos específicos requeridos.
        token_limit: Límite de tokens para el contexto (por defecto 100,000).
        
    Returns:
        Esquema combinado como un objeto JSON.
    """
    try:
        logger.info(f"Extrayendo esquemas de {len(sources)} fuentes de datos")
        result = extract_all_schemas(sources, token_limit=token_limit)
        return result
    except Exception as e:
        logger.error(f"Error al extraer esquemas: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al extraer esquemas: {str(e)}")

@router.post("/analyze")
async def analyze_multi_source(
    sources: List[Dict[str, Any]] = Body(..., 
                                       description="Lista de configuraciones de fuentes de datos"),
    transformation_description: Optional[str] = Body("", 
                                                  description="Descripción de la transformación deseada"),
    token_limit: Optional[int] = Body(100000, 
                                    description="Límite de tokens para el contexto"),
    model_name: Optional[str] = Body("meta-llama/llama-4-maverick-17b-128e-instruct", 
                                   description="Modelo LLM a utilizar"),
    llm_service: LLMServicePort = Depends(get_groq_llm_adapter) # Injected
):
    """
    Analiza múltiples fuentes de datos y genera transformaciones utilizando LLM.
    
    Este endpoint extrae esquemas de múltiples fuentes, los convierte a formato JSON
    y utiliza un LLM para generar transformaciones o análisis. Está optimizado para
    modelos con ventanas de contexto grandes como DeepSeek-V3 (128K tokens).
    
    Args:
        sources: Lista de configuraciones de fuentes. Cada fuente debe tener un campo 'type'
                con valor 'api', 'database' o 'file' y los campos específicos requeridos.
        transformation_description: Descripción opcional de la transformación deseada.
        token_limit: Límite de tokens para el contexto (por defecto 100,000).
        model_name: Modelo LLM a utilizar (por defecto: llama-4-maverick). NOTE: llm_service is already configured with a model. This param might be redundant or for overriding.
        llm_service: Servicio LLM inyectado.
        
    Returns:
        Resultado del análisis y transformaciones sugeridas.
    """
    try:
        # llm_service is now injected.
        # If model_name parameter is meant to override the injected service's model,
        # the adapter/service would need a way to temporarily change its model, or a new instance created.
        # For now, assume the injected llm_service (with its configured model) is used.
        # The original code created a new instance: llm_service = GroqLLMAdapter(model_name=model_name)
        # This implies the model_name from the request IS important.
        # However, get_groq_llm_adapter is @lru_cache'd, so it's a singleton with a fixed model.
        # This is a conflict. For this DI task, I will use the injected llm_service.
        # A more advanced DI might allow passing params to the dependency.
        
        logger.info(f"Analizando {len(sources)} fuentes de datos con el modelo {llm_service.model_name if hasattr(llm_service, 'model_name') else 'default'}")
        
        # Utilizar el extractor de esquemas directamente
        combined_schema = extract_all_schemas(sources, token_limit=token_limit)
        
        # Generar transformaciones utilizando el LLM
        result = {
            "schema": combined_schema,
            "transformation_description": transformation_description,
            "message": "Para generar transformaciones con LLM, utiliza el endpoint /v1/ai/transform-multi"
        }
        
        return result
    except Exception as e:
        logger.error(f"Error al analizar fuentes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al analizar fuentes: {str(e)}")

@router.post("/transform-multi")
async def transform_multi_source(
    input_sources: List[Dict[str, Any]] = Body(..., 
                                          description="Lista de configuraciones de fuentes de datos de entrada"),
    output_schema: Optional[Dict[str, Any]] = Body(None, 
                                               description="Esquema de salida deseado (opcional)"),
    transformation_description: str = Body(..., 
                                        description="Descripción de la transformación deseada"),
    token_limit: Optional[int] = Body(100000, 
                                    description="Límite de tokens para el contexto"),
    model_name: Optional[str] = Body("meta-llama/llama-4-maverick-17b-128e-instruct", 
                                   description="Modelo LLM a utilizar"),
    llm_service: LLMServicePort = Depends(get_groq_llm_adapter) # Injected
):
    """
    Genera transformaciones para múltiples fuentes de datos utilizando LLM.
    
    Este endpoint extrae esquemas de múltiples fuentes, los convierte a formato JSON
    y utiliza un LLM para generar código de transformación. Está optimizado para
    modelos con ventanas de contexto grandes como DeepSeek-V3 (128K tokens).
    
    Args:
        input_sources: Lista de configuraciones de fuentes de datos de entrada. (param name changed from 'sources')
        output_schema: Esquema de salida deseado (opcional).
        transformation_description: Descripción de la transformación deseada.
        token_limit: Límite de tokens para el contexto (por defecto 100,000).
        model_name: Modelo LLM a utilizar. NOTE: See note in /analyze route about model_name conflict.
        llm_service: Servicio LLM inyectado.
        
    Returns:
        Código de transformación generado y análisis.
    """
    try:
        # llm_service is injected. The direct Groq call below uses settings.groq_api_key
        # and the model_name from the request. This part of the logic is largely independent
        # of the llm_service's own methods after this refactor.
        # If the intent was to use llm_service.generate_transformation_script, this endpoint
        # would need significant redesign.
        
        logger.info(f"Generando transformaciones para {len(input_sources)} fuentes con el modelo {model_name}")
        
        # Utilizar directamente el extractor de esquemas para las fuentes de entrada
        input_schema = extract_all_schemas(input_sources, token_limit=token_limit)
        
        # Preparar la información combinada de entrada y salida
        combined_schema = {
            "input": input_schema,
            "output": output_schema
        }
        
        # Generar transformaciones utilizando el LLM
        system_prompt = """Eres un experto en transformación y análisis de datos para sistemas ETL. 
        Tu tarea es analizar los esquemas de entrada y salida proporcionados en formato JSON 
        y generar código Python que permita extraer, transformar y cargar estos datos de manera eficiente.
        
        Se te proporcionará la siguiente información:
        1. ENTRADA: Esquemas y muestras de datos de múltiples fuentes de entrada
        2. SALIDA: Esquema deseado para los datos de salida (opcional)
        3. DESCRIPCIÓN: Descripción textual de la transformación requerida
        
        Tu tarea es:
        
        1. Analizar los esquemas de entrada y salida para entender la estructura de los datos
        2. Generar código Python para extraer datos de todas las fuentes de entrada usando universal_io
        3. Crear transformaciones que conviertan los datos de entrada al formato de salida deseado
        4. Generar código para cargar los datos transformados al destino
        5. Identificar posibles problemas o inconsistencias en el proceso
        
        Utiliza las siguientes bibliotecas y módulos:
        - pandas y numpy para manipulación de datos
        - app.adapters.universal_io para extracción y carga de datos
        - otras bibliotecas estándar de Python según sea necesario
        """
        
        # Convertir el esquema combinado a formato JSON para el prompt
        import json
        schema_json = json.dumps(combined_schema, indent=2)
        
        # Construir el prompt del usuario
        user_prompt = f"""Analiza los siguientes esquemas de entrada y salida, y genera código Python 
        para un flujo ETL completo que extraiga, transforme y cargue los datos:
        
        ```json
        {schema_json}
        ```
        
        Descripción de la transformación deseada:
        {transformation_description}
        
        Proporciona tu respuesta en formato JSON con las siguientes secciones:
        - analysis: Análisis de los esquemas de entrada y salida
        - extraction_code: Código Python para extraer datos de todas las fuentes de entrada usando universal_io
        - transformation_code: Código Python para transformar los datos al formato deseado
        - loading_code: Código Python para cargar los datos transformados al destino
        - complete_etl_code: Código Python completo que integra extracción, transformación y carga
        - issues: Posibles problemas o inconsistencias identificados
        - recommendations: Recomendaciones para mejorar el flujo ETL
        
        El código debe ser completo, bien documentado y listo para ejecutar. Asegúrate de utilizar 
        app.adapters.universal_io para la extracción y carga de datos, siguiendo las mejores prácticas de ETL.
        """
        
        # Realizar la llamada a la API de Groq
        from groq import Groq
        client = Groq(api_key=settings.groq_api_key)
        
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            model=model_name,
        )
        
        # Extraer la respuesta
        response_content = response.choices[0].message.content
        
        # Intentar parsear la respuesta como JSON
        try:
            result = json.loads(response_content)
            
            # Añadir metadatos
            import time
            result["metadata"] = {
                "model": model_name,
                "timestamp": time.time(),
                "version": "1.0",
                "sources_count": len(input_sources),
                "token_limit": token_limit
            }
            
            return result
        except json.JSONDecodeError:
            # Si no se puede parsear como JSON, intentar extraer el JSON de la respuesta
            import re
            json_match = re.search(r'\{[\s\S]*\}', response_content)
            if json_match:
                try:
                    result = json.loads(json_match.group())
                    
                    # Añadir metadatos
                    import time
                    result["metadata"] = {
                        "model": model_name,
                        "timestamp": time.time(),
                        "version": "1.0",
                        "sources_count": len(input_sources),
                        "token_limit": token_limit
                    }
                    
                    return result
                except Exception:
                    pass
            
            # Si todo falla, extraer el código Python de la respuesta
            python_code_matches = re.findall(r'```python\n([\s\S]*?)```', response_content)
            if python_code_matches:
                loading_code = python_code_matches[0] if len(python_code_matches) > 0 else ""
                transformation_code = python_code_matches[1] if len(python_code_matches) > 1 else ""
                
                return {
                    "analysis": "Extraído de la respuesta no estructurada",
                    "loading_code": loading_code,
                    "transformation_code": transformation_code,
                    "raw_response": response_content,
                    "metadata": {
                        "model": model_name,
                        "timestamp": time.time(),
                        "version": "1.0",
                        "sources_count": len(input_sources),
                        "token_limit": token_limit,
                        "note": "Respuesta no estructurada, se extrajeron bloques de código"
                    }
                }
            
            # Si no hay código Python, devolver la respuesta completa
            return {
                "error": "No se pudo parsear la respuesta como JSON",
                "raw_response": response_content,
                "metadata": {
                    "model": model_name,
                    "timestamp": time.time(),
                    "version": "1.0",
                    "sources_count": len(input_sources),
                    "token_limit": token_limit
                }
            }
            
    except Exception as e:
        logger.error(f"Error al generar transformaciones: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al generar transformaciones: {str(e)}")
