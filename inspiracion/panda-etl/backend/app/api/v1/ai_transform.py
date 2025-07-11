"""
Endpoints para transformaciones de datos basadas en IA.
Integra las funcionalidades de transformación de datos de ai-based-etl-master.
"""

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
import os
import tempfile
import uuid

from app.config import settings # Keep settings for direct access if needed elsewhere, though adapter gets it via DI
from app.logger import get_logger
# from app.adapters.groq_llm_adapter import GroqLLMAdapter # Replaced by Port and DI
from app.interfaces.llm_service_port import LLMServicePort # Import Port
from app.adapters.universal_io import universal_extract_to_df, universal_write_df, UniversalIOError
from app.api.dependencies import get_api_key, get_groq_llm_adapter # Import new provider

logger = get_logger(__name__)

router = APIRouter()


class TransformationRequest(BaseModel):
    """Solicitud para transformación de datos usando IA."""
    sourceData: Dict[str, Any]
    targetSchema: Optional[Dict[str, str]] = None
    transformationDescription: str
    nodeId: str
    nodeName: str


class FlowAnalysisRequest(BaseModel):
    """Solicitud para análisis de flujo ETL."""
    flowData: Dict[str, Any]


@router.post("/transform", response_model=Dict[str, Any])
async def transform_data(
    request: TransformationRequest,
    api_key: str = Depends(get_api_key), # Keep for endpoint security
    llm_service: LLMServicePort = Depends(get_groq_llm_adapter) # Injected LLM Service
):
    """
    Genera un script de transformación para datos utilizando Groq.
    
    Args:
        request: Datos para la generación del script
        api_key: Clave API para autenticación
        llm_service: Servicio LLM inyectado
        
    Returns:
        Script de transformación generado y explicación
    """
    try:
        # llm_service is now injected
        result = llm_service.generate_transformation_script(
            source_data=request.sourceData,
            target_schema=request.targetSchema,
            transformation_description=request.transformationDescription,
            node_id=request.nodeId,
            node_name=request.nodeName
        )
        
        return result
    except Exception as e:
        logger.error(f"Error en transformación de datos: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en transformación: {str(e)}")


@router.post("/analyze-flow", response_model=Dict[str, Any])
async def analyze_flow(
    request: FlowAnalysisRequest,
    api_key: str = Depends(get_api_key), # Keep for endpoint security
    llm_service: LLMServicePort = Depends(get_groq_llm_adapter) # Injected LLM Service
):
    """
    Analiza un flujo ETL completo y proporciona sugerencias, optimizaciones y advertencias.
    
    Args:
        request: Datos del flujo ETL
        api_key: Clave API para autenticación
        llm_service: Servicio LLM inyectado
        
    Returns:
        Análisis del flujo con sugerencias, optimizaciones y advertencias
    """
    try:
        # llm_service is now injected
        result = llm_service.analyze_etl_flow(request.flowData)
        
        return result
    except Exception as e:
        logger.error(f"Error en análisis de flujo: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en análisis de flujo: {str(e)}")


@router.post("/analyze-files", response_model=Dict[str, Any])
async def analyze_files(
    source_file: UploadFile = File(...),
    target_file: UploadFile = File(...),
    api_key: str = Depends(get_api_key), # Keep for endpoint security
    llm_service: LLMServicePort = Depends(get_groq_llm_adapter) # Injected LLM Service
):
    """
    Analiza archivos de origen y destino para generar una transformación.
    
    Args:
        source_file: Archivo de datos de origen
        target_file: Archivo de datos de destino
        api_key: Clave API para autenticación
        llm_service: Servicio LLM inyectado
        
    Returns:
        DataFrame transformado y estadísticas de uso
    """
    try:
        # Crear directorios temporales para los archivos
        temp_dir = tempfile.mkdtemp()
        source_path = os.path.join(temp_dir, source_file.filename)
        target_path = os.path.join(temp_dir, target_file.filename)
        
        # Guardar los archivos
        with open(source_path, "wb") as f:
            content = await source_file.read()
            f.write(content)
        
        with open(target_path, "wb") as f:
            content = await target_file.read()
            f.write(content)
        
        # Leer archivos usando la arquitectura modular
        source_config = {
            "file_path": source_path
            # El formato se detectará automáticamente por la extensión
        }
        
        target_config = {
            "file_path": target_path
            # El formato se detectará automáticamente por la extensión
        }
        
        raw_df = universal_extract_to_df(source_config)
        target_df = universal_extract_to_df(target_config)
        
        # llm_service is now injected
        transformed_df, usage_stats = llm_service.analyze_columns_for_script(raw_df, target_df)
        
        # Guardar el resultado transformado
        result_path = os.path.join(settings.process_dir, f"transformed_{uuid.uuid4()}.csv")
        result_config = {
            "file_path": result_path,
            "target_type": "csv",
            "index": False
        }
        universal_write_df(transformed_df, result_config)
        
        # Preparar respuesta
        response = {
            "message": "Archivos procesados correctamente",
            "rows_processed": len(raw_df),
            "token_usage": usage_stats["total_tokens"],
            "processing_time": usage_stats["processing_time"],
            "result_path": result_path,
            "transformed_data": transformed_df.to_dict(orient="records")
        }
        
        return response
    except UniversalIOError as e:
        logger.error(f"Error de I/O en análisis de archivos: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error de I/O: {str(e)}")
    except Exception as e:
        logger.error(f"Error en análisis de archivos: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error en análisis de archivos: {str(e)}")


@router.get("/formats", response_model=Dict[str, List[Dict[str, Any]]])
async def get_supported_formats():
    """
    Obtiene una lista de formatos de entrada y salida soportados.
    
    Returns:
        Diccionario con listas de formatos de entrada y salida soportados
    """
    try:
        # Definir formatos soportados para archivos
        file_formats = [
            {"name": "CSV", "extensions": [".csv"], "description": "Valores separados por comas"},
            {"name": "TSV", "extensions": [".tsv"], "description": "Valores separados por tabulaciones"},
            {"name": "JSON", "extensions": [".json"], "description": "JavaScript Object Notation"},
            {"name": "Excel", "extensions": [".xlsx", ".xls"], "description": "Hojas de cálculo Microsoft Excel"},
            {"name": "Parquet", "extensions": [".parquet", ".pq"], "description": "Formato columnar Apache Parquet"},
            {"name": "SQLite", "extensions": [".db", ".sqlite"], "description": "Base de datos SQLite"},
            {"name": "XML", "extensions": [".xml"], "description": "Extensible Markup Language"},
            {"name": "YAML", "extensions": [".yaml", ".yml"], "description": "YAML Ain't Markup Language"},
            {"name": "Text", "extensions": [".txt"], "description": "Archivo de texto plano"}
        ]
        
        # Definir formatos soportados para bases de datos
        db_formats = [
            {"name": "PostgreSQL", "extensions": [], "description": "Base de datos PostgreSQL"},
            {"name": "MySQL", "extensions": [], "description": "Base de datos MySQL"},
            {"name": "SQLite", "extensions": [".db", ".sqlite"], "description": "Base de datos SQLite"},
            {"name": "MSSQL", "extensions": [], "description": "Microsoft SQL Server"},
            {"name": "Oracle", "extensions": [], "description": "Oracle Database"},
            {"name": "MongoDB", "extensions": [], "description": "Base de datos MongoDB"}
        ]
        
        return {
            "input_formats": file_formats + db_formats,
            "output_formats": file_formats + db_formats
        }
    except Exception as e:
        logger.error(f"Error al obtener formatos soportados: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al obtener formatos: {str(e)}")


@router.get("/download/{filename}")
async def download_transformed_file(filename: str):
    """
    Descarga un archivo transformado.
    
    Args:
        filename: Nombre del archivo a descargar
        
    Returns:
        Archivo para descargar
    """
    try:
        file_path = os.path.join(settings.process_dir, filename)
        
        if not os.path.exists(file_path):
            raise HTTPException(status_code=404, detail="Archivo no encontrado")
        
        return FileResponse(
            path=file_path,
            filename=filename,
            media_type="application/octet-stream"
        )
    except HTTPException:
        raise
    except UniversalIOError as e:
        logger.error(f"Error de I/O al descargar archivo: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error de I/O: {str(e)}")
    except Exception as e:
        logger.error(f"Error al descargar archivo: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error al descargar archivo: {str(e)}")
