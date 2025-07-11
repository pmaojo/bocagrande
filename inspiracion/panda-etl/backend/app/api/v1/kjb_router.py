"""
Router para la gestión de archivos KJB (Kettle Job)

Este módulo proporciona endpoints para importar y exportar archivos KJB,
facilitando la integración con herramientas de Pentaho Data Integration.
"""

from fastapi import APIRouter, File, UploadFile, HTTPException, Depends
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
import shutil
import os
import time
from app.logger import get_logger

from app.pentaho_importer import KJBParserService, KTRParserService
from app.api.dependencies import get_pipeline_manager
from app.processing.pipeline_manager import PipelineManager, PipelineError

# Modelos para la solicitud y respuesta
class KJBImportRequest(BaseModel):
    project_id: int
    create_pipelines: bool = True
    import_ktr_files: bool = True
    pipeline_name_prefix: Optional[str] = None

class PipelineInfo(BaseModel):
    id: str
    name: str
    type: str
    source_file: str

class KJBImportResponse(BaseModel):
    main_pipeline: PipelineInfo
    sub_pipelines: List[PipelineInfo] = Field(default_factory=list)
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]

router = APIRouter(
    tags=["KJB Importer"],
)

logger = get_logger(__name__)

# Define a temporary directory for uploads if it doesn't exist
TEMP_UPLOAD_DIR = "/tmp/kjb_uploads"
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

@router.post("/import", response_model=KJBImportResponse)
async def import_kjb(
    kjb_file: UploadFile = File(...),
    ktr_files: List[UploadFile] = File(None),
    request: KJBImportRequest = Depends(),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Recibe un archivo KJB y opcionalmente archivos KTR, los parsea y crea pipelines para cada uno.
    
    Args:
        kjb_file: Archivo KJB principal a importar
        ktr_files: Lista de archivos KTR relacionados (opcional)
        request: Parámetros de la solicitud
        pipeline_manager: Gestor de pipelines
        
    Returns:
        Información sobre los pipelines creados y el modelo ReactFlow
    """
    if not kjb_file.filename.endswith('.kjb'):
        raise HTTPException(status_code=400, detail="Invalid KJB file. File must have .kjb extension.")

    # Crear directorio temporal para los archivos
    import_dir = os.path.join(TEMP_UPLOAD_DIR, f"import_{int(time.time())}")
    os.makedirs(import_dir, exist_ok=True)
    
    kjb_file_path = os.path.join(import_dir, kjb_file.filename)
    ktr_dir = os.path.join(import_dir, "ktr_files")
    os.makedirs(ktr_dir, exist_ok=True)

    # Diccionario para mapear nombres de archivos KTR a sus rutas
    ktr_file_paths = {}

    try:
        # Guardar el archivo KJB
        with open(kjb_file_path, "wb") as buffer:
            shutil.copyfileobj(kjb_file.file, buffer)

        # Guardar los archivos KTR si se proporcionaron
        if ktr_files:
            for ktr_file in ktr_files:
                if ktr_file and ktr_file.filename.endswith('.ktr'):
                    ktr_path = os.path.join(ktr_dir, ktr_file.filename)
                    with open(ktr_path, "wb") as buffer:
                        shutil.copyfileobj(ktr_file.file, buffer)
                    ktr_file_paths[os.path.basename(ktr_file.filename)] = ktr_path

        # Parsear el archivo KJB
        kjb_parser = KJBParserService()
        job_model = kjb_parser.parse_kjb_file(kjb_file_path)
        
        # Convertir a formato ReactFlow
        reactflow_model = kjb_parser.convert_to_reactflow(job_model)
        
        # Lista para almacenar información sobre los pipelines creados
        created_pipelines = []
        
        # Crear pipeline principal para el KJB
        main_pipeline_id = None
        
        if request.create_pipelines:
            # Determinar nombre del pipeline principal
            if request.pipeline_name_prefix:
                pipeline_name = f"{request.pipeline_name_prefix}_{os.path.basename(kjb_file.filename).replace('.kjb', '')}"
            else:
                pipeline_name = os.path.basename(kjb_file.filename).replace('.kjb', '')
            
            # Crear pipeline principal
            try:
                main_pipeline_id = pipeline_manager.create_pipeline(
                    name=pipeline_name,
                    description=f"Importado desde {kjb_file.filename}",
                    project_id=request.project_id
                )
                # Guardar la primera versión con los nodos y conexiones
                pipeline_manager.save_pipeline_version(
                    pipeline_id=main_pipeline_id,
                    nodes=reactflow_model.nodes,
                    edges=reactflow_model.edges,
                    transform_script="# Código de transformación importado desde KJB\n\n# El DataFrame de entrada está disponible como 'df_raw'\n# El DataFrame transformado debe asignarse a 'df_transformed'\n\ndf_transformed = df_raw.copy()",
                    config={"source_file": kjb_file.filename},
                    version_name="imported_from_kjb"
                )
                
                # Añadir a la lista de pipelines creados
                created_pipelines.append(PipelineInfo(
                    id=main_pipeline_id,
                    name=pipeline_name,
                    type="kjb",
                    source_file=kjb_file.filename
                ))
            except PipelineError as e:
                raise HTTPException(status_code=400, detail=f"Error creating main pipeline: {str(e)}")
        
        # Procesar archivos KTR referenciados si se solicita
        sub_pipelines = []
        if request.import_ktr_files and request.create_pipelines:
            # Inicializar el parser de KTR
            ktr_parser = KTRParserService()
            
            # Buscar referencias a archivos KTR en el modelo de trabajo
            for entry in job_model.entries:
                if entry.type == "TRANS" and entry.ktr_file:
                    ktr_file_path = entry.ktr_file
                    ktr_file_name = os.path.basename(ktr_file_path)
                    local_ktr_path = None
                    
                    # Buscar el archivo KTR entre los subidos por el usuario
                    for uploaded_ktr_name, uploaded_ktr_path in ktr_file_paths.items():
                        if ktr_file_name.lower() == uploaded_ktr_name.lower():
                            local_ktr_path = uploaded_ktr_path
                            break
                    
                    # Si no encontramos el archivo KTR entre los subidos, buscamos en el sistema de archivos
                    if not local_ktr_path and os.path.exists(ktr_file_path):
                        local_ktr_path = ktr_file_path
                    
                    # Determinar nombre del sub-pipeline
                    if request.pipeline_name_prefix:
                        sub_pipeline_name = f"{request.pipeline_name_prefix}_{ktr_file_name.replace('.ktr', '')}"
                    else:
                        sub_pipeline_name = ktr_file_name.replace('.ktr', '')
                    
                    # Crear un pipeline para el KTR
                    try:
                        sub_pipeline_id = pipeline_manager.create_pipeline(
                            name=sub_pipeline_name,
                            description=f"Sub-pipeline importado desde {ktr_file_name} (referenciado en {kjb_file.filename})",
                            project_id=request.project_id
                        )
                        
                        # Si tenemos el archivo KTR, lo parseamos y guardamos su contenido
                        if local_ktr_path:
                            try:
                                # Parsear el archivo KTR
                                _ = ktr_parser.parse_ktr_file(local_ktr_path)
                                
                                # Convertir a formato ReactFlow (esto requeriría implementar una función similar a convert_to_reactflow en KTRParserService)
                                # Por ahora, creamos un pipeline vacío
                                ktr_nodes = []
                                ktr_edges = []
                                
                                # Guardar la versión del pipeline
                                pipeline_manager.save_pipeline_version(
                                    pipeline_id=sub_pipeline_id,
                                    nodes=ktr_nodes,
                                    edges=ktr_edges,
                                    transform_script=f"# Código de transformación importado desde KTR: {ktr_file_name}\n\n# El DataFrame de entrada está disponible como 'df_raw'\n# El DataFrame transformado debe asignarse a 'df_transformed'\n\ndf_transformed = df_raw.copy()",
                                    config={"source_file": ktr_file_name},
                                    version_name="imported_from_ktr"
                                )
                            except Exception as e:
                                logger.error(f"Error parsing KTR file {ktr_file_name}: {str(e)}")
                        
                        # Añadir a la lista de sub-pipelines
                        sub_pipeline = PipelineInfo(
                            id=sub_pipeline_id,
                            name=sub_pipeline_name,
                            type="ktr",
                            source_file=ktr_file_path
                        )
                        sub_pipelines.append(sub_pipeline)
                        created_pipelines.append(sub_pipeline)
                    except PipelineError as e:
                        # No fallamos completamente si un sub-pipeline falla
                        logger.error(f"Error creating sub-pipeline for {ktr_file_path}: {str(e)}")
        
        # Preparar respuesta
        response = KJBImportResponse(
            main_pipeline=created_pipelines[0] if created_pipelines else PipelineInfo(
                id="",
                name=os.path.basename(kjb_file.filename).replace('.kjb', ''),
                type="kjb",
                source_file=kjb_file.filename
            ),
            sub_pipelines=sub_pipelines,
            nodes=reactflow_model.nodes,
            edges=reactflow_model.edges
        )
        
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing KJB file: {str(e)}")
    finally:
        # Limpiar los archivos temporales
        if os.path.exists(import_dir):
            shutil.rmtree(import_dir)
