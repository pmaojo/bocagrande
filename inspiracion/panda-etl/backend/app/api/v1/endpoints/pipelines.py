"""
Endpoints para la gestión de pipelines ETL.

Este módulo proporciona endpoints para:
- Crear, listar, cargar y eliminar pipelines
- Guardar, listar, cargar y eliminar versiones de pipelines
- Ejecutar pipelines de forma síncrona o asíncrona
- Monitorear el estado de los procesos de ejecución
"""

from typing import List, Dict, Any, Optional
from fastapi import APIRouter, Depends, HTTPException, Path
from pydantic import BaseModel, Field

from app.api.dependencies import get_pipeline_manager
from app.processing.pipeline_manager import PipelineManager, PipelineError
from app.models.process import ProcessStatus

router = APIRouter()

# Modelos Pydantic para las solicitudes y respuestas

class NodeModel(BaseModel):
    id: str
    type: str
    position: Dict[str, float]
    data: Dict[str, Any] = Field(default_factory=dict)

class EdgeModel(BaseModel):
    id: str
    source: str
    target: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None

class PipelineCreateRequest(BaseModel):
    name: str
    description: str = ""
    project_id: int

class PipelineResponse(BaseModel):
    id: str
    name: str
    description: str
    project_id: Optional[int] = None
    created_at: str
    updated_at: str
    version_count: int
    versions: List[str] = Field(default_factory=list)

class PipelineVersionCreateRequest(BaseModel):
    nodes: List[NodeModel]
    edges: List[EdgeModel]
    transform_script: str
    config: Dict[str, Any] = Field(default_factory=dict)
    version_name: Optional[str] = None
    description: str = ""

class PipelineVersionResponse(BaseModel):
    version_id: str
    pipeline_id: str
    created_at: str
    description: str
    nodes: List[Dict[str, Any]]
    edges: List[Dict[str, Any]]
    transform_script: str
    config: Dict[str, Any]

class PipelineExecuteRequest(BaseModel):
    version: Optional[str] = None
    input_config: Optional[Dict[str, Any]] = None
    output_config: Optional[Dict[str, Any]] = None

class PipelineScheduleRequest(BaseModel):
    pipeline_id: str
    project_id: int
    version: Optional[str] = None
    input_config: Optional[Dict[str, Any]] = None
    output_config: Optional[Dict[str, Any]] = None
    name: str = ""

class PipelineExecutionResponse(BaseModel):
    pipeline_id: str
    version: Optional[str] = None
    execution_time: float
    rows_processed: int
    rows_output: int
    columns_input: List[str]
    columns_output: List[str]
    executed_at: str
    status: str

class ProcessResponse(BaseModel):
    id: int
    name: str
    type: str
    status: str
    project_id: int
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    message: str
    details: Optional[Dict[str, Any]] = None
    output: Optional[Dict[str, Any]] = None

# Endpoints para pipelines

@router.post("/pipelines", response_model=PipelineResponse, status_code=201)
def create_pipeline(
    pipeline: PipelineCreateRequest,
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Crea un nuevo pipeline.
    """
    try:
        pipeline_id = pipeline_manager.create_pipeline(
            name=pipeline.name,
            description=pipeline.description,
            project_id=pipeline.project_id
        )
        return pipeline_manager.load_pipeline(pipeline_id)
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/pipelines", response_model=List[PipelineResponse])
def list_pipelines(
    project_id: Optional[int] = None,
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Lista todos los pipelines disponibles.
    Opcionalmente filtra por project_id.
    """
    try:
        return pipeline_manager.list_pipelines(project_id=project_id)
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/pipelines/{pipeline_id}", response_model=PipelineResponse)
def get_pipeline(
    pipeline_id: str = Path(..., description="ID del pipeline"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Obtiene los detalles de un pipeline específico.
    """
    try:
        return pipeline_manager.load_pipeline(pipeline_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/pipelines/{pipeline_id}", status_code=204)
def delete_pipeline(
    pipeline_id: str = Path(..., description="ID del pipeline"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Elimina un pipeline y todas sus versiones.
    """
    try:
        pipeline_manager.delete_pipeline(pipeline_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))

# Endpoints para versiones de pipelines

@router.post("/pipelines/{pipeline_id}/versions", response_model=PipelineVersionResponse, status_code=201)
def create_pipeline_version(
    version: PipelineVersionCreateRequest,
    pipeline_id: str = Path(..., description="ID del pipeline"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Crea una nueva versión de un pipeline existente.
    """
    try:
        # Convertir los modelos Pydantic a diccionarios para el pipeline_manager
        nodes = [node.dict() for node in version.nodes]
        edges = [edge.dict() for edge in version.edges]
        
        version_id = pipeline_manager.save_pipeline_version(
            pipeline_id=pipeline_id,
            nodes=nodes,
            edges=edges,
            transform_script=version.transform_script,
            config=version.config,
            version_name=version.version_name,
            description=version.description
        )
        
        return pipeline_manager.load_pipeline_version(pipeline_id, version_id)
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/pipelines/{pipeline_id}/versions", response_model=List[Dict[str, Any]])
def list_pipeline_versions(
    pipeline_id: str = Path(..., description="ID del pipeline"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Lista todas las versiones de un pipeline.
    """
    try:
        return pipeline_manager.list_pipeline_versions(pipeline_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.get("/pipelines/{pipeline_id}/versions/{version_id}", response_model=PipelineVersionResponse)
def get_pipeline_version(
    pipeline_id: str = Path(..., description="ID del pipeline"),
    version_id: str = Path(..., description="ID de la versión"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Obtiene los detalles de una versión específica de un pipeline.
    """
    try:
        return pipeline_manager.load_pipeline_version(pipeline_id, version_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.delete("/pipelines/{pipeline_id}/versions/{version_id}", status_code=204)
def delete_pipeline_version(
    pipeline_id: str = Path(..., description="ID del pipeline"),
    version_id: str = Path(..., description="ID de la versión"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Elimina una versión específica de un pipeline.
    """
    try:
        pipeline_manager.delete_pipeline_version(pipeline_id, version_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))

# Endpoints para ejecución de pipelines

@router.post("/pipelines/{pipeline_id}/execute", response_model=PipelineExecutionResponse)
def execute_pipeline(
    execute_request: PipelineExecuteRequest,
    pipeline_id: str = Path(..., description="ID del pipeline"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Ejecuta un pipeline de forma síncrona.
    """
    try:
        result = pipeline_manager.execute_pipeline(
            pipeline_id=pipeline_id,
            version=execute_request.version,
            input_config=execute_request.input_config,
            output_config=execute_request.output_config
        )
        return result
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/pipelines/schedule", response_model=Dict[str, Any])
def schedule_pipeline_execution(
    schedule_request: PipelineScheduleRequest,
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Programa la ejecución asíncrona de un pipeline.
    """
    try:
        process_id = pipeline_manager.schedule_pipeline_execution(
            pipeline_id=schedule_request.pipeline_id,
            project_id=schedule_request.project_id,
            version=schedule_request.version,
            input_config=schedule_request.input_config,
            output_config=schedule_request.output_config,
            name=schedule_request.name
        )
        
        return {
            "process_id": process_id,
            "status": "scheduled",
            "message": f"Pipeline scheduled for execution with process ID {process_id}"
        }
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

# Endpoints para procesos

@router.get("/processes", response_model=List[Dict[str, Any]])
def list_processes(
    project_id: Optional[int] = None,
    status: Optional[str] = None,
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Lista todos los procesos disponibles.
    Opcionalmente filtra por project_id y status.
    """
    try:
        # Convertir el status de string a enum si se proporciona
        status_enum = None
        if status:
            try:
                status_enum = ProcessStatus[status.upper()]
            except KeyError:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Invalid status. Valid values are: {', '.join([s.name for s in ProcessStatus])}"
                )
        
        return pipeline_manager.list_processes(project_id=project_id, status=status_enum)
    except PipelineError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/processes/{process_id}", response_model=ProcessResponse)
def get_process(
    process_id: int = Path(..., description="ID del proceso"),
    pipeline_manager: PipelineManager = Depends(get_pipeline_manager)
):
    """
    Obtiene el estado actual de un proceso.
    """
    try:
        return pipeline_manager.get_process_status(process_id)
    except PipelineError as e:
        raise HTTPException(status_code=404, detail=str(e))
