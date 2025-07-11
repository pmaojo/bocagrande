from fastapi import APIRouter, File, UploadFile, HTTPException
import shutil
import os

from app.logger import get_logger

# Adjust import path based on the new location if necessary, 
# but app.pentaho_importer should work if 'app' is the root of the FastAPI project recognized by PYTHONPATH
from app.pentaho_importer import (
    KTRParserService,
    KJBParserService,
    TransformationModelSchema,
)

router = APIRouter(
    prefix="/pentaho", # This will be /api/v1/pentaho
    tags=["Pentaho Importer"],
)

logger = get_logger(__name__)

# Define a temporary directory for uploads if it doesn't exist
TEMP_UPLOAD_DIR = "/tmp/ktr_uploads"
os.makedirs(TEMP_UPLOAD_DIR, exist_ok=True)

@router.post("/upload-ktr/", response_model=TransformationModelSchema)
async def upload_and_parse_ktr(file: UploadFile = File(...)):
    """
    Receives a KTR file, saves it temporarily, parses it using KTRParserService,
    and returns the parsed TransformationModel as JSON.
    The final endpoint will be /api/v1/pentaho/upload-ktr/
    """
    if not file.filename.endswith('.ktr'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only .ktr files are allowed.")

    temp_file_path = os.path.join(TEMP_UPLOAD_DIR, file.filename)

    try:
        # Save the uploaded file temporarily
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Parse the KTR file
        parser = KTRParserService()
        transformation_model = parser.parse_ktr_file(temp_file_path)
        
        return TransformationModelSchema.model_validate(transformation_model)

    except HTTPException as e:
        # Re-raise HTTPExceptions to let FastAPI handle them
        raise e
    except Exception as e:
        # Log the exception for debugging
        logger.error(f"Error processing KTR file: {e}")
        raise HTTPException(status_code=500, detail=f"An error occurred while processing the KTR file: {str(e)}")
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        await file.close()

# Endpoint para importar archivos KJB
@router.post("/kjb/import/", response_model=None)
async def upload_and_parse_kjb(file: UploadFile = File(...)):
    """
    Recibe un archivo KJB, lo guarda temporalmente, lo parsea usando KJBParserService,
    y devuelve el modelo convertido a formato ReactFlow como JSON.
    El endpoint final será /api/v1/pentaho/kjb/import/
    """
    if not file.filename.endswith('.kjb'):
        raise HTTPException(status_code=400, detail="Invalid file type. Only .kjb files are allowed.")

    temp_file_path = os.path.join(TEMP_UPLOAD_DIR, file.filename)

    try:
        # Guardar el archivo subido temporalmente
        with open(temp_file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # Parsear el archivo KJB
        parser = KJBParserService()
        job_model = parser.parse_kjb_file(temp_file_path)
        
        # Convertir a formato ReactFlow
        reactflow_model = parser.convert_to_reactflow(job_model)
        
        # Devolver el modelo como JSON
        return reactflow_model.dict()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing KJB file: {str(e)}")
    finally:
        # Limpiar el archivo temporal
        if os.path.exists(temp_file_path):
            os.remove(temp_file_path)

# TODO:
# 1. Define Pydantic models for TransformationModel and its components for proper response validation and OpenAPI schema generation.
# 2. Integrate this router into the main FastAPI application (e.g., in main.py or app.py).
# 3. Add more robust error handling and logging.
