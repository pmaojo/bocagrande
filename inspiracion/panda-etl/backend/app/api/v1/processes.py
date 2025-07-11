import io
import os
import traceback
import zipfile

from app.processing.process_queue import submit_process
from app.requests import get_user_usage_data
import dateparser
from fastapi import APIRouter, Depends, HTTPException, Response
from fastapi.responses import FileResponse, StreamingResponse
from sqlalchemy.orm import Session
import csv
from io import StringIO

from app.database import get_db
from app.repositories import process_repository, user_repository
from app.repositories import project_repository
from app.models import ProcessStatus, ProcessStep
from app.schemas.process import ProcessData, ProcessSuggestion, ProcessFromKTRData

from app.models.process_step import ProcessStepStatus
from app.logger import get_logger

process_router = APIRouter()

logger = get_logger(__name__)


@process_router.post("/from-ktr")
def create_process_from_ktr_endpoint(
    ktr_data: ProcessFromKTRData,
    db: Session = Depends(get_db)
    # current_user: models.User = Depends(get_current_active_user) # Add if auth is needed
):
    project = project_repository.get_project(db, project_id=ktr_data.project_id)
    if not project:
        raise HTTPException(status_code=404, detail=f"Project with id {ktr_data.project_id} not found.")

    # Optional: Validate user access to the project if using authentication
    # if current_user and project.user_id != current_user.id:
    #     raise HTTPException(status_code=403, detail="User does not have access to this project.")

    try:
        new_process = process_repository.create_process_from_ktr(db=db, process_data=ktr_data)
    except ValueError as e:
        logger.error(f"Validation error creating process from KTR: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating process from KTR: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal server error creating process from KTR.")

    return {
        "status": "success",
        "message": "Process successfully created from KTR",
        "data": {
            "id": new_process.id,
            "name": new_process.name,
            "type": new_process.type,
            "status": str(new_process.status.name), # Return status name
            "project_id": new_process.project_id,
            "project_name": project.name, # Add project name
            "details": new_process.details, # Or a subset/confirmation
            "message": new_process.message,
            "created_at": new_process.created_at.isoformat() if new_process.created_at else None,
            "updated_at": new_process.updated_at.isoformat() if new_process.updated_at else None,
        },
    }


@process_router.get("/{process_id}")
def get_process(process_id: int, db: Session = Depends(get_db)):
    process = process_repository.get_process(db=db, process_id=process_id)

    if not process:
        raise HTTPException(status_code=404, detail="Process not found")

    return {
        "status": "success",
        "message": "Process successfully returned",
        "data": {
            "id": process.id,
            "name": process.name,
            "type": process.type,
            "status": process.status,
            "project": process.project.name,
            "project_id": f"{process.project_id}",
            "details": process.details,
            "output": process.output,
            "started_at": (
                process.started_at.isoformat() if process.started_at else None
            ),
            "completed_at": (
                process.completed_at.isoformat() if process.completed_at else None
            ),
            "created_at": (
                process.created_at.isoformat() if process.created_at else None
            ),
            "updated_at": (
                process.updated_at.isoformat() if process.updated_at else None
            ),
        },
    }


@process_router.get("")
def get_processes(db: Session = Depends(get_db)):
    processes = process_repository.get_processes(db=db)

    return {
        "status": "success",
        "message": "Processes successfully returned",
        "data": [
            {
                "id": process.id,
                "name": process.name,
                "type": process.type,
                "status": process.status,
                "project": process.project.name,
                "project_id": f"{process.project_id}",
                "details": process.details,
                "started_at": (
                    process.started_at.isoformat() if process.started_at else None
                ),
                "completed_at": (
                    process.completed_at.isoformat() if process.completed_at else None
                ),
                "created_at": (
                    process.created_at.isoformat() if process.created_at else None
                ),
                "updated_at": (
                    process.updated_at.isoformat() if process.updated_at else None
                ),
                "completed_step_count": step_count,
            }
            for process, step_count in processes
        ],
    }


@process_router.post("/start")
def start_process(process_data: ProcessData, db: Session = Depends(get_db)):

    user_api_key = user_repository.get_user_api_key(db)
    if not user_api_key:
        # This case might occur if no user/API key is set up, handle as needed
        raise HTTPException(status_code=500, detail="API key for usage tracking not found.")

    usage_data = get_user_usage_data(user_api_key.key)

    if usage_data["credits_used"] >= usage_data["total_credits"]:
        raise HTTPException(
            status_code=402,
            detail="Credit limit Reached, Wait next month or upgrade your Plan",
        )

    # Create the process record first. process_data.data will be stored in new_process_record.details
    new_process_record = process_repository.create_process(db, process_data)

    if process_data.type == "python_script":
        if not process_data.target_asset_id:
            # Rollback or delete the created process if target_asset_id is missing for python_script type
            # For simplicity here, we'll raise an error. Consider cleanup for robust implementation.
            # db.delete(new_process_record)
            # db.commit()
            raise HTTPException(status_code=400, detail="'target_asset_id' is required for 'python_script' process type.")

        target_asset = project_repository.get_asset(db, asset_id=process_data.target_asset_id)
        if not target_asset or target_asset.project_id != new_process_record.project_id:
            # db.delete(new_process_record)
            # db.commit()
            raise HTTPException(status_code=404, detail=f"Target asset with id {process_data.target_asset_id} not found in project {new_process_record.project_id}.")

        if not process_data.data or 'script_code' not in process_data.data:
            # db.delete(new_process_record)
            # db.commit()
            raise HTTPException(status_code=400, detail="'script_code' is missing in 'data' field for 'python_script' process type.")

        process_step = ProcessStep(
            process_id=new_process_record.id,
            asset_id=target_asset.id,
            output=None,
            status=ProcessStepStatus.PENDING,
        )
        db.add(process_step)
        db.commit()
        logger.log(f"Created single ProcessStep for python_script process {new_process_record.id} with asset {target_asset.id}")
    else:
        # Existing logic for other process types (e.g., 'extract')
        assets_tuple = project_repository.get_assets(db, new_process_record.project_id)
        # assets_tuple is a tuple like (assets_list, total_count)
        # We need to check if assets_list (assets_tuple[0]) is empty

        if not assets_tuple or not assets_tuple[0]:
            # db.delete(new_process_record) # Optional: cleanup created process
            # db.commit()
            raise HTTPException(status_code=404, detail=f"No Assets found in project {new_process_record.project_id} for process type {process_data.type}!")

        for asset in assets_tuple[0]:
            process_step = ProcessStep(
                process_id=new_process_record.id,
                asset_id=asset.id,
                output=None,
                status=ProcessStepStatus.PENDING,
            )
            db.add(process_step)
            db.commit()
        logger.log(f"Created ProcessSteps for all assets in project for process {new_process_record.id}")

    logger.log(f"Add process {new_process_record.id} to the queue")
    submit_process(new_process_record.id)

    return {
        "status": "success",
        "message": "Process successfully started", # Changed message for clarity
        "data": [
            {
                "id": new_process_record.id,
                "name": new_process_record.name,
                "type": new_process_record.type,
                "status": new_process_record.status,
                "project": new_process_record.project.name, # Assumes project relationship is loaded
                "project_id": f"{new_process_record.project_id}",
                "target_asset_id": process_data.target_asset_id if process_data.type == "python_script" else None
            }
        ],
    }


@process_router.post("/{process_id}/stop")
def stop_processes(process_id: int, db: Session = Depends(get_db)):

    process = process_repository.get_process(db, process_id)

    if process.status in [ProcessStatus.IN_PROGRESS, ProcessStatus.PENDING]:
        process.status = ProcessStatus.STOPPED
        db.commit()
    else:
        raise HTTPException(
            status_code=404, detail="Process not in a state to be stopped"
        )

    return {
        "status": "success",
        "message": "Processes successfully stopped!",
        "data": [{"id": process.id, "type": process.type, "status": process.status}],
    }


@process_router.post("/{process_id}/resume")
def resume_processes(process_id: int, db: Session = Depends(get_db)):

    user_api_key = user_repository.get_user_api_key(db)

    usage_data = get_user_usage_data(user_api_key.key)

    if usage_data["credits_used"] >= usage_data["total_credits"]:
        raise HTTPException(
            status_code=402,
            detail="Credit limit Reached, Wait next month or upgrade your Plan",
        )

    process = process_repository.get_process(db, process_id)

    if process.status in [ProcessStatus.STOPPED, ProcessStatus.FAILED]:
        process.status = ProcessStatus.PENDING
        db.commit()
        logger.log(f"Add to process {process.id} to the queue")
        submit_process(process.id)

    else:
        raise HTTPException(status_code=400, detail="Process not found!")

    return {
        "status": "success",
        "message": "Processes successfully stopped!",
        "data": [{"id": process.id, "type": process.type, "status": process.status}],
    }


@process_router.get("/{process_id}/download-csv")
def download_process(process_id: int, db: Session = Depends(get_db)):
    process = process_repository.get_process(db=db, process_id=process_id)
    if not process:
        return {
            "status": "error",
            "message": "Process not found",
            "data": None,
        }

    process_steps = process_repository.get_process_steps(db=db, process_id=process_id)
    if not process_steps:
        return {
            "status": "error",
            "message": "Process steps not found",
            "data": None,
        }

    completed_steps = [
        step for step in process_steps if step.status == ProcessStepStatus.COMPLETED
    ]
    if not completed_steps:
        return {
            "status": "error",
            "message": "No completed steps found",
            "data": None,
        }

    # Initialize the CSV buffer and writer
    csv_buffer = StringIO()
    csv_writer = csv.writer(
        csv_buffer, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
    )

    # Fetch date columns and number columns
    date_columns = []
    number_columns = []
    if process.type == "extract":
        if "fields" in process.details:
            for field in process.details["fields"]:
                if field["type"] == "date":
                    date_columns.append(field["key"])
                elif field["type"] == "number":
                    number_columns.append(field["key"])

    # Write the header
    headers = ["Filename"]
    if process.type == "extract" and completed_steps:
        # Extract headers from the first completed step's output keys
        headers += list(
            completed_steps[0].output[0].keys()
        )  # Assuming output is a list of dicts
    else:
        headers.append("summary")
    headers.append("___process_step_id")
    headers.append("___extraction_index")
    csv_writer.writerow(headers)

    def format_number(value):
        if value is None:
            return ""  # or return a default value like "N/A"
        try:
            float_value = float(value)
            if float_value.is_integer():
                return int(float_value)
            else:
                return float_value
        except ValueError:
            return value

    # Write data rows
    for step in completed_steps:
        if process.type == "extract":
            for index, output in enumerate(step.output):
                row = [step.asset.filename]
                for key in headers[1:-2]:  # Skip "Filename" column
                    value = output.get(key, "")
                    if key in date_columns:
                        try:
                            parsed_date = dateparser.parse(value)
                            if parsed_date:
                                value = parsed_date.strftime("%d-%m-%Y")
                        except Exception as e:
                            logger.error(
                                f"Unable to parse date {value}, fallback to extracted text. Error: {e}"
                            )
                    elif key in number_columns:
                        value = format_number(value)
                    row.append(value)
                row.append(step.id)
                row.append(index)
                csv_writer.writerow(row)
        else:
            row = [step.asset.filename, step.output.get("summary", ""), step.id, 0]
            csv_writer.writerow(row)

    # Get the CSV content from the buffer
    csv_content = csv_buffer.getvalue()

    # Create a Response object with the CSV data
    response = Response(content=csv_content)
    response.headers["Content-Disposition"] = (
        f"attachment; filename=process_{process_id}.csv"
    )
    response.headers["Content-Type"] = "text/csv"

    return response


@process_router.get("/{process_id}/get-csv")
def get_csv_content(process_id: int, db: Session = Depends(get_db)):
    process = process_repository.get_process(db=db, process_id=process_id)
    if not process:
        return {
            "status": "error",
            "message": "Process not found",
            "data": None,
        }

    process_steps = process_repository.get_process_steps(db=db, process_id=process_id)
    if not process_steps:
        return {
            "status": "error",
            "message": "Process steps not found",
            "data": None,
        }

    completed_steps = [
        step
        for step in process_steps
        if step.status == ProcessStepStatus.COMPLETED and step.output is not None
    ]
    if not completed_steps:
        return {
            "status": "error",
            "message": "No completed steps found",
            "data": None,
        }

    # Initialize the CSV buffer and writer
    csv_buffer = StringIO()
    csv_writer = csv.writer(
        csv_buffer, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL
    )

    # Fetch date columns and number columns
    date_columns = []
    number_columns = []
    if process.type == "extract":
        if "fields" in process.details:
            for field in process.details["fields"]:
                if field["type"] == "date":
                    date_columns.append(field["key"])
                elif field["type"] == "number":
                    number_columns.append(field["key"])

    # Write the header
    headers = ["Filename"]
    if process.type == "extract" and completed_steps:
        # Extract headers from the first completed step's output keys
        headers += list(
            completed_steps[0].output[0].keys()
        )  # Assuming output is a list of dicts
    else:
        headers.append("summary")

    headers.append("___process_step_id")
    headers.append("___extraction_index")
    csv_writer.writerow(headers)

    logger.info(completed_steps)

    def format_number(value):
        if value is None:
            return ""  # or return a default value like "N/A"
        try:
            float_value = float(value)
            if float_value.is_integer():
                return int(float_value)
            else:
                return float_value
        except ValueError:
            return value

    # Write data rows
    for step in completed_steps:
        if process.type == "extract":
            for index, output in enumerate(step.output):
                row = [step.asset.filename]
                for key in headers[1:-2]:  # Skip "Filename" column
                    value = output.get(key, "")
                    if key in date_columns:
                        try:
                            parsed_date = dateparser.parse(value)
                            if parsed_date:
                                value = parsed_date.strftime("%d-%m-%Y")
                        except Exception as e:
                            logger.error(
                                f"Unable to parse date {value}, fallback to extracted text. Error: {e}"
                            )
                    elif key in number_columns:
                        value = format_number(value)
                    row.append(value)

                row.append(step.id)
                row.append(index)
                csv_writer.writerow(row)
        else:
            row = [step.asset.filename, step.output.get("summary", ""), step.id, 0]
            csv_writer.writerow(row)

    # Get the CSV content from the buffer
    csv_content = csv_buffer.getvalue()

    return {
        "status": "success",
        "message": "CSV content generated successfully",
        "data": {"csv": csv_content},
    }


@process_router.get("/{process_id}/get-steps")
def get_process_steps(process_id: int, db: Session = Depends(get_db)):
    process_steps = process_repository.get_process_steps(db, process_id)

    if not process_steps:
        raise Exception("No process found!")

    return {
        "status": "success",
        "message": "Process steps successfully returned",
        "data": process_steps,
    }


@process_router.get(
    "/{process_id}/download-highlighted-pdf-zip", response_class=StreamingResponse
)
def download_process_steps_zip(process_id: int, db: Session = Depends(get_db)):

    process = process_repository.get_process(db=db, process_id=process_id)
    if not process:
        raise HTTPException(status_code=404, detail="No process found!")

    if process.type != "extractive_summary":
        raise HTTPException(status_code=404, detail="No highlighted pdf found!")

    process_steps = process_repository.get_process_steps(db, process_id)

    if not process_steps:
        raise HTTPException(status_code=404, detail="No process steps found!")

    # Initialize an in-memory file to store the zip data
    memory_file = io.BytesIO()

    file_exists = False
    # Create a zip archive in the memory file
    with zipfile.ZipFile(memory_file, "w", zipfile.ZIP_DEFLATED) as zipf:
        for step in process_steps:

            if step.output and "highlighted_pdf" in step.output:

                pdf_path = step.output["highlighted_pdf"]

                filename = os.path.basename(pdf_path)

                # Read the PDF file content
                try:
                    with open(pdf_path, "rb") as pdf_file:
                        pdf_content = pdf_file.read()

                    zipf.writestr(filename, pdf_content)

                    file_exists = True
                except FileNotFoundError:
                    continue

    if not file_exists:
        raise HTTPException(status_code=404, detail="No Highlighted pdf's exists!")

    # Set the pointer to the beginning of the in-memory file
    memory_file.seek(0)

    # Return the zip file as a StreamingResponse
    return StreamingResponse(
        memory_file,
        media_type="application/zip",
        headers={
            "Content-Disposition": f"attachment; filename=highlighted_pdfs_{process_id}.zip"
        },
    )


@process_router.get("/{process_id}/steps/{step_id}/download")
async def get_file(step_id: int, db: Session = Depends(get_db)):
    try:
        process_step = process_repository.get_process_step(db, step_id)

        if process_step is None or "highlighted_pdf" not in process_step.output:
            raise HTTPException(status_code=404, detail="No process step detail found!")

        filepath = process_step.output["highlighted_pdf"]

        # Check if the file exists
        if not os.path.isfile(filepath):
            raise HTTPException(status_code=404, detail="File not found on server")

        # Return the file
        return FileResponse(
            filepath,
            media_type="application/pdf",
            filename=f"highlighted_{process_step.asset.filename}",
        )

    except Exception as e:
        logger.error(f"Error retrieving file: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to retrieve file")


@process_router.post("/suggestion")
def get_process_suggestion(
    process_data: ProcessSuggestion, db: Session = Depends(get_db)
):
    try:
        processes = process_repository.search_relevant_process(db, process_data)

        return {
            "status": "success",
            "message": "Process steps successfully returned",
            "data": processes,
        }

    except Exception as e:
        logger.error(f"Error retrieving file: {str(e)}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Failed to retrieve file")
