from functools import wraps
from typing import List
from app.database import SessionLocal
from app.exceptions import CreditLimitExceededException
from app.models.asset_content import AssetProcessingStatus
from app.processing.process_scheduler import ProcessScheduler
from app.repositories import process_repository
from app.repositories import project_repository, ProcessRepositoryImpl
from app.interfaces.process_repository import IProcessRepository
from concurrent.futures import ThreadPoolExecutor
from app.models import ProcessStatus
from app.requests import (
    extract_data,
)
from datetime import datetime
from app.models.process_step import ProcessStepStatus
from app.repositories import user_repository
from app.config import settings
import concurrent.futures
from app.logger import Logger
import traceback

from app.utils import clean_text
from app.vectorstore.chroma import ChromaDB
import re
import pandas as pd
import os

executor = ThreadPoolExecutor(max_workers=5)

logger = Logger()

# Default process repository used when none is provided
DEFAULT_PROCESS_REPO: IProcessRepository = ProcessRepositoryImpl()


def submit_process(process_id: int) -> None:
    executor.submit(process_task, process_id)


process_execution_scheduler = ProcessScheduler(60, submit_process, logger)

# Background task processing function
def process_step_task(
    process_id: int,
    process_step_id: int,
    summaries: List[str],
    failed_docs: List[int],
    api_key: str,
):
    try:
        # Initial DB operations (open and fetch relevant data)
        with SessionLocal() as db:
            process = process_repository.get_process(db, process_id)
            project_id = process.project_id
            process_step = process_repository.get_process_step(db, process_step_id)
            filename =  process_step.asset.filename
            if process.status == ProcessStatus.STOPPED:
                return False  # Stop processing if the process is stopped

            logger.log(f"Processing file: {process_step.asset.path}")
            if process_step.status == ProcessStepStatus.COMPLETED and process.type!="extract":
                summaries.append(process_step.output.get("summary", ""))
                return True

            # Mark step as in progress
            update_process_step_status(db, process_step, ProcessStepStatus.IN_PROGRESS)

            retries = 0
            success = False
            asset_content = project_repository.get_asset_content(
                db, asset_id=process_step.asset.id
            )

        # Move the expensive external operations out of the DB session
        while retries < settings.max_retries and not success:
            try:
                if process.type == "extract":
                    # Handle non-extractive summary process
                    data = extract_process(
                        api_key, process, process_step, asset_content
                    )

                    # Update process step output outside the expensive operations
                    with SessionLocal() as db:
                        process_step = process_repository.get_process_step(db, process_step_id) # Re-fetch to attach to current session
                        update_process_step_status(
                            db,
                            process_step,
                            ProcessStepStatus.COMPLETED,
                            output=data["fields"],
                            output_references=data["context"],
                        )
                        db.commit()

                    # vectorize extraction result
                    try:
                        vectorize_extraction_process_step(project_id=project_id,
                                                        process_step_id=process_step_id,
                                                        filename=filename,
                                                        references=data["context"])
                    except Exception :
                        logger.error(f"Failed to vectorize extraction results for chat {traceback.format_exc()}")
                
                elif process.type == "python_script":
                    logger.log(f"Processing step for python_script: {process_step.id} with asset {process_step.asset.path}")
                    script_code = process.details.get('script_code')
                    if not script_code:
                        raise ValueError("script_code not found in process.details")

                    input_file_path = process_step.asset.path
                    if not os.path.exists(input_file_path):
                         raise FileNotFoundError(f"Input asset file not found: {input_file_path}")

                    df_raw = pd.read_csv(input_file_path)
                    
                    execution_globals = {'pd': pd, 'df_raw': df_raw, 'df_transformed': None}
                    
                    try:
                        exec(script_code, execution_globals)
                        df_transformed = execution_globals.get('df_transformed')
                    except Exception as e:
                        logger.error(f"Error executing user script: {e}\n{traceback.format_exc()}")
                        raise # Re-raise to be caught by the outer try-except and mark step as FAILED

                    if df_transformed is None:
                        raise ValueError("Script did not produce 'df_transformed' DataFrame.")

                    # Define output path
                    processed_dir = settings.process_dir # Corrected from processed_dir
                    if not os.path.exists(processed_dir):
                        os.makedirs(processed_dir)
                    
                    base_filename = os.path.basename(input_file_path)
                    name, ext = os.path.splitext(base_filename)
                    output_filename = f"{name}_transformed_{process_step.id}{ext}"
                    output_file_path = os.path.join(processed_dir, output_filename)
                    
                    df_transformed.to_csv(output_file_path, index=False)
                    logger.log(f"Transformed data saved to: {output_file_path}")

                    with SessionLocal() as db:
                        process_step = process_repository.get_process_step(db, process_step_id) # Re-fetch to attach to current session
                        update_process_step_status(
                            db,
                            process_step,
                            ProcessStepStatus.COMPLETED,
                            output={"output_file_path": output_file_path}
                        )
                        db.commit()
                success = True

            except CreditLimitExceededException:
                with SessionLocal() as db:
                    process = process_repository.get_process(db, process_id)
                    process_repository.update_process_status(
                        db, process, ProcessStatus.STOPPED
                    )

            except Exception:
                logger.error(traceback.format_exc())
                retries += 1
                if retries == settings.max_retries:
                    failed_docs.append(process_step.asset.id)
                    with SessionLocal() as db:
                        update_process_step_status(
                            db, process_step, ProcessStepStatus.FAILED
                        )

        return True

    except Exception:
        logger.error(traceback.format_exc())
        return False


def process_task(process_id: int):
    try:
        # Step 1: Fetch process details from the database and update its status
        with SessionLocal() as db:
            process = process_repository.get_process(db, process_id)
            process.status = ProcessStatus.IN_PROGRESS
            process.started_at = datetime.utcnow()
            db.commit()

            process_steps = process_repository.get_process_steps_with_asset_content(db, process.id, [ProcessStepStatus.PENDING.name, ProcessStepStatus.FAILED.name, ProcessStepStatus.IN_PROGRESS.name])
            if not process_steps:
                raise Exception("No process found!")

            api_key = user_repository.get_user_api_key(db)
            api_key = api_key.key
            db.refresh(process)

        # Step 2: Process each step in parallel outside the database connection
        failed_docs = []
        summaries = []

        ready_process_steps = [process_step for process_step in process_steps if process_step.asset.content.processing == AssetProcessingStatus.COMPLETED]

        all_process_steps_ready = len(ready_process_steps) == len(process_steps) # Check if all process steps are ready

        # Step 3: Concurrently process all process steps
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(
                    process_step_task,
                    process.id,
                    process_step.id,
                    summaries,
                    failed_docs,
                    api_key,
                )
                for process_step in ready_process_steps
            ]
            # Wait for all submitted tasks to complete
            concurrent.futures.wait(futures)

        # Step 4: After all steps are processed, update the process status and output in the DB
        with SessionLocal() as db:
            process = process_repository.get_process(db, process_id)

            if process.status != ProcessStatus.STOPPED:
                # If summary extraction was performed, add it to the process output
                if not all_process_steps_ready:
                    logger.info(f"Process id: [{process.id}] some steps preprocessing is missing moving to waiting queue")
                    process_execution_scheduler.add_process_to_queue(process.id)
                    db.commit()
                    # Skip status update since not all steps are ready
                    return

                process.status = (
                    ProcessStatus.COMPLETED if not failed_docs else ProcessStatus.FAILED
                )
                process.completed_at = datetime.utcnow()

            db.commit()

    except Exception as e:
        logger.error(traceback.format_exc())
        # Step 5: Handle failure cases and update the status accordingly
        with SessionLocal() as db:
            process = process_repository.get_process(db, process_id)
            process.status = ProcessStatus.FAILED
            process.message = str(e)
            db.commit()


def handle_exceptions(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except CreditLimitExceededException:
            logger.error("Credit limit exceeded")
            raise
        except Exception as e:
            logger.error(f"Error in {func.__name__}: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    return wrapper


def _collect_pdf_content(process, process_step, asset_content) -> str:
    """Return relevant PDF segments from the vector store if available.

    Parameters
    ----------
    process: The current process object containing details about the fields.
    process_step: The process step for which extraction is running.
    asset_content: Stored asset content including word count information.

    Returns
    -------
    str
        Concatenated PDF content or an empty string if retrieval fails.
    """

    if not (
        ("multiple_fields" not in process.details or not process.details["multiple_fields"])
        and asset_content.content
        and asset_content.content.get("word_count", 0) > 500
    ):
        return ""

    try:
        segment_store = ChromaDB(f"panda-etl-{process.project_id}", similarity_threshold=3)
        pdf_segments = []
        for field in process.details["fields"]:
            relevant = segment_store.get_relevant_docs(
                field["key"],
                where={
                    "$and": [
                        {"asset_id": process_step.asset.id},
                        {"project_id": process.project_id},
                    ]
                },
                k=5,
            )

            for index, metadata in enumerate(relevant["metadatas"][0]):
                segment_data = [relevant["documents"][0][index]]

                if metadata.get("previous_sentence_id", -1) != -1:
                    prev = segment_store.get_relevant_docs_by_id(
                        ids=[metadata["previous_sentence_id"]]
                    )
                    if prev["documents"] and len(prev["documents"][0]) > 0:
                        segment_data = [prev["documents"][0]] + segment_data
                    else:
                        logger.warning("Previous sentence document is empty.")

                if metadata.get("next_sentence_id", -1) != -1:
                    nxt = segment_store.get_relevant_docs_by_id(
                        ids=[metadata["next_sentence_id"]]
                    )
                    if nxt["documents"] and len(nxt["documents"][0]) > 0:
                        segment_data.append(nxt["documents"][0])
                    else:
                        logger.warning("Next sentence document is empty.")

                pdf_segments.append(" ".join(segment_data))

        return "\n".join(pdf_segments)
    except Exception as exc:  # pragma: no cover - log and fallback
        logger.error(f"Error fetching pdf segments: {exc}")
        logger.error(traceback.format_exc())
        return ""

@handle_exceptions
def extract_process(api_key, process, process_step, asset_content):
    pdf_content = _collect_pdf_content(process, process_step, asset_content)

    if not pdf_content:
        pdf_content = (
            "\n".join(item["text"] for item in asset_content.content.get("content", []) if "text" in item)
            if asset_content.content
            else None
        )

    data = extract_data(
        api_key,
        process.details,
        file_path=(process_step.asset.path if not pdf_content else None),
        pdf_content=pdf_content if pdf_content else None,
    )

    vectorstore = ChromaDB(f"panda-etl-{process.project_id}", similarity_threshold=3)
    all_relevant_docs = []

    for references in data.references:
        for reference in references:
            page_numbers = []
            for source_index, source in enumerate(reference.sources):
                if len(source) < 30:

                    best_match = find_best_match_for_short_reference(
                        source,
                        all_relevant_docs,
                        process_step.asset.id,
                        process.project_id
                    )
                    if best_match:
                        reference.sources[source_index] = best_match["text"]
                        page_numbers.append(best_match["page_number"])

                else:
                    relevant_docs = vectorstore.get_relevant_docs(
                        source,
                        where={
                            "$and": [
                                {"asset_id": process_step.asset.id},
                                {"project_id": process.project_id},
                            ]
                        },
                        k=5,
                    )
                    all_relevant_docs.append(relevant_docs)

                    most_relevant_index = 0
                    match = False
                    clean_source = clean_text(source)
                    # search for exact match Index
                    for index, relevant_doc in enumerate(relevant_docs["documents"][0]):
                        if not relevant_docs["documents"][0]:
                            logger.warning("No relevant documents found.")
                            continue
                        if clean_source in clean_text(relevant_doc):
                            most_relevant_index = index
                            match = True
                            break

                    if not match and len(relevant_docs["documents"][0]) > 0:
                        reference.sources[source_index] = relevant_docs["documents"][0][0]
                        if relevant_docs["documents"][0]:
                            page_numbers.append(
                                relevant_docs["metadatas"][0][most_relevant_index]["page_number"]
                            )
                        else:
                            logger.warning("No documents available to assign to source.")

                    if len(relevant_docs["metadatas"][0]) > 0:
                        page_numbers.append(
                            relevant_docs["metadatas"][0][most_relevant_index]["page_number"]
                        )

            if page_numbers:
                reference.page_numbers = page_numbers

    data_dict = data.model_dump()

    return {
        "fields": data_dict["fields"],
        "context": data_dict["references"],
    }

def find_best_match_for_short_reference(source, all_relevant_docs, asset_id, project_id, threshold=0.8):
    source_words = set(re.findall(r'\w+', source.lower()))
    if not source_words:
        return None  # Return None if the source is empty

    best_match = None
    best_match_score = 0

    for relevant_docs in all_relevant_docs:
        for doc, metadata in zip(relevant_docs["documents"][0], relevant_docs["metadatas"][0]):
            if metadata["asset_id"] == asset_id and metadata["project_id"] == project_id:
                doc_words = set(re.findall(r'\w+', doc.lower()))
                common_words = source_words.intersection(doc_words)
                match_score = len(common_words) / len(source_words)

                if match_score > best_match_score:
                    best_match_score = match_score
                    best_match = {"text": doc, "page_number": metadata["page_number"]}

    return best_match if best_match_score >= threshold else None


def update_process_step_status(
    db,
    process_step,
    status,
    output=None,
    output_references=None,
    repo: IProcessRepository = DEFAULT_PROCESS_REPO,
) -> None:
    """Update the status of a process step using the provided repository."""
    repo.update_process_step_status(
        db,
        process_step,
        status,
        output=output,
        output_references=output_references,
    )

def vectorize_extraction_process_step(project_id: int, process_step_id: int, filename: str, references: dict) -> None:
    # Vectorize extraction result and dump in database
    field_references = {}

    # Loop to concatenate sources for each reference
    for extraction_references in references:
        for extraction_reference in extraction_references:
            sources = extraction_reference.get("sources", [])
            if sources:
                sources_catenated = "\n".join(sources)
                field_references.setdefault(extraction_reference["name"], "")
                field_references[extraction_reference["name"]] += (
                    "\n" + sources_catenated if field_references[extraction_reference["name"]] else sources_catenated
                )

    # Only proceed if there are references to add
    if not field_references:
        return

    # Initialize Vectorstore
    vectorstore = ChromaDB(f"panda-etl-extraction-{project_id}")

    docs = [f"{filename} {key}" for key in field_references]
    metadatas = [
        {
            "project_id": project_id,
            "process_step_id": process_step_id,
            "filename": filename,
            "reference": reference
        }
        for reference in field_references.values()
    ]

    # Add documents to vectorstore
    vectorstore.add_docs(docs=docs, metadatas=metadatas, batch_size=settings.chroma_batch_size)
