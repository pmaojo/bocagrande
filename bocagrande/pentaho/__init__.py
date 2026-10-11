from pathlib import Path
from typing import Tuple, Union

from bocagrande.pentaho.convert import ConversionReport, transformation_to_pipeline
from bocagrande.pentaho.kjb import job_to_pipeline, parse_kjb
from bocagrande.pentaho.ktr import PentahoParseError, parse_ktr, parse_xml
from bocagrande.pipeline import Pipeline


def import_ktr(source: Union[bytes, str, Path]) -> Tuple[Pipeline, ConversionReport]:
    """Read a ``.ktr`` file (a path, bytes or XML text) into a pipeline plus a report of what was not converted."""
    return transformation_to_pipeline(parse_ktr(source))


def import_kjb(source: Union[bytes, str, Path]) -> Tuple[Pipeline, ConversionReport]:
    """Read a ``.kjb`` job as an outline pipeline (nothing in it runs)."""
    return job_to_pipeline(parse_kjb(source))


def import_file(source: Union[bytes, str, Path]) -> Tuple[Pipeline, ConversionReport]:
    """Import a transformation or a job, deciding by the root element of the XML."""
    root = parse_xml(source).tag
    if root == "transformation":
        return import_ktr(source)
    if root == "job":
        return import_kjb(source)
    raise PentahoParseError(f"Not a Kettle transformation or job: the root element is <{root}>")


__all__ = ["import_ktr", "import_kjb", "import_file", "ConversionReport", "PentahoParseError", "parse_ktr", "parse_kjb",
           "transformation_to_pipeline", "job_to_pipeline"]
