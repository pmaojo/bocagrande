"""Read a Pentaho Kettle job (``.kjb``) as an outline.

A job orchestrates transformations, scripts, mail and so on. Bocagrande does not run jobs: the outline keeps the
structure (entries and hops, with unique ids) so it can be seen and the transformations it points to can be
imported one by one. Every entry becomes an ``unsupported.job.<Type>`` node.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Tuple, Union

from bocagrande.pentaho.convert import ConversionReport, slug
from bocagrande.pentaho.ktr import PentahoParseError, int_of, parse_xml, text_of
from bocagrande.pipeline import Edge, Node, Pipeline

TRANSFORMATION_ENTRY = "TRANS"
SHOWN_SETTINGS = ("filename", "transname", "directory", "sql", "script")


@dataclass
class JobEntry:
    name: str
    type: str
    x: int = 0
    y: int = 0
    settings: Dict[str, str] = field(default_factory=dict)


@dataclass
class JobHop:
    from_entry: str
    to_entry: str
    enabled: bool = True
    unconditional: bool = False
    evaluation: bool = True


@dataclass
class Job:
    name: str = ""
    description: str = ""
    entries: List[JobEntry] = field(default_factory=list)
    hops: List[JobHop] = field(default_factory=list)


def parse_kjb(source: Union[bytes, str, Path]) -> Job:
    root = parse_xml(source)
    if root.tag != "job":
        raise PentahoParseError(f"Not a Kettle job: the root element is <{root.tag}>")
    job = Job(name=text_of(root, "name"), description=text_of(root, "description"))
    for entry in root.findall("./entries/entry"):
        settings = {key: text_of(entry, key) for key in SHOWN_SETTINGS if text_of(entry, key)}
        job.entries.append(JobEntry(text_of(entry, "name"), text_of(entry, "type"), int_of(entry, "xloc"),
                                    int_of(entry, "yloc"), settings))
    for hop in root.findall("./hops/hop"):
        job.hops.append(JobHop(text_of(hop, "from"), text_of(hop, "to"), text_of(hop, "enabled", "Y").upper() == "Y",
                               text_of(hop, "unconditional").upper() == "Y", text_of(hop, "evaluation", "Y").upper() == "Y"))
    return job


def job_to_pipeline(job: Job) -> Tuple[Pipeline, ConversionReport]:
    """An outline pipeline: nothing in it runs, and the report lists every entry as unsupported."""
    report = ConversionReport()
    pipeline = Pipeline(name=job.name, description=job.description)
    ids: Dict[str, str] = {}
    taken = set()
    for entry in job.entries:
        base, candidate, n = slug(entry.name), slug(entry.name), 2
        while candidate in taken:
            candidate, n = f"{base}_{n}", n + 1
        taken.add(candidate)
        ids[entry.name] = candidate
        reason = ("a transformation: import its .ktr file separately" if entry.type == TRANSFORMATION_ENTRY
                  else "job entries are not run by Bocagrande")
        report.unsupported.append({"step": entry.name, "type": entry.type, "reason": reason})
        pipeline.nodes.append(Node(id=candidate, type=f"unsupported.job.{entry.type or 'Unknown'}", label=entry.name,
                                   config={**entry.settings, "reason": reason}, position={"x": entry.x, "y": entry.y}))
    for hop in job.hops:
        if not hop.enabled:
            report.warnings.append(f"The hop {hop.from_entry} -> {hop.to_entry} is disabled and was left out")
        elif hop.from_entry not in ids or hop.to_entry not in ids:
            report.warnings.append(f"The hop {hop.from_entry} -> {hop.to_entry} refers to a missing entry and was left out")
        else:
            if not hop.unconditional:
                report.warnings.append(f"The hop {hop.from_entry} -> {hop.to_entry} only follows on "
                                       f"{'success' if hop.evaluation else 'failure'}; the outline does not keep that")
            pipeline.edges.append(Edge(ids[hop.from_entry], ids[hop.to_entry]))
    return pipeline, report


__all__ = ["Job", "JobEntry", "JobHop", "parse_kjb", "job_to_pipeline"]
