"""Read a Pentaho Kettle transformation (``.ktr``) into plain data.

Only what the converter needs is kept. Passwords are never read: a connection's password is dropped on
purpose so it cannot end up in a pipeline file, a log or an API response. XML is parsed with ``defusedxml``
because these files often come from other people (entity expansion and external entities are refused).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from xml.etree.ElementTree import Element

from defusedxml import ElementTree as SafeET
from defusedxml.common import DefusedXmlException

SENSITIVE_KEYS = ("password", "passwd", "pwd", "secret", "token", "apikey", "api_key")


class PentahoParseError(Exception):
    """The file is not a readable Kettle transformation or job."""


@dataclass
class Connection:
    name: str
    type: str
    host: str = ""
    port: str = ""
    database: str = ""
    username: str = ""


@dataclass
class Step:
    name: str
    type: str
    element: Element = field(repr=False, default=None)
    x: int = 0
    y: int = 0

    def text(self, path: str, default: str = "") -> str:
        return text_of(self.element, path, default)

    def flag(self, path: str, default: bool = False) -> bool:
        value = self.text(path)
        return default if value == "" else value.upper() == "Y"


@dataclass
class Hop:
    from_step: str
    to_step: str
    enabled: bool = True


@dataclass
class Transformation:
    name: str = ""
    description: str = ""
    parameters: Dict[str, str] = field(default_factory=dict)
    connections: Dict[str, Connection] = field(default_factory=dict)
    steps: List[Step] = field(default_factory=list)
    hops: List[Hop] = field(default_factory=list)


def text_of(element: Optional[Element], path: str, default: str = "") -> str:
    if element is None:
        return default
    found = element.find(path)
    return found.text.strip() if found is not None and found.text else default


def int_of(element: Optional[Element], path: str, default: int = 0) -> int:
    try:
        return int(text_of(element, path))
    except ValueError:
        return default


def is_sensitive(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in SENSITIVE_KEYS)


def parse_xml(source: Union[bytes, str, Path]) -> Element:
    """Parse XML safely from bytes, a string of XML or a path."""
    try:
        if isinstance(source, Path) or (isinstance(source, str) and not source.lstrip().startswith("<")):
            return SafeET.parse(str(source)).getroot()
        return SafeET.fromstring(source)
    except DefusedXmlException as exc:
        raise PentahoParseError(f"The XML uses features that are not allowed ({type(exc).__name__})") from None
    except (SafeET.ParseError, OSError) as exc:
        raise PentahoParseError(f"Cannot read the file: {exc}") from None


def parse_ktr(source: Union[bytes, str, Path]) -> Transformation:
    root = parse_xml(source)
    if root.tag != "transformation":
        raise PentahoParseError(f"Not a Kettle transformation: the root element is <{root.tag}>")

    info = root.find("info")
    model = Transformation(name=text_of(info, "name"), description=text_of(info, "description"))
    for parameter in root.findall("./info/parameters/parameter"):
        name = text_of(parameter, "name")
        if name:
            model.parameters[name] = text_of(parameter, "default_value")
    # direct children only: every step also has a <connection> that merely names one of these
    for connection in root.findall("./connection"):
        name = text_of(connection, "name")
        model.connections[name] = Connection(
            name=name, type=text_of(connection, "type").upper(), host=text_of(connection, "server"),
            port=text_of(connection, "port"), database=text_of(connection, "database"),
            username=text_of(connection, "username"))
    for hop in root.findall("./order/hop"):
        model.hops.append(Hop(text_of(hop, "from"), text_of(hop, "to"), text_of(hop, "enabled", "Y").upper() == "Y"))
    for step in root.findall("./step"):
        model.steps.append(Step(name=text_of(step, "name"), type=text_of(step, "type"), element=step,
                                x=int_of(step, "GUI/xloc"), y=int_of(step, "GUI/yloc")))
    return model


def safe_config(step: Step) -> Dict[str, Any]:
    """The simple text settings of a step, for steps that cannot be converted, without anything sensitive."""
    config: Dict[str, Any] = {}
    for child in step.element:
        if child.tag in ("name", "type", "GUI", "description") or is_sensitive(child.tag):
            continue
        if child.text and child.text.strip():
            config[child.tag] = child.text.strip()
    return config


__all__ = ["Transformation", "Step", "Hop", "Connection", "PentahoParseError", "parse_ktr", "parse_xml",
           "text_of", "int_of", "safe_config", "is_sensitive"]
