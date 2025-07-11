"""
Infraestructura: runner asíncrono para HermiT.
"""
from typing import Tuple
import asyncio
import subprocess
import os

HERMIT_JAR = os.path.join("HermiT", "HermiT.jar")

async def reason_async(owl_path: str) -> Tuple[bool, str]:
    """
    Lanza HermiT de forma asíncrona sobre un OWL y devuelve (coherente, logs).
    """
    cmd = ["java", "-jar", HERMIT_JAR, owl_path]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    logs = stdout.decode() + stderr.decode()
    # Heurística: busca "Ontology is consistent" en la salida
    ok = "Ontology is consistent" in logs or "consistent" in logs.lower()
    return ok, logs
