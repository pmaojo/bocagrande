"""
KJB Parser Service

Este módulo proporciona un servicio para parsear archivos KJB (Kettle Job)
y convertirlos al formato utilizado por MagicETL.
"""

import xml.etree.ElementTree as ET
from typing import Dict, Any, List, Optional
import os
from pydantic import BaseModel

class JobEntryModel(BaseModel):
    """Modelo para una entrada de trabajo en un Job de Kettle."""
    name: str
    type: str
    x: int
    y: int
    attributes: Dict[str, Any] = {}
    ktr_file: Optional[str] = None  # Ruta al archivo KTR si es una transformación

class JobHopModel(BaseModel):
    """Modelo para un salto (conexión) entre entradas en un Job de Kettle."""
    from_entry: str
    to_entry: str
    enabled: bool = True
    evaluation: Optional[str] = None
    unconditional: Optional[bool] = None

class JobModel(BaseModel):
    """Modelo para un Job de Kettle."""
    name: str
    description: Optional[str] = None
    directory: str = "/"
    entries: List[JobEntryModel] = []
    hops: List[JobHopModel] = []
    parameters: List[Dict[str, Any]] = []
    attributes: Dict[str, Any] = {}

class ReactFlowNode(BaseModel):
    """Modelo para un nodo en ReactFlow."""
    id: str
    type: str
    position: Dict[str, int]
    data: Dict[str, Any] = {}

class ReactFlowEdge(BaseModel):
    """Modelo para una conexión en ReactFlow."""
    id: str
    source: str
    target: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

class ReactFlowModel(BaseModel):
    """Modelo para un grafo en ReactFlow."""
    nodes: List[ReactFlowNode]
    edges: List[ReactFlowEdge]

class KJBParserService:
    """Servicio para parsear archivos KJB y convertirlos a formato ReactFlow."""
    
    def parse_kjb_file(self, file_path: str) -> JobModel:
        """
        Parsea un archivo KJB y devuelve un modelo de Job.
        
        Args:
            file_path: Ruta al archivo KJB.
            
        Returns:
            Un modelo JobModel con la información del Job.
            
        Raises:
            FileNotFoundError: Si el archivo no existe.
            Exception: Si hay un error al parsear el archivo.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"El archivo {file_path} no existe.")
        
        try:
            # Parsear el XML
            tree = ET.parse(file_path)
            root = tree.getroot()
            
            # Extraer información básica del job
            name = root.find("./name")
            name_text = name.text if name is not None else os.path.basename(file_path).replace(".kjb", "")
            
            description = root.find("./description")
            description_text = description.text if description is not None else ""
            
            directory = root.find("./directory")
            directory_text = directory.text if directory is not None else "/"
            
            # Crear el modelo de job
            job_model = JobModel(
                name=name_text,
                description=description_text,
                directory=directory_text
            )
            
            # Extraer entradas (nodos)
            for entry_elem in root.findall("./entries/entry"):
                entry_name = entry_elem.find("./name")
                entry_name_text = entry_name.text if entry_name is not None else "Unknown"
                
                entry_type = entry_elem.find("./type")
                entry_type_text = entry_type.text if entry_type is not None else "UNKNOWN"
                
                x_elem = entry_elem.find("./xloc")
                x = int(x_elem.text) if x_elem is not None else 0
                
                y_elem = entry_elem.find("./yloc")
                y = int(y_elem.text) if y_elem is not None else 0
                
                # Extraer atributos específicos según el tipo
                attributes = {}
                
                if entry_type_text == "TRANS":
                    # Para transformaciones, extraer la ruta del archivo
                    filename = entry_elem.find("./filename")
                    ktr_file_path = None
                    if filename is not None:
                        ktr_file_path = filename.text
                        attributes["filename"] = ktr_file_path
                    
                    # Extraer otros atributos relevantes
                    for attr in ["transname", "directory"]:
                        elem = entry_elem.find(f"./{attr}")
                        if elem is not None:
                            attributes[attr] = elem.text
                
                # Crear la entrada y añadirla al modelo
                job_entry = JobEntryModel(
                    name=entry_name_text,
                    type=entry_type_text,
                    x=x,
                    y=y,
                    attributes=attributes,
                    ktr_file=ktr_file_path if entry_type_text == "TRANS" else None
                )
                
                job_model.entries.append(job_entry)
            
            # Extraer saltos (conexiones)
            for hop_elem in root.findall("./hops/hop"):
                from_entry = hop_elem.find("./from")
                from_entry_text = from_entry.text if from_entry is not None else ""
                
                to_entry = hop_elem.find("./to")
                to_entry_text = to_entry.text if to_entry is not None else ""
                
                enabled = hop_elem.find("./enabled")
                enabled_value = enabled.text.lower() == "y" if enabled is not None else True
                
                evaluation = hop_elem.find("./evaluation")
                evaluation_text = evaluation.text if evaluation is not None else None
                
                unconditional = hop_elem.find("./unconditional")
                unconditional_value = unconditional.text.lower() == "y" if unconditional is not None else None
                
                # Crear el salto y añadirlo al modelo
                job_hop = JobHopModel(
                    from_entry=from_entry_text,
                    to_entry=to_entry_text,
                    enabled=enabled_value,
                    evaluation=evaluation_text,
                    unconditional=unconditional_value
                )
                
                job_model.hops.append(job_hop)
            
            return job_model
            
        except Exception as e:
            raise Exception(f"Error al parsear el archivo KJB: {str(e)}")
    
    def convert_to_reactflow(self, job_model: JobModel) -> ReactFlowModel:
        """
        Convierte un modelo de Job a formato ReactFlow.
        
        Args:
            job_model: Modelo de Job a convertir.
            
        Returns:
            Un modelo ReactFlowModel con la información del Job en formato ReactFlow.
        """
        nodes = []
        edges = []
        
        # Convertir entradas a nodos
        for entry in job_model.entries:
            # Determinar el tipo de nodo en ReactFlow
            node_type = "customNode"  # Por defecto
            if entry.type == "TRANS":
                node_type = "transform"
            elif entry.type in ["SQL", "MYSQL", "POSTGRESQL"]:
                node_type = "source"
            elif entry.type in ["WRITE_TO_LOG", "MAIL"]:
                node_type = "destination"
            
            # Crear el nodo
            node = ReactFlowNode(
                id=entry.name.lower().replace(" ", "_"),
                type=node_type,
                position={"x": entry.x, "y": entry.y},
                data={
                    "label": entry.name,
                    "type": entry.type.lower(),
                    "properties": entry.attributes
                }
            )
            
            nodes.append(node)
        
        # Convertir saltos a conexiones
        for hop in job_model.hops:
            # Crear IDs para los nodos fuente y destino
            source_id = hop.from_entry.lower().replace(" ", "_")
            target_id = hop.to_entry.lower().replace(" ", "_")
            
            # Crear la conexión
            edge = ReactFlowEdge(
                id=f"e_{source_id}_{target_id}",
                source=source_id,
                target=target_id,
                data={
                    "enabled": hop.enabled,
                    "unconditional": hop.unconditional,
                    "evaluation": hop.evaluation
                }
            )
            
            edges.append(edge)
        
        return ReactFlowModel(nodes=nodes, edges=edges)
