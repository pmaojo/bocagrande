"""
Analizador de AST para código Python del backend.

Este script analiza el código Python y genera un informe sobre la estructura AST.
"""

import ast
import json
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional, Union


class PythonASTAnalyzer:
    """Analizador de AST para código Python."""
    
    def __init__(self, root_dir: str):
        """
        Inicializa el analizador con el directorio raíz del proyecto.
        
        Args:
            root_dir: Directorio raíz del proyecto Python
        """
        self.root_dir = Path(root_dir)
        self.modules = {}
        self.classes = {}
        self.functions = {}
        self.imports = {}
        
    def analyze(self):
        """Analiza todos los archivos Python en el directorio raíz."""
        for file_path in self.root_dir.glob('**/*.py'):
            if '__pycache__' in str(file_path):
                continue
            
            relative_path = file_path.relative_to(self.root_dir)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    code = f.read()
                
                tree = ast.parse(code)
                module_info = self._analyze_module(tree, str(relative_path))
                self.modules[str(relative_path)] = module_info
            except Exception as e:
                print(f"Error al analizar {file_path}: {e}")
    
    def _analyze_module(self, tree: ast.Module, file_path: str) -> Dict[str, Any]:
        """Analiza un módulo Python y extrae información relevante."""
        module_info = {
            'file_path': file_path,
            'classes': [],
            'functions': [],
            'imports': [],
            'global_variables': []
        }
        
        for node in ast.iter_child_nodes(tree):
            # Analizar clases
            if isinstance(node, ast.ClassDef):
                class_info = self._analyze_class(node)
                module_info['classes'].append(class_info)
                self.classes[f"{file_path}:{node.name}"] = class_info
            
            # Analizar funciones
            elif isinstance(node, ast.FunctionDef):
                func_info = self._analyze_function(node)
                module_info['functions'].append(func_info)
                self.functions[f"{file_path}:{node.name}"] = func_info
            
            # Analizar importaciones
            elif isinstance(node, (ast.Import, ast.ImportFrom)):
                import_info = self._analyze_import(node)
                module_info['imports'].append(import_info)
                
                # Registrar importaciones para análisis de dependencias
                if file_path not in self.imports:
                    self.imports[file_path] = []
                self.imports[file_path].append(import_info)
            
            # Analizar variables globales
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        module_info['global_variables'].append({
                            'name': target.id,
                            'line': target.lineno
                        })
        
        return module_info
    
    def _analyze_class(self, node: ast.ClassDef) -> Dict[str, Any]:
        """Analiza una definición de clase y extrae información relevante."""
        class_info = {
            'name': node.name,
            'line': node.lineno,
            'methods': [],
            'attributes': [],
            'bases': [self._get_name(base) for base in node.bases]
        }
        
        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                method_info = self._analyze_function(item)
                class_info['methods'].append(method_info)
            
            elif isinstance(item, ast.Assign):
                for target in item.targets:
                    if isinstance(target, ast.Name):
                        class_info['attributes'].append({
                            'name': target.id,
                            'line': target.lineno
                        })
        
        return class_info
    
    def _analyze_function(self, node: ast.FunctionDef) -> Dict[str, Any]:
        """Analiza una definición de función y extrae información relevante."""
        func_info = {
            'name': node.name,
            'line': node.lineno,
            'args': self._analyze_arguments(node.args),
            'returns': self._get_return_annotation(node),
            'calls': self._extract_function_calls(node)
        }
        
        # Detectar si es un método de clase o estático
        for decorator in node.decorator_list:
            if isinstance(decorator, ast.Name):
                if decorator.id == 'classmethod':
                    func_info['type'] = 'classmethod'
                elif decorator.id == 'staticmethod':
                    func_info['type'] = 'staticmethod'
        
        if 'type' not in func_info:
            func_info['type'] = 'method' if node.args.args and node.args.args[0].arg == 'self' else 'function'
        
        return func_info
    
    def _analyze_arguments(self, args: ast.arguments) -> List[Dict[str, Any]]:
        """Analiza los argumentos de una función."""
        result = []
        
        # Procesar argumentos posicionales
        for arg in args.args:
            arg_info = {
                'name': arg.arg,
                'annotation': self._get_annotation(arg.annotation)
            }
            result.append(arg_info)
        
        return result
    
    def _extract_function_calls(self, node: ast.FunctionDef) -> List[Dict[str, Any]]:
        """Extrae las llamadas a funciones dentro de una función."""
        calls = []
        
        for child_node in ast.walk(node):
            if isinstance(child_node, ast.Call):
                call_info = {
                    'line': child_node.lineno,
                }
                
                if isinstance(child_node.func, ast.Name):
                    call_info['name'] = child_node.func.id
                elif isinstance(child_node.func, ast.Attribute):
                    call_info['name'] = f"{self._get_name(child_node.func.value)}.{child_node.func.attr}"
                else:
                    call_info['name'] = "unknown"
                
                calls.append(call_info)
        
        return calls
    
    def _analyze_import(self, node: Union[ast.Import, ast.ImportFrom]) -> Dict[str, Any]:
        """Analiza una importación y extrae información relevante."""
        if isinstance(node, ast.Import):
            return {
                'type': 'import',
                'names': [name.name for name in node.names],
                'line': node.lineno
            }
        else:  # ImportFrom
            return {
                'type': 'import_from',
                'module': node.module or '',
                'names': [name.name for name in node.names],
                'line': node.lineno
            }
    
    def _get_name(self, node: Optional[ast.AST]) -> str:
        """Obtiene el nombre de un nodo AST."""
        if node is None:
            return "None"
        elif isinstance(node, ast.Name):
            return node.id
        elif isinstance(node, ast.Attribute):
            return f"{self._get_name(node.value)}.{node.attr}"
        elif isinstance(node, ast.Call):
            return self._get_name(node.func)
        elif isinstance(node, ast.Subscript):
            return f"{self._get_name(node.value)}[...]"
        else:
            return type(node).__name__
    
    def _get_annotation(self, node: Optional[ast.AST]) -> str:
        """Obtiene la anotación de tipo de un nodo AST."""
        if node is None:
            return "None"
        return self._get_name(node)
    
    def _get_return_annotation(self, node: ast.FunctionDef) -> str:
        """Obtiene la anotación de retorno de una función."""
        if node.returns:
            return self._get_name(node.returns)
        return "None"
    
    def generate_report(self) -> Dict[str, Any]:
        """Genera un informe del análisis AST."""
        return {
            'modules': self.modules,
            'summary': {
                'total_modules': len(self.modules),
                'total_classes': len(self.classes),
                'total_functions': len(self.functions),
                'imports_by_module': {module: len(imports) for module, imports in self.imports.items()}
            }
        }
    
    def save_report(self, output_file: str):
        """Guarda el informe en un archivo JSON."""
        report = self.generate_report()
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2)
        print(f"Informe guardado en {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Analizar AST de código Python')
    parser.add_argument('--root', required=True, help='Directorio raíz del proyecto Python')
    parser.add_argument('--output', default='ast_report.json', help='Archivo de salida para el informe JSON')
    
    args = parser.parse_args()
    
    analyzer = PythonASTAnalyzer(args.root)
    analyzer.analyze()
    analyzer.save_report(args.output)


if __name__ == '__main__':
    main()
