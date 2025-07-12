import pytest
from src.adaptador_pdf import parse_pdf

def test_parse_pdf_preserva_nombres_y_orden(monkeypatch):
    # Simular pdfplumber.open para devolver un PDF de prueba
    class PaginaFalsa:
        def __init__(self, texto, tablas):
            self._texto = texto
            self._tablas = tablas
        def extract_text(self):
            return self._texto
        def extract_tables(self):
            return self._tablas
    class PDFSimulado:
        def __init__(self, paginas):
            self.pages = paginas
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
    def fake_open(pdf_path):
        paginas = [
            PaginaFalsa(
                "Graduaciones Contactológicas (T_GRADLENT.csv)\nLa clave de este fichero es la combinación de id + tienda\nNota importante...",
                [[['Campo', 'Tipo'], ['id', 'entero'], ['tienda', 'texto']]]
            ),
            PaginaFalsa(
                "Otra tabla\nLa clave de este fichero es la combinación de codigo + fecha",
                [[['Campo', 'Tipo'], ['codigo', 'texto'], ['fecha', 'fecha']]]
            )
        ]
        return PDFSimulado(paginas)
    monkeypatch.setattr('pdfplumber.open', fake_open)
    resultado = parse_pdf('dummy.pdf')
    assert 'paginas' in resultado
    assert len(resultado['paginas']) == 2
    # Comprobar la primera tabla de la primera página
    tabla = resultado['paginas'][0]['tablas'][0]
    assert tabla['columnas'] == ['Campo', 'Tipo']
    assert tabla['filas'][0]['Campo'] == 'id'
    assert tabla['filas'][1]['Campo'] == 'tienda'
    # Comprobar la segunda tabla de la segunda página
    tabla2 = resultado['paginas'][1]['tablas'][0]
    assert tabla2['columnas'] == ['Campo', 'Tipo']
    assert tabla2['filas'][0]['Campo'] == 'codigo'
    assert tabla2['filas'][1]['Campo'] == 'fecha' 