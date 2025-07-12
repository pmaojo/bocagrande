import io
import pandas as pd
import pytest

from adapter.csv_loader import read_csv

class DummyFile(io.BytesIO):
    """Simple file-like object mimicking Streamlit's UploadedFile."""
    pass


def test_read_csv_success():
    data = b"a,b\n1,2\n3,4\n"
    file = DummyFile(data)
    df = read_csv(file)
    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0].tolist() == [1, 2]


def test_read_csv_empty_raises_value_error():
    data = b"a,b\n"
    file = DummyFile(data)
    with pytest.raises(ValueError):
        read_csv(file)


def test_read_csv_no_content_error():
    file = DummyFile(b"")
    with pytest.raises(pd.errors.EmptyDataError):
        read_csv(file)


def test_read_csv_from_path(tmp_path):
    csv_file = tmp_path / "datos.csv"
    csv_file.write_text("a,b\n1,2\n")
    df = read_csv(csv_file)
    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0].tolist() == [1, 2]
