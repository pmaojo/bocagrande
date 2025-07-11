import pandas as pd
import pytest
from unittest.mock import patch

from app.adapters.universal_io import universal_extract_to_df, universal_write_df, UniversalIOError


@patch('app.adapters.universal_io.extract_db_to_df')
def test_universal_extract_db(mock_extract):
    df = pd.DataFrame({'a': [1]})
    mock_extract.return_value = df
    config = {'source_type': 'postgresql', 'connection_string': 'dummy', 'query': 'SELECT 1'}

    result = universal_extract_to_df(config)

    mock_extract.assert_called_once_with(config)
    pd.testing.assert_frame_equal(result, df)


@patch('app.adapters.universal_io.read_file_to_df')
def test_universal_extract_file_with_type(mock_read):
    df = pd.DataFrame({'b': [2]})
    mock_read.return_value = df
    config = {'source_type': 'csv', 'file_path': '/tmp/data.csv'}

    result = universal_extract_to_df(config)

    # file_format should be added to config before dispatch
    assert config['file_format'] == 'csv'
    mock_read.assert_called_once_with(config)
    pd.testing.assert_frame_equal(result, df)


@patch('app.adapters.universal_io.read_file_to_df')
def test_universal_extract_file_infer(mock_read):
    df = pd.DataFrame({'c': [3]})
    mock_read.return_value = df
    config = {'file_path': '/tmp/data.json'}

    result = universal_extract_to_df(config)

    mock_read.assert_called_once_with(config)
    pd.testing.assert_frame_equal(result, df)


def test_universal_extract_invalid_config():
    with pytest.raises(UniversalIOError):
        universal_extract_to_df({})


@patch('app.adapters.universal_io.extract_db_to_df', side_effect=Exception('boom'))
def test_universal_extract_unexpected_error(mock_extract):
    with pytest.raises(UniversalIOError):
        universal_extract_to_df({'source_type': 'postgresql'})


@patch('app.adapters.universal_io.write_df_to_db')
def test_universal_write_db(mock_write):
    df = pd.DataFrame({'a': [1]})
    config = {'target_type': 'postgresql', 'connection_string': 'dummy', 'table_name': 'tbl'}

    universal_write_df(df, config)

    mock_write.assert_called_once_with(df, config)


@patch('app.adapters.universal_io.write_df_to_file')
def test_universal_write_file_with_type(mock_write):
    df = pd.DataFrame({'b': [2]})
    config = {'target_type': 'csv', 'file_path': '/tmp/out.csv'}

    universal_write_df(df, config)

    assert config['file_format'] == 'csv'
    mock_write.assert_called_once_with(df, config)


@patch('app.adapters.universal_io.write_df_to_file')
def test_universal_write_file_infer(mock_write):
    df = pd.DataFrame({'c': [3]})
    config = {'file_path': '/tmp/out.json'}

    universal_write_df(df, config)

    mock_write.assert_called_once_with(df, config)


def test_universal_write_invalid_config():
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(UniversalIOError):
        universal_write_df(df, {})


@patch('app.adapters.universal_io.write_df_to_file', side_effect=Exception('fail'))
def test_universal_write_unexpected_error(mock_write):
    df = pd.DataFrame({'a': [1]})
    with pytest.raises(UniversalIOError):
        universal_write_df(df, {'file_path': '/tmp/out.csv'})

