import os
import json
from datetime import datetime, timedelta
import pandas as pd
from unittest.mock import Mock, patch
import pytest

import sys
sys.path.append("/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment")

from dags.etl import extract, transform, load



@pytest.fixture
def mocked_sqlite_hook(monkeypatch):
    mock_hook = Mock()
    monkeypatch.setattr("dags.etl.SqliteHook", mock_hook)
    return mock_hook.return_value

def test_extract():
    source_file = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/source/jobs.csv'
    target_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/extracted'

    extract(source_file=source_file, target_dir=target_dir)

    extracted_file = os.path.join(target_dir, 'extracted_0.json')
    print(f"Extracted file: {extracted_file}")

    assert os.path.exists(extracted_file)

def test_transform():
    extracted_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/extracted'
    transformed_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/transformed'

    print(f"Current working directory: {os.getcwd()}")
    print(f"Extracted directory path: {extracted_dir}")
    print(f"Transformed directory path: {transformed_dir}")
    
    

    transform(extracted_dir=extracted_dir, transformed_dir=transformed_dir)

    transformed_file = os.path.join(transformed_dir, 'transformed_0.json')
    print(f"Transformed file: {transformed_file}")

    assert os.path.exists(transformed_file)

def test_load(mocked_sqlite_hook):
    transformed_dir = '/mnt/c/Users/Admin/Documents/Data-Internship-Home-Assignment/staging/transformed'

    print(f"Current working directory: {os.getcwd()}")
    print(f"Transformed directory path: {transformed_dir}")

    load(transformed_dir=transformed_dir)

    assert mocked_sqlite_hook.run.called
