import pandas as pd
import tempfile

from src.storage import LocalStorage


def test_read_write_json():
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(data_dir=tmpdir)
        data = dict(a=1, b=2)
        storage.write_json(data, 'test.json')
        new_data = storage.read_json('test.json')
        assert(data == new_data)


def test_read_write_csv():
    with tempfile.TemporaryDirectory() as tmpdir:
        storage = LocalStorage(data_dir=tmpdir)
        df = pd.DataFrame([dict(a=1, b=2), dict(a=2, b=3)])
        storage.write_csv(df, 'test.csv')
        new_df = storage.read_csv('test.csv', index_col=0)
        assert(new_df.equals(df))
