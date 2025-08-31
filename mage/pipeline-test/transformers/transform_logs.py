from etl_lib import transform
from os import path
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform_data(df: pd.DataFrame, *args, **kwargs) -> dict:
    """
    Performs a transformation in Postgres
    """
    return transform(df)

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
