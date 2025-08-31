from etl_lib import load_into_postgres

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data_to_postgres(parts: dict, *args, **kwargs) -> None:
    load_into_postgres(parts)