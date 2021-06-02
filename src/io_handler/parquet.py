import apache_beam as beam
import pyarrow


def read_parquet(pcollection: beam.PCollection, file_pattern: str, pcoll_name: str = 'ReadParquet') -> beam.PTransform:
    data = pcollection | pcoll_name >> beam.io.ReadFromParquet(file_pattern=file_pattern)
    return data


def write_to_parquet(pcollection: beam.PCollection, file_path_prefix: str, schema: pyarrow.schema, file_name_suffix=''):
    _ = pcollection | 'WriteParquet' >> beam.io.WriteToParquet(
        file_path_prefix=file_path_prefix,
        file_name_suffix=file_name_suffix,
        schema=schema,
        num_shards=4
    )
