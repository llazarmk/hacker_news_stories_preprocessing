import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.io.gcp.internal.clients.bigquery import TableSchema


def read_bq(pcollection: beam.PCollection, query: str, pcoll_name: str = 'ReadBigQuery'):
    data = pcollection | pcoll_name >> beam.io.ReadFromBigQuery(query=query, use_standard_sql=True)

    return data


def write_to_bq(pcollection_input: beam.PCollection,
                table,
                pcoll_name: str = 'WriteBQ',

                schema: TableSchema = None,
                partition: dict = None):
    # partition '{ 'type': 'YEAR', 'field': 'story_date'}
    additional_bq_parameters = None
    if partition:
        additional_bq_parameters = {'timePartitioning': partition}

    _ = pcollection_input | pcoll_name >> beam.io.WriteToBigQuery(
        table=table,
        schema=schema,
        additional_bq_parameters=additional_bq_parameters,
        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
    )
