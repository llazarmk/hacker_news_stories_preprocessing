from apache_beam.io.gcp.internal.clients import bigquery
import pyarrow
import logging
from typing import Tuple, Union

logging.basicConfig(level=logging.INFO)

_map_to_parquet_type = {
    'numeric': pyarrow.int64(),
    'string': pyarrow.string(),
    'float': pyarrow.float64(),
    'date': pyarrow.date64(),
    'timestamp': pyarrow.timestamp(unit='s', tz='eu'),
    'bool': pyarrow.bool_(),
    'dict': pyarrow.map_(key_type=pyarrow.string(), item_type=pyarrow.string())

}


def schema_to_parquet(schema: Tuple) -> pyarrow.schema:
    """
    :param schema:

           SCHEMA = (('story_id', 'NUMERIC'),('story_title', 'STRING'))
    :return:

     pyarrow.schema(
        [
            ('story_id', pyarrow.int64()),
            ('story_title', pyarrow.string())
        ]
    )

    """
    parquet_fields = []
    for fields in schema:
        field_name = fields[0]
        field_type = fields[1]
        field_type = field_type.lower()
        parquet_field_type = _map_to_parquet_type[field_type]
        parquet_fields.append((field_name, parquet_field_type))
    parquet_schema = pyarrow.schema(fields=parquet_fields)
    return parquet_schema


class BQSchema:

    def __init__(self, schema: Tuple):
        """
         :param schema of the biquery table tuple

                  (('key','string','required'),
                   ('state','string','nullable')
                   ('age','integer'))

         table_schema = {
         'fields': [{'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
          {'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'}]
          }

        """

        self.schema = schema
        self.table_schema = bigquery.TableSchema()

    @property
    def validate_schema_input(self) -> bool:
        for values in self.schema:
            if len(values) > 3:
                logging.error("schema must contains at max only 3 element name,type,mode")
                return False
            if len(values) == 1:
                logging.error("schema must contain at least the column name and the type")
                return False
            elif len(values) == 2:
                if 'REQUIRED' == values[1] or 'NULLABLE' == values[1]:
                    logging.error(" Schema must have the value type of the column")
                    return False
            elif 'REQUIRED' != values[2] and 'NULLABLE' != values[2]:
                logging.error(" Typed must be required or nullable ")
                return False
        return True

    @staticmethod
    def get_bq_field_names():
        field = {'name': '', 'type': '', 'mode': ''}
        return field

    def create_schema_from_tuple(self):
        for values in self.schema:
            if len(values) == 2:
                col_name, col_type = values[0], values[1]
                col_mode = 'nullable'
            else:
                col_name, col_type, col_mode = values[0], values[1], values[2]
            self.create_schema_field_bq(col_name=col_name,
                                        col_type=col_type,
                                        col_mode=col_mode)

    def create_schema_field_bq(self, col_name: str, col_type: str, col_mode: str):
        schema_field = bigquery.TableFieldSchema()
        schema_field.name = col_name
        schema_field.type = col_type
        schema_field.mode = col_mode
        self.table_schema.fields.append(schema_field)

    def get_schema(self) -> Union[bigquery.TableSchema, None]:
        if not self.validate_schema_input:
            logging.error(" Schema input is not correct")
            return None
        self.create_schema_from_tuple()
        return self.table_schema
