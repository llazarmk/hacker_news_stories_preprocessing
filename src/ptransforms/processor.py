import apache_beam as beam

from dofn.parser import UrlParser, get_element_domain
from dofn.text_processing import TextAnalyser


class HNStoriesProcessing(beam.PTransform):

    def __init__(self,
                 feature_columns: dict = None,
                 batch_size: int = 100,
                 input_column_url: str = 'story_url',
                 ):
        """
        This ptransform

        :param feature_columns: dict
               input feature columns in the form
               feature_columns = {'id': 'story_id',
                       'feature': 'story_text',
                       'output_prefix': 'story'
                       }
        :param batch_size: int
               value of the batch size in each bundle
        :param input_column_url: str

        """
        self.feature_columns = feature_columns
        self.batch_size = batch_size
        self.input_column_url = input_column_url

    def expand(self, pcollection):
        processed_data = (pcollection
                          | "HackerNewsStoryProcessing" >> beam.ParDo(
                    TextAnalyser(feature_columns=self.feature_columns, batch_size=self.batch_size
                                 )))

        processed_data = (processed_data | 'CreateUrlPath' >> beam.ParDo(UrlParser(input_column=self.input_column_url,
                                                                                   ))
                          )
        return processed_data
