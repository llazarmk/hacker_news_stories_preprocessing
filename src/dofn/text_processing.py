from typing import Dict, Iterable, List

import apache_beam as beam
import logging

import spacy
from apache_beam.transforms import window


from .parser import remove_html_tags, remove_special_characters
from .nlp_spacy import NlpSpacy
import en_core_web_lg as en_model


class TextAnalyser(beam.DoFn):

    def __init__(self, feature_columns: dict, batch_size: int = 1000):

        """

        This DoFn executes some parsing operation
        like removal of special characters and
        uses the NlpSpacy class to clean the text
        and to perform some of the spacy functionalities
        in processing the text such es entity recognition,explained entity
        removing of stop words and punctuation.

        Then it saves the result in the pcollection
        in order to enrich the original dataset with new
        features

        :param feature_columns:  {'id':'story_id','feature':'story_text','output_prefix': "story"}

        :param batch_size: int
        """

        self.feature_columns = feature_columns
        self.id_column = self.feature_columns['id']
        self.output_prefix = self.feature_columns['output_prefix']

        self.feature_column = self.feature_columns['feature']
        self.batch_size = batch_size

        self.nlp_model: spacy = None
        self.spacy_handler: NlpSpacy = None

        self.le_col_name = f'{self.output_prefix}_label_entity'
        self.ee_col_name = f'{self.output_prefix}_explained_entity'
        self.sp_col_name = f'{self.output_prefix}_spacy_text'

    def add_default_column_to_element(self, element: List) -> List:
        for document in element:
            document[self.le_col_name] = ""
            document[self.ee_col_name] = ""
            document[self.sp_col_name] = ""
        return element

    def filter_special_char(self, element: Dict) -> Dict:
        """
        some special characters are hard to remove also with spacy
        :param element:
        :return:
        """
        document = element[self.feature_column]
        id_value = element[self.id_column]
        if document:
            try:
                element[self.feature_column] = remove_html_tags(document=document)
                element[self.feature_column] = remove_special_characters(document=document)
            except Exception as e:
                logging.exception(f"{str(id_value)} special character removal exception: {e}")
        return element

    def prepare_document_ids_for_spacy(self, column, output):
        id_values = {elem[self.id_column]: elem for elem in output}
        id_values_none = {elem[self.id_column]: elem for elem in output if not elem[column]}
        documents = ((elem[column], elem[self.id_column]) for elem in output if elem[column])
        return documents, id_values, id_values_none

    def spacy_processing(self, element, column) -> List:

        output = element.copy()
        output = self.add_default_column_to_element(output)

        documents, id_values, id_values_none = self.prepare_document_ids_for_spacy(column, output)
        output = [id_values[_id] for _id in id_values_none]
        try:
            spacy_processing = self.spacy_handler.process(text_key=documents)
            for result in spacy_processing:
                spacy_idx = result['idx']
                original_document = id_values[spacy_idx]
                original_document[self.le_col_name] = result['label_entity']
                original_document[self.ee_col_name] = result['explained_entity']
                original_document[self.sp_col_name] = result['spacy_text']
                output.append(original_document)

        except Exception as e:
            logging.exception(f"spacy processing exception: {e}")
        return output

    def process(self, element: Dict, *args, **kwargs) -> Iterable[Dict]:
        feature = self.feature_columns['feature']
        filter_special_chars_element = self.filter_special_char(element=element)
        self._batch.append(filter_special_chars_element)
        if len(self._batch) >= self.batch_size:
            for data_processed in self.spacy_processing(element=self._batch, column=feature):
                yield data_processed
            self._batch = []

    def setup(self):
        """

        Called once per DoFn instance when the DoFn instance is initialized.
        """
        if self.nlp_model is None:
            self.init_spacy_model()
        self.spacy_handler = NlpSpacy(self.nlp_model)

    def init_spacy_model(self):
        try:
            self.nlp_model = en_model.load()
        except Exception as e:
            logging.exception(f"spacy init model failed  with exception : {e}")

    def start_bundle(self):
        """
        Called once per bundle of elements before calling process on the first element of the bundle

        """
        self._batch = []

    def finish_bundle(self):
        """
        Called once per bundle of elements after calling process after the last element of the bundle,
         can yield zero or more elements.

        """
        feature = self.feature_columns['feature']
        if len(self._batch) != 0:
            for doc_processed in self.spacy_processing(element=self._batch, column=feature):
                yield window.GlobalWindows.windowed_value(doc_processed)
        self._batch = []
