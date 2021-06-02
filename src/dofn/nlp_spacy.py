import re

from typing import Dict, List, Any, Tuple

import spacy

from spacy.tokenizer import Tokenizer


def custom_tokenizer(vocab) -> spacy.tokenizer.Tokenizer:
    """ Create a tokenizer for the spacy model """
    prefix_re = re.compile(r'''^[\[\("']''')
    suffix_re = re.compile(r'''[\]\)"']$''')
    infix_re = re.compile(r'''[-~_]''')
    simple_url_re = re.compile(r'''^https?://''')
    tokenizer = Tokenizer(vocab,
                          prefix_search=prefix_re.search,
                          suffix_search=suffix_re.search,
                          infix_finditer=infix_re.finditer,
                          url_match=simple_url_re.match)
    return tokenizer


class NlpSpacy:
    """ This class performs some basic operation with spacy
        such es entity recognition,explained entity
        removing of stop words and punctuation

        import en_core_web_lg as en_model
        import spacy

        spacy_model = en_model.load()

        nlp_spacy = NlpSpacy(spacy_model)

        text_1 = 'abadfkjdddd'

        text_processing = nlp_spacy.process(text=text_1)


    """
    keys_entity = ['label_entity', 'explained_entity']
    key_spacy_text = 'spacy_text'

    def __init__(self, nlp_model):
        """
        base class for the spacy text processing
        :param nlp_model: spacy model
        """

        self.__nlp_model: spacy = nlp_model
        self.__pipe_mode = False
        self.__documents = None
        self.__nlp_model.tokenizer = custom_tokenizer(self.__nlp_model.vocab)

    def get_documents(self):
        return self.__documents

    @staticmethod
    def clean_words(document: spacy.tokens.doc.Doc) -> str:
        """

        :param document: spacy.tokens.doc.Doc from the form
                         text_1 = "fjfkdk dkdj.ffjk "
                         spacy_model = en_model.load()
                         doc = spacy_model(text_1)
        :return: str

        Perform a lemmatisation and filters the punctuations and stop words

        """
        word_cleaned = [word.lemma_.lower().strip() for word in document if not word.is_stop and not word.is_punct]
        word_cleaned = " ".join(word_cleaned)
        return word_cleaned

    @staticmethod
    def create_label_explained_entities(document: spacy.tokens.doc.Doc) -> Tuple[set, set]:
        label, explained = set(), set()
        for ent in document.ents:
            if ent.label_ != 'CARDINAL' and ent.label_ != 'ORDINAL':
                label.add(ent.label_.lower())
                explained.add(spacy.explain(ent.label_).lower())
        entities = (label, explained)
        return entities

    def create_entity(self, document: spacy.tokens.doc.Doc) -> Dict:
        """

        :param document: spacy.tokens.doc.Doc
                         text_1 = "fjfkdk dkdj.ffjk "
                         spacy_model = en_model.load()
                         doc = spacy_model(text_1)
        :return: the text,label and explanation entity from the text
        """
        result = {k: "" for k in self.keys_entity}

        entities = self.create_label_explained_entities(document)

        if entities:
            size = len(entities)
            for i in range(size):
                result[self.keys_entity[i]] = ":".join(entities[i])
        return result

    def multiple_document_processing(self) -> List:
        """ create a dictionary of text processed for every id key"""
        batch_list = []
        for doc, idx in self.__documents:
            entities_idx = {'idx': idx}
            entities_result = self.create_entity(document=doc)
            word_cleaned = self.clean_words(doc)
            entities_idx[self.key_spacy_text] = str(word_cleaned)
            entities_idx.update(entities_result)
            batch_list.append(entities_idx)
        return batch_list

    def single_document_processing(self) -> Dict:
        """

        :return: {'idx': 1, 'spacy_text': 'text4', 'text_entity': '', 'label_entity': '', 'explained_entity': ''}
        """
        word_processed = self.create_entity(document=self.__documents)
        word_cleaned = self.clean_words(word_processed)
        word_processed[self.key_spacy_text] = str(word_cleaned)
        return word_processed

    def set_parameters(self, text: str = None, text_key: Any = None):
        if text:
            self.__documents = self.__nlp_model(text)
        else:
            self.__documents = self.__nlp_model.pipe(text_key, as_tuples=True)
            self.__pipe_mode = True

    def process(self, text: str = None, text_key: Any = None) -> Dict:
        """
        pass a input as text or a generator (text_1,key_1)
        in order to remap the original id key with the corresponded transformed text

        :param text: str
        :param text_key: texts = ((text_list[i], i) for i in range(len(text_list))) generator
        :return: {'idx': 1, 'spacy_text': 'text4', 'text_entity': '', 'label_entity': '', 'explained_entity': ''}
        """
        if not text and not text_key:
            raise TypeError(" user must provide text or tuple_text_key ")

        self.set_parameters(text, text_key)
        if not self.__pipe_mode:
            word_processed = self.single_document_processing()
        else:
            word_processed = self.multiple_document_processing()
        return word_processed
