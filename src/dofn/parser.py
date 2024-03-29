"""
This module some basic functionality for parsing different type of texts

"""

import logging
from typing import Tuple, Dict, Union, Iterable
import re

import apache_beam as beam
from urllib.parse import urlparse

import tldextract
from dateutil import parser as date_parser


def is_valid_date(date_str: str) -> bool:
    try:
        date_parser.parse(date_str)
        return True
    except:
        return False


def get_domain_from_url(url):
    return tldextract.extract(url).domain


def get_element_domain(element):
    if element:
        domain = get_domain_from_url(url=element)
        return domain
    return ""


def url_parser(url: str) -> Tuple:
    """

    :param url: 'https://www.cwi.nl:80/%7Eguido/Python.html'
    :return:   'www.cwi.nl:80','/%7Eguido/Python.html'


    """
    url = urlparse(url)
    netloc = url.netloc
    path = url.path
    return netloc, path


def url_path_parser(url_path: str):
    """

    :param url_path: www.123.com/article/technology
    :return: article/technology
    """
    path = url_path.split("/")[1:]
    result = []
    for value in path:
        if value and not is_valid_date(date_str=value) and value.isalpha():
            result.append(value)
    result = "/".join(result)
    return result


def remove_html_tags(document: str = None) -> Union[str, None]:
    """Remove html tags from a string"""
    match = re.compile('<.*?>')
    return re.sub(match, '', document)


def remove_special_characters(document: str = None) -> Union[str, None]:
    """ Remove non alphanumeric values """
    match = re.compile(r"[^a-zA-Z0-9]+")
    return re.sub(match, ' ', document)


def filter_words(words: str, sep='/', k=2) -> str:
    """
    filter words with less then k values

    :param words:
    :param sep:
    :param k:
    :return:
    """
    list_words = words.split(sep)
    list_words = [word.lower() for word in list_words if len(word) >= k]
    words = " ".join(list_words)
    return words


class UrlParser(beam.DoFn):

    def __init__(self, input_column):
        """

        :param input_column: column name of the url column in the dataset
        :param output_column:  desired output column name

        This DoFn takes the input column name of the url text column
        parses the url with urllib.parse.urlparse by taking only the domain and the path
        Filters the url path considering only the alphanumeric value and outputing
        only the result that more than one string

        story_url -> story_url_domain,story_url_category


        """
        self._input_column = input_column

        self._output_column_domain = f"{input_column}_domain"

        self._output_column_category = f"{input_column}_category"

    def process(self, element: Dict, *args, **kwargs) -> Iterable[Dict]:
        output = element.copy()
        url = output[self._input_column]

        output[self._output_column_domain] = ''
        output[self._output_column_category] = ''

        try:
            if url is not None:
                url_domain, url_path = url_parser(url=url)
                output[self._output_column_domain] = get_element_domain(url_domain)
                url_category = url_path_parser(url_path=url_path)
                url_category = filter_words(words=url_category,sep='/',k=2)
                output[self._output_column_category] = url_category
        except Exception as e:
            logging.exception(f"Url parser exception: {e}")
        yield output


class SpecialCharactersFilter(beam.DoFn):
    """
    This DoFn remove the special characters from a text column
    """

    def process(self, element: Dict, *args, **kwargs) -> Iterable[Dict]:
        document = element['story_text']
        try:
            if document:
                element['story_text'] = remove_special_characters(document=document)
        except Exception as e:
            logging.exception(f" Exception SpecialCharactersFilter : {e}")
        yield element
