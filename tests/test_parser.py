import logging
import unittest
from typing import Iterable, Dict

import apache_beam as beam
import pytest

from dofn.parser import UrlParser, remove_html_tags, remove_special_characters


# from dofn.parser import remove_html_tags


def test_remove_html_tags():
    text = "The ndhdm  ddmcdm | </p> is not clear </>"
    actual = remove_html_tags(document=text)
    expected = "The ndhdm  ddmcdm |  is not clear "
    assert actual == expected


def test_remove_special_characters():
    text = "gjkjf #+ü+!§$4<> no</<>way"
    actual = remove_special_characters(document=text)
    expected = "gjkjf 4 no way"
    assert actual == expected


def test_UrlParser():
    input_column = 'url'
    output_column_domain = f"{input_column}_domain"
    output_column_categroy = f"{input_column}_category"

    element = [{'url': "https://www.washingtonpost.com/opinion/"},
               {'url': 'https://www.google.com'}]

    expected_url = [
        {
            input_column: element[0]['url'],
            output_column_domain: 'washingtonpost',
            output_column_categroy: 'opinion'
        },
        {
            input_column: element[1]['url'],
            output_column_domain: 'google',
            output_column_categroy: ''
        }
    ]

    url_category = (element
                    | beam.ParDo(UrlParser(input_column=input_column))
                    )
    assert isinstance(url_category,Iterable)
    assert isinstance(url_category[0],Dict)
    assert expected_url == url_category
    # self.assertEqual(expected_url, url_category)
