import logging
import unittest
import apache_beam as beam
import pytest

from src.dofn.parser import UrlParser, remove_html_tags, remove_special_characters


# from dofn.parser import remove_html_tags


def test_remove_html_tags(self):
    text = "The ndhdm  ddmcdm | </p> is not clear </>"
    actual = remove_html_tags(document=text)
    expected = "The ndhdm  ddmcdm |  is not clear "
    self.assertEqual(actual, expected)


def test_remove_special_characters(self):
    text = "gjkjf #+ü+!§$4<> no</<>way"
    actual = remove_special_characters(document=text)
    expected = "gjkjf 4 no way"
    self.assertEqual(actual, expected)


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

    assert expected_url == url_category
    # self.assertEqual(expected_url, url_category)
