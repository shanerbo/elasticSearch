from typing import Tuple

from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from pyquery import PyQuery as pq
import tarfile
import glob
import os
import json

MODE = 'standby'


def load_data(es: Elasticsearch) -> None:
    """
    This function loads data from the tarball "wiki-small.tar.gz" 
    to the Elasticsearch cluster

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    None
    """
    tf = tarfile.open("wiki-small.tar.gz", "r:gz")
    allFiles = tf.getnames()

    i = 1

    for member in allFiles:
        html = tf.extractfile(member)
        if html is not None:
            html_string = html.read()
            tuple = parse_html(html_string)
            tmp = {
                'title': tuple[0],
                'body': tuple[1],
            }
            data = json.dumps(tmp)
            es.index(index='wikipedia', id=i, body=data)
            i = i + 1
            # print("id:" + ' ' + str(i) + " added")
    # Fill in the code here


def parse_html(html: str) -> Tuple[str, str]:
    """
    This function parses the html, strips the tags an return
    the title and the body of the html file.

    Parameters
    ----------
    html : str
        The HTML text

    Returns
    -------
    Tuple[str, str]
        A tuple of (title, body)
    """
    raw = pq(html)
    title = raw("title").text()
    content = raw("body").text()
    tuple = (title, content)
    return tuple

    # Fill in the code here


def create_wikipedia_index(ic: IndicesClient) -> None:
    """
    Add an index to Elasticsearch called 'wikipedia'

    Parameters
    ----------
    ic : IndicesClient
        The client to control Elasticsearch index settings

    Returns
    -------
    None
    """
    # Fill in the code here

    index_name = 'wikipedia'

    # with open('./stopwords.txt') as f:
    #     # with open('/usr/share/elasticsearch/config/stopwords.txt') as f:
    #     content = f.readlines()
    # content = [x.strip() for x in content]
    if ic.exists(index_name):
        ic.close(index='wikipedia')
        ic.put_settings(
            index=index_name,
            body={
                'settings': {
                    'analysis': {
                        'analyzer': {
                            'my_analyzer': {
                                'type': 'custom',
                                'tokenizer': 'standard',
                                'filter': ['lowercase', 'my_stops', ]
                            },
                        },
                        'filter': {
                            'my_stops': {
                                'type': 'stop',
                                # 'stopwords': content,
                                'stopwords_path': 'stopwords.txt',
                            },
                        },
                    },
                },
                "mappings": {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": "my_analyzer",
                        },
                        "body": {
                            "type": "text",
                            "analyzer": "my_analyzer",
                        }
                    }
                }

            },
            # Will ignore 400 errors, remove to ensure you're prompted
        )
        ic.open(index='wikipedia')

    else:
        ic.create(
            index=index_name,
            body={
                'settings': {
                    'analysis': {
                        'analyzer': {
                            'my_analyzer': {
                                'type': 'custom',
                                'tokenizer': 'standard',
                                'filter': ['lowercase', 'my_stops', ]
                            },
                        },
                        'filter': {
                            'my_stops': {
                                'type': 'stop',
                                # 'stopwords': content,
                                'stopwords_path': 'stopwords.txt',
                            },
                        },
                    },
                },
                "mappings": {
                        "properties": {
                            "title": {
                                "type": "text",
                                "analyzer": "my_analyzer",
                            },
                            "body": {
                                "type": "text",
                                "analyzer": "my_analyzer",
                            }
                        }
                }

            },
            # Will ignore 400 errors, remove to ensure you're prompted
        )


def mode():
    return MODE
