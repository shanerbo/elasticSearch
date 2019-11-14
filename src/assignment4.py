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
    # tf = tarfile.open("/src/wiki-small.tar.gz")
    # tf.extractall()
    i = 1

    # for file in os.listdir("/workdir"):
    for x in os.walk('./en'):
        for y in glob.glob(os.path.join(x[0], '*.html')):
            # print(y)
            if y.endswith(".html"):
                with open(y, 'r') as f:
                    html_string = f.read()
                    tuple = parse_html(html_string)
                    tmp = {
                        'title': tuple[0],
                        'body': tuple[1],
                    }
                    data = json.dumps(tmp)
                    es.index(index='wikipedia', doc_type='html', id=i, body=data)
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

    with open('./stopwords.txt') as f:
    # with open('/usr/share/elasticsearch/config/stopwords.txt') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
    ic.create(
        index=index_name,
        body={
            'settings': {
                'analysis': {
                    'analyzer': {
                        'my_analyzer': {
                            'type': 'custom',
                            'tokenizer': 'standard',
                            'filter': ['lowercase',
                                       'my_stops']
                        },

                    },
                },
                'filter': {
                    'my_stops': {
                        'type': 'stop',
                        # 'stopwords_path': '/usr/share/elasticsearch/config/stopwords.txt'
                        'stopwords_path': content,
                    }
                }
            }
        },
        # Will ignore 400 errors, remove to ensure you're prompted
        ignore=400
    )
    print("------------------------------index done!------------------------------")


def mode():
    return MODE