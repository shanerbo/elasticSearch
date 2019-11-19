"""
Q1: Create index and load data
Please fill in the missing content in each function.
"""

import assignment4 as a4
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
import json

MODE = 'standby'


def main():
    """
    The main function, do not change any code here
    """

    es = Elasticsearch()
    ic = IndicesClient(es)
    a4.create_wikipedia_index(ic)
    a4.load_data(es)

    print("The top ranked title:", search_and_rank(es))
    add_synonyms_to_index(ic)
    print("The top ranked title:", search_and_rank(es))
    print(how_does_rank_work())


def search_and_rank(es: Elasticsearch) -> str:
    """
    Based on the search in Q2, rank the documents by the terms "BC", "WA" and "AB"
    in the document body.
    Return the **title** of the top result.

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    str
        The title of the top ranked document
    """

    # Fill in the code
    res = es.search(index='wikipedia',
                    body={
                        'query': {
                            "bool": {
                                'must': [
                                    {
                                        'bool': {
                                            'should': [
                                                {'match': {'body': 'lake'}},
                                                {'match': {'body': 'tour'}},
                                            ],
                                        }
                                    }
                                ],
                                'must_not': [{'match_phrase': {'body': 'please improve this article if you can.'}}],
                                'should': [{'match': {'body': {'query': 'BC', 'boost': 2}}},
                                           {'match': {'body': {'query': 'AB', 'boost': 1}}},
                                           {'match': {'body': {'query': 'WA', 'boost': 1}}}],
                                # 'should': [{'match': {'body': 'BC'}},
                                #            {'match': {'body': 'AB'}}],
                            }
                        }

                    })

    return res['hits']['hits'][0]['_source']['title']


def add_synonyms_to_index(ic: IndicesClient) -> None:
    """
    Modify the index setting, add synonym mappings for "BC" => "British Columbia",
    "WA" => "Washington" and "AB" => "Alberta"

    Parameters
    ----------
    ic : IndicesClient
        The client for control index settings in Elasticsearch

    Returns
    -------
    None
    """

    # Fill in the code
    index_name = 'wikipedia'

    with open('./stopwords.txt') as f:
        # with open('/usr/share/elasticsearch/config/stopwords.txt') as f:
        content = f.readlines()
    content = [x.strip() for x in content]
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
                            'filter': ['lowercase',
                                       'my_stops',
                                       'my_synonyms']
                        },
                    },
                    'filter': {
                        'my_stops': {
                            'type': 'stop',
                            'stopwords_path': 'stopwords.txt'
                            # 'stopwords': content,
                        },
                        "my_synonyms": {
                            "type": "synonym",
                            "synonyms": [
                                "BC => British Columbia",
                                "AB => Alberta",
                                "WA => Washington"
                            ]
                        }
                    },
                },
            },
            "mappings": {
                "properties": {
                    "title": {
                        "type": "text",
                        "analyzer": "my_analyzer"
                    },
                    "body": {
                        "type": "text",
                        "analyzer": "my_analyzer"
                    }
                }
            }
        },
    )
    ic.open(index='wikipedia')


def how_does_rank_work() -> str:
    """
    Please write the answer of the question:
    'how does rank work?' here, returning it as a str.

    Returns
    -------
    str, the answer
    """
    # Fill in the answer here
    ans = "\nThe Elasticsearch will scan through all doc that contains user's keyword in query, \n" \
          "Elasticsearch sort matching result by using relevance score, this score is based on \n" \
          "how well this document matches user's query. The higher score, the more relevant doc. \n" \
          "Elasticsearch uses numerical algorithm to determinate the most textually similar to \n" \
          "the query. Once user inputs a query, Elasticsearch will assign a score to each document.\n" \
          " In Q3.2, we only interested in tour or lake in AB, BC and WA, so we need to boost\n" \
          "documents that contains BC, AB and WA, and BC should be the top of result. That's so we \n" \
          "have to boost score of documents contains BC. Also we need to, for example, bind BC and \n" \
          "British Columbia together. So that we would not missing any documents. \n"
    return ans


if __name__ == "__main__":
    main()
