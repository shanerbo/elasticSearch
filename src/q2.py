"""
Q1: Create index and load data
Please fill in the missing content in each function.
"""
from typing import Dict, Any
import assignment4 as a4
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


def main():
    """
    The main function, do not change any code here
    """
    es = Elasticsearch()
    ic = IndicesClient(es)
    a4.create_wikipedia_index(ic)
    a4.load_data(es)

    print(f"There are {filter(es)['hits']['total']['value']} documents contains 'lake' or 'tour'")
    print(
        f"There are {search_without_improvement(es)['hits']['total']['value']} documents contains"
        " 'lake' or 'tour', but without the 'improvement required' sentense."
    )


def filter(es: Elasticsearch) -> Dict[str, Any]:
    """
    Issue a query to Elasticsearch, 
    return documents whose **body** only contains word "lake" **or** "tour".

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    Dict[str, Any]
        The raw query result from elasticsearch
    """
    # Fill in the code here
    res = es.search(index='wikipedia',
                    body={
                        'query': {
                            'bool': {'should': [{'match': {'body': 'lake'}},
                                                {'match': {'body': 'tour'}}]
                                     }
                        }
                    })

    return res


def search_without_improvement(es: Elasticsearch) -> Dict[str, Any]:
    """
    Issue a query to Elasticsearch, 
    return documents whose **body** only contains word "lake" **or** "tour",
    and not contains the sentense "Please improve this article if you can."

    Parameters
    ----------
    es : Elasticsearch
        The Elasticsearch client
    
    Returns
    -------
    Dict[str, Any]
        The raw query result from elasticsearch
    """
    # Fill in the code here
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
                                'must_not': [{'match_phrase': {'body': 'please improve this article if you can.'}}]
                            }
                        }
                    })

    return res


if __name__ == "__main__":
    main()
