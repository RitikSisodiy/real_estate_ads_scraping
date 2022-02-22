from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

# ES_HOST="http://192.168.18.101"
# ES_PORT=9200
# es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
es = Elasticsearch(hosts="http://192.168.18.101", port=9200)
print(es.ping())


# ---------------------------------------------------------------- Create operations ----------------------------------
# create a new Index
def create_index(index):
    es.indices.create(index=index, ignore=400)


# insert one record
def insert_one_data(_index, data, doc_type, id):
    res = es.index(index=_index, doc_type=doc_type, id=id, body=data)
    # index will return insert info: like as created is True or False
    print(res)


# insert Json data
def insert_data_by_bulk(json_data):
    res = helpers.bulk(es, json_data)
    print(res)


# -----------------------------------------------------------------Read Operations ------------------------------------
def search_by_index_and_id(_index, _id):
    res = es.get(
        index=_index,
        id=_id
    )
    return res


def search_by_index_and_query(_index, _doc_type, query):
    res = es.search(
        index=_index,
        body=query
    )
    return query


# ---------------------------------------------------------------Searches Operations-----------------------------------
# you need search in particular field and keyword must be word, not letter
def full_text_search(keyword, index, field):
    res = es.search(
        index=index,
        body={
            "query": {
                "match": {
                    field: keyword
                }
            }
        }
    )
    return res


def regex_search(pat, index, field):
    res = es.search(
        index=index,
        body={
            "query": {
                "regexp": {
                    field: pat
                }
            }
        }
    )
    return res


def prefix_search(prefix, index, field):
    res = es.search(
        index=index,
        body={
            "query": {
                "prefix": {
                    field: prefix
                }
            }
        }
    )
    return res


# -----------------------------------------------------------------Update Operations ---------------------------------
# update by index
def update_data_by_index(_index, _doc_type, _id, update_data):
    res = es.update(
        index=_index,
        doc_type=_doc_type,
        id=_id,
        body=update_data
    )
    return res


# update by query
def update_by_query(_index, query, field, update_data):
    _inline = "ctx._source.{field}={update_data}".format(field=field, update_data=update_data)
    _query = {
        "script": {
            "inline": _inline,
        },
        "query": {
            "match": query
        }
    }
    res = es.update_by_query(body=_query, index=_index)
    return res


# update by bulk api
def update_by_bulk(_index, _id, update_data, doc_type):
    action = [{
        "_id": _id,
        "_type": doc_type,
        "_index": _index,
        "_source": {"doc": update_data},
        "_op_type": 'update'
    }]
    res = helpers.bulk(es, action)
    return res


# -----------------------------------------------------------------Delete Operations ---------------------------------
def delete_data_by_bulk(_index, _doc_type, _id):
    action = [
        {
            '_op_type': 'delete',
            '_index': _index,
            '_type': _doc_type,
            '_id': _id,
        }
    ]
    res = helpers.bulk(es, action)
    return res


# clear all index items
def delete_index(_index):
    res = es.indices.delete(index=_index, ignore=[400, 404])
    return res


# delete content but empty item exist
def delete_by_query(_index, query):
    res = es.delete_by_query(
        index=_index,
        body={"query": {"match": query}},
        _source=True
    )
    return res


# delete all item
def delete_one(_index, _id):
    res = es.delete(index=_index, id=_id)
    return res


##################################################################################################################
''' Implementation Examples '''
'''
import elasticsearch as ES

-----------------------------------------------Insert --------------------------------------------------------------
index = "test-index"
ES.create_index(index)
ES.insert_one_data(index, data)

demo_data_2 = load_jsondata()
ES.insert_data_by_bulk(demo_data_2)

-----------------------------------------------Read--------------------------------------------------------------------
index = "test-index"
_id = 5
res = ES.search_by_index_and_id(index, _id)
_doc_type = "authors"
query = {}
ES.search_by_index_and_query(index, _doc_type, query)
----------------------------------------------Update-------------------------------------------------------------------
index = "test-index"
_id = 5
_doc_type = "authors"
update_data = {
    "doc": {"age": 26}
}
res = ES.update_data_by_index(index, _doc_type, _id, update_data)
query = {"author": "Chestermo"}
field = "age"
update_data = 50
res = ES.update_by_query(index, query, field, update_data)
update_data = {"age": 27}
res = ES.update_by_bulk(index, _id, update_data, _doc_type)
---------------------------------------Delete---------------------------------------------------------------------
_index = "test-index"
query = {"author": "Ken"}
res = ES.delete_by_query(_index, query)
_id = "1"
res = ES.delete_one(_index, _id)

---------------------------------------Prefix search and Full-text search------------------------------------------------
index = "test-index"
keyword = "Female"
field = "gender"
regex_pat = ".*?e.+"
prefix = "ma"
res = ES.full_text_search(keyword, index, field)
res = ES.regex_search(regex_pat, index, field)
res = ES.prefix_search(prefix, index, field)

------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
'''

