from elasticsearch import Elasticsearch
import settings
cred = {
    "hosts":settings.ES_HOSTS,
    "http_auth":(settings.ES_USER, settings.ES_PASSWORD)
}
# Initialize Elasticsearch client

# Define generator function to yield matching documents
def scroll_filtered_docs(index_name, query, scroll_size):
    es = Elasticsearch(**cred)
    # Perform initial search and scroll request
    search_resp = es.search(index=index_name, body=query, scroll="120m", size=scroll_size)
    scroll_id = search_resp['_scroll_id']
    hits = search_resp['hits']['hits']
    
    # Keep scrolling until all matching documents have been processed
    while hits:
        for hit in hits:
            # Check if the document matches the scroll filter
            # if scroll_filter and not es.exists(index=index_name, doc_type=doc_type, id=hit['_id'], body=scroll_filter):
            #     continue
            
            # Yield the matching document
            yield hit["_source"]
        
        # Get the next scroll request using the previous scroll ID
        search_resp = es.scroll(scroll_id=scroll_id, scroll="2m")
        scroll_id = search_resp['_scroll_id']
        hits = search_resp['hits']['hits']

# # Use the generator function to iterate through the matching documents
# for doc in scroll_filtered_docs(es, index_name, doc_type, query, scroll_size, scroll_filter):
#     # Do something with the matching document
#     print(doc)
