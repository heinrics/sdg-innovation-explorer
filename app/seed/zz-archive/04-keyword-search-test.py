import manticoresearch

# Connect to manticore #########################################################
config = manticoresearch.Configuration(
    host = "http://127.0.0.1:9308",
    retries=10  # Increase timeout to 30 seconds
)

# Example phrases to manually inspect search queries ###########################

# Example phrases
query_text = 'under the poverty line of many countries'
query_text = 'provide people with access to basic service in all countries'
query_text = 'income and and inequalit'
query_text = 'ownerships land'
query_text = 'poverty lines'
query_text = 'poverty reduction'
query_text = 'poverty of life'
query_text = 'children poverty'
query_text = 'socioeconomic poverty'
query_text = 'national plans statistics'
query_text = 'develop country statistics capacity'
query_text = 'population census'
query_text = 'resilient poverty'
query_text = 'SDG progress'
query_text = 'measuring SDG'
query_text = 'SDG monitoring'
query_text = 'poverty eradication'

# Query construction
query = {"query": {"percolate": {"documents": [{'doctext': query_text}, {'doctext': query_text}]}}}

# Search for keywords in manticore
with manticoresearch.ApiClient(config) as api_client:

    searchApi = manticoresearch.SearchApi(api_client)

    res = searchApi.percolate('pq_sdg_queries_en', query)
    print(res)
