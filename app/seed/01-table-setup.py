import manticoresearch

config = manticoresearch.Configuration(
    host = "http://127.0.0.1:9308"
)

# English table setup
with manticoresearch.ApiClient(config) as api_client:

    # client = manticoresearch.ApiClient(config)
    utilsApi = manticoresearch.UtilsApi(api_client)

    # Drop table if it already exists
    utilsApi.sql('DROP TABLE IF EXISTS pq_sdg_queries_en')

    # Create percolate table
    utilsApi.sql("CREATE TABLE pq_sdg_queries_en(doctext text) "
                 "type='pq'"
                 "html_strip='1'"                   # Removes HTML tags like char_filter
                 "case_insensitive='1'"             # Lowercases words (like lowercase filter)
                 "blend_chars='-,+,&'"              # Helps with word delimiter behavior
                 "stopwords='en'"                   # Add stopwords file
                 "min_infix_len='6'"                # infixes allow for wildcard searching with term patterns like start*, *end, *middle*
                 # "morphology='lemmatize_en_all'"  # Enables English lemmatizing with multiple roots
                 )
