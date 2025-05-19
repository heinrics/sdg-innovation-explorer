import sys
import pandas as pd
import json
import manticoresearch


# SET environment ##############################################################
################################################################################

# Computer currently running on
WORK_ENV = sys.platform # linux | darwin
EXT_DRIVE = 'tb4m2' # tb4m2, T5 EVO

# Directory paths
if WORK_ENV == 'darwin':
    PROJ_DIR = '/Users/sebastianheinrich/Dropbox/EPO-Code-FEST-SDG'
    EXT_DRIVE = '/Volumes/' + EXT_DRIVE

elif WORK_ENV == 'linux':
    PROJ_DIR = '/mnt/7adaf322-ecbb-4b5d-bc6f-4c54f7f808eb/Dropbox/EPO-Code-FEST-SDG/'
    EXT_DRIVE = '/media/heinrics/' + EXT_DRIVE

# Pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# Ingest SDG queries into manticore search #####################################
################################################################################

# Load queries
query_df = pd.read_parquet(f'{PROJ_DIR}/Data/sdg-queries/manticore-queries.parquet')

# query_df['text'] = query_df['text'].str.replace('"', '')


# Generate query list

# List of title, abstract queries
en_sdg_rule_list = (query_df.apply(lambda x: {'query': f'@@relaxed @doctext {x['text']}', # }, #,
                                              'tags': str(x['goal']) + ', ' + str(x['section'])},
                                   axis='columns')
                            .tolist())

# Include rules in queries
en_sdg_rule_list = [{"insert": {'table' : 'pq_sdg_queries_en',  'doc' : x }} for x in en_sdg_rule_list]

# Transform set of rules for ingestion
en_sdg_rule_json_str = '\n'.join(map(json.dumps, en_sdg_rule_list))

print(en_sdg_rule_json_str)


# Connect to manticore #########################################################
config = manticoresearch.Configuration(
    host = "http://127.0.0.1:9308"
)

# Ingest rules into percolate table ############################################

# English
with manticoresearch.ApiClient(config) as api_client:

    indexApi = manticoresearch.IndexApi(api_client)
    utilsApi = manticoresearch.UtilsApi(api_client)

    # Ingest english rules
    res = indexApi.bulk(en_sdg_rule_json_str)
    print(res)

    # Query the first 10 rows from the PQ table
    res = utilsApi.sql("SELECT * FROM pq_sdg_queries_en LIMIT 10;")
    print(res)

    for item in res.anyof_schema_1_validator[0]['data']:
        print(item)

    print(utilsApi.sql("SELECT count(*) FROM pq_sdg_queries_en;"))



# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "(poverty) NEAR/3 ((living) | (life) | (child*) | (socioeconomic*) | (socio-economic*) | (social welfare) | (household*) | (income*)) | (poverty line*)" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "((SDG*) | (sustainable development*)) NEAR/3 ((progress*) | (measurement*) | (measuring) | (monitor*))" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "(population census | housing census | birth registration* | death registration*)" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "(population census) | (housing census) | (birth registration*) | (death registration*)" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "@doctext poverty" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "@doctext ((poverty) NEAR/3 (living | life | child* | socioeconomic* | socio-economic* | social welfare | household* | income*)) | (poverty line*)" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "@doctext (poverty) NEAR/3 (living) | (poverty line*)" }})
# indexApi.insert({"table" : "pq_sdg_queries_en", "doc" : {"query" : "@doctext ((poverty) NEAR/3 (living | social welfare)) | (poverty line*)" }})

