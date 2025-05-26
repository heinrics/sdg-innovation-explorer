import os
import yaml
import sys
import pandas as pd
import json
import manticoresearch


# SET environment ##############################################################
################################################################################


# Computer currently running on
WORK_ENV = sys.platform # linux | darwin

# Load config from a file if exists
config_file = "config.yaml"
if os.path.exists(config_file):
    with open(config_file, "r") as f:
        config = yaml.safe_load(f)

    if WORK_ENV == 'linux':
        LOCAL_PATH = config.get("local_path_linux")
        REMOTE_DRIVE = config.get("remote_path_linux")

    elif WORK_ENV == 'darwin':
        LOCAL_PATH = config.get("local_path_darwin")
        REMOTE_DRIVE = config.get("remote_path_darwin")

else:
    LOCAL_PATH = None
    REMOTE_DRIVE = None

# Ingest SDG queries into manticore search #####################################
################################################################################

# Load queries
query_df = pd.read_parquet(f'{LOCAL_PATH}/Data/sdg-queries/manticore-queries.parquet')


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

    # Inspect ingested query rules
    # Query the first 10 rows from the PQ table
    res = utilsApi.sql("SELECT * FROM pq_sdg_queries_en LIMIT 10;")
    print(res)

    for item in res.anyof_schema_1_validator[0]['data']:
        print(item)

    print(utilsApi.sql("SELECT count(*) FROM pq_sdg_queries_en;"))
