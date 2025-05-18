import sys
import xml.etree.ElementTree as ET
import re
import pandas as pd
from collections import defaultdict

# SET environment ##############################################################
################################################################################

# Computer currently running on
WORK_ENV = sys.platform # linux | darwin
EXT_DRIVE = 'tb4m2' # tb4m2, T5 EVO

# Directory paths
if WORK_ENV == 'darwin':
    PROJ_DIR = '/Users/sebastianheinrich/Dropbox/EPO-Code-FEST-SDG'
    EXT_DRIVE = '/Volumes/' + EXT_DRIVE
    DUCKDB_TEMP_DIR = '/Users/sebastianheinrich/duckdb-tmp'

elif WORK_ENV == 'linux':
    PROJ_DIR = '/mnt/7adaf322-ecbb-4b5d-bc6f-4c54f7f808eb/Dropbox/EPO-Code-FEST-SDG/'
    EXT_DRIVE = '/media/heinrics/' + EXT_DRIVE
    DUCKDB_TEMP_DIR = '/home/heinrics/duckdb-tmp'

# Pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


# EXTRACT SDG queries from XML #################################################
################################################################################

# Namespace map of SDG query xml
ns = {
    'aqd': 'http://aurora-network.global/queries/namespace/',
    'dc': 'http://dublincore.org/documents/dcmi-namespace/'
}

def transform_query(query_line):

    # Extract raw text and strip leading and trailing spaces
    raw_text = query_line.text.strip()

    # Transform to query for manticore search
    query_text = (raw_text.replace('\n', ' ')
                  .replace('AND', ' ')
                  .replace('OR', '|')
                  .replace('W/3', 'NEAR/3'))
    # Remove multiple white spaces
    query_text = re.sub(r'\s+', ' ', query_text).strip()

    return query_text


def parse_scopus_xml(xml_string, sdg_goal):

    # Get XML root
    root = ET.fromstring(xml_string)

    # List to capture extracted queries
    extracted_queries = []

    # Iterate over all query definitions
    for query_def in root.findall('.//aqd:query-definition', ns):
        # Extract SDG section
        identifier = query_def.find('dc:identifier', ns)
        subquery_id = query_def.find('aqd:subquery-identifier', ns)
        sdg_code = identifier.text.split('/')[-1] if identifier is not None else subquery_id.text

        # Find all queries per section
        query_lines = query_def.findall('.//aqd:query-line', ns)

        # Transform each query into the manticore search syntax
        for query_line in query_lines:
            if query_line.text:
                # Transform text
                query_text = transform_query(query_line)
                # Collect transformation results and SDG info in list
                extracted_queries.append([sdg_goal, sdg_code, query_text])
            else:
                continue

    return extracted_queries


# Iterate over all SDG goals (1 xml file per goal) #############################

# Collect queries for all goals
query_list = []

for sdg_goal in range(1, 17+1):
    print(sdg_goal)

    # File path to original xml file
    # Manually downloaded from https://aurora-network-global.github.io/sdg-queries/
    path = f'{PROJ_DIR}/Data/sdg-queries/query_SDG{sdg_goal}.xml'

    # Read XML file
    with open(path, 'r', encoding='utf-8') as f:
        xml_content = f.read()

    # Parse xml, extract and transform queries
    test = parse_scopus_xml(xml_content, str(sdg_goal))
    # Collect queries in a list
    query_list.extend(test)

# Create pandas dataframe from the list of lists
query_df = pd.DataFrame(query_list, columns=['goal', 'section', 'text'])
# Serialize pandas dataframe to parquet
query_df.to_parquet(f'{PROJ_DIR}/Data/sdg-queries/manticore-queries.parquet')
