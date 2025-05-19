import sys
import pandas as pd
import dask.dataframe as dd
import manticoresearch
from ast import literal_eval
import duckdb as ddb


# base_path = '/Users/sebastianheinrich/Dropbox/Doktorat KOF ETH - arbeit/01 - Projekte - Doktorat/105-tech-frontier/'
base_path = '/mnt/7adaf322-ecbb-4b5d-bc6f-4c54f7f808eb/Dropbox/Doktorat KOF ETH - arbeit/01 - Projekte - Doktorat/105-tech-frontier/'

# Set environment ##############################################################
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

# Works
PAT_TABLE = EXT_DRIVE + '/tech-frontier/intermediate-data/pat-texts/filing_year=*/data_*.parquet'


# Pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)



# Connect to manticore #########################################################
config = manticoresearch.Configuration(
    host = "http://127.0.0.1:9308"
)


# Search in patent data ########################################################

meta = {'publication_number': 'string',
        'title_available': 'bool',
        'abstract_available': 'bool',
        'sdg': 'string'}


def pat_keyword_search(input_df):

    # print(input_df.shape)

    # Search by splitting queries into chunks

    # Split queries in sub-lists
    # doc_chunks = [docs[x:x + 1000] for x in range(0, len(docs), 1000)]


    result_df_list = []

    # Chunk dataframe in smaller parts
    chunk_size = 10000
    for start in range(0, len(input_df), chunk_size):

        chunk_df = input_df.iloc[start:start + chunk_size].copy()
        # Set index to 1-based
        chunk_df.index = range(1, len(chunk_df) + 1)

        # print(chunk_df.shape)

        # Restructure texts for queries
        docs = []

        for row in chunk_df[["title", "abstract"]].iterrows():

            doc = {}

            text = ''

            if row[1]["title"]:
                text = text + row[1]["title"]

            if row[1]["abstract"]:
                text = text + ' ' + row[1]["abstract"]

            doc['doctext'] = text

            docs.append(doc)

        # for doc_chunk in doc_chunks:

        query = {"query": {"percolate": {"documents": docs}}}

        # Search for keywords in manticore
        with manticoresearch.ApiClient(config) as api_client:

            searchApi = manticoresearch.SearchApi(api_client)

            res = searchApi.percolate('pq_sdg_queries_en', query)

        # Extract keyword identifiers from search results
        # print(len(res.hits.hits))

        # Search results
        hits_list = []

        for hit in res.hits.hits:

            # print(hit)

            # q_id = hit['_id']
            # score = hit['_score']
            tags = hit['_source']['tags'] # .split(',')

            # print(hit['_source']['query'])

            hits_list.append(
                    (tags, hit['fields']['_percolator_document_slot']))

        del query


        # Transform search results back to document-level

        # Explode document index
        hits_df = pd.DataFrame(hits_list,
                                columns=['sdg', 'doc_index']).explode(
            'doc_index').reset_index(drop=True)

        del hits_list

        # Concatenate concept ids
        hits_df = hits_df.groupby('doc_index')['sdg'].apply(
            lambda x: '[' + '; '.join(
                x) + ']').reset_index()  # .sort_values('doc_index') # .drop(columns='doc_index')

        # search_res_df = title_df.merge(abstract_df,
        #                             on='doc_index',
        #                             how='outer',
        #                             suffixes=('_title', '_abstract'))

        # keyword_df.set_index(keyword_df['doc_index'] - 1)

        # Merge identifiers from initial dataframe
        chunk_df = chunk_df[['publication_number', 'title', 'abstract']].merge(
            hits_df,
            right_on='doc_index',
            left_index=True,
            how='left')

        chunk_df = chunk_df[~chunk_df['sdg'].isna()]

        # Indicator for title and abstract existence
        chunk_df.loc[:, 'title_available'] = chunk_df['title'].astype(bool)
        chunk_df.loc[:, 'abstract_available'] = chunk_df['abstract'].astype(bool)

        result_df_list.append(chunk_df)

        # del search_res_df

        result_df = pd.concat(result_df_list)

    return result_df[list(meta.keys())]



if __name__ == '__main__':

    from dask.distributed import Client, progress
    # With 12 workers each has 10GB ram on a 120GB machine
    client = Client(n_workers=3, threads_per_worker=1)
    # http://127.0.0.1:8787/status
    print(client.dashboard_link)


    pat_dd = dd.read_parquet(PAT_TABLE,
                             blocksize='60MB')

    pat_dd = pat_dd.repartition(partition_size='60MB')

    pat_dd = pat_dd.map_partitions(pat_keyword_search, meta=meta)

    # print(pat_dd.head(1))

    # Export to parquet
    pat_dd.to_parquet(EXT_DRIVE + '/sdg-innovation-explorer/intermediate-data/pat-keywords-no-morphology/',
                      schema=meta,
                      write_index=False,
                      compression='zstd')
