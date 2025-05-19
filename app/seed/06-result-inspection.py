import sys
import pandas as pd
from ast import literal_eval


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


sdg_names = {
    '1': 'No Poverty',
    '2': 'Zero Hunger',
    '3': 'Good Health and Well-being',
    '4': 'Quality Education',
    '5': 'Gender Equality',
    '6': 'Clean Water and Sanitation',
    '7': 'Affordable and Clean Energy',
    '8': 'Decent Work and Economic Growth',
    '9': 'Industry, Innovation and Infrastructure',
    '10': 'Reduced Inequality',
    '11': 'Sustainable Cities and Communities',
    '12': 'Responsible Consumption and Production',
    '13': 'Climate Action',
    '14': 'Life Below Water',
    '15': 'Life on Land',
    '16': 'Peace, Justice and Strong Institutions',
    '17': 'Partnerships for the Goals'
}

sdg_name_df = pd.DataFrame(list(sdg_names.items()), columns=['sdg_no', 'sdg_name'])


import glob

file_list = glob.glob(EXT_DRIVE + '/sdg-innovation-explorer/intermediate-data/pat-keywords-no-morphology/*.parquet')
dfs = [pd.read_parquet(f) for f in file_list]
test = pd.concat(dfs, ignore_index=True)

test['sdg_section'] = test['sdg'].str.replace('[', '').str.replace(']', '').str.split(';')

test = test.explode('sdg_section')

test['sdg_no'] = test['sdg_section'].str.split(',').str[0].str.strip()
test['sdg_section'] = test['sdg_section'].str.strip()


test = test.merge(sdg_name_df, on='sdg_no', how='left')


test['sdg_name'].value_counts()
# sdg_name
# Clean Water and Sanitation                 813604
# Decent Work and Economic Growth            363178
# Affordable and Clean Energy                332317
# Climate Action                             164647
# Responsible Consumption and Production      90422
# Life Below Water                            86216
# Peace, Justice and Strong Institutions      47708
# Sustainable Cities and Communities          31863
# Good Health and Well-being                  14813
# Industry, Innovation and Infrastructure     10040
# Quality Education                            8031
# Partnerships for the Goals                   7316
# Zero Hunger                                  3935
# No Poverty                                   2948
# Life on Land                                 2825
# Reduced Inequality                           1617
# Gender Equality                               642