import os
import yaml
import sys
import pandas as pd
import glob

# Set environment ##############################################################
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



# # Directory paths
# if WORK_ENV == 'darwin':
#     PROJ_DIR = '/Users/sebastianheinrich/Dropbox/EPO-Code-FEST-SDG'
#     REMOTE_DRIVE = '/Volumes/' + REMOTE_DRIVE
#
# elif WORK_ENV == 'linux':
#     PROJ_DIR = '/mnt/7adaf322-ecbb-4b5d-bc6f-4c54f7f808eb/Dropbox/EPO-Code-FEST-SDG/'
#     REMOTE_DRIVE = '/media/heinrics/' + REMOTE_DRIVE


# Pandas display options
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1500)

# Read data ####################################################################
################################################################################

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

# Read search results
file_list = glob.glob(REMOTE_DRIVE + '/sdg-innovation-explorer/intermediate-data/pat-keywords-no-morphology/*.parquet')
df_list = [pd.read_parquet(f) for f in file_list]
# Instantiate dataframe
pat_sdg_df = pd.concat(df_list, ignore_index=True)


# Transform data ###############################################################
################################################################################

# Transform goals to lists
pat_sdg_df['sdg_section'] = (pat_sdg_df['sdg']
                                       .str.replace('[', '')
                                       .str.replace(']', '')
                                       .str.split(';'))

# Explode to document-goal-level
pat_sdg_df = pat_sdg_df.explode('sdg_section')

# Separate number and section
pat_sdg_df['sdg_no'] = (pat_sdg_df['sdg_section']
                                  .str.split(',')
                                  .str[0].str.strip())
pat_sdg_df['sdg_section'] = pat_sdg_df['sdg_section'].str.strip()

#Join with names
pat_sdg_df = pat_sdg_df.merge(sdg_name_df,
                              on='sdg_no',
                              how='left')


# Analyse data #################################################################
################################################################################

# Count documents per SDG goal
print(pat_sdg_df['sdg_name'].value_counts())
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
