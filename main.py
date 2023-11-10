import pandas as pd
from standardize_data import clean_facebook_dataset, clean_website_dataset, clean_google_dataset
from merge_data import merge_data, remove_duplicate_grouping_company_names_by_domain, remove_duplicate_grouping_company_names_by_domain_with_fuzz
import datetime

start_time = datetime.datetime.now()

LEVENSHTEIN_THRESHOLD = .5
MERGED_FB_WEB_DATA_FILENAME = 'merged_facebook_website_dataset'
REMOVE_DUPLICATES_FB_WEB_BY = ['Domain', 'Phone']
MERGED_FB_WEB_GOOGLE_DATA_FILENAME = 'merged_facebook_website_google_dataset'
REMOVE_DUPLICATES_FB_WEB_GOOGLE_BY = ['Phone']

# Load the datasets
facebook_df = pd.read_csv('data/facebook_dataset.csv',
                          on_bad_lines='skip', quotechar='"', encoding='utf-8')
website_df = pd.read_csv('data/website_dataset.csv',
                         on_bad_lines='skip', delimiter=';', encoding='utf-8')
google_df = pd.read_csv('data/google_dataset.csv',
                        on_bad_lines='skip', quotechar='"', encoding='utf-8')

# Clean the datasets
facebook_cleaned_df = clean_facebook_dataset(
    facebook_df, drop_where_no_phone=True)
website_cleaned_df = clean_website_dataset(
    website_df, drop_where_no_phone=True)
google_cleaned_df = clean_google_dataset(
    google_df, drop_where_no_phone=True)

# Merge datasets
facebook_website_merged_data = merge_data(
    facebook_cleaned_df, website_cleaned_df, MERGED_FB_WEB_DATA_FILENAME, REMOVE_DUPLICATES_FB_WEB_BY)
facebook_website_google_merged_data = merge_data(
    facebook_website_merged_data, google_cleaned_df, MERGED_FB_WEB_GOOGLE_DATA_FILENAME, REMOVE_DUPLICATES_FB_WEB_GOOGLE_BY)

start_time = datetime.datetime.now()

merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_grouping_company_names_by_domain(
    facebook_website_merged_data, threshold=LEVENSHTEIN_THRESHOLD)

end_time = datetime.datetime.now()
elapsed_time = end_time - start_time
print(f"Execution time Levenshtein: {elapsed_time}")

# start_time = datetime.datetime.now()
# merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_grouping_company_names_by_domain_with_fuzz(
#     facebook_website_merged_data, threshold=LEVENSHTEIN_THRESHOLD)

# end_time = datetime.datetime.now()
# elapsed_time = end_time - start_time
# print(f"Execution time thefuzz: {elapsed_time}")