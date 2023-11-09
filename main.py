import pandas as pd
from standardize_data import clean_facebook_dataset, clean_website_dataset
from merge_data import merge_data, remove_duplicate_grouping_company_names_by_domain

# Load the datasets
facebook_df = pd.read_csv('data/facebook_dataset.csv', on_bad_lines='skip' , quotechar='"', encoding='utf-8')
website_df = pd.read_csv('data/website_dataset.csv', on_bad_lines='skip', delimiter=';', encoding='utf-8')
google_df = pd.read_csv('data/google_dataset.csv', on_bad_lines='skip' , quotechar='"', encoding='utf-8')

LEVENSHTEIN_THRESHOLD = .5
# Clean the datasets
facebook_cleaned_df = clean_facebook_dataset(facebook_df, drop_where_no_phone=True)
website_cleaned_df = clean_website_dataset(website_df, drop_where_no_phone=True)

# Merge datasets
facebook_website_merged_data = merge_data(facebook_cleaned_df, website_cleaned_df)

merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_grouping_company_names_by_domain(facebook_website_merged_data, threshold=LEVENSHTEIN_THRESHOLD)