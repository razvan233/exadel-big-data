import pandas as pd
from standardize_data import clean_facebook_dataset, clean_website_dataset, clean_google_dataset
from merge_data import merge_data, remove_duplicate_company_names, remove_duplicate_grouping_company_names_by_domain_with_fuzz
import datetime
import os

MERGED_FB_WEB_DATA_FILENAME = 'merged_facebook_website_dataset'
REMOVE_DUPLICATES_FB_WEB_BY = ['Domain', 'Phone']
MERGED_FB_WEB_GOOGLE_DATA_FILENAME = 'merged_facebook_website_google_dataset'
REMOVE_DUPLICATES_FB_WEB_GOOGLE_BY = ['Phone']


def load_or_clean_dataset(raw_file_path, cleaned_file_path, delimiter, clean_function, **kwargs):
    if os.path.exists(cleaned_file_path):
        print(f"Loading cleaned dataset from {cleaned_file_path}")
        return pd.read_csv(cleaned_file_path, encoding='utf-8')
    else:
        print(f"Cleaning dataset: {raw_file_path}")
        raw_df = pd.read_csv(raw_file_path, on_bad_lines='skip', quotechar='"',
                             encoding='utf-8', delimiter=delimiter, low_memory=False)
        cleaned_df = clean_function(raw_df, **kwargs)
        cleaned_df.to_csv(cleaned_file_path, index=False, encoding='utf-8')
        return cleaned_df

def calc_stats(df: pd.DataFrame):
    total_companies = len(df['CompanyName'])
    unique_companies = df['CompanyName'].nunique()
    num_duplicates = total_companies - unique_companies

    percent_unique = (unique_companies / total_companies) * 100
    percent_duplicates = (num_duplicates / total_companies) * 100
    
    print(f"Total number of company entries: {total_companies}")
    print(f"Number of unique companies: {unique_companies} ({percent_unique:.2f}% of total)")
    print(f"Number of duplicate company entries: {num_duplicates} ({percent_duplicates:.2f}% of total)")

if __name__ == '__main__':
    # Load or clean the datasets
    facebook_cleaned_df = load_or_clean_dataset(
        raw_file_path='data/facebook_dataset.csv',
        cleaned_file_path='data/res/cleaned_standardized_facebook_dataset.csv',
        delimiter=',',
        clean_function=clean_facebook_dataset,
        drop_where_no_phone=True)

    website_cleaned_df = load_or_clean_dataset(
        raw_file_path='data/website_dataset.csv',
        cleaned_file_path='data/res/cleaned_standardized_website_dataset.csv',
        delimiter=';',
        clean_function=clean_website_dataset,
        drop_where_no_phone=True)

    google_cleaned_df = load_or_clean_dataset(
        raw_file_path='data/google_dataset.csv',
        cleaned_file_path='data/res/cleaned_standardized_google_dataset.csv',
        delimiter=',',
        clean_function=clean_google_dataset,
        drop_where_no_phone=True)
    
    # Merge datasets
    facebook_website_merged_data = merge_data(
        facebook_cleaned_df, website_cleaned_df, MERGED_FB_WEB_DATA_FILENAME, REMOVE_DUPLICATES_FB_WEB_BY)
    
    start_time = datetime.datetime.now()

    merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_company_names(
        facebook_website_merged_data, threshold=.5, group_data=True)

    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(f"\nExecution time Levenshtein for websites and facebook: {elapsed_time}")

    facebook_website_google_merged_data = merge_data(
        merged_and_no_company_name_duplicates_facebook_website_data, google_cleaned_df, MERGED_FB_WEB_GOOGLE_DATA_FILENAME, REMOVE_DUPLICATES_FB_WEB_GOOGLE_BY)
    
    start_time = datetime.datetime.now()
    print('\nStatistics before Levenshtein search:\n')
    calc_stats(facebook_website_google_merged_data)
    
    merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_company_names(
        facebook_website_google_merged_data, threshold=.4, group_data=False)
    
    
    end_time = datetime.datetime.now()
    elapsed_time = end_time - start_time
    print(f"\nExecution time Levenshtein all 3 datasets: {elapsed_time}")
    
    print('\nStatistics after Levenshtein search:\n')
    calc_stats(merged_and_no_company_name_duplicates_facebook_website_data)
    
    
    
    
    # start_time = datetime.datetime.now()
    # merged_and_no_company_name_duplicates_facebook_website_data = remove_duplicate_grouping_company_names_by_domain_with_fuzz(
    #     facebook_website_merged_data, threshold=.5)

    # end_time = datetime.datetime.now()
    # elapsed_time = end_time - start_time
    # print(f"Execution time thefuzz: {elapsed_time}")