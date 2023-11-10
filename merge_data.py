import pandas as pd
from Levenshtein import distance as levenshtein_distance
from thefuzz import fuzz

def merge_data(facebook_df: pd.DataFrame, website_df: pd.DataFrame, filename: str, remove_duplicates_by: list[str]):    
    merged_df = pd.concat([facebook_df, website_df])
    merged_df_sorted = merged_df.sort_values('Domain')

    merged_df_sorted.drop_duplicates(subset=remove_duplicates_by, keep='first', inplace=True)
    
    merged_df_sorted.to_csv(f'data/res/{filename}.csv', index=False)
    return merged_df_sorted

#Levenshtein method

def are_similar(name1, name2, threshold=.5):
    if pd.isnull(name1) or pd.isnull(name2):
        return False
    len_max = max(len(name1), len(name2))
    if len_max == 0:
        return True
    similarity = 1 - levenshtein_distance(name1, name2) / len_max    
    return similarity >= threshold

def remove_duplicate_grouping_company_names_by_domain(df: pd.DataFrame, threshold=.5):
    groups = df.groupby('Domain')
    indices_to_drop = []
    
    for domain, group in groups:
        company_names = group['CompanyName'].tolist()
        for i in range(len(company_names)):
            for j in range(i+1, len(company_names)):
                if are_similar(company_names[i], company_names[j], threshold=threshold):
                     indices_to_drop.append(group.index[j])
    
    final_df = df.drop(indices_to_drop)
    final_df.to_csv('data/res/merged_and_no_company_name_duplicates_facebook_website_dataset.csv', index=False)
    return final_df

#thefuzz method

def are_similar_with_fuzz(name1, name2, threshold=0.5):
    if pd.isnull(name1) or pd.isnull(name2):
        return False
    len_max = max(len(name1), len(name2))
    if len_max == 0:
        return True
    similarity = fuzz.ratio(name1, name2) / 100  # using a faster similarity calculation
    return similarity >= threshold

def remove_duplicate_grouping_company_names_by_domain_with_fuzz(df, threshold=0.5):
    # similarities = []
    indices_to_drop = set()

    groups = df.groupby('Domain')
    for domain, group in groups:
        company_names = group['CompanyName'].tolist()
        for i in range(len(company_names)):
            for j in range(i+1, len(company_names)):
                if are_similar_with_fuzz(company_names[i], company_names[j], threshold):
                    indices_to_drop.add(group.index[j])
                    # similarities.append([company_names[i], company_names[j], fuzz.ratio(company_names[i], company_names[j]) / 100])

    final_df = df.drop(list(indices_to_drop))

    # Write similarities to CSV in one go
    # pd.DataFrame(similarities, columns=['Name1', 'Name2', 'Similarity']).to_csv('data/res/similarities_fuzz.csv', index=False)
    
    final_df.to_csv('data/res/merged_and_no_company_name_duplicates_facebook_website_dataset_fuzz.csv', index=False)
    return final_df