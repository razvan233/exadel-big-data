import pandas as pd
from Levenshtein import distance as levenshtein_distance
def merge_data(facebook_df: pd.DataFrame, website_df: pd.DataFrame):    
    merged_df = pd.concat([facebook_df, website_df])
    merged_df_sorted = merged_df.sort_values('Domain')

    merged_df_sorted.drop_duplicates(subset=['Domain', 'Phone'], keep='first', inplace=True)
    
    merged_df_sorted.to_csv('data/res/merged_facebook_website_dataset.csv', index=False)
    return merged_df_sorted

def are_similar(name1, name2, csv_f, threshold=.5):
    if pd.isnull(name1) or pd.isnull(name2):
        return False
    len_max = max(len(name1), len(name2))
    if len_max == 0:
        return True
    similarity = 1 - levenshtein_distance(name1, name2) / len_max    
    csv_f.write(f"{name1},{name2},{similarity}\n")
    return similarity >= threshold


def remove_duplicate_grouping_company_names_by_domain(df: pd.DataFrame, threshold=.5):
    groups = df.groupby('Domain')
    indices_to_drop = []
    
    with open('data/res/similarities.csv','w', encoding='utf-8', newline='') as csv_f:
        for domain, group in groups:
            company_names = group['CompanyName'].tolist()
            for i in range(len(company_names)):
                for j in range(i+1, len(company_names)):
                    if are_similar(company_names[i], company_names[j], csv_f, threshold=threshold):
                        indices_to_drop.append(group.index[j])
    
    final_df = df.drop(indices_to_drop)
    final_df.to_csv('data/res/merged_and_no_company_name_duplicates_facebook_website_dataset.csv', index=False)
    return final_df