import pandas as pd
import numpy as np
from Levenshtein import distance as levenshtein_distance
from thefuzz import fuzz
from multiprocessing import Pool


def merge_data(df1: pd.DataFrame, df2: pd.DataFrame, filename: str, remove_duplicates_by: list[str]):
    merged_df = pd.concat([df1, df2])
    merged_df_sorted = merged_df.sort_values('Domain')

    merged_df_sorted.drop_duplicates(
        subset=remove_duplicates_by, keep='first', inplace=True)

    merged_df_sorted.to_csv(f'data/res/{filename}.csv', index=False)
    return merged_df_sorted

# Levenshtein method


def are_similar(name1, name2, threshold: float = .5):
    if pd.isnull(name1) or pd.isnull(name2):
        return False
    len_max = max(len(name1), len(name2))
    if len_max == 0:
        return True
    similarity = 1 - levenshtein_distance(name1, name2) / len_max
    return similarity >= threshold

def process_chunk(chunk, all_names, threshold):
    indices_to_drop = set()
    print('Process chuck started!')
    for i_tuple in chunk:
        for j_tuple in all_names:
            if are_similar(i_tuple[1], j_tuple[1], threshold):
                indices_to_drop.add(j_tuple[0])
    return indices_to_drop

def enhanced_hash_name(name):
    if not name or pd.isnull(name):
        return "#"
    if not isinstance(name, str):
        name = str(name) 
    name_str = name.lower()
    return f"{name_str[0]}_{len(name_str)}"

def compare_names(bucket, threshold):
    indices_to_drop = set()
    for i in range(len(bucket)):
        for j in range(i + 1, len(bucket)):
            if are_similar(bucket[i][1], bucket[j][1], threshold):
                indices_to_drop.add(bucket[j][0])
    return indices_to_drop

def remove_duplicate_company_names(df: pd.DataFrame, threshold: float = .5, group_data: bool = True, num_processes: int = 12):
    indices_to_drop = set()
    if group_data:
        groups = df.groupby('Domain')

        for domain, group in groups:
            company_names = group['CompanyName'].tolist()
            for i in range(len(company_names)):
                for j in range(i+1, len(company_names)):
                    if are_similar(company_names[i], company_names[j], threshold=threshold):
                        indices_to_drop.add(group.index[j])
    else:
        company_names = df['CompanyName'].tolist()
        buckets = {}
        for idx, name in enumerate(company_names):
            bucket_key = enhanced_hash_name(name)
            if bucket_key not in buckets:
                buckets[bucket_key] = []
            buckets[bucket_key].append((idx, name))

        indices_to_drop = set()
        with Pool(processes=num_processes) as pool:
            results = pool.starmap(compare_names, [(bucket, threshold) for bucket in buckets.values()])

            for result in results:
                indices_to_drop.update(result)

        pool.close()
        pool.join()
    valid_indices_to_drop = set(indices_to_drop).intersection(set(df.index))
    final_df = df.drop(list(valid_indices_to_drop))
    final_df.to_csv('data/res/merged_and_no_company_name_duplicates_facebook_website_dataset.csv' if group_data else 'data/res/merged_and_no_company_name_duplicates_facebook_website_google_dataset.csv', index=False)
    return final_df

# thefuzz method


def are_similar_with_fuzz(name1, name2, threshold=0.5):
    if pd.isnull(name1) or pd.isnull(name2):
        return False
    len_max = max(len(name1), len(name2))
    if len_max == 0:
        return True
    # using a faster similarity calculation
    similarity = fuzz.ratio(name1, name2) / 100
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

    final_df.to_csv(
        'data/res/merged_and_no_company_name_duplicates_facebook_website_dataset_fuzz.csv', index=False)
    return final_df
