import pandas as pd
import re

standard_columns = ['Domain', 'CompanyName', 'Address', 'Phone','Category']

domain_pattern = re.compile(r"^(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$")

def standardize_categories(categories, cat_delimiter):
    return [s.strip().title() for s in categories.split(cat_delimiter)] if isinstance(categories, str) else categories

def rename_columns(df, column_mapping):
    return df.rename(columns=column_mapping)

def standardize_company_name(name):
    if not isinstance(name, str):
        return name    
    terms_to_remove = ['Inc', 'Ltd', 'Corp', 'LLC', 'PLC', 'GmbH', 'S.A.', 'NV', 'AG']    
    regex_pattern = r'\b(?:' + '|'.join(terms_to_remove) + r')\b\.?'
    
    standardized_name = name.title().strip()    
    standardized_name = re.sub(regex_pattern, '', standardized_name).strip()

    return standardized_name

def standardize_domain(domain):
    return domain.lower().strip().replace(r'\s+', '') if isinstance(domain, str) else domain

def is_valid_domain(domain):
    if pd.isna(domain):
        return False 
    return bool(domain_pattern.match(domain))

def standardize_phone(phone):
    phone = str(phone).strip().replace(r'[^\w.]+', '').replace('+', '')
    if '.' in phone:
        phone = phone.split('.')[0]
    return phone

# Cleaning Google Dataset
def clean_google_dataset(df: pd.DataFrame, drop_where_no_phone: bool = False):
    
    df['category'] =  df['category'].apply(lambda cat:str(cat).title().strip())
    df['name'] = df['name'].apply(standardize_company_name)
    df['domain'] = df['domain'].apply(standardize_domain)
    df['domain'] = df['domain'].str.replace(r'[^\w.]+', '', regex=True)
    df = rename_columns(df, {
        'domain': 'Domain',
        'name': 'CompanyName',
        'address': 'Address',
        'phone': 'Phone',
        'category': 'Category'
    })
    df = df[standard_columns]
    if drop_where_no_phone:
        df = df.dropna(subset=['Phone'])
    df['Phone'] = df['Phone'].apply(standardize_phone)
    df['is_valid_domain'] = df['Domain'].apply(is_valid_domain)
    df_valid = df[df['is_valid_domain']]
    df = df_valid.drop(columns=['is_valid_domain'])
    df.to_csv('data/res/cleaned_standardized_google_dataset.csv', index=False)
    return df

# Cleaning Facebook Dataset
def clean_facebook_dataset(df: pd.DataFrame, drop_where_no_phone: bool = False):
    df['categories'] = standardize_categories(df['categories'], '|')
    df['name'] = df['name'].apply(standardize_company_name)
    df['domain'] = df['domain'].apply(standardize_domain)
    df['domain'] = df['domain'].str.replace(r'[^\w.]+', '', regex=True)
    df = rename_columns(df, {
        'domain': 'Domain',
        'name': 'CompanyName',
        'address': 'Address',
        'phone': 'Phone',
        'categories': 'Category'
    })
    df = df[standard_columns]
    if drop_where_no_phone:
        df = df.dropna(subset=['Phone'])
    df['Phone'] = df['Phone'].apply(lambda ph: str(ph).strip())
    df['Phone'] = df['Phone'].str.replace('.0', '', regex=False)
    df['is_valid_domain'] = df['Domain'].apply(is_valid_domain)
    df_valid = df[df['is_valid_domain']]
    df = df_valid.drop(columns=['is_valid_domain'])
    df.to_csv('data/res/cleaned_standardized_facebook_dataset.csv', index=False)
    return df

# Cleaning Website Dataset
def clean_website_dataset(df: pd.DataFrame, drop_where_no_phone: bool = False):
    df['s_category'] = standardize_categories(df['s_category'], '&')
    df['address'] = df['main_city'] + ', ' + df['main_country'] + ', ' +df['main_region'] 
    df['root_domain'] = df['root_domain'].apply(standardize_domain)
    df['site_name'] = df['site_name'].apply(standardize_company_name)
    df = rename_columns(df, {
        'root_domain': 'Domain', 
        'site_name': 'CompanyName',
        'address': 'Address',
        'phone': 'Phone',
        's_category': 'Category'
    })
    df = df[standard_columns]
    if drop_where_no_phone:
        df = df.dropna(subset=['Phone'])
    df['Phone'] = df['Phone'].apply(lambda ph: str(ph).strip())
    df['Phone'] = df['Phone'].str.replace('.0', '', regex=False)
    df['is_valid_domain'] = df['Domain'].apply(is_valid_domain)
    df_valid = df[df['is_valid_domain']]
    df = df_valid.drop(columns=['is_valid_domain'])
    df.to_csv('data/res/cleaned_standardized_website_dataset.csv', index=False)
    return df
