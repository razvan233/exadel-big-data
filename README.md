# Big data - Exadel

## Prerequisites

Before running the script `main.py`, ensure that you have created the following directory structure in the project root:
```
project-root/
├── data/
│ └── res/
│   └── cleaned_standardized_google_dataset.csv
│   └── cleaned_standardized_facebook_dataset.csv
│   └── cleaned_standardized_website_dataset.csv
│   └── merged_facebook_website_dataset.csv
│   └── merged_facebook_website_google_dataset.csv
│   └── merged_and_no_company_name_duplicates_facebook_website_dataset.csv
│   └── merged_and_no_company_name_duplicates_facebook_website_google_dataset.csv
│ ├── facebook_dataset.csv
│ ├── google_dataset.csv
│ └── website_dataset.csv
└── .gitignore
└── standardized_data.py
└── merge_data.py
└── main.py
```
## Setup

Follow these steps to prepare the environment for running the script:

1. Create a folder named `data` in the project root.
2. Place the following datasets into the `data` folder:
   - `facebook_dataset.csv`
   - `google_dataset.csv`
   - `website_dataset.csv`
3. Create a folder named `res` inside the `data` directory. This is where the results will be stored after running `main.py`.

## Running the Script

With the datasets in place and the directory structure set up, you can run the script:

```sh
python main.py
```

## Parallel Processing for Dataset Merging

For the final merging step between the Google dataset and the previously merged datasets (Websites and Facebook), parallel processing is implemented to enhance performance. This approach allows for faster processing of large datasets by utilizing multiple CPU cores. The function `remove_duplicate_company_names` implements parallel processing when the `group_data` parameter is set to `False`. This function uses Python's `multiprocessing` library to distribute the task across multiple processes, significantly speeding up the duplicate removal and merge process.

## Hash Function and Bucketing for Company Name Comparison

### Hash Function: `enhanced_hash_name`

The `enhanced_hash_name` function is crucial for categorizing company names into buckets to optimize the comparison process. This function lowers the computational complexity by reducing the number of comparisons needed.

- **Functionality**: It takes a company name and produces a hash value based on specific characteristics of the name. If the name is null or not a string, a default hash "#" is returned. Otherwise, the hash is formed by combining the first character of the name (in lowercase) with the length of the name. This approach is a balanced way to categorize names, leveraging both the starting character and the overall length.

- **Example**: For a company name "Exadel", the hash would be "e6", indicating the first letter 'e' and the name length '6'.

### Bucket Creation and Comparison: `compare_names`

The `compare_names` function is designed to operate on a bucket of names, using the hashes created by `enhanced_hash_name`. It identifies similar names within each bucket, reducing the scope of comparisons to potentially similar names only.

- **Process**: This function iterates through pairs of names within a bucket. It utilizes the `are_similar` function (used to calculate Levenshtein distance).

- **Efficiency**: By only comparing names within the same bucket, `compare_names` significantly cuts down on the number of comparisons, especially in large datasets. This targeted approach makes the process of finding similar or duplicate names much more efficient.

## Efficiency Comparison

### With Hashing and Bucketing

- The use of `enhanced_hash_name` and `compare_names` significantly reduces computational complexity.
- Hashing categorizes company names into buckets based on their characteristics, reducing the number of comparisons needed.
- The `compare_names` function only compares names within the same bucket, which is much more efficient for large datasets.

### Without Hashing and Bucketing

- If hashing and bucketing were not used, the process would involve comparing each company name with every other name in the dataset.
- This approach leads to a quadratic complexity, O(n²), where 'n' is the number of names. For large datasets, this can become computationally infeasible.
- Without bucketing, the script would not benefit from the efficiencies of parallel processing, further increasing processing time.


## Test different solutions for strings similarity:

####  Using `thefuzz` library, only for the first merging after comparing multiple tests on execution time.

```
Execution time: 00:00:02.529302
```

#### Using `levenshtein_distance` from `Levenshtein` library.

```
Execution time Levenshtein for websites and facebook: 00:00:02.439366
Execution time Levenshtein for first merged dataset and google dataset: 0:00:17.134709
```