# Big data - Exadel

## Prerequisites

Before running the script `main.py`, ensure that you have created the following directory structure in the project root:

project-root/
├── source/
│ └── res/
│   └── results.csv
│ ├── facebook_dataset.csv
│ ├── google_dataset.csv
│ └── website_dataset.csv
└── main.py

## Setup

Follow these steps to prepare the environment for running the script:

1. Create a folder named `source` in the project root.
2. Place the following datasets into the `data` folder:
   - `facebook_dataset.csv`
   - `google_dataset.csv`
   - `website_dataset.csv`
3. Create a folder named `res` inside the `data` directory. This is where the results will be stored after running `main.py`.

## Running the Script

With the datasets in place and the directory structure set up, you can run the script:

```sh
python main.py
