# Taxi Usage and City Popularity in Manhattan

## Description

Our project provides visualizations to analyze the temporal and spatial relationships between taxi trips in Manhattan in 2021 and the city’s popularity. we obtain records of taxi trips with various attributes about the costs, times, and locations through the 2021 NYC Yellow Taxi Trip Dataset and leverage the Google Popular Times datasets as a proxy of the regional dynamics.

We predict taxi trip pick-up and drop-off coordinates with classical machine learning models and visualize the two datasets using a bivaraite choropleths. We also provide filters on the interfaces for users to facet the data based on different taxi ride charactersitics and temporal conditions.

For the implementation, we mainly uses the following packages:
- Data cleaning and preprocessing: `numpy`, `pandas`, and `shapely`
- Coordinate prediction: `scikit-learn`
- Visualizations: `plotly` and `dash`

Check the [`./doc/`](doc) directory for the full report and poster.

## Directory Structure

```
root
└───README.md                   # User guide
└───.gitignore                  # .gitignore
└───requirements.in             # Dependency management (for dev purpose)
└───requirements.txt            # List of all dependencies
|
└───code
│   └───assets                  # Assets for visualization
│   |   └───... 
│   └───coordinate_estimation   # Directory including all ML models and artifacts
│   |   └───... 
│   └───data                    # Directory including raw and clean datasets
│   |   └───...
│   └───utils                   # Utility functions
│   |   └───...
│   └───app.py                  # Main application
|
└───doc
    └───team034poster.pdf       # Final project poster
    └───team034report.pdf       # Final project report
```

## Installation

### 1. Create Conda Environment

Create a conda environment with the following command:
```
$ conda create -n taxi-trip-visual-analytics python=3.10
```

### 2. Install Dependencies
```
$ pip install -r requirements.txt
```

### 3. Set Up Mapbox Access Token

Create a mapbox free-tier account [here](https://www.mapbox.com/) to get a personal access token. Create `.mapbox_token` file under [`./code/data/`](code/data) directory and store the personal access token inside it.

## Execution

To start the app, execute the following under `./code/` directory:
```
$ python app.py
```

## Development

### Update Dependencies

To add or remove dependencies, edit the [requirements.in](requirements.in) file and run the following command:
```
$ pip-compile requirements.in
```

This will auto-resolve the packages and versions in `requirements.txt`.

### Data Collection: Download Google Popular Times Data

To collect raw Google Popular Times data, add your API key to [./code/data/populartime.py](code/data/populartime.py) run the following under [`./code/data/`](code/data) directory:
```
$ python populartime.py
```

### Download Cleaned Non-Sampled Data

We store our cleaned non-sampled taxi trip data on Google Drive, to download them, run the following under [`./code/data/`](code/data) directory:
```
$ python download_clean_data.py
```

### Data Cleaning

The cleaned and sample cleaned data are provided in the [./code/data/clean/](code/data/clean/) and [./code/data/sample/](code/data/sample/) directories, respectively. To re-execute data cleaning, execute the following under [`./code/data/`](code/data) directory:
```
$ python data_cleaning [--data <dataset_name>] [--create_sample]
```
where `dataset_name` needs to be one of the following: `all`, `taxi`, `popular_times`, `land_use` and `--create_sample` is an optional flag that creates additional clean sample of size 1000 from the cleaned data.