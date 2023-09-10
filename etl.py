"""ETL pipeline for analysing products data across the globe for the last 60 years"""
from datetime import datetime
from zipfile import ZipFile
import json
import glob
import os
import configparser
import pandas as pd
import requests
import dask.dataframe as dd
import boto3

# ----------------------------------------------------------------
# Config file
config = configparser.ConfigParser()
config.read('dl.cfg')


# ----------------------------------------------------------------
# Prepare source data - unzip data folder to have access to most data sources
def prepare_data_sources(current_directory):
    """Unzip folder with source data from 'data' folder of the repository for future use.

    Args:
        current_directory: path to the current directory for this PC.

    """
    print("********* STEP 1 START ************")
    print("Preparing input data sources - unzipping an archive in the 'data' folder.")
    # loading the data.zip and creating a zip object
    with ZipFile(f'{current_directory}\\{config["LOCAL"]["INPUT_DATA"]}', 'r') as zip_object:
        # Extracting all the members of the zip into a specific location.
        zip_object.extractall(
            path=f"{current_directory}\\data")
    print(f"Current directory is {current_directory}")
    print("Step1 - Preparation of input data sources is finished successfully.")
    print("********* STEP 1 FINISHED ************")


# ----------------------------------------------------------------
# Units data source
def process_units_data(s3, final_directory):
    """Load input 'Units.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """
    print("********* STEP 2 (units) START ************")

    data_units = pd.read_csv('data/Units.csv')
    data_units_final = data_units.rename(
        columns={'Unit Name': 'unit_name', 'Description': 'description'})

    # saving result file
    data_units_final.to_csv(
        f'{final_directory}/dim_unit.csv', index=False)

    # upload file to S3
    s3.upload_file(f'{final_directory}/dim_unit.csv',
                   f'{config["AWS"]["OUTPUT_DATA"]}', 'dim_unit.csv')
    print("********* STEP 2 (units) FINISHED ************")


# ----------------------------------------------------------------
# ItemGroup data source
def process_item_group_data(s3, final_directory):
    """Load input 'ItemGroup.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 3 (item group) START ************")
    data_item_group = pd.read_csv('data/ItemGroup.csv')

    data_item_group_final = data_item_group.rename(
        columns={'Item Group Code': 'item_group_code', 'Item Group': 'item_group',
                 'Item Code': 'item_code', 'Item': 'item'})\
        .drop(columns=['Factor', 'CPC Code', 'HS Code', 'HS07 Code', 'HS12 Code'])

    # saving result file
    data_item_group_final.to_csv(
        f'{final_directory}/dim_item_group.csv', index=False)

    # upload file to S3
    s3.upload_file(f'{final_directory}/dim_item_group.csv',
                   config["AWS"]["OUTPUT_DATA"], 'dim_item_group.csv')
    print("********* STEP 3 (item group) FINISHED ************")


# ----------------------------------------------------------------
# Flags data source
def process_flags_data(s3, final_directory):
    """Load input 'Flags.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 4 (flags) START ************")
    data_flags = pd.read_csv('data/Flags.csv')

    # data transformations
    data_flag_final = data_flags.rename(
        columns={'Flag': 'flag', 'Description': 'description'})

    # saving result file
    data_flag_final.to_csv(
        f'{final_directory}/dim_flag.csv', index=False)

    # upload file to S3
    s3.upload_file(f'{final_directory}/dim_flag.csv',
                   config["AWS"]["OUTPUT_DATA"], 'dim_flag.csv')
    print("********* STEP 4 (flags) FINISHED ************")


# ----------------------------------------------------------------
# Elements data source
def process_elements_data(s3, final_directory):
    """Load input 'Elements.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 5 (elements) START ************")
    data_elements = pd.read_csv('data/Elements.csv')

    # data transformations
    data_element_final = data_elements.rename(
        columns={'Element Code': 'element_code', 'Element': 'element',
                 'Unit': 'unit', 'Description': 'description'})

    # saving result file
    data_element_final.to_csv(
        f'{final_directory}/dim_element.csv', index=False)

    # upload file to S3
    s3.upload_file(f'{final_directory}/dim_element.csv',
                   config["AWS"]["OUTPUT_DATA"], 'dim_element.csv')
    print("********* STEP 5 (elements) FINISHED ************")


# ----------------------------------------------------------------
# CountryGroup data source
def process_country_group_data(s3, final_directory):
    """Load input 'CountryGroup.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 6 (country group) START ************")
    data_country_group = pd.read_csv('data/CountryGroup.csv')

    # data transformations
    data_country_group_final = data_country_group.rename(
        columns={'Country Group': 'country_group',
                 'Country': 'country', 'M49 Code': 'm49_code'})\
        .drop(columns=['Country Group Code',
                       'Country Code', 'ISO2 Code', 'ISO3 Code'])

    # saving result file
    data_country_group_final.to_csv(
        f'{final_directory}/dim_country_group.csv', index=False)

    # upload file to S3
    s3.upload_file(f'{final_directory}/dim_country_group.csv',
                   config["AWS"]["OUTPUT_DATA"], 'dim_country_group.csv')
    print("********* STEP 6 (country group) FINISHED ************")


# ----------------------------------------------------------------
# WorldData data source
def process_world_file_data(s3, final_directory):
    """Load input 'WorldData.csv' data,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.
        Also creates a list of countries with their unique codes and saves it
        locally as a separate file.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 7 (world data) START ************")
    file_world_data = "data/WorldData.csv"
    world_data = dd.read_csv(file_world_data, encoding="cp1252")

    # create a file with unique values for Countries (and their codes)
    countries = world_data[[
        'Area', 'Area Code (M49)']].drop_duplicates().compute()
    countries['Area Code (M49)'] = countries['Area Code (M49)'].str.lstrip("'")
    current_datetime = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    str_current_datetime = str(current_datetime)
    countries.to_csv(f'data/countries_list_{str_current_datetime}.csv')

    # data transformations
    world_data_final = world_data.rename(
        columns={'Area Code (M49)': 'area_code_m49', 'Item Code': 'item_code',
                 'Element Code': 'element_code', 'Year': 'year', 'Unit': 'unit_name',
                 'Value': 'value', 'Flag': 'flag_code'})\
        .drop(columns=['Area Code', 'Area', 'Item Code (CPC)', 'Item', 'Element', 'Year Code'])

    # remove star from area code
    world_data_final['area_code_m49'] = world_data_final['area_code_m49'].str.lstrip(
        "'")

    # saving result file
    world_data_final.to_csv(
        f'{final_directory}/fact_world_data.csv', single_file=True)

    # upload file to S3
    s3.upload_file(f'{final_directory}/fact_world_data.csv',
                   config["AWS"]["OUTPUT_DATA"], 'fact_world_data.csv')
    print("********* STEP 7 (world data) FINISHED ************")


# ----------------------------------------------------------------
# API restcountries  data source
def process_countries_data(s3, final_directory):
    """Take file with unique countries codes,
        connect to API endpoint with countries data using
        unique countries codes, get information about countries,
        make transformations and
        store the result CSV file first locally and then upload to S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
        - final_directory: path to the output of the current ETL pipeline
    """

    print("********* STEP 8 (API countries) START ************")
    # get the list of countries codes that will be used as a parameter for API calls
    list_of_files = glob.glob('data/countries_list_*')
    latest_file = max(list_of_files, key=os.path.getctime)
    print(latest_file)

    df_countries = pd.read_csv(latest_file)
    list_of_countries = []
    for i in df_countries.values:
        list_of_countries.append(f'{i[2]:03d}')

    print(len(list_of_countries))

    # make API calls to get information about the countries, save results in json files
    result_file_countries = []
    for i in list_of_countries:
        url = "https://restcountries.com/v3.1/alpha/"+i + \
            "?fields=ccn3,flags,name,capital,languages,area,population"
        r = requests.get(url, timeout=10)
        if r.status_code >= 201:
            continue
        data = r.json()
        result_file_countries.append(data)

    # result_file_countries
    json_object = json.dumps(result_file_countries, indent=4)

    # Writing to countries_info.json
    with open("data/countries_info.json", "w", encoding="utf-8") as outfile:
        outfile.write(json_object)

    # data transformations
    data_ctb = json.load(open('data/countries_info.json', encoding="utf-8"))
    countries_info_data = []
    for item in data_ctb:
        if item['languages']:
            item['languages'] = ', '.join(str(value)
                                          for value in item['languages'].values())
        if item['capital']:
            item['capital'] = ', '.join(value for value in item['capital'])
        if item['name']['nativeName']:
            del item['name']['nativeName']

        countries_info_data.append(item)

    countries_info_data_final = pd.json_normalize(countries_info_data)

    # saving result file
    countries_info_data_final.to_csv(
        f'{final_directory}/dim_country_info.csv')

    # saving result file in S3 bucket
    s3.upload_file(f'{final_directory}/dim_country_info.csv',
                   config["AWS"]["OUTPUT_DATA"], 'dim_country_info.csv')
    print("********* STEP 8 (API countries) FINISHED ************")


# =================================================================
# 4.2 Data Quality Checks
# ----------------------------------------------------------------
def quality_checks(s3):
    # ------------------------------------------------------------
    """Check that each result file is saved locally in the correct folder,
        and is not empty,
        and verify that the file is fully uploaded to the 
        correct AWS S3 bucket.

    Args:
        - s3: boto3 client for using AWS S3 bucket
    """
    # data quality checks for dim_unit.csv
    # check that the file is saved in the right directory
    print("********* STEP 9 (quality checks) START ************")
    path = 'output/dim_unit.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_unit.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_unit.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # data quality checks for dim_item_group.csv

    # check that the file is saved in the right directory
    path = 'output/dim_item_group.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_item_group.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_item_group.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # quality checks for dim_flag.csv

    # check that the file is saved in the right directory
    path = 'output/dim_flag.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_flag.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_flag.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # quality checks for dim_element.csv

    # check that the file is saved in the right directory
    path = 'output/dim_element.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_element.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_element.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # quality checks for dim_country_group.csv

    # check that the file is saved in the right directory
    path = 'output/dim_country_group.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_country_group.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_country_group.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # quality checks for fact_world_data.csv

    # check that the file is saved in the right directory
    path = 'output/fact_world_data.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/fact_world_data.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='fact_world_data.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')

    # ------------------------------------------------------------
    # quality checks for dim_country_info.csv

    # check that the file is saved in the right directory
    path = 'output/dim_country_info.csv'

    if os.path.isfile(path) is True:
        print('The file is saved in the correct local folder')
    else:
        print('The file not in saved, or in the wrong folder!')

    # check that the file is not empty
    if os.stat('output/dim_country_info.csv').st_size != 0:
        print('The file is not empty')
    else:
        print('The file is EMPTY!')

    # check that the file is uploaded to S3 bucket
    response = s3.list_objects_v2(
        Bucket=config["AWS"]["OUTPUT_DATA"],
        Prefix='dim_country_info.csv',
    )

    if 'Contents' in response:
        print('Object has fully uploaded to S3')
    else:
        print('Object has not fully uploaded to S3')
    print("********* STEP 9 (quality checks) FINISHED ************")


def main():
    """Load input data,
        process the data to extract dimension and fact tables,
        store processed data in CSV format in AWS S3 bucket.

        Args: None

        Output:
        S3 bucket with the following CSV files: 
        - units.csv file 
        ....

    """
    current_directory = os.getcwd()
    s3 = boto3.client('s3')
    prepare_data_sources(current_directory)

    os.environ['AWS_ACCESS_KEY_ID'] = config["AWS"]['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY'] = config["AWS"]['AWS_SECRET_ACCESS_KEY']

    # creating output folder where the result files will be uploaded
    # current_directory = os.getcwd()

    final_directory = os.path.join(current_directory, r'output')
    if not os.path.exists(final_directory):
        os.makedirs(final_directory)

    process_units_data(s3, final_directory)
    process_item_group_data(s3, final_directory)
    process_flags_data(s3, final_directory)
    process_elements_data(s3, final_directory)
    process_country_group_data(s3, final_directory)
    process_world_file_data(s3, final_directory)
    process_countries_data(s3, final_directory)
    quality_checks(s3)


if __name__ == "__main__":
    main()
