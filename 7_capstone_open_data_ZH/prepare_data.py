from configparser import ConfigParser
import json
import logging
import os
import time
import boto3
import pandas as pd


module_logger = logging.getLogger("__name__")

non_mot_count_url_dict = {
    2009: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/6d51f9a8-66f8-41d9-8563-1220a1bd83d2/download/2009_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2010: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/d846bd8b-ae61-4221-bd3e-dc21a1617153/download/2010_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2011: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/ba48be9c-f694-4408-a493-10fe1c4dd080/download/2011_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2012: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/68028f00-c8cc-43aa-9c52-2c2dcb290620/download/2012_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2013: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/a18ec58b-8e37-4a08-a2ca-1024213b1926/download/2013_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2014: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/83d84c74-e879-4da7-9b84-e9453df2c186/download/2014_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2015: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/0b7d6081-cb18-4052-a776-36185127353d/download/2015_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2016: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/6a1626c9-1547-4030-b0d7-ff9226087385/download/2016_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2017: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/548ad6a6-2f8c-4af6-80eb-789a751d4ffd/download/2017_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2018: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/87147138-ff50-4631-ad0d-f1d3aaf6af30/download/2018_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2019: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/33b3e7d3-f662-43e8-b018-e4b1a254f1f4/download/2019_verkehrszaehlungen_werte_fussgaenger_velo.csv",
    2020: u"https://data.stadt-zuerich.ch/dataset/83ca481f-275c-417b-9598-3902c481e400/resource/b9308f85-9066-4f5b-8eab-344c790a6982/download/2020_verkehrszaehlungen_werte_fussgaenger_velo.csv",
}


def get_prepared_non_mot_counts(source_urls, target_dir):
    """Download csv files containing the counts of non-motorized traffic,
    loop through all the files, clean the data and write the preprocessed
    files into the target directory.

    Note 1: The specific datetime format is necessary for Redshift's COPY
    statement to work. This caused quite some headache. See here:
    https://stackoverflow.com/questions/28287434/how-to-insert-timestamp-column-into-redshift

    Note 2: Alternative approach for parsing CSV files from the web
    (if they were much bigger and would not fit into memory):
    https://stackoverflow.com/questions/35371043/use-python-requests-to-download-csv

    Parameters
    ----------
    source_urls : dict
        Dictionary containing the URLs for the files as values and
        the respective year as key.
    target_dir : str
        Relative path to the target directory for saving the cleaned files.
    """
    for year, url in source_urls.items():
        module_logger.info(f"Loading non_mot_count file for year {str(year)} ...")
        df = pd.read_csv(url, delimiter=',')

        df.fillna(0, inplace=True)
        for col in ["VELO_IN", "VELO_OUT", "FUSS_IN", "FUSS_OUT"]:
            df[col] = df[col].astype(int)
        df["DATUM"] = pd.to_datetime(df["DATUM"]).astype(str)

        assert df.isna().any() is not False, \
            f"Missing values in dataframe non_mot_counts_{int(year)}"

        # Write to file
        df.to_csv(
            f"{target_dir}non_mot_counts_{str(year)}.csv",
            index=False,
            header=False
        )

        module_logger.info("Done with that file. Sleeping for 10 secs now ...\n")
        time.sleep(10)

    module_logger.info("All non_mot_count files prepared and saved.\n")


def get_prepared_non_mot_locations(source_file, target_dir):
    """Transform the JSON containing the location data of the
    counting stations to CSV. This is necessary because "Amazon Redshift
    can't parse complex, multi-level data structures." (Docs, see here:)
    https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html))

    Note also that this file has to be downloaded manually as the API
    does not allow programmatic requests.
    """
    with open(source_file, 'r') as j:
        contents = json.loads(j.read())

    loc_list = []
    for n in range(0, len(contents["features"])):
        loc_list.append(contents["features"][n]["properties"])

    # Create df_loc
    df_loc = pd.DataFrame(loc_list)
    for col in ["bis", "von"]:
        df_loc[col] = pd.to_datetime(df_loc[col]).astype(str).replace("NaT", "")
    df_loc["objectid"] = df_loc["objectid"].astype('int32')

    # Create seperate df for coordinates
    list_long = []
    list_lat = []
    for n in range(0, len(contents["features"])):
        list_long.append(contents["features"][n]["geometry"]["coordinates"][0])
        list_lat.append(contents["features"][n]["geometry"]["coordinates"][1])
    df_coord = pd.DataFrame({"long" : list_long, "lat": list_lat})

    # Merge and write to csv
    df = pd.concat([df_loc, df_coord], axis=1)
    assert df.isna().any() is not False, \
        f"Missing values in dataframe non_mot_locations"
    df.to_csv(f"{target_dir}non_mot_locations.csv", index=False, header=False)
    module_logger.info("Locations file prepared and saved.\n")


def get_s3_params(param_file, section):
    """Read and return necessary parameters for connecting to s3.

    Parameters
    ----------
    param_file : str
        Filename of the configuration file (.cfg)
    section : str
        Section containing the relevant parameters.

    Returns
    -------
    tuple
        Necessary parameters and credentials for cluster creation:
        AWS Key ID, AWS Secret Key ID.
    """
    # Create a parser to read config file
    parser = ConfigParser()
    parser.read(param_file)

    # Get section, default to AWS
    s3_params = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            s3_params[param[0]] = param[1]
    else:
        raise Exception(f"Section {section} not found in the {param_file} file.")

    return s3_params


def upload_to_s3(s3_params, local_dir, s3_bucket, s3_target_dir):
    """[summary]

    Parameters
    ----------
    s3_params : tuple
        AWS Key ID, AWS Secret Key ID.
    local_dir : str
        Relative path to directory to upload.
    s3_bucket : str
        Name of target bucket on s3.
    s3_target_dir : str
        Absolute path (prefix) for data on s3.

    Note to myself: Working with the boto.client ist easier.
    Also have a look at comments int the upload jpynb for some
    additional info.
    """
    s3 = boto3.resource("s3",
                        region_name="eu-west-1",
                        aws_access_key_id=s3_params.get('key'),
                        aws_secret_access_key=s3_params.get('secret')
                        )
    for root, dirs, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            # Construct the full S3 path, set "/" as delimiter for key prefix
            relative_path = os.path.relpath(local_path, local_dir)
            if s3_target_dir is not None:
                s3_path = os.path.join(s3_target_dir, relative_path).replace("\\", "/")
            else:
                s3_path = relative_path.replace("\\", "/")
            # Check if file already exists, if yes: skip, if no: upload
            try:
                s3.head_object(Bucket=s3_bucket, Key=s3_path)
                print(f"File already on S3! Skipping {s3_path}...")
            except:
                print(f"Uploading {s3_path} ...")
                s3.upload_file(local_path, s3_bucket, s3_path)


# TODO
non_mot_counts_source_urls = non_mot_count_url_dict
non_mot_counts_target_dir = "../data/prep/non_mot_counts/"
non_mot_locations_source_file = "../data/raw/non_mot_locations/standorte_verkehrszaehlung.json"
non_mot_locations_target_dir = "../data/prep/non_mot_locations/"
local_dir = "data/prep/"
s3_bucket = "raph-dend-zh-data"
s3_target_dir = "data/prep/"


def main():
    get_prepared_non_mot_counts(
        non_mot_counts_source_urls,
        non_mot_counts_target_dir
    )
    get_prepared_non_mot_locations(
        non_mot_locations_source_file,
        non_mot_locations_target_dir
    )
    s3_params = get_s3_params("dwh.cfg", "AWS")
    upload_to_s3(s3_params, local_dir, s3_bucket, s3_target_dir)


if __name__ == "__main__":
    main()
