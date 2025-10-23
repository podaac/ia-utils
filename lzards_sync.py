import argparse
import configparser
import logging
import os
import boto3
import requests
import csv
import json
import time
import sys

from hashlib import md5

try:
    from ia_dev_tools.launchpad import get_token
    IA_DEV_TOOLS_AVAILABLE = True
except ImportError:
    IA_DEV_TOOLS_AVAILABLE = False
    print("ia_dev_tools was not imported")

'''
This script parses a given text file of granule file names, and generates
LZARDS backup requests for each file.

inputs:
    --config: the path to the config file (in cfg/ini format)
              see example: /templates/lzards_sync_sit.cfg

    --input: a csv file, where each line is of the format:
             collection, granule_ur, granule_s3_path, checksum_type, checksum_value

The script then uses the configured buckets, aws credentials, and lzards
options to generate a backup request for each line in the --input file
according to the sample request from lzards user docs, shown below.

-----------------------------------------------------------------------------------
Example backup request from LZARDS wiki/user guide:
-----------------------------------------------------------------------------------
{
    "provider": "FAKE_DAAC",
    "objectUrl": "...presigned S3 URL...",
    "expectedMd5Hash": "55a9b94250865a9564d43ee157a7e940",
    "expectedSha256Hash": "2e20b301476c37e81d4f0a18fa306481b81fa2b124cdf640c235e932cc4ec487",
    "metadata": {
        "filename": "AAAA_BBBB_0001.foo",
        "collection": "SOME_FAKE_COLLECTION_0002",
        "granuleId": "1234"
    }
}
-----------------------------------------------------------------------------------
Example backup request, recreated from LZARDS uat backup status:
-----------------------------------------------------------------------------------
{
    'provider': 'pocloud_sit',
    'objectUrl': 'https://podaac-sit-cumulus-protected.s3.us-west-2.amazonaws.com/JASON_CS_S6A_L0_STR/S6A_ST_0__STR_____20201214T210315_20201214T225901_20201214T225946_6945_003_087_044_EUM__OPE_NR____.SEN6.ISPData.dat<EXTRA_METADATA>',
    'expectedMd5Hash': 'b47447d2e9fefb8959ed7c45bfc8a8e9',
    'metadata': {
        'filename': 's3://podaac-sit-cumulus-protected/JASON_CS_S6A_L0_STR/S6A_ST_0__STR_____20201214T210315_20201214T225901_20201214T225946_6945_003_087_044_EUM__OPE_NR____.SEN6.ISPData.dat',
        'granuleId': 'S6A_ST_0__STR_____20201214T210315_20201214T225901_20201214T225946_6945_003_087_044_EUM__OPE_NR____.SEN6',
        'collection': 'JASON_CS_S6A_L0_STR___F'
    }
}
-----------------------------------------------------------------------------------
'''

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
global backup_url, search_url, lzards_provider, aws_session, s3, granule_bucket, cmr_json_bucket, timeout, tmp_folder, LAUNCHPAD_TOKEN_PATH
COL_HEADERS = f'collection,granule_ur,granule_file,checksum_type,checksum\n'

def use_token():
    """Short-circuit to decide if we're using token dispenser or reading from file

    Returns
    -------
    Token as string; use in header auth

    """
    if IA_DEV_TOOLS_AVAILABLE:
        return get_token()
    elif LAUNCHPAD_TOKEN_PATH:
        return extract_token_from_json(LAUNCHPAD_TOKEN_PATH)
    else:
        logger.error('TOKEN is not set via token dispenser nor launchpad file, exiting...')
        sys.exit(1)

def extract_token_from_json(file_path):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            return data.get('token')
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"Error reading JSON file at {file_path}: {e}")
        sys.exit()


def send_backup_request(backup_json, granule_ur, max_retries=5, retry_delay=60):
    if backup_json is None:
        logger.error('Backup JSON is empty/null, skipping.')
        return

    for attempt in range(1, max_retries + 1):
        launchpad_token = use_token()
        logger.debug(f"Sending backup request for granule: {granule_ur}")

        response = requests.post(
            backup_url,
            json=backup_json,
            headers={'Authorization': f'Bearer {launchpad_token}'}
        )
        response_code = response.status_code

        if response_code == 201:
            return response
        elif response_code == 401:
            logger.error(
                f"Authentication error (attempt {attempt}/{max_retries}): is the launchpad token fresh? {granule_ur[:5]}..."
            )
        elif response_code == 422:
            logger.error(f"Malformed/invalid backup request for {granule_ur}")
        else:
            logger.error(f"Unexpected response code: {response_code}.")

        if attempt < max_retries:
            print(f"Retry {attempt}/{max_retries} with {retry_delay} delay...")
            time.sleep(retry_delay)

    logger.error(f"Max retries reached ({max_retries}) for granule: {granule_ur}")
    sys.exit(1)


def parse_s3_url(url):
    if not url.startswith('s3://'):
        return None
    parts = url[5:].split('/')
    if not parts:
        return None
    return parts[0], '/'.join(parts[1:])


def download_and_checksum(s3_object_path):
    """
    Downloads a file from S3 given its full path (bucket/key), calculates MD5 checksum, and deletes local copy.

    Args:
            s3_object_path: The full path (bucket/key) of the S3 object.

    Returns:
            The MD5 checksum of the downloaded file as a hexadecimal string,
            or None if there's an error.
    """
    try:
        logger.info(f'{s3_object_path} does not have checksum calculated in CMR, downloading and calculating...')
        bucket_name, file_key = parse_s3_url(s3_object_path)

        parts = file_key.rsplit("/", 1)
        if len(parts) == 1:
            file_name = parts[0]
        file_name = parts[-1]

        with open('tmp' + file_name, 'wb') as f:
            s3.Object(bucket_name=bucket_name, key=file_key).download_fileobj(f)

        checksum = md5()
        with open('tmp' + file_name, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                checksum.update(chunk)
        checksum_str = checksum.hexdigest()

        os.remove('tmp' + file_name)

        return checksum_str
    except Exception as e:
        logger.error(f'Error downloading and calculating checksum for {s3_object_path}: {e}')
        return None  # Indicate error


def build_granule_object_url(s3_path):
    bucket, key = parse_s3_url(s3_path)
    s3_client = aws_session.client('s3')
    return s3_client.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=timeout)


def build_backup_request(collection, granule_ur, s3_path, checksum_type, checksum):
    presigned_url = build_granule_object_url(s3_path)
    backup_json = {"provider": lzards_provider, "objectUrl": presigned_url}

    if checksum_type == "CHECKSUM_TYPE_MISSING_FROM_CMR" and checksum == "CHECKSUM_VALUE_MISSING_FROM_CMR":
        # If checksum/checksum_type is missing, download and calculate for MD5
        backup_json["expectedMd5Hash"] = download_and_checksum(s3_path)
    elif checksum_type == "MD5":
        backup_json["expectedMd5Hash"] = checksum
    elif checksum_type == "SHA512":
        backup_json["expectedSha512Hash"] = checksum
    elif checksum_type == "SHA256":
        backup_json["expectedSha256Hash"] = checksum
    else:
        logger.error(f'unexpected checksum_type: {checksum_type} for {s3_path}; checksum value: {checksum}')
        return None  # Exit out and move to next granule

    metadata_json = {"filename": s3_path, "collection": collection,
                     "granuleId": granule_ur}
    backup_json["metadata"] = metadata_json
    return backup_json


def csv_to_dict(filename) -> []:
    """
    This function converts a CSV file to a dictionary of lists.
    Args:
            filename: The path to the CSV file.
    Returns:
            A list of dictionaries containing the rows of data in the CSV
    """
    try:
        with open(filename, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            csv_list = []
            for row in reader:
                csv_list.append(row)

    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        sys.exit()
    except UnicodeDecodeError:
        print(f"Error: issue decoding file '{filename}'.")
        sys.exit()
    else:
        return csv_list


def process_delta_file(delta_file):
    # fields in the csv come from the delta report:
    # [0] = collection
    # [1] = granule_ur (granule id)
    # [2] = granule_file (granule s3 url)
    # [3] = checksum_type
    # [4] = checksum_value

    sent_count = 0
    total = 0

    delta_dict = csv_to_dict(delta_file)

    for item in delta_dict:
        # Check if target string exists in any value of the dictionary (using list comprehension)
        if any('s3://' in value for value in item.values()):
            total += 1

    for d in delta_dict:
        if d.get('granule_file').startswith('s3://'):
            backup_json = build_backup_request(d.get('collection'),
                                               d.get('granule_ur'),
                                               d.get('granule_file'),
                                               d.get('checksum_type'),
                                               d.get('checksum'))
            if backup_json:
                sent_count += 1
                logger.info(f'{sent_count} / {total}: Sending {d.get("collection")}: {d.get("granule_ur")}\n\t'
                            f'{d.get("granule_file")}')
                resp = send_backup_request(backup_json, d.get('granule_ur'))
                backup_request_id = 'N/A'
                try:
                    backup_request_id = resp.json().get('id')
                except requests.exceptions.RequestException as e:
                    print(f"An error occurred with the request: {e}")
                except KeyError:
                    print(f"The 'id' key was not found in the JSON response. Resp: {resp}")
                except requests.exceptions.JSONDecodeError:
                    print(f"Failed to decode JSON from the response. Resp: {resp}")
                logger.info(f'send_backup_request status: {resp.status_code}, backup_request_id: {backup_request_id}')
            else:
                logger.error(f'Could not build backup request for {d}')
    logger.info(f'{sent_count} records processed and sent')


def run():
    global lzards_provider, backup_url, search_url, aws_session, s3, granule_bucket, cmr_json_bucket, timeout, \
        tmp_folder, LAUNCHPAD_TOKEN_PATH

    parser = argparse.ArgumentParser(description='Configure aws, and LZARDS settings')
    parser.add_argument('--config', type=str, required=True,
                        help='The config file to read provider, url, aws profile, etc. from')
    parser.add_argument('--input', type=str, required=True,
                        help='the input list (txt file) of which granule files to backup')
    parser.add_argument('--version', '-v', action='version', version='%(prog)s 1.1')

    # parse the command line args...
    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config)
    # ...now make sure we can read the input delta file...
    delta_file = args.input
    if not open(delta_file).readable():
        raise FileNotFoundError('Unable to open/read the input file')
    # ...then set up our lzards & aws connections...
    LAUNCHPAD_TOKEN_PATH = config['DEFAULT'].get('launchpad_token_path')
    lzards_provider = config['LZARDS'].get('Provider')
    lzards_ep = config['LZARDS'].get('Endpoint')
    backup_url = f'https://{lzards_ep}/api/backups'
    search_url = f'https://{lzards_ep}/api/backups?provider={lzards_provider}'
    aws_profile = config['AWS'].get('Credentials')
    if aws_profile:
        aws_session = boto3.Session(profile_name=aws_profile)
    else:
        aws_session = boto3.Session()  # Use default profile on system
    timeout = config['AWS'].get('UrlTimeout')
    tmp_folder = config['DEFAULT'].get('TempFolder')
    s3 = aws_session.resource('s3')
    granule_bucket = config['AWS'].get("GranuleFileBucket")
    cmr_json_bucket = config['AWS'].get("CMRJsonFileBucket")
    # ... finally, start generating backup requests for entries in the input file
    process_delta_file(delta_file)


if __name__ == "__main__":
    run()