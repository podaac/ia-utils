"""
lzards_delta_report.py
Generates a delta report comparing CMR granules with LZARDS granules
"""

import configparser
import os
import argparse
import sys
import time
import logging
import requests
import boto3
import json
from botocore.exceptions import ClientError

try:
    from ia_dev_tools.launchpad import get_token
    IA_DEV_TOOLS_AVAILABLE = True
except ImportError:
    IA_DEV_TOOLS_AVAILABLE = False
    print("ia_dev_tools was not imported")

log_level_name = os.environ.get('LOG_LEVEL', 'INFO').upper()
numeric_level = getattr(logging, log_level_name, None)
if not isinstance(numeric_level, int):
    # Handle invalid input
    raise ValueError(f'Invalid log level in LOG_LEVEL env var: {log_level_name}')

logging.basicConfig(
    level=numeric_level,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("debug.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)
global lzards_provider, lzards_url, aws_session, \
    granule_bucket, cmr_json_bucket, misc_bucket, \
    s3, cmr_url, size_map, token_file_path
COL_HEADERS = 'collection,granule_ur,granule_file,checksum_type,checksum,status,size\n'

checksum_types = {
    'expectedMd5Hash': 'MD5',
    'expectedSha256Hash': 'SHA256',
    'expectedSha512Hash': 'SHA512',
}


def use_token():
    """Short-circuit to decide if we're using token dispenser or reading from file

    Returns
    -------
    Token as string; use in header auth

    """
    if IA_DEV_TOOLS_AVAILABLE:
        return get_token('LAUNCHPAD_TOKEN')
    elif token_file_path:
        return get_token_from_file(token_file_path)
    else:
        logger.error('Launchpad Token was not set via token dispenser nor launchpad file. '
                     'Do you have the config for "launchpad_token_path" in cfg or '
                     'ia_dev_tools.launchpad library installed? Exiting...')
        sys.exit(1)


def get_token_from_file(filepath):
    """
    Opens the launchpad token file, parses it, and returns the value of the 'token' field.

    Args:
        filepath (str): The path to the JSON file.

    Returns:
        str: The value of the 'token' field, or None if the file or key is not found.
    """
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
            return data.get("token")
    except FileNotFoundError:
        print(f"Error: The launchpad file at {filepath} was not found.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from launchpad file at {filepath}.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def parse_lzards_checksum_type(item):
    """Parses the checksum type and value from a dictionary item.
    Args:
            item: A dictionary containing potential checksum keys.
    Returns:
            A list containing two elements:
                    - The checksum type (MD5, SHA256, SHA512, or UNKNOWN)
                    - The checksum value (None if not found)
    """
    for key, value in checksum_types.items():
        if item.get(key) is not None:
            return [value, item.get(key)]
    return ['UNKNOWN', 'None']


def _fetch_paginated_items(collection, include_events):
    all_items = []
    page_number = 1
    page_limit = 10000
    count_sum = 0

    events_param = str(include_events).lower()

    while True:
        # Build the full URL with all parameters
        url = (
            f'{lzards_url}&status=archived&metadata[collection]={collection}'
            f'&pageLimit={page_limit}&pageNumber={page_number}'
            f'&requestSummary=false&includeEvents={events_param}'
        )
        headers = {'Authorization': 'Bearer ' + use_token()}

        try:
            logger.debug(f'Getting items: {url}')
            response = requests.get(url, headers=headers, timeout=600)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx, 5xx)
            response_data = response.json()

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data (url={url}): {e}")
            # Return an empty list for this call; allows the other call to proceed
            return []

        current_items = response_data.get('items', [])
        count_sum += len(current_items)

        logger.info(
            f'(Events={events_param}) Page: {response_data.get("pageNumber")} - '
            f'Fetched: {count_sum}/{response_data.get("total")}'
        )

        if not current_items:
            break  # No more items, exit this loop

        all_items.extend(current_items)
        page_number += 1

    return all_items


def get_all_lzard_items(collection):
    # 1. Fetch items with includeEvents=false
    logger.debug(f"--- Fetching items for '{collection}' (includeEvents=false) ---")
    items_no_events = _fetch_paginated_items(collection, include_events=False)

    # 2. Fetch items with includeEvents=true
    logger.debug(f"--- Fetching items for '{collection}' (includeEvents=true) ---")
    items_with_events = _fetch_paginated_items(collection, include_events=True)

    # 3. Merge the two lists
    all_items = items_no_events + items_with_events

    logger.info(
        f"Total items fetched for '{collection}': {len(all_items)} "
        f"({len(items_no_events)} w/o events, {len(items_with_events)} w/ events)"
    )

    return all_items


def get_lzards_status_from_collection_set(collection_set, output_file):
    """
    Get LZARDS status for each collection in collection_set, and return output in output_file
    Parameters
    ----------
    collection_set: a set of collections, with version subfix
    output_file: name of output file

    Returns
    -------
    None
    """
    resp_items = []
    logger.info(f'Getting granule data from {len(collection_set)} collections from LZARDS:')
    counter = 1
    for collection in collection_set:
        logger.info(f'{counter} / {len(collection_set)}: {collection}')
        counter += 1
        try:
            # resp = requests.get(f'{lzards_url}&status=archived&metadata[collection]={collection}&pageLimit=10000',
            #                     headers={'Authorization': 'Bearer ' + use_token()}, timeout=300).json()
            resp = get_all_lzard_items(collection)
            logger.info(f'get_lzards_status_from_collection_set {collection} resp length: {len(resp)}')
        except requests.exceptions.JSONDecodeError as json_decode_error:
            logger.error(f'Issue with use_token() (has the token expired already?)\n{json_decode_error}')
            sys.exit()
        if len(resp) != 0:
            resp_items.append(resp)
        else:
            logger.warning(f'{collection} did not have any data in LZARDS '
                           f'({lzards_url}&status=archived&metadata[collection]={collection}&requestSummary=false)')
            # logger.warning(f'Response: {resp}')
    logger.info(f'Writing LZARDS resp items to {output_file}...')
    with open(output_file, 'a', encoding="utf-8") as outfile:
        outfile.write(COL_HEADERS)
        for items in resp_items:
            if items:
                for item in items:
                    if item:
                        meta_json = item.get('metadata', {})
                        collection = meta_json.get('collection', '-')
                        granule_ur = meta_json.get('granuleId', '-')
                        granule_file = meta_json.get('filename', '-')
                        checksum_type = parse_lzards_checksum_type(item)
                        status = item.get('status', '-')

                        # Log missing keys if desired
                        missing_keys = [key for key in ['collection', 'granuleId', 'filename']
                                        if key not in meta_json]
                        if missing_keys:
                            logger.warning(f"Missing keys in item: {missing_keys}")

                        joined_line = f'{collection},{granule_ur},{granule_file},{checksum_type[0]},{checksum_type[1]},{status}\n'
                        outfile.write(joined_line)
                    else:
                        logger.error(f'Issue with getting item from {items}')
                        exit()
            else:
                logger.error(f'Issue with getting items from {resp_items}')
                exit()
    logger.debug(f'LZARDS response saved to {output_file}')


def generate_delta_report(lzards_file, cmr_file, output_file):
    """
    Take in the lzards file and cmr file, compares and generates delta on when lzards lacks file that exists in CMR
    Parameters
    ----------
    lzards_file
    cmr_file
    output_file

    Returns
    -------
    generated file along with print out of stats
    """
    lzards_granule_set = set()
    with open(lzards_file, 'r', encoding="utf-8") as lzards_granule_files:
        for line in lzards_granule_files:
            if COL_HEADERS in line:
                continue
            parts = line.split(',')
            lzards_granule_set.add((parts[0], parts[1], parts[2], parts[3], parts[4], parts[5][:-1]))

    cmr_granule_set = set()
    with open(cmr_file, 'r', encoding="utf-8") as cmr_granule_files:
        for line in cmr_granule_files:
            if COL_HEADERS in line:
                continue
            parts = line.split(',')
            cmr_granule_set.add((parts[0], parts[1], parts[2], parts[3], parts[4], 'archived'))

    missing_granules_from_lzards = cmr_granule_set.difference(lzards_granule_set)

    # Catch when "MD5 is in LZARD but not in CMR", remove from "needs to be backed up"
    md5_missing = {cmr for cmr in cmr_granule_set
                   if cmr[3] == 'CHECKSUM_TYPE_MISSING_FROM_CMR'
                   for lzard in lzards_granule_set
                   if cmr[2] == lzard[2] and lzard[3] in ('MD5', 'SHA256', 'SHA512')}

    for granule in md5_missing:
        missing_granules_from_lzards.remove(granule)

    missing_count = 0
    not_in_s3_count = 0

    with open(output_file, 'w', encoding="utf-8") as out_file:
        out_file.write(COL_HEADERS)
        count = 0
        total = len(missing_granules_from_lzards)
        for granule in missing_granules_from_lzards:
            count += 1
            logger.info(f'{count}/{total} processing to {output_file}...')

            if granule[2] in size_map:
                size = size_map[granule[2]]
            else:
                size = 0

            if granule[2].startswith('s3://') and validate_s3_exists(granule[2]):
                new_granule = (granule[0], granule[1], granule[2], granule[3], granule[4], 'CAN_BE_PASSED_TO_LZARDS', str(size))
                missing_count += 1
            else:
                new_granule = (granule[0], granule[1], granule[2], granule[3], granule[4], 'FILE_MISSING_FROM_S3', str(size))
                not_in_s3_count += 1
            out_file.write(','.join(new_granule))
            out_file.write('\n')

    if len(missing_granules_from_lzards):
        logger.info(f'{len(missing_granules_from_lzards)} files are not on LZARDS')
        logger.info(f'{missing_count} can be uploaded to LZARDS')
    else:
        logger.info(f'No files to upload to LZARDS')
    if not_in_s3_count:
        logger.info(f'{not_in_s3_count} files can\'t be found within s3, thus can\'t be uploaded')
    logger.info(f'{output_file} created!')


def validate_s3_exists(s3_url):
    """
    Validates S3 URL does exist and is accessible
    Parameters
    ----------
    s3_url: S3 url string

    Returns
    -------
    bool: true if S3 URL exists

    """
    bucket_name, key = parse_s3_url(s3_url)
    try:
        s3.Object(bucket_name, key).load()
    except ClientError as e:
        if e.response['Error']['Code'] == "404":
            logger.error(f'{s3_url} is in CMR but not in S3')
            return False
        raise e
    return True


def parse_s3_url(url):
    """
    Converts a S3 URL to its parts (bucket and key)
    Parameters
    ----------
    url: a S3 URL

    Returns
    -------
    Tuple of bucket and key

    """
    if not url.startswith('s3://'):
        return None
    parts = url[5:].split('/')
    if not parts:
        return None
    return parts[0], '/'.join(parts[1:])


def collections_file_to_list(collections_file) -> []:
    """
    Converts collections file to list
    Parameters
    ----------
    collections_file: file of collections

    Returns
    -------
    List of collections

    """
    try:
        with open(collections_file, 'r', encoding="utf-8") as f:
            collections_list = f.readlines()
    except FileNotFoundError:
        logger.error(f"Error: File '{collections_file}' not found.")
        sys.exit()
    else:
        collections_list = [line.rstrip() for line in collections_list]
    return collections_list


def get_cmr_granules(args, cmr_out) -> []:
    """
    Get CMR granules from input (collections and config)
    Parameters
    ----------
    args: args parameter used from run()
    cmr_out: output file name

    Returns
    -------
    list of granules

    """
    revision_date = None
    if args.start and args.end:
        logger.info(f'CMR Scan DateTime between: {args.start}-{args.end}')
        revision_date = f'{args.start},{args.end}'
    elif args.start:
        logger.info(f'CMR Scan DateTime after: {args.start}')
        revision_date = args.start
    elif not args.start and args.end:
        logger.error(f'Just having --end as {args.end} without --start is invalid')
        sys.exit()

    with open(cmr_out, 'a', encoding="utf-8") as cmr_log:
        cmr_log.write(COL_HEADERS)

    granule_string = ''

    collections_list = collections_file_to_list(args.input)
    collection_sets_with_versions = set()

    logger.info(f'Getting granule data from {len(collections_list)} collections from CMR:')
    counter = 1
    for collection_short_name in collections_list:
        logger.info(f'{counter} / {len(collections_list)}: {collection_short_name}')
        counter += 1
        granule_result, collection_set_with_version = get_granules_info_from_cmr(collection_short_name, revision_date)
        collection_sets_with_versions = collection_sets_with_versions.union(collection_set_with_version)
        for granule in granule_result:
            granule_string += ",".join(granule.values()) + '\n'

    with open(cmr_out, 'a', encoding="utf-8") as cmr_file:
        cmr_file.write(granule_string)

    return collection_sets_with_versions


def fetch_data_recursively(url, headers, all_items=[]):
    response = requests.get(url, headers=headers)
    resp_headers = response.headers
    resp_body = response.json()

    if resp_body.get('errors'):
        logger.error(f'Error with CMR response: \n{resp_body.get("errors")}\n(has the token expired already?)')
        sys.exit()

    response.raise_for_status()  # Raise an exception for error HTTP statuses

    data = resp_body
    all_items.extend(data.get('items'))

    next_page_token = resp_headers.get('CMR-Search-After')
    if next_page_token:
        next_headers = headers.copy()
        next_headers['CMR-Search-After'] = next_page_token
        fetch_data_recursively(url, next_headers, all_items)

    return all_items


def get_granules_info_from_cmr(collection_short_name, revision_date=None) -> []:
    """
    Get a list of granules for specific collection and revision date
    Parameters
    ----------
    collection_short_name: the collection id
    revision_date: start/end date (revision_date) of granule id

    Returns
    -------
    list of granules

    """
    if revision_date:
        query_url = f'{cmr_url}?short_name={collection_short_name}&revision_date={revision_date}&page_size=2000'
    else:
        query_url = f'{cmr_url}?short_name={collection_short_name}&page_size=2000'
    try:
        # Needs a loop with page_size and extract CMR-Search-After from header
        # resp = requests.get(query_url, headers={'Authorization': use_token()}, timeout=60).json()

        headers = {
            "Authorization": f'{use_token()}',
        }
        # Initialize grand list per collection
        all_items = []
        all_items = fetch_data_recursively(query_url, headers, all_items)
        logger.info(f'Found {len(all_items)} granules for {collection_short_name}')

    except requests.exceptions.JSONDecodeError as json_decode_error:
        logger.error(f'Issue with use_token() (has the token expired already?)\n{json_decode_error}')
        sys.exit()
    granules = []

    collection_id_with_version_set = set()

    for granule in all_items:
        granule_files = granule.get('umm').get('DataGranule').get('ArchiveAndDistributionInformation')
        # This is pretty much betting on lzards_backup lambda in ingest doesn't change:
        # https://github.com/nasa/cumulus/blob/1d24e9be8038e1d59f59d6f6bec025acdd32f9cb/packages/message/src/Collections.ts#L5
        collection_id_with_version = f'{collection_short_name}___{granule.get("umm").get("CollectionReference").get("Version")}'
        collection_id_with_version_set.add(collection_id_with_version)
        granule_file_s3_urls = granule.get('umm').get('RelatedUrls')
        granule_files = merge_cmr_related_url_with_granule_files(granule_files, granule_file_s3_urls)
        granule_ur = granule.get('meta').get('native-id')
        for granule_file in granule_files:
            size_map[granule_file.get('URL', granule_file.get('Name'))] = granule_file.get('SizeInBytes')
            granules.append({'collection': collection_id_with_version,
                             'granule_ur': granule_ur,
                             'granule_file': granule_file.get('URL', granule_file.get('Name')),
                             'checksum_type': granule_file.get('Checksum', {'Algorithm': 'CHECKSUM_TYPE_MISSING_FROM_CMR'}).get('Algorithm'),
                             'checksum': granule_file.get('Checksum', {'Value': 'CHECKSUM_VALUE_MISSING_FROM_CMR'}).get('Value'),
                             'status': '-',
                             'sizeInBytes': str(granule_file.get('SizeInBytes', '0'))})
    return granules, collection_id_with_version_set


def merge_cmr_related_url_with_granule_files(granule_files, granule_file_s3_urls):
    """
    Double loop though each granule with urls to find s3:// URLS and append to granule_files
    Parameters
    ----------
    granule_files
    granule_file_s3_urls

    Returns
    -------

    """
    for granule_file in granule_files:
        for granule_file_s3_url in granule_file_s3_urls:
            if (granule_file_s3_url.get('URL').startswith(f's3://{granule_bucket}') and
                    granule_file_s3_url.get('URL').endswith(granule_file.get('Name'))):
                granule_file['URL'] = granule_file_s3_url.get('URL')
            elif (granule_file_s3_url.get('URL').startswith(f's3://{cmr_json_bucket}') and
                  granule_file_s3_url.get('URL').endswith(granule_file.get('Name'))):
                granule_file['URL'] = granule_file_s3_url.get('URL')
            elif (granule_file_s3_url.get('URL').startswith(f's3://{misc_bucket}') and
                  granule_file_s3_url.get('URL').endswith(granule_file.get('Name'))):
                granule_file['URL'] = granule_file_s3_url.get('URL')
    return granule_files


def run():
    """
    Main entry point of script
    Returns
    -------
    None

    """
    global lzards_provider, lzards_url, aws_session, \
        granule_bucket, cmr_json_bucket, misc_bucket, \
        s3, cmr_url, size_map, token_file_path

    parser = argparse.ArgumentParser(description='Configure aws, and LZARDS settings')
    parser.add_argument('--config', type=str, required=True,
                        help='The config file to read provider, url, aws profile, etc. from')
    parser.add_argument('--input', type=str, required=True,
                        help='The list of specific collections to check, '
                             'the LZARDS backup status.')
    parser.add_argument('--cleanup', action="store_true",
                        help='Delete lzards and s3 tmp files if report generation succeeds')
    parser.add_argument('--start', type=str, required=False,
                        help='Scan start time of "CMR Revision DateTime"; '
                             'formate of YYYY-MM-DDTHH:MM:SS.sssZ Ex. 2024-02-26T23:54:54.461Z')
    parser.add_argument('--end', type=str, required=False,
                        help='Scan end time of "CMR Revision DateTime"; '
                             'formate of YYYY-MM-DDTHH:MM:SS.sssZ Ex. 2024-02-26T23:54:54.461Z')
    parser.add_argument('--version', '-v', action='version', version='%(prog)s 1.2')

    # parse the command line args and config file...
    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config)

    # ...then set up our lzards & aws connections...
    lzards_provider = config['LZARDS'].get('Provider')
    lzards_ep = config['LZARDS'].get('Endpoint')
    lzards_url = f'https://{lzards_ep}/api/backups?provider={lzards_provider}'
    aws_profile = config['AWS'].get('Credentials')
    if aws_profile:
        aws_session = boto3.Session(profile_name=aws_profile)
    else:
        aws_session = boto3.Session()  # Use default profile on system
    granule_bucket = config['AWS'].get("GranuleFileBucket")
    cmr_json_bucket = config['AWS'].get("CMRJsonFileBucket")
    misc_bucket = config['AWS'].get("MiscFileBucket")
    s3 = aws_session.resource('s3')
    cmr_ep = config['CMR'].get('Endpoint')
    cmr_url = f'https://{cmr_ep}/search/granules.umm_json'
    size_map = {}

    # ...then initialize our temp files...
    tmp_path = config['DEFAULT'].get("TempFolder")
    lzards_status_file = config['DEFAULT'].get("Lzards_report_file_prefix")
    cmr_status_file = config['DEFAULT'].get("CMR_report_file_prefix")
    delta_report_file = config['DEFAULT'].get("Delta_report_file_prefix")
    time_stamp = time.time_ns()
    lzards_file = f'{tmp_path}/{lzards_status_file}_{time_stamp}.csv'
    cmr_file = f'{tmp_path}/{cmr_status_file}_{time_stamp}.csv'
    delta_file = f'{tmp_path}/{delta_report_file}_{time_stamp}.csv'

    # and get the token file path...
    token_file_path = config['DEFAULT'].get("launchpad_token_path")

    # ... finally, run our queries
    collection_list_with_version = get_cmr_granules(args, cmr_file)
    get_lzards_status_from_collection_set(collection_list_with_version, lzards_file)
    generate_delta_report(lzards_file, cmr_file, delta_file)
    if args.cleanup:
        os.remove(lzards_file)
        os.remove(cmr_file)


if __name__ == "__main__":
    run()