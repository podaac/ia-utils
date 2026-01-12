"""
Entry point for the Cumulus deletion tool.
This script utilizes curl to submit a DELETE request to the desired Cumulus endpoint.
This includes removal from CMR and deletion from Cumulus.
It is adapted from the legacy/Run_curl_removefromCMR-deletefromCumulus.sh script
and its SWOT variant.
"""
import logging
import json
import os.path
import subprocess
import sys
from datetime import datetime
from io import TextIOWrapper

import click

from launchpad import get_token

logging.basicConfig(format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

VENUES = {
    'uat': 'https://<uat-url>.execute-api.us-west-2.amazonaws.com/dev',  
    'ops': 'https://<sit-url>.execute-api.us-west-2.amazonaws.com/dev',  
}


def process_deletions(granules: list, default_collection: str, endpoint_url: str,
                      record_file : TextIOWrapper, dry_run: bool) -> None:
    """
    Processes the deletion of granules from Cumulus using curl commands.

    :param granules: List of granule IDs (with optional collection IDs) to delete.
    :param default_collection: Default collection ID to use if not specified per granule.
    :param endpoint_url: The Cumulus endpoint URL to use for deletion.
    :param record_file: File to write the status of each deletion for tracking purposes.
    :param dry_run: If True, only prints the curl commands without executing them.
    """
    for granule_index, granule in enumerate(granules, start=1):
        collection_id, granule_id = granule
        collection_id = collection_id or default_collection

        logger.info(f"Granule {granule_index}: Step 1 - Running curl to remove from CMR via Cumulus API for {granule_id}")

        granule_url = "/".join([endpoint_url, 'granules', granule_id])
        curl_command = (f"curl --request PATCH {granule_url} "
                        "--header 'Cumulus-API-Version: 2' "
                        "--header \"Authorization: Bearer {TOKEN}\" "
                        "--header 'Content-Type: application/json' "
                        "--data '{\"action\": \"removeFromCmr\"}'")

        # Always log the curl command, regardless of dry run status
        logger.info(curl_command)

        if dry_run:
            logger.info(f"Dry run enabled, not executing curl command for granule ID: {granule_id}")
            record_file.write(f"{granule_id}: Skipped CMR delete (dry-run)\n")
        else:
            try:
                # Replace {TOKEN} placeholder with actual token, retrieved fresh for each request
                result = subprocess.run(
                    curl_command.replace('{TOKEN}', get_token('TOKEN')), shell=True, check=True, capture_output=True
                )
                parsed_result = json.loads(result.stdout.decode('utf-8'))

                if "status" in parsed_result and parsed_result["status"].lower() == "success":
                    logger.info("Granule %s deleted successfully from CMR.", granule_id)
                    record_file.write(f"{granule_id}: Deleted from CMR\n")
                elif "error" in parsed_result:
                    if "name" in parsed_result and parsed_result["name"] == "RecordDoesNotExist":
                        logger.info(parsed_result["message"])
                        record_file.write(f"{granule_id}: Does not exist in CMR\n")
                    else:
                        logger.error("Unexpected error while deleting from CMR: %s", parsed_result["message"])
                        record_file.write(f"{granule_id}: CMR Delete Failed\n")
                else:
                    logger.error("Unexpected response from CMR: %s", parsed_result)
                    record_file.write(f"{granule_id}: Unexpected CMR Response\n")
            except subprocess.CalledProcessError as err:
                logger.error(f"Failed to delete granule ID from CMR, reason: %s", err)
                record_file.write(f"{granule_id}: CMR Delete Failed\n")
            except json.decoder.JSONDecodeError:
                logger.error(f"Failed to decode a JSON response from curl, raw output:\n%s", result.stdout.decode('utf-8'))

        logger.info(f"Granule {granule_index}: Step 2 - Running curl to delete from Cumulus for {granule_id}")

        # If a collection ID is provided, either from the granule file or command line,
        # use the /granules/{collectionId}/{granuleId} endpoint
        if collection_id:
            granule_url = "/".join([endpoint_url, 'granules', collection_id, granule_id])
            log_identifier = f"{collection_id}/{granule_id}"
        # Otherwise, fall back to the deprecated /granules/{granuleId} endpoint
        else:
            granule_url = "/".join([endpoint_url, 'granules', granule_id])
            log_identifier = granule_id

        curl_command = (f"curl --request DELETE {granule_url} "
                        "--header \"Authorization: Bearer {TOKEN}\"")

        logger.info(curl_command)

        if dry_run:
            logger.info(f"Dry run enabled, not executing curl command for granule ID: {log_identifier}")
            record_file.write(f"{log_identifier}: Skipped Cumulus delete (dry-run)\n")
        else:
            try:
                result = subprocess.run(
                    curl_command.replace('{TOKEN}', get_token('TOKEN')), shell=True, check=True, capture_output=True
                )
                logger.debug(result.stdout.decode('utf-8'))
                parsed_result = json.loads(result.stdout.decode('utf-8'))

                if "detail" in parsed_result and parsed_result["detail"].lower() == "record deleted":
                    logger.info("Successfully deleted granule %s from Cumulus.", granule_id)
                    record_file.write(f"{log_identifier}: Deleted from Cumulus\n")
                elif "error" in parsed_result:
                    if parsed_result["statusCode"] == 404:
                        logger.info(parsed_result["message"])
                        record_file.write(f"{log_identifier}: Does not exist in Cumulus\n")
                    else:
                        logger.error("Unexpected error while deleting from Cumulus: %s", parsed_result["message"])
                        record_file.write(f"{log_identifier}: Cumulus Delete Failed\n")
                else:
                    logger.error("Unexpected response from Cumulus: %s", parsed_result)
                    record_file.write(f"{log_identifier}: Unexpected Cumulus Response\n")
            except subprocess.CalledProcessError as err:
                logger.error("Failed to delete granule ID %s from Cumulus, reason: %s", granule_id, err)
                record_file.write(f"{log_identifier}: Cumulus Delete Failed\n")
            except json.decoder.JSONDecodeError:
                logger.error(f"Failed to decode a JSON response from curl, raw output:\n%s", result.stdout.decode('utf-8'))


@click.command()
@click.argument('granule-file')
@click.option('-v', '--venue',
              required=False, type=click.Choice(VENUES.keys(), case_sensitive=False), default="sit",
              help='\b\nCumulus venue endpoint to be used for granule deletion requests.\n'
                   'By default, the SIT endpoint is used.\n')
@click.option('-c', '--collection',
              required=False, type=str, default=None, metavar='COLLECTION_ID',
              help='\b\nCollection ID to include in the /granules/{collectionId}/{granuleId} Cumulus deletion endpoint.\n'
                   'If not provided, the deprecated /granules/{granuleId} endpoint is used instead.\n'
                   'When the input granule file specifies collection names, this option is ignored for said granules.\n')
@click.option('-o', '--output-report-dir',
              required=False, default=os.getcwd(), type=click.Path(exists=True, file_okay=False, writable=True),
              help='\b\nOutput directory for reports files.\n'
                   'By default, the report is written to the current working directory\n'
                   'using the following naming convention: delete.<GRANULE_FILE>.done.YYYYmmdd_HHMMSS\n')
@click.option('-d', '--dry-run',
              required=False, is_flag=True,
              help='\b\nPerform a dry run without actually deleting anything.\n'
                   'The curl commands will only be printed and not executed.')
def delete_command(granule_file : str, venue : str, collection : str, output_report_dir : str, dry_run: bool) -> None:
    """
    Deletes granules from Cumulus using curl commands.
    The granule IDs should be provided in GRANULE_FILE, one per line.
    Each line can optionally include a collection name before the granule ID, separated by whitespace.

    \b
    Example:
        GRANULE_ID_1
        COLLECTION_NAME_2 GRANULE_ID_2
        GRANULE_ID_3
        COLLECTION_NAME_4 GRANULE_ID_4
        ...

    When a collection is specified, the /granules/{collectionId}/{granuleId} endpoint is used
    for deletion from Cumulus. When no collection is specified, the deprecated /granules/{granuleId}
    endpoint is used instead. Note this does not apply to removal from CMR, which always uses the granule ID only.
    """
    logger.info('Running Cumulus Deletion Tool with args: %s', ' '.join(sys.argv[1:]))

    if not granule_file:
        raise ValueError("Granule file must be specified.")

    with open(granule_file, 'r') as file:
        granule_ids = [line.strip() for line in file if line.strip()]

    if not granule_ids:
        logger.warning("No valid granule IDs found in the provided file, nothing to delete.")
        return

    granules = []

    for idx, line in enumerate(granule_ids, start=1):
        parsed_granule = line.split()

        if len(parsed_granule) == 1:
            granule_id = parsed_granule[0]
            granules.append((None, granule_id))
        elif len(parsed_granule) == 2:
            collection_id, granule_id = parsed_granule
            granules.append((collection_id, granule_id))
        else:
            logger.warning(
                "Invalid granule line on line %d (too many whitespace-separated values), skipping: %s", idx, line
            )

    endpoint_url = VENUES[venue.lower()]
    logger.info("Using %s Cumulus endpoint: %s", venue, endpoint_url)

    output_record_name = f"delete.{os.path.basename(granule_file)}.done.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_record_file = os.path.join(output_report_dir, output_record_name)
    logger.info("Output record file will be written to: %s", os.path.abspath(output_record_file))

    logger.info("RUNNING Remove from CMR and Delete from Cumulus with input file: %s", granule_file)

    with open(output_record_file, 'w') as record_file:
        process_deletions(granules, collection, endpoint_url, record_file, dry_run)

    logger.info("FINISHED Remove from CMR and Deleting from Cumulus: %s", granule_file)


def main():
    delete_command()


if __name__ == "__main__":
    main()
