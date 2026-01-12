"""
Entry point for the CMR deletion tool.
This script utilizes HTTP requests to submit a DELETE request to the desired CMR endpoint.
"""
import json
import logging
import os.path
import requests
import sys
from datetime import datetime
from io import TextIOWrapper
from urllib.parse import urlparse

import click

from launchpad import get_token

logging.basicConfig(format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PROVIDERS = {
    'POCLOUD': 'POCLOUD',
    'POCUMULUS': 'POCUMULUS'
}

VENUES = {
    'sit': "https://cmr.sit.earthdata.nasa.gov/ingest/providers/{provider}/granules",
    'uat': "https://cmr.uat.earthdata.nasa.gov/ingest/providers/{provider}/granules",
    'ops': "https://cmr.earthdata.nasa.gov/ingest/providers/{provider}/granules",
}


def process_deletions(granule_ids: list, endpoint_url: str,
                      record_file : TextIOWrapper, dry_run: bool) -> None:
    """
    Processes the deletion of granules from CMR using curl commands.

    :param granule_ids: List of granule IDs to delete.
    :param endpoint_url: The CMR endpoint URL to use for deletion. Should include the provider in the URL.
    :param record_file: File to write the status of each deletion for tracking purposes.
    :param dry_run: If True, only prints the curl commands without executing them.
    """
    for granule_index, granule_id in enumerate(granule_ids, start=1):
        logger.info(f"Granule {granule_index}: Submitting CMR deletion request for {granule_id}")

        granule_url = "/".join([endpoint_url, granule_id])

        # Assign the authentication token header, refreshing if necessary
        headers = {
            "Authorization": f"{get_token('TOKEN')}",
            "Content-Type": "application/json"
        }

        if dry_run:
            logger.info(f"Dry run enabled, not executing the following Delete request for granule ID: {granule_id}")

            curl_command = (f"curl -i -X DELETE "
                            "--header \"Authorization: {TOKEN}\" "
                            "--header \"Content-Type: application/json\" "
                            f"{granule_url}'")

            logger.info(curl_command)

            record_file.write(f"{granule_id}: Skipped (dry-run)\n")
        else:
            response = requests.delete(granule_url, headers=headers)

            if response.status_code == requests.codes.ok:
                logger.info("Granule %s deleted successfully from CMR.", granule_id)
                record_file.write(f"{granule_id}: Deleted from CMR\n")
            else:
                try:
                    result = response.json()

                    if "errors" in result:
                        if response.status_code == requests.codes.not_found:
                            logger.info("Granule %s not found in CMR, details:", granule_id)
                            for error in result["errors"]:
                                logger.info("\t" + error)
                            record_file.write(f"{granule_id}: Not found in CMR\n")
                        else:
                            logger.error("Failed to delete %s from CMR due to following errors:", granule_id)
                            for error in result["errors"]:
                                logger.error("\t" + error)
                            record_file.write(f"{granule_id}: CMR Delete Failed\n")
                    else:
                        logger.error("Unexpected response from CMR: %s", str(result))
                        record_file.write(f"{granule_id}: Unexpected CMR response\n")
                except json.decoder.JSONDecodeError:
                    logger.debug("Failed to decode a JSON response")
                    logger.debug("Raw output from response:\n%s", response.text)

                    logger.error("Failed to delete %s from CMR, status code: %d, reason: %s", granule_id, response.status_code, response.reason)
                    record_file.write(f"{granule_id}: CMR Delete Failed ({response.reason})\n")

@click.command()
@click.argument('granule-file')
@click.option('-p', '--provider',
              required=False, type=click.Choice(PROVIDERS.keys(),  case_sensitive=False), default='POCLOUD',
              help='\b\nCMR provider to be used for granule deletion requests.\n'
                   'By default, the POCLOUD provider is used.\n')
@click.option('-v', '--venue',
              required=False, type=click.Choice(VENUES.keys(),  case_sensitive=False), default='sit',
              help='\b\nCMR venue endpoint to be used for granule deletion requests.\n'
                    f'sit resolves to {urlparse(VENUES["sit"]).hostname}\n'
                    f'uat resolves to {urlparse(VENUES["uat"]).hostname}\n'
                    f'ops resolves to {urlparse(VENUES["ops"]).hostname}\n'
                   f'By default, the SIT endpoint is used.\n')
@click.option('-o', '--output-report-dir',
              required=False, default=os.getcwd(),
              type=click.Path(exists=True, file_okay=False, writable=True),
              help='\b\nOutput directory for report files.\n'
                   'By default, the report is written to the current working directory\n'
                   'using the following naming convention: delete.<GRANULE_FILE>.done.YYYYmmdd_HHMMSS\n')
@click.option('-d', '--dry-run',
              required=False, is_flag=True,
              help='\b\nPerform a dry run without actually deleting anything.\n'
                   'The Delete requests will only be printed as sample curl commands and not executed.')
def delete_command(granule_file : str, provider : str, venue : str, output_report_dir : str, dry_run: bool) -> None:
    """
    Deletes granules from CMR using a curl command.
    The granule IDs should be provided in GRANULE_FILE, one per line.
    """
    logger.info("Running CMR deletion tool with args: %s", ' '.join(sys.argv[1:]))

    if not granule_file:
        raise ValueError('Granule file must be specified.')

    with open(granule_file, 'r') as infile:
        granule_ids = [line.strip() for line in infile if line.strip()]

    if not granule_ids:
        logger.warning("No valid granule IDs found in the provided file, nothing to delete.")
        return

    endpoint_url = VENUES[venue.lower()].format(provider=PROVIDERS[provider.upper()])
    logger.info("Using %s CMR endpoint: %s", venue, endpoint_url)

    output_record_name = f"delete.{os.path.basename(granule_file)}.done.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_record_file = os.path.join(output_report_dir, output_record_name)
    logger.info("Output record file will be written to: %s", os.path.abspath(output_record_file))

    logger.info("RUNNING Remove from CMR with input file: %s", granule_file)

    with open(output_record_file, 'w') as record_file:
        process_deletions(granule_ids, endpoint_url, record_file, dry_run)

    logger.info("FINISHED Remove from CMR: %s", granule_file)


def main():
    delete_command()


if __name__ == '__main__':
    main()
