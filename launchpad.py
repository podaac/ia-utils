from os import environ
import logging
from token_dispenser_client import token_dispenser_client as tds_client

CLIENT_ID = 'iadevtools'
logger = logging.getLogger(__name__)

def get_token(default_env_name=None):
    if default_env_name is not None:
        token = environ.get(default_env_name)
        if token is not None:
            logger.debug('Found launchpad token in envvar %s', default_env_name)
            return token

    tds_lambda_arn = environ.get('TDS_LAMBDA_ARN')
    return tds_client.get_token(CLIENT_ID, lambda_arn=tds_lambda_arn)['sm_token']
