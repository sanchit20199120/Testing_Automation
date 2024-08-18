import base64
import json
import boto3
from botocore.exceptions import ClientError

# to get the credentials we are using secret manager service from aws(to hold the secrets securely)
def get_dsn(dsn_name:str, dsn_prefix: str = 'secret_', aws_region_name: str = 'us-east-2'):


    secret_name = dsn_prefix + dsn_name

#create a Secret Manager Client

# add aws_access_key_id  and aws_secret_access_key to get the access to aws account
    session = boto3.session.Session(
        aws_access_key_id = "<'aws_access_key_id>'>",
        aws_secret_access_key = "<'secret_key'>"
    )
    client = session.client(
        service_name='secretsmanager',
        region_name=aws_region_name
    )

    try:

        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"The secret '{secret_name}' was not found.")
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            print(f"The request for secret '{secret_name}' is invalid.")
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            print(f"The parameters for secret '{secret_name}' are invalid.")
        else:
            print(f"Unexpected error: {e}")
        return None
    else:
        if 'SecretString' in get_secret_value_response:
            return json.loads(get_secret_value_response['SecretString'])
        else:
            return base64.b64decode(get_secret_value_response['SecretBinary'])
