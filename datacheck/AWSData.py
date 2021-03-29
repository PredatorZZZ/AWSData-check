import os

import boto3
import botocore.exceptions as aws_err
from datacheck.LogInfo import LogInfo


class AWSData:
    S3_LINK_TEMPLATE = "https://console.aws.amazon.com/s3/" \
                       "home?region={REGION}&bucket={BUCKET}&prefix={PREFIX}"

    get_aws_logger = LogInfo()

    def __init__(self, aws_profile, aws_region):
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.session = boto3.Session(profile_name=self.aws_profile)

    def _create_aws_client(self, aws_service):
        return self.session.client(aws_service)

    @get_aws_logger.info
    def get_aws_glue_data(self, table):
        output = {
            'Database': None,
            'S3Location': None
        }
        database_name, table_name = table.split('.')
        glue_client = self._create_aws_client('glue')
        try:
            glue_response = glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
            output['Database'] = \
                glue_response['Table']['DatabaseName']
            output['S3Location'] = \
                glue_response['Table']['StorageDescriptor']['Location']
            return output
        except aws_err.ClientError:
            return output

    @get_aws_logger.info
    def get_aws_s3_data(self, table):
        output = {
            'S3Link': None,
            'FileResults': None
        }
        s3_client = self._create_aws_client('s3')
        table_data = self.get_aws_glue_data(table)
        if table_data['S3Location']:
            bucket = table_data['S3Location'].split('/')[2]
            prefix = '/'.join(table_data['S3Location'].split('/')[3:])
            s3_response = s3_client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )
            output['S3Link'] = self.S3_LINK_TEMPLATE \
                .replace('{REGION}', self.aws_region) \
                .replace('{BUCKET}', bucket) \
                .replace('{PREFIX}', prefix)
            output['FileResults'] = [
                item['Key'] for item in s3_response['Contents']
            ]
            return output
        return output

    @get_aws_logger.info
    def get_aws_athena_data(self, table):
        database_name, table_name = table.split('.')
        download_file_name = f'athena_{database_name}_{table_name}.csv'
        athena_client = self._create_aws_client('athena')
        s3_client = self._create_aws_client('s3')
        athena_request = athena_client.start_query_execution(
            QueryString=f'SELECT * FROM {table_name} LIMIT 10;',
            QueryExecutionContext={
                'Database': database_name
            }
        )
        athena_response = athena_client.get_query_execution(
            QueryExecutionId=athena_request['QueryExecutionId']
        )
        output_file_path = athena_response['QueryExecution'] \
            ['ResultConfiguration']['OutputLocation']
        bucket = output_file_path.split('/')[2]
        key = '/'.join(output_file_path.split('/')[3:])
        while True:
            try:
                s3_client.download_file(
                    bucket, key, download_file_name
                )
                return f'{os.getcwd()}/{download_file_name}'
            except aws_err.ClientError:
                continue
