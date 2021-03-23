import argparse
import json
import datetime
import logging
import sys
import os

from py.xml import html
import boto3
import botocore.exceptions as aws_err


class AWSData:

    def __init__(self, aws_profile, aws_region):
        self.aws_profile = aws_profile
        self.aws_region = aws_region
        self.session = boto3.Session(profile_name=self.aws_profile)

    def __create_aws_client(self, aws_service):
        return self.session.client(aws_service)

    def get_aws_glue_data(self, table):
        database_name, table_name = table.split('.')
        glue_client = self.__create_aws_client('glue')
        try:
            glue_response = glue_client.get_table(
                DatabaseName=database_name,
                Name=table_name
            )
            output = {
                'Database':
                    glue_response['Table']['DatabaseName'],
                'S3Location':
                    glue_response['Table']['StorageDescriptor']['Location']
            }
            return output
        except aws_err.ClientError:
            return {
                'Database': None,
                'S3Location': None
            }

    def get_aws_s3_data(self, table):
        output = {
            'FileResults': None
        }
        s3_client = self.__create_aws_client('s3')
        table_data = self.get_aws_glue_data(table)
        if table_data['S3Location']:
            bucket = table_data['S3Location'].split('/')[2]
            prefix = '/'.join(table_data['S3Location'].split('/')[3:])
            s3_response = s3_client.list_objects(
                Bucket=bucket,
                Prefix=prefix
            )
            output['FileResults'] = [
                item['Key'] for item in s3_response['Contents']
            ]
            return output
        return output

    def get_aws_athena_data(self, table):
        database_name, table_name = table.split('.')
        download_file_name = f'athena_{database_name}_{table_name}.csv'
        athena_client = self.__create_aws_client('athena')
        s3_client = self.__create_aws_client('s3')
        athena_request = athena_client.start_query_execution(
            QueryString=f'SELECT * FROM {table_name} LIMIT 10;',
            QueryExecutionContext={
                'Database': database_name
            }
        )
        athena_response = athena_client.get_query_execution(
            QueryExecutionId=athena_request['QueryExecutionId']
        )
        output_file_path = athena_response['QueryExecution']\
            ['ResultConfiguration']['OutputLocation']
        bucket = output_file_path.split('/')[2]
        key = '/'.join(output_file_path.split('/')[3:])
        while 1:
            try:
                s3_client.download_file(
                    bucket, key, download_file_name
                )
                return f'{os.getcwd()}/{download_file_name}'
            except aws_err.ClientError:
                continue


class HTMLReport:

    TITLE = "AWS tables checker"

    def __init__(self, file_name):
        self.file_name = file_name
        self.date_time = datetime.datetime.today()
        self.data_results = []
        self.tables_processed = 0

    def _generate_report(self):
        css_href = "assets/style.css"
        html_css = html.link(href=css_href, rel="stylesheet", type="text/css")
        head = html.head(
            html.meta(charset="utf-8"),
            html.title(self.TITLE),
            html_css
        )
        table = [
            html.h2("Results"),
            html.table(
                self.data_results,
                id="results-table",
            ),
        ]
        body = html.body(
            html.h1(self.TITLE),
            html.p(f'Report generated at {self.date_time.strftime("%Y-%m-%d %H:%M:%S")}'),
            html.p(f'Tables processed: {self.tables_processed}')
        )
        body.extend(table)
        doc = html.html(head, body)
        unicode_doc = f"<!DOCTYPE html>\n{doc.unicode(indent=2)}"
        unicode_doc = unicode_doc.encode("utf-8", errors="xmlcharrefreplace")
        return unicode_doc.decode("utf-8")

    def insert_data(self, tables_data: dict):
        self.tables_processed = len(tables_data.keys())
        for raw_table_name, raw_table_data in tables_data.items():
            database_name, table_name = raw_table_name.split('.')
            self.data_results.append(
                html.div(
                    html.tr(
                        [
                            html.th('Database:', bgcolor="#C0C0C0"),
                            html.th(database_name),
                            html.th('Table name:', bgcolor="#C0C0C0"),
                            html.th(table_name)
                        ]
                    ),
                    html.tr(
                        [
                            html.th('AWS Glue results:',
                                    colspan="4",
                                    style="text-align:center",
                                    bgcolor="#C0C0C0")
                        ]
                    ),
                    html.tr(
                        [
                            html.th(raw_table_data['Glue'],
                                    colspan="4")
                        ]
                    ),
                    html.tr(
                        [
                            html.th('AWS S3 results:',
                                    colspan="4",
                                    style="text-align:center",
                                    bgcolor="#C0C0C0")
                        ]
                    ),
                    html.tr(
                        [
                            html.th(raw_table_data['S3'],
                                    colspan="4")
                        ]
                    ),
                    html.tr(
                        [
                            html.th('AWS Athena results:',
                                    colspan="4",
                                    style="text-align:center",
                                    bgcolor="#C0C0C0")
                        ]
                    ),
                    html.tr(
                        [
                            html.th(raw_table_data['Athena'],
                                    colspan="4")
                        ]
                    ),
                    _class="processed-table"
                )
            )

    def save_report(self):
        with open(self.file_name, 'w') as file:
            file.write(self._generate_report())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Small utils for quick AWS tables data check"
    )
    parser.add_argument('--path',
                        default='./tables.json',
                        help='Path to JSON file with tables list',
                        type=str)
    parser.add_argument('--aws-profile',
                        default='default',
                        help='AWS profile name (see at ~/.aws/credentials)',
                        type=str)
    parser.add_argument('--aws-region',
                        default='us-east-1',
                        help='AWS region (see at ~/.aws/config)',
                        type=str)
    parser.add_argument('--output-file',
                        default='output.html',
                        help='Report file name (output.html as default)',
                        type=str)
    args = parser.parse_args()

    with open(f'{args.path}', 'r') as json_file_config:
        config_data = json.load(json_file_config)
    process_tables = config_data.get('tables')
    if process_tables:
        tables_data = dict()
        aws_data = AWSData(args.aws_profile, args.aws_region)
        html_report = HTMLReport(args.output_file)
        for process_table in process_tables:
            tables_data[process_table] = {
                'Glue': aws_data.get_aws_glue_data(process_table),
                'S3': aws_data.get_aws_s3_data(process_table),
                'Athena': aws_data.get_aws_athena_data(process_table)
            }
        html_report.insert_data(tables_data)
        html_report.save_report()
    else:
        logging.error('JSON config file is incorrect')
        sys.exit(0)
