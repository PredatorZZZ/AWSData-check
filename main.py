import argparse
import json
import logging
import sys

from datacheck import AWSData
from datacheck import HTMLReport


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

    if len(process_tables) != len(set(process_tables)):
        logging.error('Found duplicates')

    process_tables = set(process_tables)

    if process_tables:
        tables_data = dict()
        aws_data = AWSData(args.aws_profile, args.aws_region)
        html_report = HTMLReport(args.output_file)
        for process_table in process_tables:
            process_table = process_table.lower()
            tables_data[process_table] = {
                'Glue': aws_data.get_aws_glue_data(process_table),
                'S3': aws_data.get_aws_s3_data(process_table),
                'Athena': aws_data.get_aws_athena_data(process_table)
            }
        html_report.insert_data(tables_data)
        html_report.save_report()
    else:
        sys.exit(0)
