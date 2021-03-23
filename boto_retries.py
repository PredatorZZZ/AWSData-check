import boto3
from botocore import config
import botocore.exceptions as aws_exp

if __name__ == '__main__':
    conf = config.Config(retries={
        'max_attempts': 10,
        'mode': 'standard'
    })
    client = boto3.client('emr', config=conf)
    paginator = client.get_paginator('list_clusters')
    cluster_paginator = paginator.paginate()
    with open('aws.txt', 'w') as file:
        while 1:
            try:
                for index, item in enumerate(cluster_paginator):
                    print(f'Index: {index}  Item: {item["Clusters"]}')
                    file.write(f'Index: {index}  Item :{item}\n')
            except aws_exp.ClientError:
                continue
