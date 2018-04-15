from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import decimal
from boto3.dynamodb.conditions import Key, Attr

REGION_NAME = 'ap-northeast-1'
TASK_CONFIGURATION_TABLE_NAME = 'task_configuration'
PRIMARY_KEY = 'component_name'
SORT_KEY = 'configuration_name'
CONFIGURATION_VALUE = 'configuration_value'


# Helper class to convert a DynamoDB item to JSON.
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def get_configuration(component_name, configuration_name):
    dynamodb = boto3.resource('dynamodb', region_name=REGION_NAME)

    table = dynamodb.Table(TASK_CONFIGURATION_TABLE_NAME)
    response = table.get_item(
        Key={
            PRIMARY_KEY: component_name,
            SORT_KEY: configuration_name
        }
    )
    return response[CONFIGURATION_VALUE]
