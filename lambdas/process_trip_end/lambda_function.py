import boto3
import json
import logging

# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Trips')

