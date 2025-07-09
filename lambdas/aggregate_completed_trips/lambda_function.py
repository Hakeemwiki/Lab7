import boto3
import json
import os
from datetime import datetime
from decimal import Decimal
from collections import defaultdict

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("Trips")

s3 = boto3.client("s3")
S3_BUCKET = os.environ.get("S3_BUCKET", "nsp-bolt-trip-analytics")
