import unittest
from unittest.mock import Mock, patch
import os
from scripts.compute_daily_metrics import scan_dynamodb_table  # Corrected import

class TestComputeDailyMetrics(unittest.TestCase):
    @patch('boto3.client')
    def test_scan_dynamodb_table_success(self, mock_dynamodb):
        # Mock the DynamoDB client with a region
        mock_dynamodb.return_value.scan.return_value = {"Items": [{"trip_id": {"S": "trip_001"}}], "LastEvaluatedKey": None}
        with patch.dict(os.environ, {'AWS_DEFAULT_REGION': 'eu-north-1'}):  # Set a default region
            items = scan_dynamodb_table("TripData")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["trip_id"], "trip_001")

if __name__ == '__main__':
    unittest.main()