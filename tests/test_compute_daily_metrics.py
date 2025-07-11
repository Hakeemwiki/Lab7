import unittest
from unittest.mock import Mock, patch
import os
from scripts.GenerateTripMetrics import scan_dynamodb_table  # Adjust import based on script location

class TestComputeDailyMetrics(unittest.TestCase):
    @patch('boto3.client')
    def test_scan_dynamodb_table_success(self, mock_dynamodb):
        mock_dynamodb.return_value.scan.return_value = {"Items": [{"trip_id": {"S": "trip_001"}}], "LastEvaluatedKey": None}
        items = scan_dynamodb_table("TripData")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["trip_id"], "trip_001")

if __name__ == '__main__':
    unittest.main()