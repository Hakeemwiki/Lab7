import unittest
import json
from unittest.mock import Mock, patch
from trip_handlers.match_and_complete import lambda_handler, find_counterpart, create_completed_record

class TestMatchAndComplete(unittest.TestCase):
    def setUp(self):
        self.start_item = {
            "trip_id": {"S": "trip_001"},
            "sort_key": {"S": "START#2025-07-11 11:47:00"},
            "pickup_datetime": {"S": "2025-07-11 11:47:00"},
            "estimated_fare": {"N": "10.00"}
        }
        self.end_item = {
            "trip_id": {"S": "trip_001"},
            "sort_key": {"S": "END#2025-07-11 11:48:00"},
            "dropoff_datetime": {"S": "2025-07-11 11:48:00"},
            "fare_amount": {"N": "10.00"}
        }
        self.event = {
            "Records": [{"eventName": "INSERT", "dynamodb": {"NewImage": self.start_item}}]
        }

    def test_find_counterpart(self):
        with patch('boto3.resource') as mock_dynamodb:
            mock_table = Mock()
            mock_table.query.return_value = {"Items": [self.end_item]}
            mock_dynamodb.return_value.Table.return_value = mock_table
            counterpart = find_counterpart("trip_001", "START#")
            self.assertEqual(counterpart, self.end_item)

    @patch('boto3.resource')
    def test_lambda_handler_success(self, mock_dynamodb):
        mock_table = Mock()
        mock_table.query.return_value = {"Items": [self.end_item]}
        mock_table.put_item.return_value = None
        mock_dynamodb.return_value.Table.return_value = mock_table
        result = lambda_handler(self.event, None)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['body'], 'Matching completed')

if __name__ == '__main__':
    unittest.main()