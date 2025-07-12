import unittest
import json
from unittest.mock import Mock, patch
from trip_handlers.process_trip_finish import lambda_handler, validate_data, prepare_record

class TestProcessTripFinish(unittest.TestCase):
    def setUp(self):
        self.valid_payload = {
            "trip_id": "trip_001",
            "dropoff_datetime": "2025-07-11 11:47:00",
            "fare_amount": "10.00"
        }
        self.event = {
            "Records": [
                {"kinesis": {"data": json.dumps(self.valid_payload).encode('utf-8')}}
            ]
        }

    def test_validate_data_valid(self):
        self.assertTrue(validate_data(self.valid_payload))

    def test_validate_data_invalid(self):
        invalid_payload = {"trip_id": "trip_001"}  # Missing required fields
        self.assertFalse(validate_data(invalid_payload))

    @patch('boto3.resource')
    def test_lambda_handler_success(self, mock_dynamodb):
        mock_table = Mock()
        mock_dynamodb.return_value.Table.return_value = mock_table
        result = lambda_handler(self.event, None)
        self.assertEqual(result['statusCode'], 200)
        self.assertEqual(result['body'], 'End events ingested')

if __name__ == '__main__':
    unittest.main()