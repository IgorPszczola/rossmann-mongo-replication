import unittest
from unittest.mock import Mock, patch

import status


class StatusTest(unittest.TestCase):
    def test_normalize_ignores_database_identity_fields(self):
        document = {
            "_id": "source-id",
            "source_id": "source-id",
            "transaction_id": "tx-123",
            "nested": {"value": 1},
        }

        self.assertEqual(
            status.normalize(document),
            {"transaction_id": "tx-123", "nested": {"value": 1}},
        )

    def test_status_fails_when_postgres_is_unavailable(self):
        mongo_client = Mock()
        redis_client = Mock()

        with patch.object(
            status, "get_mongo_client", side_effect=[mongo_client, mongo_client]
        ), patch.object(
            status, "get_redis_client", return_value=redis_client
        ), patch.object(status, "get_postgres_data", return_value=None):
            exit_code = status.main()

        self.assertEqual(exit_code, 1)


if __name__ == "__main__":
    unittest.main()
