import unittest
from unittest.mock import Mock

import requests

from ingestors.api_client import OpenF1Client


class OpenF1ClientTests(unittest.TestCase):
    def make_client(self, responses, **kwargs):
        session = Mock()
        session.get.side_effect = responses
        sleep = Mock()
        client = OpenF1Client(
            sustained_delay=0,
            timeout=12,
            max_retries=2,
            session=session,
            sleep=sleep,
            **kwargs,
        )
        return client, session, sleep

    @staticmethod
    def response(status=200, payload=None, headers=None):
        response = Mock()
        response.status_code = status
        response.headers = headers or {}
        response.json.return_value = [] if payload is None else payload
        if status >= 400:
            response.raise_for_status.side_effect = requests.HTTPError(str(status))
        return response

    def test_success_passes_params_and_timeout(self):
        client, session, sleep = self.make_client([self.response(payload=[{"speed": 300}])])

        result = client.fetch_data("car_data", {"session_key": 1})

        self.assertEqual(result, [{"speed": 300}])
        session.get.assert_called_once_with(
            "https://api.openf1.org/v1/car_data",
            params={"session_key": 1},
            timeout=12,
        )
        sleep.assert_not_called()

    def test_429_uses_retry_after_then_succeeds(self):
        client, session, sleep = self.make_client([
            self.response(429, headers={"Retry-After": "7"}),
            self.response(payload=[{"ok": True}]),
        ])

        self.assertEqual(client.fetch_data("laps", {}), [{"ok": True}])
        self.assertEqual(session.get.call_count, 2)
        sleep.assert_called_once_with(7.0)

    def test_invalid_retry_after_uses_exponential_backoff(self):
        client, _, sleep = self.make_client([
            self.response(429, headers={"Retry-After": "later"}),
            self.response(payload=[]),
        ])

        client.fetch_data("laps", {})

        sleep.assert_called_once_with(1.0)

    def test_retryable_server_error_stops_at_limit(self):
        client, session, sleep = self.make_client([
            self.response(503),
            self.response(503),
            self.response(503),
        ])

        self.assertEqual(client.fetch_data("weather", {}), [])
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1.0, 2.0])

    def test_transport_error_retries_then_succeeds(self):
        client, session, sleep = self.make_client([
            requests.Timeout("slow"),
            self.response(payload=[{"ok": True}]),
        ])

        self.assertEqual(client.fetch_data("drivers", {}), [{"ok": True}])
        self.assertEqual(session.get.call_count, 2)
        sleep.assert_called_once_with(1.0)

    def test_transport_error_stops_at_retry_limit(self):
        client, session, sleep = self.make_client([
            requests.ConnectionError("offline"),
            requests.ConnectionError("offline"),
            requests.ConnectionError("offline"),
        ])

        self.assertEqual(client.fetch_data("drivers", {}), [])
        self.assertEqual(session.get.call_count, 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [1.0, 2.0])

    def test_sustained_delay_is_applied_before_request(self):
        session = Mock()
        session.get.return_value = self.response(payload=[])
        sleep = Mock()
        client = OpenF1Client(sustained_delay=2.1, session=session, sleep=sleep)

        client.fetch_data("drivers", {})

        sleep.assert_called_once_with(2.1)

    def test_invalid_json_is_rejected(self):
        response = self.response(payload=[])
        response.json.side_effect = ValueError("invalid JSON")
        client, _, _ = self.make_client([response])

        self.assertEqual(client.fetch_data("drivers", {}), [])

    def test_non_retryable_client_error_returns_immediately(self):
        client, session, sleep = self.make_client([self.response(404)])

        self.assertEqual(client.fetch_data("missing", {}), [])
        session.get.assert_called_once()
        sleep.assert_not_called()

    def test_non_list_payload_is_rejected(self):
        client, _, _ = self.make_client([self.response(payload={"unexpected": True})])

        self.assertEqual(client.fetch_data("drivers", {}), [])

    def test_constructor_rejects_invalid_retry_configuration(self):
        with self.assertRaises(ValueError):
            OpenF1Client(sustained_delay=-1)
        with self.assertRaises(ValueError):
            OpenF1Client(timeout=0)
        with self.assertRaises(ValueError):
            OpenF1Client(max_retries=-1)

    def test_endpoint_accessors_use_expected_endpoint_and_params(self):
        client, _, _ = self.make_client([self.response(payload=[])])
        client.fetch_data = Mock(return_value=[])

        cases = (
            (client.get_car_data, (9693, 4), "car_data", {"session_key": 9693, "driver_number": 4}),
            (client.get_car_laps, (9693, 4), "laps", {"session_key": 9693, "driver_number": 4}),
            (client.get_meetings, (2025,), "meetings", {"year": 2025}),
            (client.get_sessions, (2025, "Race"), "sessions", {"year": 2025, "session_name": "Race"}),
            (client.get_drivers, (9693,), "drivers", {"session_key": 9693}),
            (client.get_stints, (9693, 4), "stints", {"session_key": 9693, "driver_number": 4}),
            (client.get_pit_stops, (9693, 4), "pit", {"session_key": 9693, "driver_number": 4}),
            (client.get_weather, (9693,), "weather", {"session_key": 9693}),
            (client.get_race_control, (9693,), "race_control", {"session_key": 9693}),
            (client.get_positions, (9693, 4), "position", {"session_key": 9693, "driver_number": 4}),
            (client.get_locations, (9693, 4), "location", {"session_key": 9693, "driver_number": 4}),
        )
        for accessor, args, endpoint, params in cases:
            with self.subTest(endpoint=endpoint):
                client.fetch_data.reset_mock()
                accessor(*args)
                client.fetch_data.assert_called_once_with(endpoint, params)


if __name__ == "__main__":
    unittest.main()
