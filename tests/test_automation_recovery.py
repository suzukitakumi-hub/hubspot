import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import requests
import schedule_hubspot_elearning_dm as dm
import mba_sales_email_open_monitor as mba
import uscpa_sales_email_open_monitor as uscpa


class RecoveryTests(unittest.TestCase):
    def test_scheduled_email_is_not_published_twice_even_after_window(self):
        scheduled = datetime(2026, 9, 9, 18, tzinfo=dm.JST)
        email = {'id': '1', 'name': 'test', 'state': 'SCHEDULED', 'publishDate': dm.hubspot_utc_string(scheduled)}
        args = Mock(send_date_jst='2026-09-09', send_time_jst='18:00', target_name='test', source_email_id='', apply=True)
        with patch.object(dm, 'parse_args', return_value=args), patch.object(dm, 'load_dotenv'), patch.object(dm, 'list_marketing_emails', return_value=[email]), patch.object(dm, 'find_source_email', return_value=email), patch.object(dm, 'fetch_email', return_value=email), patch.object(dm, 'write_output'), patch.object(dm, 'publish_email') as publish, patch.object(dm, 'clone_email') as clone, patch('builtins.print'):
            dm.main()
        publish.assert_not_called()
        clone.assert_not_called()

    def test_sheets_transport_recovers_503(self):
        calls = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                calls.append(1)
                self.send_response(503 if len(calls) == 1 else 200)
                self.end_headers()
                self.wfile.write(b'{}')

            def log_message(self, *args):
                pass

        session = requests.Session()
        fake_client = Mock()
        fake_client.http_client.session = session
        with patch('gspread.service_account', return_value=fake_client), patch.object(uscpa.Path, 'exists', return_value=True):
            uscpa.get_gspread_client('unused')
        adapter = session.get_adapter('https://sheets.googleapis.com/')
        self.assertNotIn('POST', adapter.max_retries.allowed_methods)
        session.mount('http://127.0.0.1:', adapter)
        server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            response = session.get(f'http://127.0.0.1:{server.server_port}/', timeout=5)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(len(calls), 2)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()
            session.close()

    def test_api_error_does_not_create_a_worksheet(self):
        spreadsheet = Mock()
        spreadsheet.worksheet.side_effect = RuntimeError('503 exhausted')
        with self.assertRaises(RuntimeError):
            uscpa.ensure_worksheet(spreadsheet, 'test')
        spreadsheet.add_worksheet.assert_not_called()

    def test_mba_uses_the_same_sheets_retry_policy(self):
        session = requests.Session()
        fake_client = Mock()
        fake_client.http_client.session = session
        with patch('gspread.service_account', return_value=fake_client), patch.object(mba.Path, 'exists', return_value=True):
            mba.get_gspread_client('unused')
        retry = session.get_adapter('https://sheets.googleapis.com/').max_retries
        self.assertEqual(retry.total, 5)
        self.assertNotIn('POST', retry.allowed_methods)

    def test_transient_adhoc_list_state_is_retried(self):
        client = object.__new__(uscpa.HubSpot)
        client.session = Mock()
        failed = Mock(status_code=400, text='{"context":{"invalidProcessingType":["ADHOC"]}}')
        recovered = Mock(status_code=200, text='{"results":[]}', json=lambda: {"results": []})
        client.session.request.side_effect = [failed, recovered]
        with patch.object(uscpa.time, 'sleep'):
            self.assertEqual(client.request('GET', '/crm/v3/lists/6567/memberships'), {"results": []})
        self.assertEqual(client.session.request.call_count, 2)

    def test_morning_and_delayed_runs_schedule_18_not_immediate_send(self):
        scheduled = datetime(2026, 9, 9, 18, tzinfo=dm.JST)
        for hour in (9, 13, 17):
            with self.subTest(hour=hour), patch.object(dm, 'datetime', wraps=datetime) as clock:
                clock.now.return_value = scheduled.replace(hour=hour)
                dm.ensure_send_window(scheduled, 720, 180)
                source = {'subject': 'test', 'content': {}, 'to': {}, 'from': {}, 'subscriptionDetails': {}}
                draft = dict(source, id='1', name='test', publishDate=dm.hubspot_utc_string(scheduled), sendOnPublish=False)
                with patch.object(dm, 'patch_email', return_value=draft) as update, patch.object(dm, 'publish_email') as publish, patch.object(dm, 'fetch_email', return_value=dict(draft, state='SCHEDULED')):
                    result = dm.publish_existing_or_new_draft('1', source, 'test', scheduled)
                self.assertFalse(update.call_args.args[1]['sendOnPublish'])
                self.assertEqual(update.call_args.args[1]['publishDate'], '2026-09-09T09:00:00Z')
                self.assertEqual(result['state'], 'SCHEDULED')
                publish.assert_called_once_with('1')

    def test_22_hour_run_still_rejected(self):
        scheduled = datetime(2026, 9, 9, 18, tzinfo=dm.JST)
        with patch.object(dm, 'datetime', wraps=datetime) as clock:
            clock.now.return_value = scheduled.replace(hour=22)
            with self.assertRaises(RuntimeError):
                dm.ensure_send_window(scheduled, 720, 180)

    def test_bad_send_mode_stops_before_publish(self):
        scheduled = datetime(2099, 9, 9, 18, tzinfo=dm.JST)
        with patch.object(dm, 'patch_email', return_value={'id': '1', 'sendOnPublish': True}), patch.object(dm, 'publish_email') as publish:
            with self.assertRaises(RuntimeError):
                dm.publish_existing_or_new_draft('1', {}, 'test', scheduled)
            publish.assert_not_called()


if __name__ == '__main__':
    unittest.main()
