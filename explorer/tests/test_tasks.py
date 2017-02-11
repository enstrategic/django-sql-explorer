from django.test import TestCase
from explorer.tasks import execute_query, snapshot_queries, truncate_querylogs
from explorer.tests.factories import SimpleQueryFactory
from django.core import mail
try:
    from mock import Mock, patch
except ImportError:
    from unittest.mock import Mock, patch
from six import StringIO
from explorer.models import QueryLog
from datetime import datetime, timedelta


class TestTasks(TestCase):

    @patch('boto3.client')
    def test_async_results(self, mocked_s3):
        s3_client = Mock()
        s3_client.generate_presigned_url = Mock()
        s3_client.generate_presigned_url.return_value = 'http://s3.com/your-file.csv'
        mocked_s3.return_value = s3_client

        q = SimpleQueryFactory(sql='select 1 "a", 2 "b", 3 "c";', title="testquery")
        execute_query(q.id, 'cc@epantry.com')

        output = StringIO()
        output.write('a,b,c\r\n1,2,3\r\n')

        self.assertEqual(len(mail.outbox), 1)
        self.assertIn('[SQL Explorer] Report ', mail.outbox[0].subject)
        # self.assertEqual(conn.upload.call_args[0][1].getvalue(), output.getvalue())
        self.assertEqual(mocked_s3.upload_fileobj.call_count, 1)

    @patch('boto3.client')
    def test_async_results_failswith_message(self, mocked_s3):
        s3_client = Mock()
        s3_client.generate_presigned_url = Mock()
        s3_client.generate_presigned_url.return_value = 'http://s3.com/your-file.csv'
        mocked_s3.return_value = s3_client

        q = SimpleQueryFactory(sql='select x from foo;', title="testquery")
        execute_query(q.id, 'cc@epantry.com')

        output = StringIO()
        output.write('a,b,c\r\n1,2,3\r\n')

        self.assertEqual(len(mail.outbox), 1)
        self.assertIn('[SQL Explorer] Report ', mail.outbox[0].subject)
        # self.assertEqual(conn.upload.call_args[0][1].getvalue(), "no such table: foo")

    @patch('boto3.client')
    def test_snapshots(self, mocked_s3):
        s3_client = Mock()
        s3_client.generate_presigned_url = Mock()
        s3_client.generate_presigned_url.return_value = 'http://s3.com/your-file.csv'
        mocked_s3.return_value = s3_client

        SimpleQueryFactory(snapshot=True)
        SimpleQueryFactory(snapshot=True)
        SimpleQueryFactory(snapshot=True)
        SimpleQueryFactory(snapshot=False)

        snapshot_queries()
        self.assertEqual(s3_client.generate_presigned_url.call_count, 3)

    def test_truncating_querylogs(self):
        QueryLog(sql='foo').save()
        QueryLog.objects.filter(sql='foo').update(run_at=datetime.now() - timedelta(days=30))
        QueryLog(sql='bar').save()
        QueryLog.objects.filter(sql='bar').update(run_at=datetime.now() - timedelta(days=29))
        truncate_querylogs(30)
        self.assertEqual(QueryLog.objects.count(), 1)
