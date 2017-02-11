from datetime import date, datetime, timedelta
import random
import string

from django.core.mail import send_mail

from explorer import app_settings
from explorer.exporters import get_exporter_class
from explorer.models import Query, QueryLog

if app_settings.ENABLE_TASKS:
    from celery import task
    from celery.utils.log import get_task_logger
    import boto3
    logger = get_task_logger(__name__)
else:
    from explorer.utils import noop_decorator as task
    import logging
    logger = logging.getLogger(__name__)


@task
def execute_query(query_id, email_address):
    q = Query.objects.get(pk=query_id)
    exporter = get_exporter_class('csv')(q)
    random_part = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(20))
    resp_url = _upload('%s.csv' % random_part, exporter.get_file_output())

    subj = '[SQL Explorer] Report "%s" is ready' % q.title
    msg = 'Download results:\n\r%s' % resp_url

    send_mail(subj, msg, app_settings.FROM_EMAIL, [email_address])


@task
def snapshot_query(query_id):
    logger.info("Starting snapshot for query %s..." % query_id)
    q = Query.objects.get(pk=query_id)
    exporter = get_exporter_class('csv')(q)
    k = 'query-%s.snap-%s.csv' % (q.id, date.today().strftime('%Y%m%d-%H:%M:%S'))
    logger.info("Uploading snapshot for query %s as %s..." % (query_id, k))
    resp_url = _upload(k, exporter.get_file_output())
    logger.info("Done uploading snapshot for query %s. URL: %s" % (query_id, resp_url))


def _upload(key, data):
    s3_client = boto3.client('s3', aws_access_key_id=app_settings.S3_ACCESS_KEY,
                             aws_secret_access_key=app_settings.S3_SECRET_KEY)
    data.seek(0)
    s3_client.put_object(Bucket=app_settings.S3_BUCKET, Key=key, Body=data.read())
    return s3_client.generate_presigned_url('get_object', Params={'Bucket': app_settings.S3_BUCKET, 'Key': key},
                                            ExpiresIn=None)


@task
def snapshot_queries():
    logger.info("Starting query snapshots...")
    qs = Query.objects.filter(snapshot=True).values_list('id', flat=True)
    logger.info("Found %s queries to snapshot. Creating snapshot tasks..." % len(qs))
    for qid in qs:
        snapshot_query.delay(qid)
    logger.info("Done creating tasks.")


@task
def truncate_querylogs(days):
    qs = QueryLog.objects.filter(run_at__lt=datetime.now() - timedelta(days=days))
    logger.info('Deleting %s QueryLog objects older than %s days.' % (qs.count, days))
    qs.delete()
    logger.info('Done deleting QueryLog objects.')
