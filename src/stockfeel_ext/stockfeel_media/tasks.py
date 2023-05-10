import datetime

from django.utils import timezone
from django.conf import settings

from team.models import Team

from cerem.tasks import fetch_site_tracking_data
from cerem.utils import kafka_headers
from datahub.models import DataSync
from cerem.tasks import aggregate_from_cerem
from .models import EsunsecID

from ..extension import stockfeel

@stockfeel.periodic_task()
def sync_esunsec_ids(from_datetime, to_datetime, *args, **kwargs):
    team = Team.objects.first()
    hour_ago = timezone.now() - datetime.timedelta(hours=1)
    params = {
        'actions': ['view', 'proceed'],
        'from_datetime': hour_ago
    }
    if from_datetime:
        params['from_datetime'] = from_datetime

    if to_datetime:
        params['to_datetime'] = to_datetime

    items_to_create = []
    for row in fetch_site_tracking_data(team, **params):
        event = {
            'datetime': row[kafka_headers.DATETIME],
            'title': row[kafka_headers.TITLE],
            'uid': row[kafka_headers.USER],
            'cid': row[kafka_headers.CID],
            'path': row[kafka_headers.PATH],
            'action': row[kafka_headers.ACTION],
            'params': row[kafka_headers.PARAMS],
            'target': row[kafka_headers.TARGET],
        }
        if event['target'] == 'esunsecs':
            items_to_create.append(
                EsunsecID(
                    esunsec_id=event['params']['uid'],
                    cid=event['cid'],
                )
            )
    EsunsecID.objects.bulk_create(items_to_create, batch_size=settings.BATCH_SIZE, ignore_conflicts=True)
