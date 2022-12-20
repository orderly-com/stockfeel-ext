import orjson
import datetime

from django.utils import timezone
from django.http import JsonResponse
from django.conf import settings
from dateutil import parser
from rest_framework.permissions import AllowAny
from rest_framework.views import APIView
from rest_framework import status

from external_app.models import ExternalAppApiKey
from cerem.tasks import aggregate_from_cerem

from ..extension import stockfeel


@stockfeel.api('v1/<signature>/behaviors/')
class ImportArticleList(APIView):

    permission_classes = [AllowAny]

    def post(self, request, *args, **kwargs):
        now = timezone.now()

        signature = kwargs.get('signature')
        api_key = request.headers.get('X-Api-Key')

        if not any([signature, api_key]):
            return JsonResponse({'result': False, 'msg': {'title': 'Value Missing', 'text': 'Signature or api_key is missing.'}}, status=status.HTTP_400_BAD_REQUEST)

        team = ExternalAppApiKey.get_team(signature, api_key)

        if not team:
            return JsonResponse({'result': False, 'msg': {'title': 'Not Valid', 'text': 'api_key is not valid or is expired.'}}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            data = orjson.loads(request.body.decode('utf-8'))
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Data is not valid or is not well formated.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if 'datasource' not in data:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Datasource is missing.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if 'data' not in data:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': 'Data is missing.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if len(data['data']) > settings.BATCH_SIZE_L:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid data', 'text': f'Max row of data per request is {settings.BATCH_SIZE_L}.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        content_type = request.GET.get('content_type')
        min_date = request.GET.get('min_date')
        max_date = request.GET.get('max_date')
        member_id = request.GET.get('member_id')
        limit = request.GET.get('limit')
        offset = request.GET.get('offset')
        max_trace_to = now - datetime.timedelta(days=30)

        if content_type != 'read_events':
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid content_type', 'text': 'Invalid content_type, only "read_events" is supported.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)
        try:
            min_date = parser.parse(min_date)
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid date given', 'text': f'{min_date} cannot be parsed as a datetime.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        try:
            max_date = parser.parse(max_date)
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid date given', 'text': f'{max_date} cannot be parsed as a datetime.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        if not isinstance(member_id, str):
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid member_id', 'text': f'member_id must be a string.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        try:
            limit = int(limit)
        except:
            limit = 100
        limit = min(limit, 100)
        try:
            offset = int(offset)
        except:
            offset = 0

        min_date = max(min_date, max_trace_to)
        max_date = max(max_date, max_trace_to)
        pipeline = [
            {
                '$match': {
                    'target': 'esunsecs',
                    'params.uid': member_id,
                    'datetime': {
                        '$gte': min_date,
                        '$lte': max_date
                    }
                }
            },
            {
                '$sort': {'datetime': 1}
            },
            {
                '$limit': offset + limit
            },
            {
                '$skip': offset
            },
            {
                '$project': {
                    'datetime': 1,
                    'member_id': '$params.uid',
                    'post_id': '$post_id',
                    'url': '$path'
                }
            }
        ]
        data = aggregate_from_cerem(team.id, 'readbases', pipeline)
        return JsonResponse({'result': True, 'data': data}, status=status.HTTP_200_OK)
