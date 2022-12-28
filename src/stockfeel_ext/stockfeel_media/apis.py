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

from media_ext.media_media.models import ArticleBase

from ..extension import stockfeel


@stockfeel.api('v1/<signature>/behaviors/')
class QueryBehaviors(APIView):

    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        now = timezone.now()

        signature = kwargs.get('signature')
        api_key = request.headers.get('X-Api-Key')

        if not any([signature, api_key]):
            return JsonResponse({'result': False, 'msg': {'title': 'Missing credential values', 'text': 'Missing Signature or api_key.'}}, status=status.HTTP_400_BAD_REQUEST)

        team = ExternalAppApiKey.get_team(signature, api_key, can_query_dataset=True)

        if not team:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid api key', 'text': 'api key is invalid or is expired.'}}, status=status.HTTP_401_UNAUTHORIZED)

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
            min_date = parser.parse(min_date).replace(tzinfo=timezone.get_current_timezone())
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid date given', 'text': f'{min_date} cannot be parsed as a datetime.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        try:
            max_date = parser.parse(max_date).replace(tzinfo=timezone.get_current_timezone())
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
        for item in data:
            del item['_id']
        return JsonResponse({'result': True, 'data': data}, status=status.HTTP_200_OK)


@stockfeel.api('v1/<signature>/post/detail/')
class QueryPosts(APIView):

    permission_classes = [AllowAny]

    def get(self, request, *args, **kwargs):
        now = timezone.now()

        signature = kwargs.get('signature')
        api_key = request.headers.get('X-Api-Key')

        if not any([signature, api_key]):
            return JsonResponse({'result': False, 'msg': {'title': 'Missing credential values', 'text': 'Missing Signature or api_key.'}}, status=status.HTTP_400_BAD_REQUEST)

        team = ExternalAppApiKey.get_team(signature, api_key, can_query_dataset=True)

        if not team:
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid api key', 'text': 'api key is invalid or is expired.'}}, status=status.HTTP_401_UNAUTHORIZED)

        post_id = request.GET.get('id')
        try:
            article = ArticleBase.objects.get(external_id=post_id)
        except:
            return JsonResponse({'result': False, 'msg': {'title': 'Not Found', 'text': 'Post with given id not found'}}, status=status.HTTP_404_NOT_FOUND)

        data = {
            'post_id': article.external_id,
            'title': article.title,
            'path': article.path,
            'tags': list(article.categories.values_list('name', flat=True)),
            'attributes': article.attributions.get('sf_sensed_keywords', [])
        }
        return JsonResponse({'result': True, 'data': data}, status=status.HTTP_200_OK)
