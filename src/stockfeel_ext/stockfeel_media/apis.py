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
from .models import EsunsecID
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

        if member_id is not None and not isinstance(member_id, str):
            return JsonResponse({'result': False, 'msg': {'title': 'Invalid member_id', 'text': f'member_id must be a string.'}}, status=status.HTTP_406_NOT_ACCEPTABLE)

        try:
            limit = int(limit)
        except:
            limit = 1000
        limit = min(limit, 1000)
        try:
            offset = int(offset)
        except:
            offset = 0

        # min_date = max(min_date, max_trace_to)
        # max_date = max(max_date, max_trace_to)
        match_stage = {
            '$match': {
                'datetime': {
                    '$gte': min_date,
                    '$lte': max_date
                },
                'articlebase_id': {
                    '$ne': None
                }
            }
        }
        if member_id:
            cids = list(EsunsecID.objects.filter(esunsec_id=member_id).values_list('cid', flat=True))
            match_stage['$match']['cid'] = {'$in': cids}
        else:
            cids = list(EsunsecID.objects.values_list('cid', flat=True))
            match_stage['$match']['cid'] = {'$in': cids}
        pipeline = [
            match_stage,
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
                    'cid': '$cid',
                    'post_id': '$articlebase_id',
                    'url': '$path'
                }
            }
        ]
        data = aggregate_from_cerem(team.id, 'readbases', pipeline)
        cid_map = {}
        post_id_map = {}
        results = []
        for item in data:
            del item['_id']
            if item['cid'] not in cid_map:
                try:
                    cid_map[item['cid']] = EsunsecID.objects.filter(cid=item['cid']).first().esunsec_id
                except:
                    cid_map[item['cid']] = None
            if not cid_map[item['cid']]:
                continue
            if item['post_id'] not in post_id_map:
                try:
                    post_id_map[item['post_id']] = ArticleBase.objects.get(id=item['post_id']).external_id
                except:
                    post_id_map[item['post_id']] = None
            if not post_id_map[item['post_id']]:
                continue
            item['post_id'] = post_id_map[item['post_id']]
            item['member_id'] = cid_map[item['cid']]
            del item['cid']
            results.append(item)

        return JsonResponse({'result': True, 'data': results}, status=status.HTTP_200_OK)


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
