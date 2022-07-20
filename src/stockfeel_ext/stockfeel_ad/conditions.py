from typing import Any, Tuple

from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg, F

from filtration.conditions import SingleSelectCondition, ChoiceCondition
from filtration.registries import condition

from team.models import Attribution


@condition
class AdBehaviorCondition(SingleSelectCondition):
    NOT_IMPRESSED = 'NOT_IMPRESSED'
    IMPRESSED = 'read'
    CLICK = 'click'
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        response_option = ChoiceCondition('回應類型')
        response_option.choice(
            {'id': self.NOT_IMPRESSED, 'text': '未曝光'},
            {'id': self.IMPRESSED, 'text': '未回應'},
            {'id': self.CLICK, 'text': '點擊'},
        )
        self.add_options(response_type=response_option)

    def filter(self, client_qs: QuerySet, ad_id) -> Tuple[QuerySet, Q]:
        if ad_id is None:
            return client_qs, Q()

        response_type = self.options.get('response_type')
        if response_type == self.NOT_IMPRESSED:
            q = ~Q(attributions__has_key=ad_id)
        else:
            params = {
                f'attributions__{ad_id}': response_type # ad#$股感站內廣告_墊檔：加line好友（全站）0927/0207#$364: read
            }
            q = Q(**params)

        return client_qs, q

    def real_time_init(self, team, *args, **kwargs):
        pattern = rf'^.*#\$.*#\$.*#\$.$'
        qs = Attribution.objects.filter(name__iregex=pattern).values_list('name', flat=True)
        data = []
        type_map = {
            'overview': '長橫幅',
            'article': '文章頁',
            'member': '會員中心頁',
            'rectangular': '矩形'
        }
        platform_map = {
            '1': '電腦',
            '2': '手機',
            '3': '平板'
        }
        for name in qs:
            ad, ad_name, ad_type, platform = name.split('#$')
            ad_type = type_map.get(ad_type)
            platform = platform_map.get(platform)
            data.append(
                {
                    'id': name,
                    'text': f'{ad_name} ({ad_type}, {platform})'
                }
            )

        self.choice(*data)
