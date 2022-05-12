

from typing import Any, Tuple, Optional, Dict, List
from dateutil.relativedelta import relativedelta

from django.db.models.query import Q
from django.db.models import QuerySet, Count, Avg

from filtration.conditions import Condition, RangeCondition, DateRangeCondition, SelectCondition, ChoiceCondition, SingleSelectCondition
from team.models import Attribution
from ..extension import stockfeel

class KYCAttributionCondition(SelectCondition):
    ATTRIBUTION_KEY = 'None'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:

        params = {f'attributions__{self.ATTRIBUTION_KEY}__in': choices}
        q = Q(**params)

        return client_qs, q

    def real_time_init(self, team, *args, **kwargs):
        qs = team.clientbase_set.filter(removed=False)
        choices = set(qs.values_list(f'attributions__{self.ATTRIBUTION_KEY}', flat=True))

        data = [{'id': text, 'text': text} for text in choices]

        self.choice(*data)


class KYCValueTagCondition(SelectCondition):
    VALUETAG_KEY = 'None'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, client_qs: QuerySet, choices: Any) -> Tuple[QuerySet, Q]:

        params = {f'attributions__{self.VALUETAG_KEY}__in': choices}
        q = Q(**params)

        return client_qs, q

    def real_time_init(self, team, *args, **kwargs):
        qs = team.clientbase_set.filter(removed=False)
        team.valuetag_set.filter(name__startswith=f'kyc_{self.VALUETAG_KEY}').values('name')
        choices = set(qs.values_list(f'attributions__{self.VALUETAG_KEY}', flat=True))

        data = [{'id': text, 'text': text} for text in choices]

        self.choice(*data)


@stockfeel.condition(tab='kyc')
class EducationCondition(SelectCondition):
    ATTRIBUTION_KEY = '最高學歷'
