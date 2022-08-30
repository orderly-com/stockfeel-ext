import datetime
import itertools
from dateutil import relativedelta

from django.utils import timezone
from django.db.models.functions import TruncDate, ExtractMonth, ExtractYear, Cast
from django.db.models import F, Count, Func, Max, Min, IntegerField

from charts.exceptions import NoData
from charts.registries import chart_category
from charts.drawers import PieChart, BarChart, LineChart
from filtration.conditions import DateRangeCondition, ModeCondition

from cerem.tasks import clickhouse_client

from orderly_core.team.charts import client_behavior_charts

@client_behavior_charts.chart(name='用戶活躍度')
class ClientActivityDistribution(BarChart):

    def explain_x(self):
        return '活躍度分類'

    def explain_y(self):
        return '人數（%）'

    def get_labels(self):
        return ['新註冊用戶', '流失用戶', '沈默用戶', '活躍用戶']

    def draw(self):
        now = timezone.now()
        clients = all_clients = self.team.clientbase_set.filter(removed=False)
        client_count = clients.count()
        if not client_count:
            raise NoData('尚無會員資料')
        new_client_count = clients.filter(join_datetime__gt=now-datetime.timedelta(days=7)).count()
        clients = clients.exclude(join_datetime__gt=now-datetime.timedelta(days=7))

        silent_client_count = clients.filter(media_info__article_count=0).count()
        clients = clients.exclude(media_info__article_count=0)

        active_client_count = clients.filter(media_info__last_read_datetime__gte=now-datetime.timedelta(days=7)).count()

        churning_client_count = all_clients.count() - new_client_count - silent_client_count - active_client_count

        self.create_label(
            data=[
                new_client_count / client_count * 100,
                churning_client_count / client_count * 100,
                silent_client_count / client_count * 100,
                active_client_count / client_count * 100
            ],
            notes={'tooltip_value': '{data} %'}
        )
