from urllib.parse import unquote

from datahub.data_flows import channels
from cerem.utils import kafka_headers
from ..extension import stockfeel

@stockfeel.data_handler(channel=channels.EVENT_FROM_CLICKHOUSE)
def handle_event_data(data):
    try:
        data[kafka_headers.PATH] = 'https://www.stockfeel.com.tw' + unquote(data[kafka_headers.PATH])
    except:
        pass

    return data
