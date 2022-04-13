import base64
import urllib

from datahub.data_flows import channels
from cerem.utils import kafka_headers
from ..extension import stockfeel

@stockfeel.data_handler(channel=channels.EVENT_FROM_CLICKHOUSE)
def handle_event_data(data):
    try:
        uid = data[kafka_headers.USER]
        uid = base64.b64encode(uid.encode()).decode().rstrip('=')
        data[kafka_headers.USER] = urllib.parse.quote(uid)
    except:
        pass

    try:
        data[kafka_headers.PATH] = 'https://www.stockfeel.com.tw' + urllib.parse.unquote(data[kafka_headers.PATH])
    except:
        pass

    return data
