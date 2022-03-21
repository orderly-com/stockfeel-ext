from extension.datahub import CHANNELS

from ..extension import stockfeel

@stockfeel.data_handler(channel=CHANNELS.FETCH_FROM_KAFKA)
def decode_event_uid(data):
    return data
