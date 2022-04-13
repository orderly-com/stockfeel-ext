from django.conf import settings

from media_ext.media_importly.models import Read
from media_ext.media_media.models import ArticleBase, ReadBase
from team.models import ClientBase

from ..extension import stockfeel

@stockfeel.periodic_task()
def find_reader_and_article():
    client_map = {}
    for client in ClientBase.objects.values('id', 'external_id'):
        client_map[client['external_id']] = client['id']

    article_map = {}
    for article in ArticleBase.objects.values('id', 'title'):
        article_map[article['title']] = article['id']


    reads_to_update = []
    readbases_to_create = []
    for read in Read.objects.filter(path='', readbase__isnull=True).values():
        try:
            title = read['title']
            readbase = ReadBase(
                articlebase_id=article_map[read['title']],
                clientbase_id=client_map[read['cid']],
                datetime=read['datetime'],
                attributions=read['attributions'],
                title=read['title'],
                path=f'https://www.stockfeel.com.tw/{title}/',
                uid=read['uid'],
                cid=read['cid'],
                team_id=1,
                datasource_id=read['datasource_id']
            )
            readbases_to_create.append(readbase)
            reads_to_update.append(Read(id=read['id'], readbase=readbase))
        except Exception as e:
            pass

    ReadBase.objects.bulk_create(readbases_to_create, settings.BATCH_SIZE_M)

    for readbase in reads_to_update:
        readbase.readbase_id = readbase.readbase.id

    Read.objects.bulk_update(reads_to_update, ['readbase_id'], settings.BATCH_SIZE_M)
