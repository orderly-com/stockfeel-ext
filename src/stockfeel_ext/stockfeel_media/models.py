from core.models import BaseModel
from django.db import models

class EsunsecID(BaseModel):
    class Meta:
        unique_together = (('cid', 'esunsec_id'),)
        indexes = [
            models.Index(fields=['cid']),
        ]
    esunsec_id = models.TextField(blank=False)
    cid = models.TextField(blank=False)
