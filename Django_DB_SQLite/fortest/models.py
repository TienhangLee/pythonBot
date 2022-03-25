from django.db import models

# Create your models here.
class cryptoPrice(models.Model):
    # 交易对
    instrumentId = models.CharField(max_length=200)

    # 价格深度
    depth = models.CharField(max_length=200)

    # 时间戳
    timestamp = models.CharField(max_length=200)