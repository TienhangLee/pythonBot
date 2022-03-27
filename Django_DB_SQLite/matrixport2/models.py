from django.db import models

# Create your models here.
class equityValue(models.Model):
    # 
    customerId = models.CharField(max_length=200)

    # 总资产估值
    totalEquity = models.CharField(max_length=200)

    # 时间戳
    timestamp = models.CharField(max_length=200)