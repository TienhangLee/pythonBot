from django.db import models

# Create your models here.
class equityValue(models.Model):
    # 
    customerId = models.CharField(max_length=200)

    # 总资产估值
    totalEquity = models.CharField(max_length=200)

    # 时间戳
    timestamp = models.CharField(max_length=200)
    
    
class profitRecord(models.Model):
    # 
    customerId = models.CharField(max_length=200)

    # 总资产估值
    profitRecord = models.CharField(max_length=200)

    # 时间戳
    timestamp = models.CharField(max_length=200)
    
    
class TRC20Address(models.Model):
        # 
    customerId = models.CharField(max_length=200)
    # 
    address = models.CharField(max_length=200)
    
    chain = models.CharField(max_length=200)
    
    username = models.CharField(max_length=200)
    
    
class ERC20Address(models.Model):
        # 
    customerId = models.CharField(max_length=200)
    # 
    address = models.CharField(max_length=200)
    
    chain = models.CharField(max_length=200)
    
    username = models.CharField(max_length=200)
    
    
class FILAddress(models.Model):
        # 
    customerId = models.CharField(max_length=200)
    # 
    address = models.CharField(max_length=200)
    
    chain = models.CharField(max_length=200)
    
    username = models.CharField(max_length=200)
