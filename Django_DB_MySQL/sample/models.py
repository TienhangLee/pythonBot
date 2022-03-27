from django.db import models

class Customer(models.Model):
    name = models.CharField(max_length=200)
    phonenumber = models.CharField(max_length=200)
    address = models.CharField(max_length=200)
    
    
# Create your models here.
