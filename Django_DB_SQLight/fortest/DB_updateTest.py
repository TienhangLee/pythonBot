from django.db import models
from fortest.models import cryptoPrice
from django.http import JsonResponse
import time, datetime
import os
import django
from fortest.testTools import Tools


'''
string --> datetime
a= "2018-09-20 23:42:55"
o=datetime.datetime.strptime(o, "%Y-%m-%d %H:%M:%S")
o.minute, o.hour, o.day

10位數時間戳轉換成時間格式
time.time() --> 13位時間

b=time.localtime(int(1646631198))
time.strftime("%Y-%m-%d %H:%M:%S",b)


時間格式轉換成位數時間戳
a= "2018-09-20 23:42:55"
c=time.strptime(a, "%Y-%m-%d %H:%M:%S")
int(time.mktime(c))

'''

info = {'code': '0',
 'msg': '',
 'data': [{'asks': [['38121.5', '0.32587456', '0', '7'],
    ['38121.6', '0.01', '0', '1'],
    ['38123.4', '0.0002', '0', '1']],
   'bids': [['38121.4', '0.13689543', '0', '7'],
    ['38121.3', '0.00015', '0', '1'],
    ['38120.4', '0.0002', '0', '1']],
   'ts': '1646634614480'}]}

#record = cryptoPrice.objects.create(instrumentId='BTC-USDT' ,
#                            depth=info['data'][0] ,
#                            timestamp=info['data'][0]['ts'])



def addcryptoPrice(request):    
  instrumentId = 'BTC-USDT'
  
  while True:
    time.sleep(5)
    info=Tools.get_instrumentId_depth(instrumentId)
    record = cryptoPrice.objects.create(instrumentId='BTC-USDT' ,
                            depth=info['data'][0] ,
                            timestamp=info['data'][0]['ts'])
  
    print('test record_add')
   
  return JsonResponse({'ret': 0, 'id':record.id})



def deletecryptoPrice(request):    
  #返回一個queryset對象,包含所有的表紀錄
  while True:
    
    #時間設定 看情況
    time.sleep(3)
    dCd=cryptoPrice.objects.values() 
   # delete_id_list=[]
    for Cdd in dCd:
      #邏輯：找出符合條件的數據，紀錄下id
      for name, value in Cdd.items():    
        time.sleep(0.5)    
        #先分開記錄id，看之後是否符合條件加入
        if name == 'id':
          value_id = value
        
        #以下可以設定條件：      
        if name == 'timestamp':    #要注意處理timestamp是10位還13位數
          # 先看是幾位數
          if len(str(value)) == 13:
            value = int(value)      
            value = int(value / 1000)          
          elif len (str(value)) == 10:     
            pass        
 
          #判斷條件
          if int(value) < 1646665410:     
            deleteCrypto = cryptoPrice.objects.get(id=value_id)
            deleteCrypto.delete()
            print(f'{value_id}的數據已刪除') 
            # f'{name} : {value} | '
           # delete_id_list.append(value_id)           
    
        #delete_id_list有東西後才繼續往下操作    
        #整理出delete_id_list再刪除法
    #   print('delete_id_list_1245',delete_id_list)       
      #  if len(delete_id_list) !=0:
      #    for i in delete_id_list:
      #      try:
      #        deleteCrypto = cryptoPrice.objects.get(id=i)
      #        deleteCrypto.delete()
      #      except:
      #        pass
      #      time.sleep(0.5)
    print('test record_delete')
    
    
    time.sleep(3)
    dCd=cryptoPrice.objects.values() 
    for Cdd in dCd:
          #邏輯：找出符合條件的數據，紀錄下id
      for name, value in Cdd.items():    
        time.sleep(0.5)    
        #先分開記錄id，看之後是否符合條件加入
        if name == 'id':
          value_id = value
    
    deleteCrypto = cryptoPrice.objects.get(id=value_id)
    
    
  return JsonResponse({'ret': 0})


def del_cryptoPrice(request):    
  while True:
    time.sleep(2)
    print('test record_delete')

  return JsonResponse({'ret': 0})


def show_cryptoPrice(request):
        # 返回一个 QuerySet 对象 ，包含所有的表记录
    qs = cryptoPrice.objects.values()

    # 将 QuerySet 对象 转化为 list 类型
    # 否则不能 被 转化为 JSON 字符串
    retlist = list(qs)

    return JsonResponse({'ret': 0, 'retlist': retlist})