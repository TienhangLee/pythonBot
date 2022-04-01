from django.db import models
from matrixport2.models import *
from django.http import JsonResponse
import time, datetime
import os
import django
from matrixport2.Matrix_baseTools import Tools


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

def addequityValue(request):    
  while True:
    
    asset_valuation_result=Tools.get_asset_valuation()
        
    timestamp=int(int(asset_valuation_result['data'][0]['ts'])/1000)+28800 #加了時區的八小時
    b=time.localtime(int(timestamp))
    timestamp_datetime=time.strftime("%Y-%m-%d %H:%M:%S",b)
    
    presentTime=int(time.time())
    a="2022-03-15 0:00:00"    #以后可在此改基准时间
    c=time.strptime(a, "%Y-%m-%d %H:%M:%S")
    begintime=int(time.mktime(c))
    
    passedDay = int((presentTime - begintime)/86400)
    
    totalBal=float(asset_valuation_result['data'][0]['totalBal'])
    daylyInterest=0.08/365*800000
 
    instrestCost=daylyInterest*passedDay
    totalBalance=Tools.precfloat(((totalBal-800000)-instrestCost),2)    #总资产估值
    
    
    record = equityValue.objects.create(customerId = 'Matrixport' ,
                            totalEquity = str(totalBalance) ,
                            timestamp = timestamp_datetime)
  
    print('test record_add')
    time.sleep(60)
   
  return JsonResponse({'ret': 0, 'id':record.id})


def deleteEquityValue(request):    
  #返回一個queryset對象,包含所有的表紀錄
  while True:
    #時間設定 看情況
    time.sleep(60)
    dCd=equityValue.objects.values() 
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
          timestamp = int(value)
          nowTime=int(time.time())
          if nowTime - timestamp > 259200:  #如果是3天前的數據
            deleteCrypto = equityValue.objects.get(id=value_id)
            deleteCrypto.delete()
            print(f'{value_id}的數據已刪除') 
          
        #  if int(value) < 1646665410:     
        #    deleteCrypto = equityValue.objects.get(id=value_id)
        #    deleteCrypto.delete()
         #   print(f'{value_id}的數據已刪除') 
            # f'{name} : {value} | '
           # delete_id_list.append(value_id)           
    print('test record_delete')
    
  #  time.sleep(3)
  ##  dCd=equityValue.objects.values() 
   # for Cdd in dCd:
          #邏輯：找出符合條件的數據，紀錄下id
  #    for name, value in Cdd.items():    
  #      time.sleep(0.5)    
        #先分開記錄id，看之後是否符合條件加入
  #      if name == 'id':
  #        value_id = value
    
  #  deleteCrypto = equityValue.objects.get(id=value_id)
    
  return JsonResponse({'ret': 0})


def del_equityValue(request):    
  while True:
    time.sleep(2)
    print('test record_delete')

  return JsonResponse({'ret': 0})


def show_equityValue(request):
        # 返回一个 QuerySet 对象 ，包含所有的表记录
    qs = equityValue.objects.values()

    # 将 QuerySet 对象 转化为 list 类型
    # 否则不能 被 转化为 JSON 字符串
    retlist = list(qs)

    return JsonResponse({'ret': 0, 'retlist': retlist[len(retlist)-1]})
  
  
def show_trc20Address(request):
        # 返回一个 QuerySet 对象 ，包含所有的表记录
  qs = TRC20Address.objects.values()
  
  ur =  request.GET.get('username',None)
  if ur:
    qs = qs.filter(username=ur)
      
  cd =  request.GET.get('customerId',None)
  if cd:
    qs = qs.filter(customerId=cd)

  # 将 QuerySet 对象 转化为 list 类型
  retlist = list(qs)
  if len(retlist) ==0:
    return JsonResponse({'ret': 1, 'msg': 'No qualified data. Your params would be wrong.'})
  else:
    return JsonResponse({'ret': 0, 'retlist': retlist[0]})  #给最旧然后没用过的资料
   
  
def show_erc20Address(request):
        # 返回一个 QuerySet 对象 ，包含所有的表记录
  qs = ERC20Address.objects.values()
  
  ur =  request.GET.get('username',None)
  if ur:
    qs = qs.filter(username=ur)
      
  cd =  request.GET.get('customerId',None)
  if cd:
    qs = qs.filter(customerId=cd)

  # 将 QuerySet 对象 转化为 list 类型
  retlist = list(qs)
  if len(retlist) ==0:
    return JsonResponse({'ret': 1, 'msg': 'No qualified data. Your params would be wrong.'})
  else:
    return JsonResponse({'ret': 0, 'retlist': retlist[0]})  #给最旧然后没用过的资料
  
def show_filAddress(request):
        # 返回一个 QuerySet 对象 ，包含所有的表记录
  qs = FILAddress.objects.values()
  
  ur =  request.GET.get('username',None)
  if ur:
    qs = qs.filter(username=ur)
      
  cd =  request.GET.get('customerId',None)
  if cd:
    qs = qs.filter(customerId=cd)

  # 将 QuerySet 对象 转化为 list 类型
  retlist = list(qs)
  if len(retlist) ==0:
    return JsonResponse({'ret': 1, 'msg': 'No qualified data. Your params would be wrong.'})
  else:
    return JsonResponse({'ret': 0, 'retlist': retlist[0]})  #给最旧然后没用过的资料