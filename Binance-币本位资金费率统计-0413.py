#!/usr/bin/env python
# coding: utf-8

import hashlib
from urllib import parse
import urllib.parse
import json
import math
import numpy as np
import asyncio
import websockets
import json
import requests
import dateutil.parser as dp
from dateutil import tz
import hmac
import base64
import zlib
import datetime
import pandas as pd
import time
import nest_asyncio
import math
from decimal import Decimal, getcontext, setcontext,ROUND_DOWN, ROUND_UP,ROUND_CEILING
nest_asyncio.apply()

def get_timestamp():
    now = datetime.datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"

baseUrl="https://dapi.binance.com"

[2]:


info=requests.get(url=baseUrl+"/dapi/v1/exchangeInfo")

#p={'symbol':swap_instrument_id,'limit':9}
#result=requests.get(url=baseUrl+"/dapi/v1/fundingRate",params=p)
#past_funding_rate=eval(result.text)


true=True
false=False
k=eval(info.text)


k


kk=[]
for i in k["symbols"]:
    if i['contractType']=='PERPETUAL':
        kk.append(i["symbol"])

yoy=[]
for i in kk: 
    p={'symbol':i,'limit':100}
    result=requests.get(url=baseUrl+"/dapi/v1/fundingRate",params=p)
    kok=pd.DataFrame(eval(result.text))
    yoy.append(kok)
    time.sleep(0.1)
yoy_result=pd.concat(yoy)

yoy_result

pd.set_option('display.max_rows',None)
y=[]
for x in yoy_result['symbol'].unique():
    if x!='instrument_id':
        y.append(x)
youmean=[]
you=[]
you1mean=[]
you1=[]
you2mea=[]
you2=[]
you2mean=[]
for x in y:
    qoo=yoy_result[yoy_result['symbol']==x].sort_values(by="fundingTime",ascending=False)
    #print(qoo)
    qoo['fundingRate']=pd.to_numeric(qoo['fundingRate'])
    bb=qoo['fundingRate'].values
    youmean.append(bb.mean()*100)
    you.append(bb.mean()/bb.std(ddof=1))
    you1mean.append(bb[:3].mean()*100)
    you1.append(bb[:3].mean()/bb[:3].std(ddof=1))
    you2mean.append(bb[:30].mean()*100)
    you2.append(bb[:30].mean()/bb[:30].std(ddof=1))
    data={'Mean':youmean,'Allindex':you,'lastnightMean':you1mean,'lastnight':you1,'last10daysMean':you2mean,'last10days':you2}
youou=pd.DataFrame(data,index=y,columns=['Mean','Allindex','lastnightMean','lastnight','last10daysMean','last10days'])
youou.sort_values(by=['last10daysMean'],ascending=False)




