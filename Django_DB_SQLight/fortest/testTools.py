from django.shortcuts import render
from django.http import HttpResponse, JsonResponse

import okex.Account_api as Account
import okex.Funding_api as Funding
import okex.Market_api as Market
import okex.Public_api as Public
import okex.Trade_api as Trade
import okex.subAccount_api as SubAccount
import okex.status_api as Status
import json
import numpy as np
import asyncio
import websockets
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


api_key = "dd7af096-f986-4d84-ad35-a80b6837e7af"
secret_key = "D08BF29F3DD27F9B15780764DC583307"
passphrase = "uuu234"

flag = '0'
fundingAPI = Funding.FundingAPI(api_key, secret_key, passphrase, False, flag)
publicAPI = Public.PublicAPI(api_key, secret_key, passphrase, False, flag)
tradeAPI = Trade.TradeAPI(api_key, secret_key, passphrase, False, flag)
accountAPI = Account.AccountAPI(api_key, secret_key, passphrase, False, flag)
marketAPI = Market.MarketAPI(api_key, secret_key, passphrase, False, flag)


class Tools:
    
    def precfloat(num,digi):
        return int(round(float(num)*math.pow(10,digi)))/math.pow(10,digi)

    def get_timestamp():
        now = datetime.datetime.now()
        t = now.isoformat("T", "milliseconds")
        return t + "Z"

    def get_okxserver_time():
        result = publicAPI.get_system_time()
        return result
    
    def get_instrumentId_depth(instrumentId):
        result = marketAPI.get_orderbook(instrumentId, '5')
        return result
    
    

class httpTools:
        
    def message_test(request):
        timestamp=Tools.get_okxserver_time()
       # print('hereoik')
        timestamp = json.dumps(timestamp)
      
        return JsonResponse(timestamp, safe=False)
    
    
    def message_test_HttpResponse(request):        
        return HttpResponse('这是httpresponse')
    
    
    def message_test_JsonResponse(request):
        timestamp=Tools.get_okxserver_time()
        print('time123',timestamp)
        print('type,',type(timestamp))
       # a=json.dumps(timestamp)
       # print('wwe',a)
        return JsonResponse(timestamp)



