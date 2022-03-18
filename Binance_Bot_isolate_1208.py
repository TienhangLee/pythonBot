from binance import Client
from binance import AsyncClient, BinanceSocketManager
from multiprocessing import Process, Queue, Pool,Manager
import os,sys
import numpy as np
import asyncio
import websockets
import json
import requests
import psutil
import hmac
import base64
import zlib
import datetime
from datetime import datetime
import pandas as pd
import time
import nest_asyncio
import math
from decimal import Decimal, getcontext, setcontext,ROUND_DOWN, ROUND_UP,ROUND_CEILING
import asyncio
import nest_asyncio
from dateutil.parser import parse
nest_asyncio.apply()

def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

def take_long_order(mode_take_long_order,*args,**kwargs):
    swap_instrument_id = mode_take_long_order['swap_instrument_id']
    spot_instrument_id = mode_take_long_order['spot_instrument_id']
    spot_price = mode_take_long_order['spot_price']
    swap_price = mode_take_long_order['swap_price']
    spot_size = mode_take_long_order['spot_size']
    swap_size = mode_take_long_order['swap_size']
    spot_order_type = mode_take_long_order['spot_order_type']
    swap_order_type = mode_take_long_order['swap_order_type']
    # spot下单
    if float(spot_size) != 0:
        try:
            isolated_margin_results=client.get_isolated_margin_account()
            for i in isolated_margin_results['assets']:
                if i['baseAsset']['asset']==spot_instrument_id.split(spot_instrument_id[-4:])[0]:
                    borrowed_USDT=float(i['quoteAsset']['borrowed'])+float(i['quoteAsset']['interest'])
                    self_USDT=float(i['quoteAsset']['netAsset'])
                    total_USDT=float(i['quoteAsset']['totalAsset'])
                    available_USDT=float(i['quoteAsset']['free'])
                    margin_level=float(i['marginLevel'])
                    margin_ratio=float(i['marginRatio'])       
                    if available_USDT>=spot_size*spot_price:
                        A=client.create_margin_order(Symbol=spot_instrument_id,isIsolated='True', side='BUY', type=spot_order_type, price=spot_price, quantity=spot_size)
                        time.sleep(0.1)
                    else:            
                        check_margin_result=client.get_max_margin_loan(asset='USDT',isolatedSymbol=spot_instrument_id)
                        time.sleep(0.1)
                        #看还有多少可以借贷
                        if check_margin_result['amount']>spot_size*spot_price*1.05:
                            if margin_level>2:                
                                borrow_amount=spot_size*spot_price*1.05-available_USDT
                            else:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=200)
                             #   usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 200, toSymbol=spot_instrument_id)                                
                                #因为牵涉到爆仓所以划转金额高些
                                borrow_amount=spot_size*spot_price*1.05-available_USDT                    
                            borrow_result=client.create_margin_loan(asset='USDT', isIsolated='TRUE', symbol=spot_instrument_id,amount=borrow_amount)
                            time.sleep(0.1)           
                        else:#从spot钱包划转usdt过来,再借usdt
                            if margin_level>2:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=100)
                               # usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 100, toSymbol=spot_instrument_id) 
                                time.sleep(0.1)
                            else:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=200)
                            #    usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 200, toSymbol=spot_instrument_id) 
                                #因为牵涉到爆仓所以划转金额高些
                                time.sleep(0.1)
                            borrow_amount=spot_size*spot_price*1.05-available_USDT
                            borrow_result=client.create_margin_loan(asset='USDT', isIsolated='TRUE', symbol=spot_instrument_id,amount=borrow_amount)
                            time.sleep(0.1)
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='BUY', type=spot_order_type, price=spot_price, quantity=spot_size)
                        #time.sleep(0.1)
        except:
            A = 'none'
    else:
        A='none'
    # swap下单
    if float(swap_size) != 0:           
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='SELL',type=swap_order_type, price=swap_price, quantity=swap_size, timeInForce='GTC')
        except:
            B = 'none'    
    else:
        B = 'none'
    return A,B

def take_short_order(mode_take_short_order,*args,**kwargs):
    swap_instrument_id = mode_take_short_order['swap_instrument_id']
    spot_instrument_id = mode_take_short_order['spot_instrument_id']
    spot_price = mode_take_short_order['spot_price']
    swap_price = mode_take_short_order['swap_price']
    spot_size = mode_take_short_order['spot_size']
    swap_size = mode_take_short_order['swap_size']
    spot_order_type = mode_take_short_order['spot_order_type']
    swap_order_type = mode_take_short_order['swap_order_type']
    # spot下单
    if float(spot_size) != 0:
        try:
            isolated_margin_results=client.get_isolated_margin_account()
            for i in isolated_margin_results['assets']:
                coin_symbol=spot_instrument_id.split(spot_instrument_id[-4:])[0]
                if i['baseAsset']['asset']==coin_symbol:
                    borrowed_coin=float(i['baseAsset']['borrowed'])+float(i['baseAsset']['interest'])
                    total_coin_asset=float(i['baseAsset']['totalAsset'])
                    available_coin=float(i['baseAsset']['free'])
                    margin_level=float(i['marginLevel'])
                    margin_ratio=float(i['marginRatio'])       
                    if available_coin>=spot_size:
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='SELL', type=spot_order_type, price=spot_price, quantity=spot_size)
                        time.sleep(0.1)
                    else:            
                        check_margin_result=client.get_max_margin_loan(asset=coin_symbol, isolatedSymbol=spot_instrument_id)
                        time.sleep(0.1)
                        #看还有多少可以借贷
                        if check_margin_result['amount']>spot_size*1.05:
                            if margin_level>2:                
                                borrow_amount=spot_size*1.05-available_coin
                            else:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=200)
                              #  usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 200, toSymbol=spot_instrument_id) 
                                #因为牵涉到爆仓所以划转金额高些
                                borrow_amount=spot_size*1.05-available_coin                    
                            borrow_result=client.create_margin_loan(asset=coin_symbol, isIsolated='TRUE', symbol=spot_instrument_id,amount=borrow_amount)
                            time.sleep(0.1)           
                        else:#从spot钱包划转usdt过来,再借usdt
                            if margin_level>2:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=100)
                               # usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 100, toSymbol=spot_instrument_id) 
                                time.sleep(0.1)
                            else:
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=spot_instrument_id,amount=200)
                             #   usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 200, toSymbol=spot_instrument_id) 
                                #因为牵涉到爆仓所以划转金额高些
                                time.sleep(0.1)
                                #再检查一次看是否已经没币可借
                                check_margin_result=client.get_max_margin_loan(asset=coin_symbol, isolatedSymbol=spot_instrument_id)
                                time.sleep(0.1)
                                if check_margin_result['amount']>spot_size*1.05:
                                    pass
                                else:
                                    spot_size=0
                            borrow_amount=spot_size*1.05-available_coin
                            borrow_result=client.create_margin_loan(asset=coin_symbol, isIsolated='TRUE', symbol=spot_instrument_id,amount=borrow_amount)
                            time.sleep(0.1)
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='SELL', type=spot_order_type, price=spot_price, quantity=spot_size)
                        #time.sleep(0.1)
        except:
            A = 'none'
    else:
        A='none'
    # swap下单
    if float(swap_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='BUY',type=swap_order_type, price=swap_price, quantity=swap_size, timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return A,B

def take_close_long_order(mode_take_close_long_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_long_order['swap_instrument_id']
    spot_instrument_id = mode_take_close_long_order['spot_instrument_id']
    spot_close_price = mode_take_close_long_order['spot_close_price']
    swap_close_price = mode_take_close_long_order['swap_close_price']
    spot_close_size = mode_take_close_long_order['spot_close_size']
    swap_close_size = mode_take_close_long_order['swap_close_size']
    spot_order_type = mode_take_close_long_order['spot_order_type']
    swap_order_type = mode_take_close_long_order['swap_order_type']
    # spot下单
    if float(spot_close_size) != 0:
        try:
            isolated_margin_results=client.get_isolated_margin_account()
            for i in isolated_margin_results['assets']:
                if i['baseAsset']['asset']==spot_instrument_id.split(spot_instrument_id[-4:])[0]:
                    borrowed_USDT=float(i['quoteAsset']['borrowed'])+float(i['quoteAsset']['interest'])
                    self_USDT=float(i['quoteAsset']['netAsset'])
                    total_USDT=float(i['quoteAsset']['totalAsset'])
                    available_USDT=float(i['quoteAsset']['free'])
                    margin_level=float(i['marginLevel'])
                    margin_ratio=float(i['marginRatio']) 
                    borrowed_coin=float(i['baseAsset']['borrowed'])
                    total_coin_asset=float(i['baseAsset']['totalAsset'])
                    available_coin=float(i['baseAsset']['free'])    
                    #先卖币得usdt
                    if available_coin>spot_close_size:
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='SELL', type='LIMIT_MAKER', price=spot_close_price, quantity=spot_close_size)
                        time.sleep(0.1)
                    else:    
                        spot_close_size=available_coin
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='SELL', type='LIMIT_MAKER', price=spot_close_price, quantity=spot_close_size)
                        time.sleep(0.1)          
                    #再还usdt 
                    if borrowed_USDT != 0:
                        repay_margin_result = client.repay_margin_loan(asset='USDT', amount=borrowed_USDT,isIsolated='TRUE',symbol=spot_instrument_id)
        except:
            A='none'
    else:
        A = 'none'
    # swap下单
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='BUY',type=swap_order_type, price=swap_close_price, quantity=swap_close_size,timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return A, B

def take_close_short_order(mode_take_close_short_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_short_order['swap_instrument_id']
    spot_instrument_id = mode_take_close_short_order['spot_instrument_id']
    spot_close_price = mode_take_close_short_order['spot_close_price']
    swap_close_price = mode_take_close_short_order['swap_close_price']
    spot_close_size = mode_take_close_short_order['spot_close_size']
    swap_close_size = mode_take_close_short_order['swap_close_size']
    spot_order_type = mode_take_close_short_order['spot_order_type']
    swap_order_type = mode_take_close_short_order['swap_order_type']
    spot_minQty_digit = mode_take_close_short_order['spot_minQty_digit']
    # spot下单
    if float(spot_close_size) != 0:
        try:
            isolated_margin_results=client.get_isolated_margin_account()
            for i in isolated_margin_results['assets']:
                coin_symbol=spot_instrument_id.split(spot_instrument_id[-4:])[0]
                if i['baseAsset']['asset']==spot_instrument_id.split(spot_instrument_id[-4:])[0]:
                    borrowed_USDT=float(i['quoteAsset']['borrowed'])+float(i['quoteAsset']['interest'])
                    self_USDT=float(i['quoteAsset']['netAsset'])
                    total_USDT=float(i['quoteAsset']['totalAsset'])
                    available_USDT=float(i['quoteAsset']['free'])
                    margin_level=float(i['marginLevel'])
                    margin_ratio=float(i['marginRatio']) 
                    borrowed_coin=float(i['baseAsset']['borrowed'])+float(i['baseAsset']['interest'])
                    total_coin_asset=float(i['baseAsset']['totalAsset'])
                    available_coin=float(i['baseAsset']['free'])  
                    #先用usdt买回币
                    if available_USDT>spot_close_size*spot_close_price:
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='BUY', type='LIMIT_MAKER', price=spot_close_price, quantity=spot_close_size)
                        time.sleep(0.1)
                    else:    
                        spot_close_size=precfloat(available_USDT/spot_close_price,spot_minQty_digit)
                        A=client.create_margin_order(symbol=spot_instrument_id,isIsolated='True', side='BUY', type='LIMIT_MAKER', price=spot_close_price, quantity=spot_close_size)
                        time.sleep(0.1)     
                    #再还币
                    if borrowed_coin != 0:
                        if available_coin>=borrowed_coin:
                            repay_margin_result = client.repay_margin_loan(asset=coin_symbol, amount=borrowed_coin,isIsolated='TRUE',symbol=spot_instrument_id)
                        else:
                            repay_margin_result = client.repay_margin_loan(asset=coin_symbol, amount=available_coin,isIsolated='TRUE',symbol=spot_instrument_id)
        except:
            A = 'none'
    else:
        A = 'none'
    # swap下单
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='SELL',type=swap_order_type, price=swap_close_price, quantity=swap_close_size, timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return A, B

def take_open_long_final_open_order(mode_take_open_long_final_open_order,*args,**kwargs):
    swap_instrument_id = mode_take_open_long_final_open_order['swap_instrument_id']
    swap_price = mode_take_open_long_final_open_order['swap_price']
    swap_size = mode_take_open_long_final_open_order['swap_size']
    swap_order_type = mode_take_open_long_final_open_order['swap_order_type']
    # swap继续开空
    if float(swap_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='SELL',type=swap_order_type, price=swap_price, quantity=swap_size,timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_open_short_final_open_order(mode_take_open_short_final_open_order,*args,**kwargs):
    swap_instrument_id = mode_take_open_short_final_open_order['swap_instrument_id']
    swap_price = mode_take_open_short_final_open_order['swap_price']
    swap_size = mode_take_open_short_final_open_order['swap_size']
    swap_order_type = mode_take_open_short_final_open_order['swap_order_type']
    # swap继续开多
    if float(swap_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='BUY',type=swap_order_type, price=swap_price, quantity=swap_size,timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_open_long_final_close_order(mode_take_open_long_final_close_order,*args,**kwargs):
    swap_instrument_id = mode_take_open_long_final_close_order['swap_instrument_id']
    swap_price = mode_take_open_long_final_close_order['swap_price']
    swap_size = mode_take_open_long_final_close_order['swap_size']
    swap_order_type = mode_take_open_long_final_close_order['swap_order_type']
    # swap仓位多于spot,要平空
    if float(swap_size) != 0:
     #   print('swap_instrument_id_1',swap_instrument_id)
     #   print('swap_size_1',swap_size)
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='BUY', type=swap_order_type, price=swap_price, quantity=swap_size, timeInForce='GTC',reduceOnly='true')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_open_short_final_close_order(mode_take_open_short_final_close_order,*args,**kwargs):
    swap_instrument_id = mode_take_open_short_final_close_order['swap_instrument_id']
    swap_price = mode_take_open_short_final_close_order['swap_price']
    swap_size = mode_take_open_short_final_close_order['swap_size']
    swap_order_type = mode_take_open_short_final_close_order['swap_order_type']
    # swap仓位多于spot,要平多
    if float(swap_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id, side='SELL',type=swap_order_type, price=swap_price, quantity=swap_size, timeInForce='GTC',reduceOnly='true')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_close_long_final_close_order(mode_take_close_long_final_close_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_long_final_close_order['swap_instrument_id']
    swap_close_price = mode_take_close_long_final_close_order['swap_close_price']
    swap_close_size = mode_take_close_long_final_close_order['swap_close_size']
    swap_order_type = mode_take_close_long_final_close_order['swap_order_type']
    # swap平的仓位不够,要继续平空
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id,side='BUY',type=swap_order_type,price=swap_close_price, quantity=swap_close_size,timeInForce='GTC',reduceOnly='true')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_close_short_final_close_order(mode_take_close_short_final_close_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_short_final_close_order['swap_instrument_id']
    swap_close_price = mode_take_close_short_final_close_order['swap_close_price']
    swap_close_size = mode_take_close_short_final_close_order['swap_close_size']
    swap_order_type = mode_take_close_short_final_close_order['swap_order_type']
    # swap要继续平多
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id,side='SELL',type=swap_order_type,price=swap_close_price, quantity=swap_close_size, timeInForce='GTC',reduceOnly='true')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_close_long_final_open_order(mode_take_close_long_final_open_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_long_final_open_order['swap_instrument_id']
    swap_close_price = mode_take_close_long_final_open_order['swap_close_price']
    swap_close_size = mode_take_close_long_final_open_order['swap_close_size']
    swap_order_type = mode_take_close_long_final_open_order['swap_order_type']
    # swap开空
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id,side='SELL',type=swap_order_type,price=swap_close_price, quantity=swap_close_size,timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def take_close_short_final_open_order(mode_take_close_short_final_open_order,*args,**kwargs):
    swap_instrument_id = mode_take_close_short_final_open_order['swap_instrument_id']
    swap_close_price = mode_take_close_short_final_open_order['swap_close_price']
    swap_close_size = mode_take_close_short_final_open_order['swap_close_size']
    swap_order_type = mode_take_close_short_final_open_order['swap_order_type']
    # swap开多
    if float(swap_close_size) != 0:
        try:
            B = client.futures_create_order(symbol=swap_instrument_id,side='BUY',type=swap_order_type, price=swap_close_price, quantity=swap_close_size,timeInForce='GTC')
        except:
            B = 'none'
    else:
        B = 'none'
    return B

def precfloat(num,digi):
    return int(round(float(num)*math.pow(10,digi)))/math.pow(10,digi)

def funding_recalculate(client,swap_instrument_id):  
    past_funding_rate=client.futures_funding_rate(symbol=swap_instrument_id,limit='1')
    time.sleep(0.3)
    day3Afundrate=[]
    instId_fu = None
    for i in past_funding_rate:
        day3Afundrate.append(precfloat(i['fundingRate'], 6))
        instId_fu = i['symbol']
    Afundrate = np.array(day3Afundrate)
    return instId_fu, np.mean(Afundrate)

def sendmessage(message):
    url = ''
    HEADERS = {"Content-Type": "application/json ;charset=utf-8 "}
    message = message
    String_textMsg = {
        'msgtype':'text',
        'text':{'content':message},
   #     'at':{'atMobiles':{'15201106731'}, 'isAtAll':0 #要＠所有人就是1  }
               }
    String_textMsg = json.dumps(String_textMsg)
    res = requests.post(url, data=String_textMsg, headers=HEADERS)

def sendmessage_to_customer(message):
    url = ''
    HEADERS = {"Content-Type": "application/json ;charset=utf-8 "}
    message = message
    String_textMsg = {
        'msgtype':'text',
        'text':{'content':message}}
    String_textMsg = json.dumps(String_textMsg)
    res = requests.post(url, data=String_textMsg, headers=HEADERS)

def close_long_judge(mode_close_long_judge, *args, **kwargs):
    Target_Amount_Close = mode_close_long_judge['Target_Amount_Close']
    margin_balance = mode_close_long_judge['margin_balance']
    swap_position = mode_close_long_judge['swap_position']
    swap_close_size = mode_close_long_judge['swap_close_size']
    spot_close_size = mode_close_long_judge['spot_close_size']
    Present_Funding_Rate = mode_close_long_judge['present_funding']
    Average_Funding_Rate = mode_close_long_judge['history_funding']
    So_Sc_mode = mode_close_long_judge['So_Sc_mode']
    swap_min_size = mode_close_long_judge['swap_min_size']
    
    close_long_mode = 'off'
    close_long_final_open_mode = 'off'
    close_long_final_close_mode = 'off'
    #这里Target_Amount_Close=0,swap是short,<0
    if margin_balance - Target_Amount_Close > 0 and abs(margin_balance - Target_Amount_Close) > float(spot_close_size):  # spot超过TA,要平仓
        if So_Sc_mode == 'on':         #等有看So_Sc_mode再看资金费率的影响
            if Present_Funding_Rate * Average_Funding_Rate >0:
                close_long_mode = 'on'
            else:
                close_long_mode = 'off'
        elif So_Sc_mode =='off':     #此时有可能是持仓差过大或大涨需要仓位调整
            close_long_mode = 'on'
        #币安的swap_position等于币
        #spot_size是币,swap_size也是币
    elif margin_balance - Target_Amount_Close > 0 and abs(margin_balance - Target_Amount_Close) < float(spot_close_size):
        if -1*swap_position - margin_balance > float(swap_min_size):  # swap多于spot,平空
            close_long_final_close_mode = 'on'
        elif margin_balance + swap_position > float(swap_min_size):  # swap少于spot,开空
            close_long_final_open_mode = 'on'
    return close_long_mode, close_long_final_open_mode, close_long_final_close_mode    

def close_short_judge(mode_close_short_judge, *args, **kwargs):
    Target_Amount_Close = mode_close_short_judge['Target_Amount_Close']
    margin_balance = mode_close_short_judge['margin_balance']
    swap_position = mode_close_short_judge['swap_position']
    swap_close_size = mode_close_short_judge['swap_close_size']
    spot_close_size = mode_close_short_judge['spot_close_size']
    Present_Funding_Rate = mode_close_short_judge['present_funding']
    Average_Funding_Rate = mode_close_short_judge['history_funding']
    So_Sc_mode = mode_close_short_judge['So_Sc_mode']
    swap_min_size = mode_close_short_judge['swap_min_size']

    close_short_mode = 'off'
    close_short_final_open_mode = 'off'
    close_short_final_close_mode = 'off'
    #这里spot_balance<0,Target_Amount_close=0
    if margin_balance - Target_Amount_Close < 0 and abs(margin_balance - Target_Amount_Close) > float(spot_close_size):  # spot超过TA,要平仓
        if So_Sc_mode == 'on':         #等有看So_Sc_mode再看资金费率的影响
            if Present_Funding_Rate * Average_Funding_Rate >0:
                close_short_mode = 'on'
            else:
                close_short_mode = 'off'
        elif So_Sc_mode =='off':     #此时有可能是持仓差过大或大涨需要仓位调整
            close_short_mode = 'on'
    elif margin_balance - Target_Amount_Close < 0 and abs(margin_balance - Target_Amount_Close) < float(spot_close_size):
        #swap仓位比spot多
        if (swap_position + margin_balance )>0 and abs(swap_position + margin_balance) > float(swap_min_size):  # swap多于spot,平多
            close_short_final_close_mode = 'on'
        #spot仓位比swap多
        elif (margin_balance + swap_position )<0 and abs(margin_balance + swap_position) > float(swap_min_size):  # swap少于spot,开多
            close_short_final_open_mode = 'on'
    return close_short_mode, close_short_final_open_mode, close_short_final_close_mode

def open_long_judge(mode_open_long_judge, *args, **kwargs):
    Target_Amount = mode_open_long_judge['Target_Amount']
    margin_balance = mode_open_long_judge['margin_balance']
    swap_position = mode_open_long_judge['swap_position']
    swap_size = mode_open_long_judge['swap_size']
    spot_size = mode_open_long_judge['spot_size']
    swap_min_size = mode_open_long_judge['swap_min_size']

    # margin_balance>0, swap_position是short且<0
    open_long_mode = 'off'
    open_long_final_open_mode = 'off'
    open_long_final_close_mode = 'off'
    if Target_Amount - margin_balance > 0 and abs(Target_Amount - margin_balance) > float(spot_size):  # 还大于一个 spot_size,继续建仓
        open_long_mode = 'on'
    elif Target_Amount - margin_balance > 0 and abs(Target_Amount - margin_balance) < float(spot_size):  # spot已经接近Target_Amount,小于一个spot_size,spot不动,看swap
        if margin_balance + swap_position > float(swap_min_size):       #因为posSide是net,所以持仓本身带著正负
            open_long_final_open_mode = 'on'  # swap要建仓 开空
        elif -1*swap_position - margin_balance > float(swap_min_size):
            open_long_final_close_mode = 'on'  # swap要平仓  平空
    return open_long_mode, open_long_final_open_mode, open_long_final_close_mode

def open_short_judge(mode_open_short_judge, *args, **kwargs):
    Target_Amount = mode_open_short_judge['Target_Amount']*-1  #因为是负开仓,所以*-1
    margin_balance = mode_open_short_judge['margin_balance']
    swap_position = mode_open_short_judge['swap_position']
    swap_size = mode_open_short_judge['swap_size']
    spot_size = mode_open_short_judge['spot_size']
    swap_min_size = mode_open_short_judge['swap_min_size']

    open_short_mode = 'off'
    open_short_final_open_mode = 'off'
    open_short_final_close_mode = 'off'
    #这边TA是<0,margin_balance<0, swap_position是long且>0
    if Target_Amount - margin_balance < 0 and abs(Target_Amount - margin_balance) > float(spot_size):  # 还大于一个spot_size,继续建仓
        open_short_mode = 'on'
    elif Target_Amount - margin_balance < 0 and abs(Target_Amount - margin_balance) < float(spot_size):  # spot已经接近Target_Amount,小于一个spot_size,spot不动,看swap
        # swap建仓比spot少,swap要建仓 开多
        if  margin_balance + swap_position <0 and abs(margin_balance + swap_position) > float(swap_min_size):
            open_short_final_open_mode = 'on'
        #swap建仓比spot多,swap要平仓 平多
        elif margin_balance + swap_position >0 and abs(swap_position+ margin_balance) > float(swap_min_size):
            open_short_final_close_mode = 'on'
    return open_short_mode, open_short_final_open_mode, open_short_final_close_mode

async def USD_M_data(streams,MQ,TradingPair):
    while True:
        try:
            #先握手,create
            client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #建立永续合约的连结,e.g.'dotusdt@depth', 'linkusdt@depth',<symbol>@markPrice
            ts = bm.futures_multiplex_socket(streams)                  
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                   #     print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                   # print('res_future_market__1',res)
                    if res['data']['e']=='depthUpdate':
                        instr = res['data']['s']   #目前没用,先留著
                        TP = res['data']['s']  
                        if MQ[TP]["DEPTH5_SWAP"].empty() == True:
                            MQ[TP]["DEPTH5_SWAP"].put(res["data"])
                        elif MQ[TP]["DEPTH5_SWAP"].empty() == False:
                            MQ[TP]["DEPTH5_SWAP"].get()
                            MQ[TP]["DEPTH5_SWAP"].put(res["data"])

                    elif res['data']['e']=='markPriceUpdate':
                        instr = res['data']['s']   #目前没用,先留著
                        TP = res['data']['s']  
                        #记录mark_price
                        if MQ[TP]["MARK_PRICE_SWAP"].empty() == True:
                            MQ[TP]["MARK_PRICE_SWAP"].put(res['data']['p'])
                        elif MQ[TP]["MARK_PRICE_SWAP"].empty() == False:
                            MQ[TP]["MARK_PRICE_SWAP"].get()
                            MQ[TP]["MARK_PRICE_SWAP"].put(res['data']['p'])
                        #记录funding_rate_币安ws只有当下的资金费率,没有next资金费率
                        if MQ[TP]["FUNDING_RATE"].empty() == True:
                            MQ[TP]["FUNDING_RATE"].put(res["data"]['r'])
                        elif MQ[TP]["FUNDING_RATE"].empty() == False:
                            MQ[TP]["FUNDING_RATE"].get()
                            MQ[TP]["FUNDING_RATE"].put(res["data"]['r'])
                        #记录现货指数价格,币安的放在future的ws里
                        if MQ[TP]["INDEX_PRICE_SPOT"].empty() == True:
                            MQ[TP]["INDEX_PRICE_SPOT"].put(res["data"]['i'])
                        elif MQ[TP]["INDEX_PRICE_SPOT"].empty() == False:
                            MQ[TP]["INDEX_PRICE_SPOT"].get()
                            MQ[TP]["INDEX_PRICE_SPOT"].put(res["data"]['i'])
                                        
            # await client.close_connection() #要配合async def USD_M_data那行的间距,目前看用不到

        except Exception as e:
            print("disconneted，connecting SWAP MQ……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
      #      kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def SPOT_data(streams,MQ,TradingPair):
    while True:
        try:
            #先握手,create
            client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #建立现货的连结,e.g.'dotusdt@depth', 'linkusdt@depth'
            ts = bm.multiplex_socket(streams)            
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                     
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    #print('res_spot_market',res)
                    if len(res['data']) !=0:                       
                        TP = res['stream'].split(res['stream'][-13:])[0].upper()  
                        if MQ[TP]["DEPTH5_SPOT"].empty() == True:
                            MQ[TP]["DEPTH5_SPOT"].put(res["data"])
                        elif MQ[TP]["DEPTH5_SPOT"].empty() == False:
                            MQ[TP]["DEPTH5_SPOT"].get()
                            MQ[TP]["DEPTH5_SPOT"].put(res["data"])

        except Exception as e:
            print("disconneted，connecting SPOT MQ……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
        #    kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def SPOT_Account_update(api_key, api_secret,AQ,TradingPair):
    #币安的spot钱包的stream_account_update是每次只有推送成交的交易对,其他没送
    #先用restapi输出spot_balance
    client = Client(api_key, api_secret)
    total_balance_result=client.get_account()
    #此api会叫出所有的合约,包括仓位=0的,因此未有仓位的i['free']=0
    for h in TradingPair:
        for i in total_balance_result['balances']:
            if i['asset']==h.split(h[-4:])[0]:
                TP=h              
                if AQ[TP]["POSITION_SWAP"].empty() == True:
                    AQ[TP]["POSITION_SWAP"].put(float(i['free']))
                elif AQ[TP]["POSITION_SWAP"].empty() == False:
                    AQ[TP]["POSITION_SWAP"].get()
                    AQ[TP]["POSITION_SWAP"].put(float(i['free']))   

    while True:
        try:
          #  client = Client(api_key, api_secret)
            #client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #spot的user data stream
            ts = bm.user_socket()           
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                       # print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    if res['e'] =='outboundAccountPosition':
                        for i in range(len(res['B'])):              #从送来的res中做出spot_list
                            if res['B'][i]['a'] != 'USDT' and res['B'][i]['a'] != 'BNB':
                                TP=res['B'][i]['a']+'USDT'  #按照全体格式,这里再加usdt
                              #  print('TP_123',TP)
                                if AQ[TP]["POSITION_SPOT"].empty() == True:
                                    AQ[TP]["POSITION_SPOT"].put(res['B'][i]['f'])
                                elif AQ[TP]["POSITION_SPOT"].empty() == False:
                                    AQ[TP]["POSITION_SPOT"].get()
                                    AQ[TP]["POSITION_SPOT"].put(res['B'][i]['f'])
  
        except Exception as e:
            print("disconneted，connecting SPOT_Account_update……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
          #  kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def MARGIN_Account_update_1(api_key, api_secret,AQ,margin_instrument_id):
    #币安的margin钱包的stream_account_update是每次只有推送成交的交易对,其他没送
    #先用restapi输出margin_balance
    margin_instrument_id='C98USDT'
    client = Client(api_key, api_secret)
    margin_balance_results=client.get_isolated_margin_account()
    for i in margin_balance_results['assets']:
        if i['baseAsset']['asset']==margin_instrument_id.split(margin_instrument_id[-4:])[0]:
            TP = margin_instrument_id
            if AQ[TP]["POSITION_MARGIN"].empty() == True:
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))
            elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                AQ[TP]["POSITION_MARGIN"].get()
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))   
        
    while True:
        try:
          #  client = Client(api_key, api_secret)
            #client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #spot的user data stream
            ts = bm.isolated_margin_socket(symbol=margin_instrument_id)           
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                       # print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    if res['e'] =='outboundAccountPosition':
                        for i in range(len(res['B'])):              #从送来的res中做出spot_list
                            if res['B'][i]['a'] != 'USDT' and res['B'][i]['a'] != 'BNB':
                                TP=res['B'][i]['a']+'USDT'  #按照全体格式,这里再加usdt
                              #  print('TP_123',TP)
                                if AQ[TP]["POSITION_MARGIN"].empty() == True:
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
                                elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                                    AQ[TP]["POSITION_MARGIN"].get()
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
  
        except Exception as e:
            print("disconneted，connecting SPOT_Account_update……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
          #  kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def MARGIN_Account_update_2(api_key, api_secret,AQ,margin_instrument_id):
    #币安的margin钱包的stream_account_update是每次只有推送成交的交易对,其他没送
    #先用restapi输出margin_balance
    margin_instrument_id='C98USDT'
    client = Client(api_key, api_secret)
    margin_balance_results=client.get_isolated_margin_account()
    for i in margin_balance_results['assets']:
        if i['baseAsset']['asset']==margin_instrument_id.split(margin_instrument_id[-4:])[0]:
            TP = margin_instrument_id
            if AQ[TP]["POSITION_MARGIN"].empty() == True:
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))
            elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                AQ[TP]["POSITION_MARGIN"].get()
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))   
        
    while True:
        try:
          #  client = Client(api_key, api_secret)
            #client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #spot的user data stream
            ts = bm.isolated_margin_socket(symbol=margin_instrument_id)           
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                       # print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    if res['e'] =='outboundAccountPosition':
                        for i in range(len(res['B'])):              #从送来的res中做出spot_list
                            if res['B'][i]['a'] != 'USDT' and res['B'][i]['a'] != 'BNB':
                                TP=res['B'][i]['a']+'USDT'  #按照全体格式,这里再加usdt
                              #  print('TP_123',TP)
                                if AQ[TP]["POSITION_MARGIN"].empty() == True:
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
                                elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                                    AQ[TP]["POSITION_MARGIN"].get()
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
  
        except Exception as e:
            print("disconneted，connecting SPOT_Account_update……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
          #  kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def MARGIN_Account_update_3(api_key, api_secret,AQ,margin_instrument_id):
    #币安的margin钱包的stream_account_update是每次只有推送成交的交易对,其他没送
    #先用restapi输出margin_balance
    margin_instrument_id='C98USDT'
    client = Client(api_key, api_secret)
    margin_balance_results=client.get_isolated_margin_account()
    for i in margin_balance_results['assets']:
        if i['baseAsset']['asset']==margin_instrument_id.split(margin_instrument_id[-4:])[0]:
            TP = margin_instrument_id
            if AQ[TP]["POSITION_MARGIN"].empty() == True:
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))
            elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                AQ[TP]["POSITION_MARGIN"].get()
                AQ[TP]["POSITION_MARGIN"].put(float(i['baseAsset']['free']))   
        
    while True:
        try:
          #  client = Client(api_key, api_secret)
            #client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #spot的user data stream
            ts = bm.isolated_margin_socket(symbol=margin_instrument_id)           
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                       # print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    if res['e'] =='outboundAccountPosition':
                        for i in range(len(res['B'])):              #从送来的res中做出spot_list
                            if res['B'][i]['a'] != 'USDT' and res['B'][i]['a'] != 'BNB':
                                TP=res['B'][i]['a']+'USDT'  #按照全体格式,这里再加usdt
                              #  print('TP_123',TP)
                                if AQ[TP]["POSITION_MARGIN"].empty() == True:
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
                                elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                                    AQ[TP]["POSITION_MARGIN"].get()
                                    AQ[TP]["POSITION_MARGIN"].put(res['B'][i]['f'])
  
        except Exception as e:
            print("disconneted，connecting SPOT_Account_update……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
          #  kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def USD_M_Account_update(api_key, api_secret,AQ,TradingPair):
    #币安的U本位钱包的stream_account_update是每次只有推送成交的交易对,其他没送
    #先用restapi输出swap_position
    client = Client(api_key, api_secret)
    total_swap_position_result=client.futures_account()
    #此api会叫出所有的合约,包括仓位=0的,因此未有仓位的i['positionAmt']=0
    for h in TradingPair:
        for i in total_swap_position_result['positions']:
            if i['symbol']==h:
               # Operation_info[h]['swap_position']=float(i['positionAmt'])  #注意这里是持仓数目,不是notional
                TP=h              
                if AQ[TP]["POSITION_SWAP"].empty() == True:
                    AQ[TP]["POSITION_SWAP"].put(float(i['positionAmt']))
                elif AQ[TP]["POSITION_SWAP"].empty() == False:
                    AQ[TP]["POSITION_SWAP"].get()
                    AQ[TP]["POSITION_SWAP"].put(float(i['positionAmt']))                
    while True:
        try:
        # client = Client(api_key, api_secret)
            #client = await AsyncClient.create()
            bm = BinanceSocketManager(client)    
            #spot的user data stream
            ts = bm.futures_socket()           
            async with ts as tscm:
                while True:
                    try:
                        res = await tscm.recv()
                #        print(res)
                    except:
                        try:
                            res = await tscm.recv()
                        except Exception as e:
                            print(e)
                            break
                    if res['e'] =='ACCOUNT_UPDATE':   #重要：币安合约只有给account_update,只有发生变化的交易对                                               
                        for i in res['a']['P']:
                            TP=i['s']              
                            if AQ[TP]["POSITION_SWAP"].empty() == True:
                                AQ[TP]["POSITION_SWAP"].put(i['pa'])
                            elif AQ[TP]["POSITION_SWAP"].empty() == False:
                                AQ[TP]["POSITION_SWAP"].get()
                                AQ[TP]["POSITION_SWAP"].put(i['pa'])                              
                                                                                                      
        except Exception as e:
            print("disconneted，connecting SWAP_Account_update……")
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
        #   kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

async def trade(api_key, api_secret, MQ, AQ,TradingPair,param_set_list):
    while True:   
        try:
            client = Client(api_key, api_secret)
            #先撤spot单,用的工具包spot只有單筆撤銷,雖然binance有撤銷該symbol的所有訂單的功能
            #pending_spot_symbol_list=[]
            beginning_pending_spot_order_result=client.get_open_orders()
            if len(beginning_pending_spot_order_result)!=0:
                for i in beginning_pending_spot_order_result:
                    spot_instrument_id=i['symbol']
                    uncomplete_order_id=i['orderId']
                #    p={'symbol':spot_instrument_id,'orderId':uncomplete_order_id}
                    try:
                        revoke_result=client.cancel_order(symbol=i['symbol'],orderId=i['orderId'])
                        time.sleep(0.2)
                    except:
                        pass            
            #先撤swap单
            pending_swap_symbol_list=[]
            beginning_pending_swap_order_result=client.futures_get_open_orders()
            if len(beginning_pending_swap_order_result)!=0:
                for i in beginning_pending_swap_order_result:
                    pending_symbol= i['symbol']
                    pending_swap_symbol_list.append(pending_symbol)
                aa_list=set(pending_swap_symbol_list)  #去重
                if len(aa_list) != 0:
                    for i in aa_list:
                        try:
                            revoke_result=client.futures_cancel_all_open_orders(symbol=i)
                            time.sleep(0.2)
                        except:
                            pass            
            maker_commission_spot = param_set_list['maker_commission_spot']
            taker_commission_spot = param_set_list['taker_commission_spot']
            maker_commission_swap = param_set_list['maker_commission_swap']
            taker_commission_swap = param_set_list['taker_commission_swap']
            tolerate_limit = param_set_list['tolerate_limit']
            order_limit = param_set_list['order_limit']             #每次开仓出手是几美元
            close_short_index = param_set_list['close_short_index']  #平仓是开仓的几倍

            Nowtime = datetime.now()
            print(Nowtime)
            record_minute = Nowtime.minute
            record_hour = Nowtime.hour

            # 先找instruments参数
            Necessary_info = {}
            Operation_info = {}
            for i in TradingPair:
                Necessary_info[i] = {}
                Operation_info[i] = {}

            for i in TradingPair:
                #先唤醒TradingPair的帐户帐户
                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=i,amount=10)
               # usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 10, toSymbol=i) 
                time.sleep(0.2)

            #查询逐仓帐户逐仓倍数（marginratio)
            isolated_margin_results=client.get_isolated_margin_account()
            time.sleep(0.2)
            for i in TradingPair:   
                for j in isolated_margin_results['assets']:
                    if j['baseAsset']['asset']==i.split(i[-4:])[0]:
                        Necessary_info[i]['isolated_account_marginRatio']=float(j['marginRatio'])
                        
            for i in TradingPair: 
                #评估要转多少到各个逐仓帐户
                Necessary_info[i]['Total_money'] = float(TradingPair[i])
                Transfer_amount=Necessary_info[i]['Total_money']*1/(Necessary_info[i]['isolated_account_marginRatio']-1)
                #将u转到各个帐户
               # usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= Transfer_amount, toSymbol=i) 
                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=i,amount=200)
                time.sleep(0.2)

            print('TradingPair',TradingPair)

            for i in TradingPair:
                Necessary_info[i]['swap_instrument_id']=i  #币安不加 -SWAP
                Necessary_info[i]['spot_instrument_id']=i
                Necessary_info[i]['Total_money'] = TradingPair[i]

                #找swap的exchange info
                exchange_swap_info_result=client.futures_exchange_info()
                time.sleep(0.3)
                for j in exchange_swap_info_result['symbols']:
                    if j['symbol']==Necessary_info[i]['swap_instrument_id']:
                        Necessary_info[i]['swap_tick_size']=float(j['filters'][0]['tickSize'])
                        Necessary_info[i]['swap_tick_digit']=np.log10(1/float(j['filters'][0]['tickSize']))
                        Necessary_info[i]['notional']=float(j['filters'][5]['notional'])   #这是最小合约下单价格,5USD,不是一张合约等于多少,币安没有contract value
                        Necessary_info[i]['swap_min_notional'] = float(j['filters'][5]['notional'])   #这里min_size写成 notional
                        Necessary_info[i]['quantityPrecision'] =float(j['quantityPrecision'])

                #找spot的exchange info
                exchange_spot_info_result=client.get_exchange_info()
                time.sleep(0.3)
                for j in exchange_spot_info_result['symbols']:
                    if j['symbol'] == Necessary_info[i]['spot_instrument_id']:  # need to notice here's insId (SWAP)
                        Necessary_info[i]['spot_tick_size'] = float(j['filters'][0]['tickSize'])
                        Necessary_info[i]['spot_tick_digit'] = np.log10(1 /float(j['filters'][0]['tickSize']))
                        Necessary_info[i]['spot_min_notional'] = float(j['filters'][3]['minNotional'])
                        Necessary_info[i]['spot_minQty'] = float(j['filters'][2]['minQty']) #spot下单到最小的位数
                        Necessary_info[i]['spot_minQty_digit']=np.log10(1/float(j['filters'][2]['minQty']))

                # Average_Funding_Rate
                instId_fu, Necessary_info[i]['Average_Funding_Rate'] = funding_recalculate(client,i)  #需要有client
               
                # 币安没有Predict_Funding_Rate,只有当下的funding_rate
                try:
                    Necessary_info[i]['Present_Funding_Rate']=float(MQ[i]["FUNDING_RATE"].get(timeout=0.2))                    
                except:
                    Necessary_info[i]['Present_Funding_Rate']=float(client.futures_mark_price(symbol=i)['lastFundingRate'])

                Operation_info[i]={}
                Operation_info[i]['spot_bids_price5']=[]
                Operation_info[i]['spot_asks_price5']=[]
                Operation_info[i]['swap_bids_price5']=[]
                Operation_info[i]['swap_asks_price5']=[]


                #取spot5档数据,注意ws跟restAPI数据型态不同
                try:
                    spot_depth5 = MQ[i]["DEPTH5_SPOT"].get(timeout=0.2)
                 #   print('我spot_depth5用ws__1')
                #    for j in range(5):
                #        Operation_info[i]['spot_bids_price5'].append(float(spot_depth5['b'][j][0]))
                #        Operation_info[i]['spot_asks_price5'].append(float(spot_depth5['a'][j][0]))
                except:
                    spot_depth5 = client.get_order_book(symbol=i,limit='5')
                 #   print('我spot_depth5用restapi___2')
                    time.sleep(0.1)
                for j in range(5):
                    Operation_info[i]['spot_bids_price5'].append(float(spot_depth5['bids'][j][0]))
                    Operation_info[i]['spot_asks_price5'].append(float(spot_depth5['asks'][j][0]))
                # print(i) 
               

                #取swap5档数据,注意ws跟restAPI数据型态不同
                try:
                    swap_depth5 = MQ[i]["DEPTH5_SWAP"].get(timeout=0.2)
                    for j in range(5):
                        Operation_info[i]['swap_bids_price5'].append(float(swap_depth5['b'][j][0]))
                        Operation_info[i]['swap_asks_price5'].append(float(swap_depth5['a'][j][0]))
                except:
                    swap_depth5 = client.futures_order_book(symbol=i,limit='5')
                    for j in range(5):
                        Operation_info[i]['swap_bids_price5'].append(float(swap_depth5['bids'][j][0]))
                        Operation_info[i]['swap_asks_price5'].append(float(swap_depth5['asks'][j][0]))    
                    time.sleep(0.1)
                
                Operation_info[i]['spot_swap_update_mode'] = 'off'
                Operation_info[i]['swap_pending_list_left'] = 'off'
                Operation_info[i]['spot_pending_list_left'] = 'off'
                Operation_info[i]['swap_pending_order_result']=[]
                Operation_info[i]['spot_pending_order_result']=[]

                #计算每小时的buy/sell成交数量
                Operation_info[i]['spot_buy_trading_orders']=0
                Operation_info[i]['spot_sell_trading_orders']=0
                Operation_info[i]['swap_buy_trading_orders']=0
                Operation_info[i]['swap_sell_trading_orders']=0

                #计算每小时的成交净值
                Operation_info[i]['spot_buy_trading_net_amount']=0
                Operation_info[i]['spot_sell_trading_net_amount']=0
                Operation_info[i]['swap_buy_trading_net_amount']=0
                Operation_info[i]['swap_sell_trading_net_amount']=0
                Operation_info[i]['spot_trading_buy_size']=0
                Operation_info[i]['spot_trading_sell_size']=0
                Operation_info[i]['swap_trading_buy_size']=0
                Operation_info[i]['swap_trading_sell_size']=0
                Operation_info[i]['spot_trading_fee']=0
                Operation_info[i]['swap_trading_fee']=0

                #用来判断持仓歪掉时,是要平仓还是建仓,先设成是open状态
                Operation_info[i]['position_direction']='open'

            while True:
                try:
                    for h in TradingPair:
                        #先在这里设spot/swap size
                        open_long_mode = 'off'
                        open_short_mode = 'off'
                        close_long_mode = 'off'
                        close_short_mode = 'off'
                        open_long_final_open_mode = 'off'
                        open_short_final_open_mode = 'off'
                        open_long_final_close_mode = 'off'
                        open_short_final_close_mode = 'off'
                        close_long_final_open_mode = 'off'
                        close_short_final_open_mode = 'off'
                        close_long_final_close_mode = 'off'
                        close_short_final_close_mode = 'off'
                        So_Sc_mode = 'on'
                        no_pending_order = 'on'  #一开始没有遗留单
                        mode_take_long_order = {}
                        mode_take_short_order = {}
                        #Average_Funding_Rate
                        Nowtime = datetime.now()

                        #在资金费率开始前半小时跟后两小时机器人不要动,12600秒内不要动
                        if Nowtime.hour == 7 or Nowtime.hour == 15 or Nowtime.hour == 23:
                            if Nowtime.minute == 30 or Nowtime.minute == 31:
                                print('now for sleep')
                                time.sleep(12600)
                        
                    #    #因为加了停止机器人机制,所以改在机器人启动后再取平均资金费率, -->改 在主进程抓
                    #    if Nowtime.hour == 11 or Nowtime.hour == 19 or Nowtime.hour == 3:
                    #        if Nowtime.minute == 2:
                                # instId_fu, Necessary_info[h]['Average_Funding_Rate'] = funding_recalculate(client, h + '-SWAP')  #直接用restAPI
                    #            try:
                    #                Necessary_info[h]['Average_Funding_Rate'] = MQ[h]["HISTORY_FUNDING"].get(timeout=0.1)
                    #            except:
                    #                pass   #如果没有average_funding_rate不变
                        
                        #每分钟找present_funding_rate
                        new_record_minute = Nowtime.minute
                        if new_record_minute != record_minute:
                            #print(new_record_minute)
                            try:                  
                                Necessary_info[h]['Present_Funding_Rate']=float(MQ[h]["FUNDING_RATE"].get(timeout=0.2)) 
                            except:
                                pass
                            record_minute = new_record_minute
                            
                        time.sleep(0.3) #避免过度提取ws                            
                        #swap中间价
                        Operation_info[h]['new_swap_bids_price5'] = []
                        Operation_info[h]['new_swap_asks_price5'] = []
                        try:
                            new_swap_depth5 = MQ[h]["DEPTH5_SWAP"].get(timeout=0.2)
                            for i in range(5):
                                Operation_info[h]['new_swap_bids_price5'].append(float(new_swap_depth5['b'][i][0]))
                                Operation_info[h]['new_swap_asks_price5'].append(float(new_swap_depth5['a'][i][0]))

                        except:
                            new_swap_depth5 = client.futures_order_book(symbol=h,limit='5')
                            time.sleep(0.1)
                            for i in range(5):
                                Operation_info[h]['new_swap_bids_price5'].append(float(new_swap_depth5['bids'][i][0]))
                                Operation_info[h]['new_swap_asks_price5'].append(float(new_swap_depth5['asks'][i][0]))

                        new_swap_ask=float(Operation_info[h]['new_swap_bids_price5'][0])
                        new_swap_bid=float(Operation_info[h]['new_swap_asks_price5'][0])
                        swap_present_price = precfloat((new_swap_ask + new_swap_bid)/2,Necessary_info[h]['swap_tick_digit'])    #这里先用swap_present_price,没用new_swap_present_price

                        #new spot中间价
                        Operation_info[h]['new_spot_bids_price5'] = []
                        Operation_info[h]['new_spot_asks_price5'] = []
                        try:
                            new_spot_depth5 = MQ[h]["DEPTH5_SPOT"].get(timeout=0.2)
                        #      print('我spot_depth5用ws__1')
                        #      for i in range(5):
                        #           Operation_info[h]['new_spot_bids_price5'].append(float(new_spot_depth5['b'][i][0]))
                        #           Operation_info[h]['new_spot_asks_price5'].append(float(new_spot_depth5['a'][i][0]))
                        except:
                            new_spot_depth5 = client.get_order_book(symbol=h,limit='5')
                        #     print('我spot_depth5用restapi___3')
                            time.sleep(0.1)
                        for i in range(5):
                            Operation_info[h]['new_spot_bids_price5'].append(float(new_spot_depth5['bids'][i][0]))
                            Operation_info[h]['new_spot_asks_price5'].append(float(new_spot_depth5['asks'][i][0]))
                    #         print(h)
                            

                        new_spot_ask = float(Operation_info[h]['new_spot_bids_price5'][0])
                        new_spot_bid = float(Operation_info[h]['new_spot_asks_price5'][0])
                        new_spot_present_price = precfloat((new_spot_ask + new_spot_bid)/2,Necessary_info[h]['spot_tick_digit'])                            
                        #现货指数
                        #swap标记价格
                        try:
                            spot_index_price = float(MQ[h]["INDEX_PRICE_SPOT"].get(timeout=0.2))       
                            swap_mark_price = float(MQ[h]["MARK_PRICE_SWAP"].get(timeout=0.2))                    

                        except:
                            swap_ticker_result = client.futures_mark_price(symbol=h)
                            time.sleep(0.2)
                            spot_index_price = float(swap_ticker_result['indexPrice'] )
                            swap_mark_price = float(swap_ticker_result['markPrice'] )

                        #swap_size,spot_size
                        #这写法可能多馀,先留者
                        # if new_spot_present_price > order_limit:      #币安的spot出手是以币计算,swap是以notional计算
                        swap_size = precfloat(order_limit/swap_present_price, Necessary_info[h]['quantityPrecision']) 
                        spot_size = precfloat(order_limit/new_spot_present_price, Necessary_info[h]['spot_minQty_digit'])  #考虑到合约可能是张,但是在spot这里要换算成几个币,所以*contract_val
                        swap_close_size = swap_size * close_short_index
                        spot_close_size = spot_size * close_short_index
                        swap_min_size = precfloat(Necessary_info[h]['swap_min_notional']/swap_present_price, Necessary_info[h]['quantityPrecision'])


                        #   elif new_spot_present_price < order_limit:     #看usdt swap最少出手的价值,如果没超过order_limit就计算
                        #       swap_size = precfloat(order_limit/swap_present_price, Necessary_info[h]['swap_minQty_digit']) 
                        #       spot_size = precfloat(order_limit/new_spot_present_price, Necessary_info[h]['spot_minQty_digit'])                                
                        #       swap_close_size = swap_size * close_short_index
                        #       spot_close_size = spot_size * close_short_index
                        
                        # 处理剩馀swap订单
                        pending_swap_revoke_mode = 'off'
                        if Operation_info[h]['swap_pending_list_left'] == 'on':
                            time.sleep(0.1)
                            Operation_info[h]['swap_pending_order_result'] = client.futures_get_open_orders(symbol=Necessary_info[h]['swap_instrument_id'])
                            if len(Operation_info[h]['swap_pending_order_result']) == 0:  # 表示已经没有未成交单子
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            else:
                                if len(Operation_info[h]['swap_pending_order_result']) > 2:  # 有两张以上的单,全撤
                                    print('重复下单')  # 这里以后做个钉钉通知
                                    tutu = h + '-SWAP,v5有多馀的挂单没撤掉,现在全撤'
                                    sendmessage(tutu)
                                    pending_swap_revoke_mode = 'on'
                                else:  # 一张未完成单,继续
                                    # 做出当下五档差别的判断
                                    Operation_info[h]['new_swap_bids_price5'] = []
                                    Operation_info[h]['new_swap_asks_price5'] = []
                                    Operation_info[h]['new_swap_bids_size5'] = []
                                    Operation_info[h]['new_swap_asks_size5'] = []
                                    try:
                                        new_swap_depth5 = MQ[h]["DEPTH5_SWAP"].get(timeout=0.2)
                                        for i in range(5):
                                            Operation_info[h]['new_swap_bids_price5'].append(float(new_swap_depth5['b'][i][0]))
                                            Operation_info[h]['new_swap_asks_price5'].append(float(new_swap_depth5['a'][i][0]))
                                            Operation_info[h]['new_swap_bids_size5'].append(float(new_swap_depth5['b'][i][1]))
                                            Operation_info[h]['new_swap_asks_size5'].append(float(new_swap_depth5['a'][i][1])) 

                                    except:
                                        new_swap_depth5 = client.futures_order_book(symbol=h,limit='5')
                                        time.sleep(0.1)
                                        for i in range(5):
                                            Operation_info[h]['new_swap_bids_price5'].append(float(new_swap_depth5['bids'][i][0]))
                                            Operation_info[h]['new_swap_asks_price5'].append(float(new_swap_depth5['asks'][i][0]))
                                            Operation_info[h]['new_swap_bids_size5'].append(float(new_swap_depth5['bids'][i][1]))
                                            Operation_info[h]['new_swap_asks_size5'].append(float(new_swap_depth5['asks'][i][1])) 


                                    
                                    # 如果五档变化了，撤单
                                    if Operation_info[h]['swap_bids_price5'][:3] != Operation_info[h]['new_swap_bids_price5'][:3] or Operation_info[h]['swap_asks_price5'][:3] != Operation_info[h]['new_swap_asks_price5'][:3]:
                                        pending_swap_revoke_mode = 'on'
                                    # 如果五档没有变化
                                    elif Operation_info[h]['swap_bids_price5'][:3] == Operation_info[h]['new_swap_bids_price5'][:3] and Operation_info[h]['swap_asks_price5'][:3] == Operation_info[h]['new_swap_asks_price5'][:3]:
                                    
                                        if Operation_info[h]['swap_pending_order_result'][0]['side'] == 'SELL':
                                            # 看是不是best价
                                            if float(Operation_info[h]['swap_pending_order_result'][0]['price']) == float(Operation_info[h]['new_swap_bids_price5'][0]):
                                                # 再看是不是quantity是否等于订单剩馀的quantity（币安没有送订单笔数）,是就撤单
                                                if float(Operation_info[h]['swap_pending_order_result'][0]['origQty'])-float(Operation_info[h]['swap_pending_order_result'][0]['executedQty']) == float(Operation_info[h]['new_swap_asks_size5'][0]):
                                                    pending_swap_revoke_mode = 'on'                                                
                                        #    elif float(Operation_info[h]['swap_pending_order_result'][0]['price']) != float(Operation_info[h]['new_swap_bids_price5'][0]): #不是best价格,但现在不一定一直要挂在best价,所以不必撤单 0827
                                        #        pending_swap_revoke_mode = 'on'

                                        elif Operation_info[h]['swap_pending_order_result'][0]['side'] == 'BUY':
                                            if float(Operation_info[h]['swap_pending_order_result'][0]['price']) == float(Operation_info[h]['new_swap_bids_price5'][0]):
                                                if float(Operation_info[h]['swap_pending_order_result'][0]['origQty'])-float(Operation_info[h]['swap_pending_order_result'][0]['executedQty']) == float(Operation_info[h]['new_swap_bids_size5'][0]):
                                                    pending_swap_revoke_mode = 'on'
                                    #        elif float(Operation_info[h]['swap_pending_order_result'][0]['price']) != float(Operation_info[h]['new_swap_bids_price5'][0][0]):
                                    #            pending_swap_revoke_mode = 'on'

                        if pending_swap_revoke_mode == 'on':
                            if len(Operation_info[h]['swap_pending_order_result']) != 0:
                                #先撤swap单
                                pending_swap_symbol_list=[]                                        
                                for i in Operation_info[h]['swap_pending_order_result']:
                                    pending_symbol= i['symbol']
                                    pending_swap_symbol_list.append(pending_symbol)
                                aa_list=set(pending_swap_symbol_list)  #去重
                                if len(aa_list) != 0:
                                    for i in aa_list:
                                        try:
                                            revoke_result=client.futures_cancel_all_open_orders(symbol=i)
                                            time.sleep(0.2)
                                        except:
                                            pass
                            Operation_info[h]['swap_pending_list_left'] = 'off'
                            # 使旧的swap五档=新的swap五档
                            Operation_info[h]['swap_bids_price5'][:5] = Operation_info[h]['new_swap_bids_price5'][:5]
                            Operation_info[h]['swap_asks_price5'][:5] = Operation_info[h]['new_swap_asks_price5'][:5]


                        # 处理剩馀spot订单
                        pending_spot_revoke_mode = 'off'
                        if Operation_info[h]['spot_pending_list_left'] == 'on':
                            time.sleep(0.1)
                            Operation_info[h]['spot_pending_order_result'] = client.get_open_orders(symbol=Necessary_info[h]['spot_instrument_id'])
                            # print('spot_left_result',Operation_info[h]['spot_pending_order_result'])
                            if len(Operation_info[h]['spot_pending_order_result']) == 0:  # 表示已经没有未成交单子
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                            else:
                                if len(Operation_info[h]['spot_pending_order_result']) > 2:  # 有两张以上的单,全撤
                                    tutu = h + '-SPOT,v5有多馀的挂单没撤掉,现在全撤'
                                    sendmessage(tutu)
                                    print('spot repeat order')  # 这里以后做个钉钉通知
                                    pending_spot_revoke_mode ='on'
                                
                                else:  # 一张未完成单,继续
                                    # 做出当下五档差别的判断
                                    Operation_info[h]['new_spot_bids_price5'] = []
                                    Operation_info[h]['new_spot_asks_price5'] = []
                                    Operation_info[h]['new_spot_bids_size5'] = []
                                    Operation_info[h]['new_spot_asks_size5'] = []


                                    try:
                                        time.sleep(0.3)  #避免过度提取ws                                            
                                        new_spot_depth5 = MQ[h]["DEPTH5_SPOT"].get(timeout=0.2)
                                        #  print('我spot_depth5还是用ws___4')
                                        #   for i in range(5):
                                        #       Operation_info[h]['new_spot_bids_price5'].append(float(new_spot_depth5['b'][i][0]))
                                        #       Operation_info[h]['new_spot_asks_price5'].append(float(new_spot_depth5['a'][i][0]))
                                    except:
                                        new_spot_depth5 = client.get_order_book(symbol=h,limit='5')
                                        print('我spot_depth5还是用restAPI___4')
                                        time.sleep(0.1)

                                    for i in range(5):
                                        Operation_info[h]['new_spot_bids_price5'].append(float(new_spot_depth5['bids'][i][0]))
                                        Operation_info[h]['new_spot_asks_price5'].append(float(new_spot_depth5['asks'][i][0]))
                                        Operation_info[h]['new_spot_bids_size5'].append(float(new_spot_depth5['bids'][i][1]))
                                        Operation_info[h]['new_spot_asks_size5'].append(float(new_spot_depth5['asks'][i][1]))

                                    if Operation_info[h]['spot_bids_price5'][:3] != Operation_info[h]['new_spot_bids_price5'][:3] or Operation_info[h]['spot_asks_price5'][:3] != Operation_info[h]['new_spot_asks_price5'][:3]:
                                        pending_spot_revoke_mode = 'on'
                                    # 如果五档没有变化
                                    elif Operation_info[h]['spot_bids_price5'][:3] == Operation_info[h]['new_spot_bids_price5'][:3] and Operation_info[h]['spot_asks_price5'][:3] == Operation_info[h]['new_spot_asks_price5'][:3]:
                                        if Operation_info[h]['spot_pending_order_result'][0]['side'] == 'SELL':
                                            # 看是不是best价
                                            if float(Operation_info[h]['spot_pending_order_result'][0]['price']) == float(Operation_info[h]['new_spot_asks_price5'][0]):
                                                # 再看是不是quantity是否等于订单剩馀的quantity（币安没有送订单笔数）,是就撤单
                                                if float(Operation_info[h]['spot_pending_order_result'][0]['origQty'])-float(Operation_info[h]['spot_pending_order_result'][0]['executedQty']) == float(Operation_info[h]['new_spot_asks_size5'][0]):
                                                    pending_spot_revoke_mode = 'on'
                                                    #    elif float(Operation_info[h]['swap_pending_order_result'][0]['price']) != float(Operation_info[h]['new_swap_bids_price5'][0][0]): #不是best价格,但现在不一定一直要挂在best价,所以不必撤单 0827
                                            #        pending_swap_revoke_mode = 'on'

                                            elif Operation_info[h]['spot_pending_order_result'][0]['side'] == 'BUY':
                                                if float(Operation_info[h]['spot_pending_order_result'][0]['price']) == float(Operation_info[h]['new_spot_bids_price5'][0]):
                                                    if (float(Operation_info[h]['spot_pending_order_result'][0]['origQty']) - float(Operation_info[h]['spot_pending_order_result'][0]['executedQty'])) == float(Operation_info[h]['new_spot_bids_size5'][0]):
                                                        pending_spot_revoke_mode = 'on'
                                        #        elif float(Operation_info[h]['swap_pending_order_result'][0]['price']) != float(Operation_info[h]['new_swap_bids_price5'][0][0]):
                                        #            pending_swap_revoke_mode = 'on'
                                

                        if pending_spot_revoke_mode == 'on':
                            if len(Operation_info[h]['spot_pending_order_result']) != 0:
                                #先撤spot单
                                pending_spot_symbol_list=[]  
                                for i in Operation_info[h]['spot_pending_order_result']:
                                    spot_instrument_id=i['symbol']
                                    uncomplete_order_id=i['orderId']
                            #        p={'symbol':spot_instrument_id,'orderId':uncomplete_order_id}
                                    try:
                                        revoke_result=client.cancel_order(symbol=i['symbol'],orderId=i['orderId'])
                                        time.sleep(0.2)
                                    except:
                                        pass
                                
                            Operation_info[h]['spot_pending_list_left'] = 'off'
                            # 使旧的swap五档=新的swap五档
                            Operation_info[h]['spot_bids_price5'][:5] = Operation_info[h]['new_spot_bids_price5'][:5]
                            Operation_info[h]['spot_asks_price5'][:5] = Operation_info[h]['new_spot_asks_price5'][:5]
                        # 如果不撤單,就不用下单
                        if Operation_info[h]['swap_pending_list_left'] == 'on' or Operation_info[h]['spot_pending_list_left'] == 'on':  # 要看有没有剩馀的单
                            no_pending_order='off'  #拿掉大涨跟持仓差的影响
                            So_Sc_mode='off'  #拿掉So_Sc_mode的影响

                        #找出TA
                        Target_Amount = Necessary_info[h]['Total_money']/new_spot_present_price
                        
                        #找spot_balance,swap_position                                                          
                        # 找spot_balance
                   #     try:
                   #         Operation_info[h]['spot_balance'] = float(AQ[h]["POSITION_SPOT"].get(timeout=0.2))
                   #     except:
                   #         total_balance_result=client.get_account()
                   #         time.sleep(0.2)
                   #         instr_fu=h.split(h[-4:])[0]
                   #         for i in total_balance_result['balances']:
                   #             if i['asset']==instr_fu:
                   #                 Operation_info[h]['spot_balance'] = float(i['free'])

                        # 找margin_balance
                        try:
                            Operation_info[h]['margin_balance'] = float(AQ[h]["POSITION_MARGIN"].get(timeout=0.2))
                        except:
                            margin_balance_result=client.get_isolated_margin_account()
                            asset_list=[]
                            for i in margin_balance_result['assets']:    
                                asset_list.append(i['baseAsset']['asset']+'USDT')
                            if h in asset_list:
                                for i in margin_balance_result['assets']: 
                                    if i['baseAsset']['asset']==h.split(h[-4:])[0]:
                                        Operation_info[h]['margin_balance']=float(i['baseAsset']['free'])                                        
                            else:
                                Operation_info[h]['margin_balance']=0    
                        # 找swap_position
                        try:
                            Operation_info[h]['swap_position'] = float(AQ[h]["POSITION_SWAP"].get(timeout=0.2)) #注意这里是持仓数目,不是notional
                        except:
                            total_swap_position_result=client.futures_account()
                            time.sleep(0.2)
                            for i in total_swap_position_result['positions']:
                                if i['symbol']==h:
                                    Operation_info[h]['swap_position']=float(i['positionAmt'])  #注意这里是持仓数目,不是notional
       
                        # 加入大涨调仓判断,大涨后要尽速平仓
                        if no_pending_order == 'on': #先看有没有遗留单,有就不走调仓判断
                            if Operation_info[h]['margin_balance'] > 0 : #这时候是spot开多,swap开空
                                if Operation_info[h]['margin_balance'] * new_spot_present_price > Necessary_info[h]['Total_money'] * 1.1:
                                    So_Sc_mode = 'off'
                                    #                               mode_open_long_judge = {'Target_Amount': Target_Amount,
#                                                        'margin_balance': Operation_info[h]['margin_balance'],
#                                                        'swap_position': Operation_info[h]['swap_position'],
#                                                        'swap_size': swap_size,
#                                                        'spot_size': spot_size,
                                #                          'contract_value':Necessary_info[h]['contract_val']}

                                    mode_close_long_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                            'margin_balance': Operation_info[h]['margin_balance'],
                                                            'swap_position': Operation_info[h]['swap_position'],
                                                            'swap_close_size': swap_close_size,
                                                            'spot_close_size': spot_close_size,
                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                            'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                            'So_Sc_mode': So_Sc_mode,
                                                            'swap_min_size':swap_min_size}
                                                            
    #                              open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)
                                    close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)
                                    print('h_dk',h)
                                    print('超过1.1倍的平仓')
                                    print('close_long_mode_234',close_long_mode)
                                    print('close_long_final_open_mode_234',close_long_final_open_mode)
                                    print('close_long_final_close_mode_234',close_long_final_close_mode)

                            elif Operation_info[h]['margin_balance'] < 0 : #这时候是spot开空,swap开多
                                if Operation_info[h]['margin_balance'] * new_spot_present_price < Necessary_info[h]['Total_money'] * -1.1:
                                    So_Sc_mode = 'off'
                                    #查询开负仓还有多少可开
                    #                  max_avail_size_result = float(client.get_max_margin_loan(asset=h.split(h[-4:])[0],isolatedSymbol=h)['amount'])
                    #                  time.sleep(0.1)                                       
                    #                  if float(Necessary_info[h]['Present_Funding_Rate']) < 0.05 / 100:  # 借币日利率
                                    # 小于的话就不要开仓,抵不过借币日利率
                    #                      Target_Amount=0
                    #                  elif max_avail_size_result < spot_size:
                                        #TA就等于目前的margin_balance然后不要增加
                    #                      Target_Amount= abs(Operation_info[h]['margin_balance'])  #因为在def会*-1,所以这里加上abs
                    #                  else:
                    #                      pass #照常通过,继续开spot负仓                                                
                                    mode_close_short_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                            'margin_balance': Operation_info[h]['margin_balance'],
                                                            'swap_position': Operation_info[h]['swap_position'],
                                                            'swap_close_size': swap_close_size,
                                                            'spot_close_size': spot_close_size,
                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                            'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                            'So_Sc_mode': So_Sc_mode,
                                                            'swap_min_size':swap_min_size}
            #                        open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                                    close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)
                            
                        # 持仓差的调整
                        if no_pending_order == 'on':  # 先看有没有遗留单,有就不走持仓差调整
                            if Operation_info[h]['position_direction']=='open':   #现在是开仓方向
                                if Operation_info[h]['margin_balance'] > 0: #现在是开正仓
                                    if abs(Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'])* new_spot_present_price  > (tolerate_limit*1.5):
                                        So_Sc_mode = 'off'
                                        mode_open_long_judge = {'Target_Amount': Target_Amount,
                                                                        'margin_balance': Operation_info[h]['margin_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'swap_min_size':swap_min_size}
                                        #开正仓
                                        open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)  # 开仓模式
                                elif Operation_info[h]['spot_balance'] < 0: #现在是开负仓
                                    if abs(Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'])* new_spot_present_price  > (tolerate_limit*1.5):
                                        So_Sc_mode = 'off'
                                        max_avail_size_result = float(client.get_max_margin_loan(asset=h.split(h[-4:])[0],isolatedSymbol=h)['amount'])
                                        time.sleep(0.1)
                                        if max_avail_size_result<spot_size:
                                            So_Sc_mode = 'off'
                                            if abs(Operation_info[h]['swap_position']) > abs(Operation_info[h]['margin_balance']):
                                                # 已经没法继续开负仓,然后永续合约多于现货仓位,TA就等于目前的spot_balance然后不要增加,平swap的仓位
                                                Target_Amount_Close = abs(Operation_info[h]['margin_balance']) 
                                                mode_close_short_judge = {'Target_Amount_Close': Target_Amount_Close,  # Target_Amount_Close這裡設等於0
                                                            'margin_balance': Operation_info[h]['margin_balance'],
                                                            'swap_position': Operation_info[h]['swap_position'],
                                                            'swap_close_size': swap_close_size,
                                                            'spot_close_size': spot_close_size,
                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                            'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                            'So_Sc_mode': So_Sc_mode,
                                                            'swap_min_size':swap_min_size}
                        #                        open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                                                close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)
                                            
                                            elif abs(Operation_info[h]['swap_position']) < abs(Operation_info[h]['margin_balance']):
                                                # 已经没法继续开负仓,然后现货约多于合约仓位,TA就等于目前的spot_balance然后不要增加,建swap的仓位
                                                Target_Amount= abs(Operation_info[h]['margin_balance'])  #因为在def会*-1,所以这里加上abs                                                
                                                mode_open_short_judge = {'Target_Amount': Target_Amount,
                                                                        'margin_balance': Operation_info[h]['margin_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'swap_min_size':swap_min_size}
                                            # 开负仓
                                                open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)           

                            elif Operation_info[h]['position_direction']=='close':  #现在是平仓方向 

                                if Operation_info[h]['margin_balance'] > 0:  # 这时候是spot开多,swap开空
                                    if abs(Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'])* new_spot_present_price  > (tolerate_limit*1.5):
                                        # 持仓差超过tolerate_limit*1.3时启动,跳过So_Sc_mode,可能会有资金不足,所以选择平仓,
                                        So_Sc_mode = 'off'                                      
                                        mode_close_long_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                                'margin_balance': Operation_info[h]['margin_balance'],
                                                                'swap_position': Operation_info[h]['swap_position'],
                                                                'swap_close_size': swap_close_size,
                                                                'spot_close_size': spot_close_size,
                                                                'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                                'So_Sc_mode': So_Sc_mode,
                                                                'swap_min_size':swap_min_size}   
                                        close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)
                                      #  print('持仓差的平仓')
                                      #  print('close_long_mode_1',close_long_mode)
                            
                                elif Operation_info[h]['margin_balance'] < 0:  # 这时候是spot开空,swap开多
                                    if abs(Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'])* new_spot_present_price  > (tolerate_limit*1.3):
                                        # 持仓差超过tolerate_limit*1.3时启动,跳过So_Sc_mode,可能有spot无法继续卖,所以选择平仓模式
                                        So_Sc_mode = 'off'                                                                            
                                        mode_close_short_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                                'margin_balance': Operation_info[h]['margin_balance'],
                                                                'swap_position': Operation_info[h]['swap_position'],
                                                                'swap_close_size': swap_close_size,
                                                                'spot_close_size': spot_close_size,
                                                                'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                                'So_Sc_mode': So_Sc_mode,
                                                                'swap_min_size':swap_min_size}
                                        close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)


                        mode_So_Sc = {'maker_commission_spot': maker_commission_spot,
                                    'taker_commission_spot': taker_commission_spot,
                                    'maker_commission_swap': maker_commission_swap,
                                    'taker_commission_swap': taker_commission_swap,
                                    'swap_present_price': swap_present_price,
                                    'spot_index_price': spot_index_price   #这改用现货指数,不用中间价'spot_present_price': new_spot_present_price
                                    }
                        
                        if swap_mark_price > spot_index_price:  # 可以开正仓,先没加commission
                            #So=10
                            #Sc=-10
                            So = swap_mark_price - spot_index_price  # So是正数
                            Sc = spot_index_price - swap_mark_price  # Sc没必要
                        elif spot_index_price > swap_mark_price:  # 可以开负仓
                            #So=-10
                            #Sc=10
                            So = swap_mark_price - spot_index_price  # So没必要
                            Sc = spot_index_price - swap_mark_price  # Sc是正数
                        elif spot_index_price == swap_mark_price:
                            So=0
                            Sc=0
                            So_Sc_mode ='off'
                        
                        #以下是正常流程
                        if no_pending_order == 'on':
                            if So_Sc_mode =='on':
                                if swap_mark_price > spot_index_price:
                                    if Necessary_info[h]['Average_Funding_Rate'] >= 0:
                                        if So > Necessary_info[h]['Average_Funding_Rate'] * spot_index_price:
                                            # 如果目前资金费率小于万三,不开仓,可能有借币日利率,看客人的钱
                                            if float(Necessary_info[h]['Present_Funding_Rate']) < 0.0003 :
                                                pass
                                            else:
                                                mode_open_long_judge = {'Target_Amount': Target_Amount,
                                                                        'margin_balance': Operation_info[h]['margin_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'swap_min_size':swap_min_size}
                                                #开正仓
                                                open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)  # 开仓模式
                                    
                                    elif Necessary_info[h]['Average_Funding_Rate'] < 0:
                                        #让平仓增加5倍难度
                                        if So/5 > -1* Necessary_info[h]['Average_Funding_Rate'] * spot_index_price:                                               
                                            if float(Necessary_info[h]['Present_Funding_Rate']) < 0.0003:
                                                pass
                                            else:
                                                #平均资金费率要大于万三
                                                if abs(Necessary_info[h]['Average_Funding_Rate']) > 0.0003:
                                                    mode_close_short_judge = {'Target_Amount_Close': 0,
                                                                            # Target_Amount_Close這裡設等於0
                                                                            'margin_balance': Operation_info[h]['margin_balance'],
                                                                            'swap_position': Operation_info[h]['swap_position'],
                                                                            'swap_close_size': swap_close_size,
                                                                            'spot_close_size': spot_close_size,
                                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                            'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                                            'So_Sc_mode': So_Sc_mode,
                                                                            'swap_min_size':swap_min_size}
                                                    # 平负仓
                                                    close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)

                                elif swap_mark_price < spot_index_price:
                                    if Necessary_info[h]['Average_Funding_Rate'] >= 0:
                                        #让平仓增加5倍难度
                                        if Sc/5 > Necessary_info[h]['Average_Funding_Rate'] * spot_index_price:
                                            
                                            if float(Necessary_info[h]['Present_Funding_Rate']) > 0.0003:
                                                pass
                                            else:
                                                # 平均资金费率要大于万三
                                                if abs(Necessary_info[h]['Average_Funding_Rate']) > 0.0003:
                                                    mode_close_long_judge = {'Target_Amount_Close': 0,
                                                                            # Target_Amount_Close這裡設等於0
                                                                            'margin_balance': Operation_info[h]['margin_balance'],
                                                                            'swap_position': Operation_info[h]['swap_position'],
                                                                            'swap_close_size': swap_close_size,
                                                                            'spot_close_size': spot_close_size,
                                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                            'history_funding': Necessary_info[h]['Average_Funding_Rate'],
                                                                            'So_Sc_mode': So_Sc_mode,
                                                                            'swap_min_size':swap_min_size}
                                                    # 平正仓
                                                    close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)
                                                    print('h_34',h)
                                                    print('Sc',Sc)
                                                    print('是正常流程走的平仓')
                                    
                                    elif Necessary_info[h]['Average_Funding_Rate'] < 0:
                                        if Sc > -1 * Necessary_info[h]['Average_Funding_Rate'] * spot_index_price:
                                            # 如果目前资金费率>0,且加上下期资金费率大于正千一,就不要开负仓
                                            if float(Necessary_info[h]['Present_Funding_Rate']) > 0.0003:
                                                pass
                                            else:                              
                                                #查询开负仓还有多少可开
                                                max_avail_size_result = float(client.get_max_margin_loan(asset=h.split(h[-4:])[0],isolatedSymbol=h)['amount'])
                                                time.sleep(0.1)
                                                if max_avail_size_result>spot_size*1.05:
                                                    pass
                                                else:
                                                    usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=i,amount=200)
                                                 #   usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount=100, toSymbol=i)
                                                    time.sleep(0.1)
                                                    check_margin_result=client.get_max_margin_loan(asset=h.split(h[-4:])[0], isolatedSymbol=h)
                                                    time.sleep(0.1)
                                                    if check_margin_result['amount']>spot_size*1.05:
                                                        pass
                                                    else:#表示没币可借了,不要继续开仓
                                                        Target_Amount= abs(Operation_info[h]['margin_balance'])    
                                                if float(Necessary_info[h]['Present_Funding_Rate']) < 0.0003:  # 借币日利率
                                                # 小于的话就不要开仓,抵不过借币日利率
                                                    Target_Amount= abs(Operation_info[h]['margin_balance']) 
                                                elif max_avail_size_result < spot_size:
                                                    #TA就等于目前的margin_balance然后不要增加
                                                    Target_Amount= abs(Operation_info[h]['margin_balance'])  #因为在def会*-1,所以这里加上abs
                                                else:
                                                    pass #照常通过,继续开spot负仓 
                                                mode_open_short_judge = {'Target_Amount': Target_Amount,
                                                                        'margin_balance': Operation_info[h]['margin_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'swap_min_size':swap_min_size}
                                            # 开负仓
                                                open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                        
                        #print('h_1',h)
                        #print('open_long_mode_2',open_long_mode)
                        #print('open_short_mode_2',open_short_mode)
                        #print('close_long_mode_2',close_long_mode)
                       # print('close_short_mode_2',close_short_mode)
                        #print('open_long_final_open_mode_2',open_long_final_open_mode)
                        #print('open_long_final_close_mode_2',open_long_final_close_mode)
                        #print('close_long_final_open_mode_2',close_long_final_open_mode)
                        #print('close_long_final_close_mode_2',close_long_final_close_mode)
                                    
                        if open_long_mode =='on' or open_long_final_open_mode=='on' or open_long_final_close_mode=='on' or open_short_mode=='on' or open_short_final_open_mode =='on' or  open_short_final_close_mode =='on' or close_short_mode =='on' or close_short_final_open_mode =='on' or close_short_final_close_mode =='on' or close_long_mode =='on' or close_long_final_open_mode =='on' or close_long_final_close_mode =='on' :
                            time.sleep(0.3)         
                            #取spot5档数据,注意ws跟restAPI数据型态不同
                            #这里准备新的五档数据给下面下单用
                            # 新的swap五档
                            Operation_info[h]['spot_bids_price5'] = []
                            Operation_info[h]['spot_asks_price5'] = []
                            try:
                                spot_depth5 = MQ[h]["DEPTH5_SPOT"].get(timeout=0.2)
                                #  for i in range(5):
                                #      Operation_info[h]['spot_bids_price5'].append(float(spot_depth5['b'][i][0]))
                                #      Operation_info[h]['spot_asks_price5'].append(float(spot_depth5['a'][i][0]))
                            except:
                                spot_depth5 = client.get_order_book(symbol=h,limit='5')
                                print('我spot_depth5还是用restAPI___#')
                                time.sleep(0.1)
                            for i in range(5):
                                Operation_info[h]['spot_bids_price5'].append(float(spot_depth5['bids'][i][0]))
                                Operation_info[h]['spot_asks_price5'].append(float(spot_depth5['asks'][i][0]))
                #                    print('h_',h)
                            #取swap5档数据,注意ws跟restAPI数据型态不同
                            Operation_info[h]['swap_bids_price5'] = []
                            Operation_info[h]['swap_asks_price5'] = []
                            try:
                                swap_depth5 = MQ[h]["DEPTH5_SWAP"].get(timeout=0.2)
                                for i in range(5):
                                    Operation_info[h]['swap_bids_price5'].append(float(swap_depth5['b'][i][0]))
                                    Operation_info[h]['swap_asks_price5'].append(float(swap_depth5['a'][i][0]))
                            except:
                                swap_depth5 = client.futures_order_book(symbol=h,limit='5')
                                for i in range(5):
                                    Operation_info[h]['swap_bids_price5'].append(float(swap_depth5['bids'][i][0]))
                                    Operation_info[h]['swap_asks_price5'].append(float(swap_depth5['asks'][i][0]))    
                                time.sleep(0.1)

                        if open_long_mode =='on' or open_long_final_open_mode=='on' or open_long_final_close_mode=='on' or open_short_mode=='on' or open_short_final_open_mode =='on' or  open_short_final_close_mode =='on':
                            Operation_info[h]['position_direction']='open'
                        elif close_short_mode =='on' or close_short_final_open_mode =='on' or close_short_final_close_mode =='on' or close_long_mode =='on' or close_long_final_open_mode =='on' or close_long_final_close_mode =='on':
                            Operation_info[h]['position_direction']='close'
                #         print(h)
                #         print('spot_bid',Operation_info[h]['spot_bids_price5'])
                #         print('spot_ask',Operation_info[h]['spot_asks_price5'])
                #         print('swap_bid',Operation_info[h]['swap_bids_price5'])
                #         print('swap_ask',Operation_info[h]['swap_asks_price5'])


                        if open_long_mode == 'on':
                            if (tolerate_limit/2/swap_present_price) > Operation_info[h]['margin_balance']+ Operation_info[h]['swap_position'] > \
                                    (tolerate_limit / 4/ swap_present_price):
                                # 现货仓位多于合约仓位,合约卖best价,稍微调整现货买2档
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_bids_price5'][1]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}  
                            
                            elif (tolerate_limit / swap_present_price) > Operation_info[h]['margin_balance']+ Operation_info[h]['swap_position'] > \
                                    (tolerate_limit / 2 / swap_present_price):
                                # 现货仓位多于合约仓位,合约卖中间价,现货买五档
                                #处理挂单中间价的问题
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_bids_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': swap_price,
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}

                            elif Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] > (tolerate_limit / swap_present_price):
                                # 现货仓位多于合约仓位,Spot_size=0, swap卖出挂买一价limit, 0607 合约卖中间价
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_bids_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': swap_price,
                                                'spot_size': '0',
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'} 

                            elif -1*(tolerate_limit / 2 /  swap_present_price) < Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] <-1*(tolerate_limit / 4 / swap_present_price):  
                                # 合约仓位多于现货仓位,现货买best价,合约稍微调整卖2档                                                                     
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_bids_price5'][0]), Necessary_info[h]['spot_tick_digit']) ,
                                                'swap_price': precfloat(float(Operation_info[h]['swap_asks_price5'][1]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}

                            elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] <-1*(tolerate_limit / 2 / swap_present_price):  
                                # 合约仓位多于现货仓位,现货买中间价,合约卖五档
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_price = precfloat((float(Operation_info[h]['spot_bids_price5'][0])), Necessary_info[h]['spot_tick_digit'])                                                                          
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': spot_price,
                                                'swap_price': precfloat(float(Operation_info[h]['swap_asks_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                            
                            elif Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] < -1*(tolerate_limit / swap_present_price):  
                                # 合约仓位多于现货仓位,spot买在对手价
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_price = precfloat((float(Operation_info[h]['spot_bids_price5'][0])), Necessary_info[h]['spot_tick_digit'])  
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': spot_price,
                                                'swap_price': precfloat(float(Operation_info[h]['swap_asks_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': '0',
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}

                            else:  # 平时设置在Best
                                mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_bids_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                        
                        if open_short_mode == 'on':  #swap是long_position,margin_balance<0
                            if -1*(tolerate_limit / 2 / swap_present_price) < Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / 4 / swap_present_price):
                                # 现货short仓位多于合约long仓位,合约买best价,现货调整卖2档                                   
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                    'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                    'spot_price': precfloat(float(Operation_info[h]['spot_asks_price5'][1]),Necessary_info[h]['spot_tick_digit']),
                                                    'swap_price': precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                    'spot_size': spot_size,
                                                    'swap_size': swap_size,
                                                    'spot_order_type': 'LIMIT_MAKER',
                                                    'swap_order_type': 'LIMIT'}

                            elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / 2 / swap_present_price):
                                # 现货short仓位多于合约long仓位,合约买中间价,现货卖五档
                                #处理挂单中间价的问题
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_bids_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_short_order = { 'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                    'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                    'spot_price': precfloat(float(Operation_info[h]['spot_asks_price5'][4]),Necessary_info[h]['spot_tick_digit']),
                                                    'swap_price': swap_price,
                                                    'spot_size': spot_size,
                                                    'swap_size': swap_size,
                                                    'spot_order_type': 'LIMIT_MAKER',
                                                    'swap_order_type': 'LIMIT'}
                            
                            elif Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / swap_present_price):
                                # 现货仓位多于合约仓位,Spot_size=0, swap买入挂卖一价limit,0607swap挂中间价                                    
                                #处理挂单中间价的问题
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_bids_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_asks_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': swap_price,
                                                'spot_size': '0',
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                            
                            elif (tolerate_limit / 2 / swap_present_price) > Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] >(tolerate_limit / 4 / swap_present_price):
                                # 合约仓位多于现货仓位,现货卖best价,合约调整买2档                                   
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': precfloat(float(Operation_info[h]['swap_bids_price5'][1]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                            
                            elif (tolerate_limit / swap_present_price) > Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] >(tolerate_limit / 2 / swap_present_price):
                                # 合约仓位多于现货仓位,现货卖中间价,合约买五档
                                #处理挂单中间价的问题
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0])), Necessary_info[h]['spot_tick_digit'])
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': spot_price,
                                                'swap_price': precfloat(float(Operation_info[h]['swap_bids_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                            
                            elif Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] > (tolerate_limit / swap_present_price):
                                # 合约仓位多于现货仓位,spot卖在对手价,0607spot挂中间价
                                #处理挂单中间价的问题
                                #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0])), Necessary_info[h]['spot_tick_digit'])
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': spot_price,
                                                'swap_price': precfloat(float(Operation_info[h]['swap_bids_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': '0',
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}

                            else:  # 平时设置在Best,卖spot,买swap,在best价
                                mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                'spot_price': precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                'swap_price': precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                'spot_size': spot_size,
                                                'swap_size': swap_size,
                                                'spot_order_type': 'LIMIT_MAKER',
                                                'swap_order_type': 'LIMIT'}
                        
                        if close_short_mode =='on':  #平负仓，spot平空,swap平多.margin_balance<0,swap_position>0
                            if -1*(tolerate_limit / 2 / swap_present_price) < Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / 4 / swap_present_price):  
                                # 平仓时现货仓位多于合约仓位,现货要加速平空,现货买在best价,合约调整卖在2档                                    
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_bids_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_asks_price5'][1]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}

                            elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / 2 / swap_present_price):  
                                # 平仓时现货仓位多于合约仓位,现货要加速平空,现货买在中间价,合约卖在五档
                                #处理挂单中间价的问题
                                #如果spot best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_bids_price5'][0])), Necessary_info[h]['spot_tick_digit'])                                        
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': spot_close_price,
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_asks_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}
                            
                            elif Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] < -1*(tolerate_limit / swap_present_price):  
                                #平负仓，spot平空买入,swap平多卖出.margin_balance<0,swap_position>0
                                # 平仓时现货仓位多于合约仓位,spot买太慢,现货买在对手价,spot买在中间价
                                #如果spot best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_bids_price5'][0])), Necessary_info[h]['spot_tick_digit'])  
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': spot_close_price,
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_asks_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': '0',
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}
                            
                            elif (tolerate_limit / 2 / swap_present_price) > Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] > (tolerate_limit / 4 / swap_present_price):  
                                #平负仓，spot平空买入,swap平多卖出.margin_balance<0,swap_position>0
                                # 平仓时合约仓位多于现货仓位,合约要加速平多,swap卖在best价,现货调整买在2档
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_bids_price5'][1]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}
                            
                            elif (tolerate_limit / swap_present_price) > Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] > (tolerate_limit / 2 / swap_present_price):  #平负仓，spot平空买入,swap平多卖出.spot_balance<0,swap_position>0
                                # 平仓时合约仓位多于现货仓位,合约要加速平多,swap卖在中间价,现货买在五档
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_bids_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': swap_close_price,
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}
                            
                            elif Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] >(tolerate_limit / swap_present_price):  
                                #平负仓，spot平空,swap平多.margin_balance<0,swap_position>0
                                # 平仓时合约仓位多于现货仓位,合约要加速平多,合约卖在对手价,0607合约卖在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_bids_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': swap_close_price,
                                                        'spot_close_size': '0',
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}
                            
                            else:  # 平时设置在best,spot平空买入,swap平多卖出
                                mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_bids_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT',
                                                        'spot_minQty_digit':Necessary_info[h]['spot_minQty_digit']}                         

                        if close_long_mode =='on':  
                            if (tolerate_limit / 4 / swap_present_price) > Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] > (tolerate_limit / 4 / swap_present_price):
                                #spot平多,swap平空,margin_balance>0,swap_position<0
                                # 平仓时现货仓位多于合约仓位,现货要加速平多,现货卖best价,合约调整买2档
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_bids_price5'][1]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}    

                            elif (tolerate_limit / swap_present_price) > Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] > (tolerate_limit / 2 / swap_present_price):
                                #spot平多,swap平空,margin_balance>0,swap_position<0
                                # 平仓时现货仓位多于合约仓位,现货要加速平多,现货卖中间价,合约买五档
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask               
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_close_price = precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit'])       
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': spot_close_price,
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_bids_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}
                            
                            elif Operation_info[h]['margin_balance'] +Operation_info[h]['swap_position'] > (tolerate_limit / swap_present_price):
                                # 平仓时现货仓位多于合约仓位,spot卖太慢,现货卖在对手价,0607spot卖在中间价
                                if float(Operation_info[h]['spot_bids_price5'][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_asks_price5'][0]):
                                    spot_close_price = precfloat((float(Operation_info[h]['spot_asks_price5'][0]) + float(Operation_info[h]['spot_bids_price5'][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                else:
                                    spot_close_price = precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit'])  
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': spot_close_price,
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_bids_price5'][4]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': '0',
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}

                            elif -1*tolerate_limit / 2 / swap_present_price < Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] < -1*(tolerate_limit / 4 / swap_present_price):
                                # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在best价,现货调整卖在2档      
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_asks_price5'][2]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'} 

                            elif -1*tolerate_limit / swap_present_price < Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] < -1*(tolerate_limit / 2 / swap_present_price):
                                # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在中间价,现货卖在五档                                    
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_asks_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': swap_close_price,
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}
                            
                            elif Operation_info[h]['swap_position'] + Operation_info[h]['margin_balance'] < -1*tolerate_limit / swap_present_price:
                                # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在对手价 0607 swap买在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_asks_price5'][4]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': swap_close_price,
                                                        'spot_close_size': '0',
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}
                            
                            else:  # 平时设置在best,spot平多卖出,swap平空买入
                                mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_close_price': precfloat(float(Operation_info[h]['spot_asks_price5'][0]), Necessary_info[h]['spot_tick_digit']),
                                                        'swap_close_price': precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_close_size': spot_close_size,
                                                        'swap_close_size': swap_close_size,
                                                        'spot_order_type': 'LIMIT_MAKER',
                                                        'swap_order_type': 'LIMIT'}

                        # swap_min_size = precfloat(Necessary_info[h]['swap_min_notional']/swap_present_price, Necessary_info[h]['quantityPrecision'])
                        if open_long_final_open_mode == 'on':  # swap继续开仓开空,swap_position<0,margin_balance>0
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_long_final_open_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_size= swap_min_size
                            else:
                                swap_size=0                                
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size']< float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0]))/2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_price = precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_open_long_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_price': swap_price,      #买在best价
                                                                'swap_size': swap_size,
                                                                    'swap_order_type': 'LIMIT'}

                        if open_short_final_open_mode == 'on':  # swap继续开仓开多,swap_position>0,margin_balance<0
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_short_final_open_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] +Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_size= swap_min_size
                            else:
                                swap_size=0
                            # 因为成交慢,试著开多在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_price = precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_open_short_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_price': swap_price,      #买在best价
                                                                'swap_size': swap_size,
                                                                    'swap_order_type': 'LIMIT'}
                        
                        if open_long_final_close_mode == 'on':  # swap仓位多于spot,要平空,swap_position<0,margin_balance>0
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_long_final_close_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] +Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                           # print('margin_balance_23',Operation_info[h]['margin_balance'])
                          #  print('swap_position_34',Operation_info[h]['swap_position'])
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_size= swap_min_size
                            else:
                                swap_size=0
                            # 因为成交慢,试著平空/买在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_price = precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_open_long_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                            'swap_price': swap_price,  # 买在best价
                                                            'swap_size': swap_size,
                                                            'swap_order_type': 'LIMIT'}
                        
                        if open_short_final_close_mode == 'on':  # swap仓位多于spot,要平多,swap_position>0,margin_balance<0
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_short_final_close_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] +Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_size= swap_min_size
                            else:
                                swap_size=0
                            # 因为成交慢,试著平多/卖在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5']['asks'][0]):
                                swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_price = precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_open_short_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                            'swap_price': swap_price,  # 买在best价
                                                            'swap_size': swap_size,
                                                            'swap_order_type': 'LIMIT'}
                        
                        if close_long_final_close_mode == 'on':  # spot平多,swap继续平空a买在对手价,
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_long_final_close_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_close_size= swap_min_size
                            else:
                                swap_close_size=0
                            # 因为成交慢,试著平空/买在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_price = precfloat(float(Operation_info[h]['swap_bids_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_close_long_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_close_price': swap_price,  # 买在best价
                                                                'swap_close_size': swap_close_size,
                                                                'swap_order_type': 'LIMIT'}
                        
                        if close_short_final_close_mode == 'on':  # swap继续平多,
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_short_final_close_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_close_size= swap_min_size
                            else:
                                swap_close_size=0
                            # 因为成交慢,试著平多/卖在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_close_price = precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit'])
                            mode_take_close_short_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_close_price': swap_close_price,  # 买在best价
                                                                'swap_close_size': swap_close_size,
                                                                'swap_order_type': 'LIMIT'}
                        
                        if close_long_final_open_mode == 'on':  # swap继续开空,
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_final_open_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position']) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_close_size= swap_min_size
                            else:
                                swap_close_size=0
                            # 因为成交慢,试著开空/卖在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_close_price = precfloat(float(Operation_info[h]['swap_asks_price5'][0]), Necessary_info[h]['swap_tick_digit'])

                            mode_take_close_long_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_close_price': swap_close_price,  # 买在best价
                                                                'swap_close_size': swap_close_size,
                                                                'swap_order_type': 'LIMIT'}
                        
                        if close_short_final_open_mode == 'on':  # swap开多,
                            tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_final_open_mode,现在相差' + \
                                str((Operation_info[h]['margin_balance'] + Operation_info[h]['swap_position'] ) * swap_present_price) + '美金'
                            sendmessage(tutu)
                            if abs(Operation_info[h]['margin_balance']+Operation_info[h]['swap_position'])>= swap_min_size:                                    
                                swap_close_size= swap_min_size
                            else:
                                swap_close_size=0
                            # 因为成交慢,试著开多/买在对手价, 0607改成挂在中间价
                            #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                            if float(Operation_info[h]['swap_bids_price5'][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_asks_price5'][0]):
                                swap_close_price = precfloat((float(Operation_info[h]['swap_asks_price5'][0]) + float(Operation_info[h]['swap_bids_price5'][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                            else:
                                swap_close_price = precfloat((float(Operation_info[h]['swap_bids_price5'][0])), Necessary_info[h]['swap_tick_digit'])
                            mode_take_close_short_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                'swap_close_price': swap_close_price,  # 买在best价
                                                                'swap_close_size': swap_close_size,
                                                                'swap_order_type': 'LIMIT'}
                        #print('funding_rate_result_1',funding_rate_result)

                        if open_long_mode == 'on':
                            # 下单,买spot,卖swap
                            spot_order_result, swap_order_result = take_long_order(mode_take_long_order)
                          #  print('spot_order_result_1',spot_order_result)
                          #  print('swap_order_result_1',swap_order_result)
                            time.sleep(0.1)
                            if spot_order_result == 'none':
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                            elif spot_order_result != 'none':
                                if int(spot_order_result['orderId'])!=0:
                                    Operation_info[h]['spot_pending_list_left'] = 'on'
                                    #记录买进spot几单了
                                    Operation_info[h]['spot_buy_trading_orders']=Operation_info[h]['spot_buy_trading_orders']+1
                                else:
                                    Operation_info[h]['spot_pending_list_left'] = 'off'                             
                            
                            if swap_order_result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif swap_order_result !='none':
                                if int(swap_order_result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                        
                        if open_short_mode == 'on':
                            # 下单,买swap,卖spot
                            spot_order_result, swap_order_result = take_short_order(mode_take_short_order)
                            time.sleep(0.1)
                            if spot_order_result == 'none':
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                            elif spot_order_result != 'none':
                                if int(spot_order_result['orderId'])!=0:
                                    Operation_info[h]['spot_pending_list_left'] = 'on'
                                    #记录卖出spot几单了
                                    Operation_info[h]['spot_sell_trading_orders']=Operation_info[h]['spot_sell_trading_orders']+1
                                else:
                                    Operation_info[h]['spot_pending_list_left'] = 'off'                             
                            
                            if swap_order_result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif swap_order_result !='none':
                                if int(swap_order_result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单了
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:
                                    Operation_info[h]['swap_pending_list_left'] = 'off'

                        if close_long_mode == 'on':
                            #卖spot,买swap
                            spot_close_order_result, swap_close_order_result = take_close_long_order(mode_take_close_long_order)
                            print('spot_close_order_result_3',spot_close_order_result)
                            print('swap_close_order_result_3',swap_close_order_result)
                            time.sleep(0.1)
                            if spot_close_order_result == 'none':
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                            elif spot_close_order_result != 'none':
                                if int(spot_close_order_result['orderId'])!=0:
                                    Operation_info[h]['spot_pending_list_left'] = 'on'
                                    #记录卖出spot几单
                                    Operation_info[h]['spot_sell_trading_orders']=Operation_info[h]['spot_sell_trading_orders']+1
                                else:
                                    Operation_info[h]['spot_pending_list_left'] = 'off'                             
                            
                            if swap_close_order_result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif swap_close_order_result !='none':
                                if int(swap_close_order_result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:
                                    Operation_info[h]['swap_pending_list_left'] = 'off'                            

                        if close_short_mode == 'on':
                            #买spot,卖swap
                            spot_close_order_result, swap_close_order_result = take_close_short_order(mode_take_close_short_order)
                            time.sleep(0.1)
                            if spot_close_order_result == 'none':
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                            elif spot_close_order_result != 'none':
                                if int(spot_close_order_result['orderId'])!=0:
                                    Operation_info[h]['spot_pending_list_left'] = 'on'
                                    #记录买进spot几单
                                    Operation_info[h]['spot_buy_trading_orders']=Operation_info[h]['spot_buy_trading_orders']+1
                                else:
                                    Operation_info[h]['spot_pending_list_left'] = 'off'                             
                            
                            if swap_close_order_result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif swap_close_order_result !='none':
                                if int(swap_close_order_result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:
                                    Operation_info[h]['swap_pending_list_left'] = 'off'

                        if open_long_final_open_mode == 'on':
                            #下單,卖swap
                            result = take_open_long_final_open_order(mode_take_open_long_final_open_order)
                            time.sleep(0.1)
                            #print('mode_take_open_long_final_open_order',mode_take_open_long_final_open_order)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'

                        if open_short_final_open_mode == 'on':
                            #下單,买swap
                            result = take_open_short_final_open_order(mode_take_open_short_final_open_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'
                        
                        if open_long_final_close_mode == 'on':
                            # 下单,买swap
                            result = take_open_long_final_close_order(mode_take_open_long_final_close_order)
                            #print('open_long_close_result',result)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'

                        if open_short_final_close_mode == 'on':
                            # 下单,卖出swap
                            result = take_open_short_final_close_order(mode_take_open_short_final_close_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'

                        if close_long_final_close_mode == 'on':
                            # 下单
                            result = take_close_long_final_close_order(mode_take_close_long_final_close_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'

                        if close_short_final_close_mode == 'on':
                            # 下单,卖出swap
                            result = take_close_short_final_close_order(mode_take_close_short_final_close_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'
                        
                        if close_long_final_open_mode == 'on':
                            # 下单
                            result = take_close_long_final_open_order(mode_take_close_long_final_open_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录卖出swap几单
                                    Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'

                        if close_short_final_open_mode == 'on':
                            # 下单
                            result = take_close_short_final_open_order(mode_take_close_short_final_open_order)
                            time.sleep(0.1)
                            if result == 'none':
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                            elif result != 'none':
                                if int(result['orderId'])!=0:
                                    Operation_info[h]['swap_pending_list_left'] = 'on'
                                    #记录买进swap几单
                                    Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                else:   
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                            Operation_info[h]['spot_pending_list_left'] = 'off'
                            
                        # 更新新的spot五档
                            Operation_info[h]['spot_bids_price5'] = []
                            Operation_info[h]['spot_asks_price5'] = []
                            try:
                                spot_depth5 = MQ[h]["DEPTH5_SPOT"].get(timeout=0.2)
                        #        for i in range(5):
                        #            Operation_info[h]['spot_bids_price5'].append(float(spot_depth5['b'][i][0]))
                        #            Operation_info[h]['spot_asks_price5'].append(float(spot_depth5['a'][i][0]))
                            except:
                                spot_depth5 = client.get_order_book(symbol=h,limit='5')
                                print('我spot_depth5还是用restAPI___#')
                                time.sleep(0.1)
                            for i in range(5):
                                Operation_info[h]['spot_bids_price5'].append(float(spot_depth5['bids'][i][0]))
                                Operation_info[h]['spot_asks_price5'].append(float(spot_depth5['asks'][i][0]))
                        #               print('h_',h)
                            

                            #更新新的swap五档,注意ws跟restAPI数据型态不同
                            Operation_info[h]['swap_bids_price5'] = []
                            Operation_info[h]['swap_asks_price5'] = []
                            try:
                                swap_depth5 = MQ[h]["DEPTH5_SWAP"].get(timeout=0.2)
                                for i in range(5):
                                    Operation_info[h]['swap_bids_price5'].append(float(swap_depth5['b'][i][0]))
                                    Operation_info[h]['swap_asks_price5'].append(float(swap_depth5['a'][i][0]))
                            except:
                                swap_depth5 = client.futures_order_book(symbol=h,limit='5')
                                for i in range(5):
                                    Operation_info[h]['swap_bids_price5'].append(float(swap_depth5['bids'][i][0]))
                                    Operation_info[h]['swap_asks_price5'].append(float(swap_depth5['asks'][i][0]))    
                                time.sleep(0.1)
                        
                        Nowtime = datetime.now()
                        new_record_second = Nowtime.second
                        new_record_minute = Nowtime.minute
                        new_record_hour = Nowtime.hour
                        new_record_day = Nowtime.day
                        
                        timestamp = datetime.now().isoformat(" ", "seconds") 
                        #new_record_hour != record_hour:
                        if new_record_hour < 8:
                            if new_record_minute <59:
                                if new_record_second > 30:
                                    amount_spot_info=[]
                                    amount_swap_info=[]
                                    pair_info=[]

                                    for h in TradingPair:
                                        #先找spot成交記錄
                                        spot_fills_result = client.get_my_trades(symbol=h)
                                        time.sleep(0.1)
                                        for i in spot_fills_result:
                                            Operation_info[h]['spot_trading_fee']=Operation_info[h]['spot_trading_fee']+float(i['commission'])*float(i['price']) 
                                            timestamp=int(int(i['time'])/1000)   
                                            #转换成localtime
                                            time_local = time.localtime(timestamp)
                                            #转换成新的时间格式(2016-05-05 20:28:54)
                                            dt =time.strftime("%Y-%m-%d %H:%M:%S",time_local)
                                            ddt=parse(dt)
                                            #先选日期段
                                            if ddt.day== new_record_day-1:
                                                if i['isBuyer']==False:
                                                    Operation_info[h]['spot_sell_trading_net_amount']=Operation_info[h]['spot_sell_trading_net_amount']+float(i['commission'])*float(i['price'])  
                                                    Operation_info[h]['spot_trading_sell_size']=Operation_info[h]['spot_trading_sell_size']+float(i['qty']) 

                                                elif i['isBuyer']==True:
                                                    Operation_info[h]['spot_buy_trading_net_amount']=Operation_info[h]['spot_buy_trading_net_amount']+float(i['commission'])*float(i['price'])  
                                                    Operation_info[h]['spot_trading_buy_size']=Operation_info[h]['spot_trading_buy_size']+float(i['qty']) 
                                        
                                        #swap成交记录   
                                        swap_instrument_id=h
                                        swap_fills_result = client.futures_account_trades(symbol=h)
                                        time.sleep(0.1)
                                        for i in swap_fills_result:
                                            #先算交易费
                                            Operation_info[h]['swap_trading_fee']=Operation_info[h]['swap_trading_fee']+float(i['commission'])    #它u本位合约的反佣跟手续费是用usdt,直接用
                                            timestamp=int(int(i['time'])/1000)   
                                            #转换成localtime
                                            time_local = time.localtime(timestamp)
                                            #转换成新的时间格式(2016-05-05 20:28:54)
                                            dt =time.strftime("%Y-%m-%d %H:%M:%S",time_local)
                                            ddt=parse(dt)
                                            #先选日期段
                                            if ddt.day== new_record_day-1:
                                                if i['side']=='SELL':
                                                    Operation_info[h]['swap_sell_trading_net_amount']=Operation_info[h]['swap_sell_trading_net_amount']+float(i['quoteQty'])
                                                    Operation_info[h]['swap_trading_sell_size']=Operation_info[h]['swap_trading_sell_size']+float(i['qty']) 
                                                elif i['side']=='BUY':
                                                    Operation_info[h]['swap_buy_trading_net_amount']=Operation_info[h]['swap_buy_trading_net_amount']+float(i['quoteQty'])
                                                    Operation_info[h]['swap_trading_buy_size']=Operation_info[h]['swap_trading_buy_size']+float(i['qty'])
                                        
                                        #计算交易size
                                        average_spot_trading_size=precfloat((Operation_info[h]['spot_trading_sell_size']-Operation_info[h]['spot_trading_buy_size']),2)
                                        average_swap_trading_size=precfloat((Operation_info[h]['swap_trading_sell_size']-Operation_info[h]['swap_trading_buy_size']),2)
                                        if precfloat(Operation_info[h]['spot_trading_buy_size'],2) == 0 :
                                            spot_buy_average_price = 0
                                        else:
                                            spot_buy_average_price = precfloat(Operation_info[h]['spot_buy_trading_net_amount']/Operation_info[h]['spot_trading_buy_size'],2)                                            
                                        if precfloat(Operation_info[h]['spot_trading_sell_size'],2) == 0 :
                                            spot_sell_average_price = 0
                                        else:
                                            spot_sell_average_price = precfloat(Operation_info[h]['spot_sell_trading_net_amount']/Operation_info[h]['spot_trading_sell_size'],2)                                            
                                        if precfloat(Operation_info[h]['swap_trading_buy_size'],2) == 0 :
                                            swap_buy_average_price = 0
                                        else:
                                            swap_buy_average_price = precfloat(Operation_info[h]['swap_buy_trading_net_amount']/Operation_info[h]['swap_trading_buy_size'],2)                                            
                                        if precfloat(Operation_info[h]['swap_trading_sell_size'],2) == 0 :
                                            swap_sell_average_price = 0
                                        else:
                                            swap_sell_average_price = precfloat(Operation_info[h]['swap_sell_trading_net_amount']/Operation_info[h]['swap_trading_sell_size'],2)
                                        
                                        key_list = []
                                        value_list = []
                                        key_list = ["现货买入成交额(USD)", "现货买入成交量(币)", "现货买入平均价格(USD)","现货卖出成交额(USD)", "现货卖出成交量(币)", "现货卖出平均价格(USD)",'现货交易累积手续费（USD)']
                                        value_list = [str(precfloat(Operation_info[h]['spot_buy_trading_net_amount'],2)),str(precfloat(Operation_info[h]['spot_trading_buy_size'],2)),str(spot_buy_average_price),str(precfloat(Operation_info[h]['spot_sell_trading_net_amount'],2)),str(precfloat(Operation_info[h]['spot_trading_sell_size'],2)),str(spot_sell_average_price),str(precfloat(Operation_info[h]['spot_trading_fee'],5))]
                                        amount_spot_pair_info = dict(zip(key_list, value_list))
                                        B={h:amount_spot_pair_info}
                                        amount_spot_info.append(B)

                                        key_list = []
                                        value_list = []
                                        key_list = ["U本位合约买入成交额(USD)", "U本位合约买入成交量(币)", "U本位合约买入平均价格(USD)","U本位合约卖出成交额(USD)", "U本位合约卖出成交量(币)", "U本位合约卖出平均价格(USD)", "U本位合约交易累积手续费(USD)"]
                                        value_list = [str(precfloat(Operation_info[h]['swap_buy_trading_net_amount'],2)),str(precfloat(Operation_info[h]['swap_trading_buy_size'],2)),str(swap_buy_average_price),str(precfloat(Operation_info[h]['swap_sell_trading_net_amount'],2)),str(precfloat(Operation_info[h]['swap_trading_sell_size'],2)),str(swap_sell_average_price),str(precfloat(Operation_info[h]['swap_trading_fee'],5))]
                                        amount_swap_pair_info = dict(zip(key_list, value_list))
                                        C={h:amount_swap_pair_info}
                                        amount_swap_info.append(C)

                                        #tutu = timestamp + "," + '交易对 '+ h +':'+'现货买进'+str(Operation_info[h]['spot_buy_trading_orders'])+'次,'+ '现货卖出'+str(Operation_info[h]['spot_sell_trading_orders'])+'次,'+'U本永续买进'+str(Operation_info[h]['swap_buy_trading_orders'])+'次,'+'U本永续卖出'+str(Operation_info[h]['swap_sell_trading_orders'])+'次。'
                                        key_list = []
                                        value_list = []
                                        key_list = ["现货下单买进(非成交）", "现货下单卖出(非成交）", "U本永续下单买进(非成交）", "U本永续下单卖出(非成交）"]
                                        value_list = [str(Operation_info[h]['spot_buy_trading_orders'])+'次', str(Operation_info[h]['spot_sell_trading_orders'])+'次',str(Operation_info[h]['swap_buy_trading_orders'])+'次',str(Operation_info[h]['swap_sell_trading_orders'])+'次']
                                        pair_order_info = dict(zip(key_list, value_list))
                                        A={h:pair_order_info}
                                        pair_info.append(A)
                                    
                                    tutu_1=amount_spot_info
                                    sendmessage(tutu_1)
                                    
                                    tutu_2=pair_info
                                    sendmessage(tutu_2)

                                    tutu_3=amount_swap_info
                                    sendmessage(tutu_3)
                                      
                except Exception as e:
                    #因为全部是用rest api
                    print(e)
                    print("disconnected，connecting tradeA……")
                    # error存成csv
                    mapping = {}
                    kk = []
                    path = '/root/' + Customer_name + '_error_report.csv'
                    key_list = ['timestamp', 'error']
                    for key, value in zip(key_list, [datetime.now(), e]):
                        mapping[key] = value
                    kk.append(eval(json.dumps(mapping)))
                    kkk = pd.DataFrame(kk)
            #        kkk.to_csv(path, mode='a+', header=True, index=False)
                    break

        except Exception as e:
            print(e)
            print("disconnected，connecting tradeB……")
            # error存成csv
            mapping = {}
            kk = []
            path = '/root/' + Customer_name + '_error_report.csv'
            key_list = ['timestamp', 'error']
            for key, value in zip(key_list, [datetime.now(), e]):
                mapping[key] = value
            kk.append(eval(json.dumps(mapping)))
            kkk = pd.DataFrame(kk)
     #       kkk.to_csv(path, mode='a+', header=True, index=False)
            continue

def run_ws(api_key, api_secret,TT,MQ,AQ,streams,TradingPair,param_set_list):
    client = Client(api_key, api_secret)
    loop = asyncio.get_event_loop()
    # future_market_data
    if TT=="USD_M_data":
        loop.run_until_complete(USD_M_data(streams,MQ,TradingPair))
    # spot_data
    if TT=="SPOT_data":
        loop.run_until_complete(SPOT_data(streams,MQ,TradingPair))
    # future_account_data
    if TT=="USD_M_Account_data":
        loop.run_until_complete(USD_M_Account_update(api_key, api_secret, AQ, TradingPair))
    # spot_account_data
    if TT=="SPOT_Account_data":
        loop.run_until_complete(SPOT_Account_update(api_key, api_secret, AQ, TradingPair))
    # 交易（下单，撤单，改单等）
    if TT== "trade":
        loop.run_until_complete(trade(api_key, api_secret, MQ, AQ,TradingPair,param_set_list))
    
def run_ws_margin(api_key, api_secret,TT,AQ,margin_instrument_id):
    client = Client(api_key, api_secret)
    loop = asyncio.get_event_loop()
    # margin_account_data    
    if TT=="Margin_Account_data_1":
        loop.run_until_complete(MARGIN_Account_update_1(api_key, api_secret, AQ, margin_instrument_id))
    # margin_account_data
    if TT=="Margin_Account_data_2":
        loop.run_until_complete(MARGIN_Account_update_2(api_key, api_secret, AQ, margin_instrument_id))
    # margin_account_data
    if TT=="Margin_Account_data_3":
        loop.run_until_complete(MARGIN_Account_update_3(api_key, api_secret, AQ, margin_instrument_id))

    loop.close()
           
if __name__ == "__main__":   
    #币安帐密
    api_key = ''
    api_secret =''
    client = Client(api_key, api_secret)

    TradingPair = {'DOGEUSDT':600,'ALICEUSDT':600,'C98USDT':500}
    Customer_name=' little binance'
    margin_instrument_id_1='DOGEUSDT'
    margin_instrument_id_2='ALICEUSDT'
    margin_instrument_id_3='C98USDT'

    # 母帐户U本位,U的总资产
    spot_asset_results=client.get_account()
    time.sleep(0.5)
    spot_price_results=client.get_orderbook_ticker()
    time.sleep(0.5)
    future_account_assets_result=client.futures_account()
    time.sleep(0.5)
    total_spot_asset_price=0
    for i in spot_asset_results['balances']:
        if float(i['free']) != 0 or float(i['locked']) !=0:
            if i['asset'] != 'LDUSDT':
                symbol=i['asset'] +'USDT'
                for j in spot_price_results:
                    if j['symbol'] == symbol:
                        asset_price=(float(i['free'])+float(i['locked']))*(float(j['bidPrice'])+float(j['askPrice']))/2              
                        total_spot_asset_price=total_spot_asset_price+asset_price
        if i['asset'] == 'USDT':
            total_USDT_asset=float(i['free']) 
    for i in future_account_assets_result:
        future_total_balance=float(future_account_assets_result['totalCrossWalletBalance'])                    
    account_asset = total_spot_asset_price+future_total_balance+total_USDT_asset        
    timestamp = datetime.now().isoformat(" ", "seconds")
    total_balance_hour = account_asset


    MarketQ = {}
    for i in TradingPair:
        MarketQ[i] = {"DEPTH5_SWAP": "", "DEPTH5_SPOT":"", "HISTORY_FUNDING": "","FUNDING_RATE":"","P_INFO_SWAP":'',"P_INFO_SPOT":'','INDEX_TICKERS_SPOT':'','INDEX_TICKERS_SWAP':'',"INDEX_PRICE_SPOT":"","MARK_PRICE_SWAP":""}
        for j in MarketQ[i]:
            MarketQ[i][j] = Queue()

    AccountQ = {}
    for i in TradingPair:
        AccountQ[i] = {"POSITION_SWAP": "", "POSITION_SPOT": "","POSITION_MARGIN": "",'ORDERS_SWAP':"",'ORDERS_SPOT':"",'ORDERS_MARGIN':""}
        for j in AccountQ[i]:
            AccountQ[i][j] = Queue()
    
    Nowtime = datetime.now()
    record_minute = Nowtime.minute
    record_hour = Nowtime.hour

    # 先找instruments参数
    Necessary_info = {}
    for i in TradingPair:
        Necessary_info[i] = {}
        Necessary_info[i]['swap_instrument_id']=i  #币安不加 -SWAP
        Necessary_info[i]['spot_instrument_id']=i
        Necessary_info[i]['Total_money'] = TradingPair[i]

        #找swap的exchange info
        exchange_swap_info_result=client.futures_exchange_info()
        time.sleep(0.3)
        for j in exchange_swap_info_result['symbols']:
            if j['symbol']==Necessary_info[i]['swap_instrument_id']:
                Necessary_info[i]['swap_tick_size']=float(j['filters'][0]['tickSize'])
                Necessary_info[i]['swap_tick_digit']=np.log10(1/float(j['filters'][0]['tickSize']))
                Necessary_info[i]['notional']=float(j['filters'][5]['notional'])   #这是最小合约下单价格,5USD,不是一张合约等于多少,币安没有contract value
                Necessary_info[i]['swap_min_notional'] = float(j['filters'][5]['notional'])   #这里min_size写成 notional
                Necessary_info[i]['quantityPrecision'] =float(j['quantityPrecision'])

        #找spot的exchange info
        exchange_spot_info_result=client.get_exchange_info()
        time.sleep(0.3)
        for j in exchange_spot_info_result['symbols']:
            if j['symbol'] == Necessary_info[i]['spot_instrument_id']:  # need to notice here's insId (SWAP)
                Necessary_info[i]['spot_tick_size'] = float(j['filters'][0]['tickSize'])
                Necessary_info[i]['spot_tick_digit'] = np.log10(1 /float(j['filters'][0]['tickSize']))
                Necessary_info[i]['spot_min_notional'] = float(j['filters'][3]['minNotional'])
                Necessary_info[i]['spot_minQty'] = float(j['filters'][2]['minQty']) #spot下单到最小的位数
                Necessary_info[i]['spot_minQty_digit']=np.log10(1/float(j['filters'][2]['minQty']))

        # Average_Funding_Rate
      #  instId_fu, Necessary_info[i]['Average_Funding_Rate'] = funding_recalculate(client, i)   判断跟在主进程while true里的重复
        # 币安没有Predict_Funding_Rate,只有当下的funding_rate
        try:
            Necessary_info[i]['Present_Funding_Rate']=float(MarketQ[i]["FUNDING_RATE"].get(timeout=0.2))                    
        except:
            Necessary_info[i]['Present_Funding_Rate']=float(client.futures_mark_price(symbol=i)['lastFundingRate'])
    
    param_set_list = {'maker_commission_spot':0.001,
                      'taker_commission_spot':0.001,
                      'maker_commission_swap':0.0002,
                      "taker_commission_swap":0.0004,
                      'tolerate_limit':200,    #仓位歪掉的凭据
                      'order_limit':100,       #每次开仓量
                      'close_short_index' : 1   #平仓要几倍的出手量（相对于开仓）
                      }

    #'dotusdt@depth<5>@100ms', 'linkusdt@depth',<symbol>@markPrice
    stream_USD_Market=[]
    for h in TradingPair:
        swap_instrument_id = h.lower()  #币安要小写
        depth_stream=swap_instrument_id+'@depth5@100ms'
        stream_USD_Market.append(depth_stream)
        mark_price_stream=swap_instrument_id+'@markPrice'
        stream_USD_Market.append(mark_price_stream)

    stream_SPOT_Market=[]
    for h in TradingPair:
        spot_instrument_id = h.lower()  #币安要小写
        depth_stream=spot_instrument_id+'@depth5@100ms'
        stream_SPOT_Market.append(depth_stream)
    
    #account方面是自己产生listenkey,所以没有频道
    stream_USD_Account = []
    stream_SPOT_Account = []
    stream_Margin_Account = []
    #trade没有stream
    stream_trade=[]
    ws_USD_Market_data = Process( target=run_ws, args=(api_key, api_secret,"USD_M_data",MarketQ,AccountQ,stream_USD_Market,TradingPair,param_set_list))
    ws_SPOT_Market_data = Process( target=run_ws, args=(api_key, api_secret,"SPOT_data",MarketQ,AccountQ,stream_SPOT_Market,TradingPair,param_set_list))
    ws_USD_Account_data = Process( target=run_ws, args=(api_key, api_secret,"USD_M_Account_data",MarketQ,AccountQ,stream_USD_Account,TradingPair,param_set_list))
    #现货帐户ws用不到,所以关掉
    #ws_SPOT_Account_data = Process( target=run_ws, args=(api_key, api_secret,"SPOT_Account_data",MarketQ,AccountQ,stream_SPOT_Account,TradingPair,param_set_list))
    ws_trade = Process( target=run_ws, args=(api_key, api_secret,"trade",MarketQ,AccountQ,stream_trade,TradingPair,param_set_list))
    ws_Margin_Account_data_1 = Process( target=run_ws_margin, args=(api_key, api_secret,"Margin_Account_data_1",AccountQ,margin_instrument_id_1))
    ws_Margin_Account_data_2 = Process( target=run_ws_margin, args=(api_key, api_secret,"Margin_Account_data_2",AccountQ,margin_instrument_id_2))
    ws_Margin_Account_data_3 = Process( target=run_ws_margin, args=(api_key, api_secret,"Margin_Account_data_3",AccountQ,margin_instrument_id_3))

    ws_USD_Market_data.start()
    ws_SPOT_Market_data.start()
    ws_USD_Account_data.start()
    #ws_SPOT_Account_data.start()
    ws_Margin_Account_data_1.start()
    ws_Margin_Account_data_2.start()
    ws_Margin_Account_data_3.start()
    ws_trade.start()

    while True:
        try:     
            Nowtime = datetime.now()
            new_record_second = Nowtime.second
            new_record_minute = Nowtime.minute
            new_record_hour = Nowtime.hour
            Operation_info = {}
            for i in TradingPair:
                Operation_info[i] = {}
                Operation_info[i]['spot_bids_price5_forc'] = []
                Operation_info[i]['spot_asks_price5_forc'] = [] 
                                   
                try:
                    spot_depth5 = MarketQ[i]["DEPTH5_SPOT"].get(timeout=0.2)
                 #   for j in range(5):
                 #       Operation_info[i]['spot_bids_price5_forc'].append(float(spot_depth5['b'][j][0]))
                 #       Operation_info[i]['spot_asks_price5_forc'].append(float(spot_depth5['a'][j][0]))
                except:
                    spot_depth5 = client.get_order_book(symbol=h,limit='5')
                    print('我spot_depth5还是用restAPI___8')
                    time.sleep(0.1)
                for j in range(5):
                    Operation_info[i]['spot_bids_price5_forc'].append(float(spot_depth5['bids'][j][0]))
                    Operation_info[i]['spot_asks_price5_forc'].append(float(spot_depth5['asks'][j][0]))
                     #   print('h_',h)                        

                new_spot_ask=float(Operation_info[i]['spot_bids_price5_forc'][0])
                new_spot_bid=float(Operation_info[i]['spot_asks_price5_forc'][0])
                spot_present_price = precfloat((new_spot_ask + new_spot_bid)/2,Necessary_info[i]['swap_tick_digit'])

                #找spot_balance,swap_position                                                          
                #找spot_balance
          #      try:
          #          Operation_info[i]['spot_balance'] = float(AccountQ[i]["POSITION_SPOT"].get(timeout=0.2))                   
          #      except:
          #          total_balance_result=client.get_account()
          #          time.sleep(0.2)
          #          instr_fu=i.split(i[-4:])[0]
          #          for j in total_balance_result['balances']:
          #              if j['asset']==instr_fu:
          #                  Operation_info[i]['spot_balance'] = float(j['free'])
                
                # 找margin_balance
                try:
                    Operation_info[i]['margin_balance'] = float(AccountQ[i]["POSITION_MARGIN"].get(timeout=0.2))
                except:
                    margin_balance_result=client.get_isolated_margin_account()
                    asset_list=[]
                    for j in margin_balance_result['assets']:    
                        asset_list.append(j['baseAsset']['asset']+'USDT')
                    if i in asset_list:
                        for j in margin_balance_result['assets']: 
                            if j['baseAsset']['asset']==i.split(i[-4:])[0]:
                                Operation_info[i]['margin_balance']=float(j['baseAsset']['free'])                                        
                    else:
                        Operation_info[i]['margin_balance']=0 

                # 找swap_position
                try:
                    Operation_info[i]['swap_position'] = float(AccountQ[i]["POSITION_SWAP"].get(timeout=0.2)) #注意这里是持仓数目,不是notional                   
                except:
                    total_swap_position_result=client.futures_account()
                    time.sleep(0.2)
                    for j in total_swap_position_result['positions']:
                        if j['symbol']==i:
                            Operation_info[i]['swap_position']=float(j['positionAmt'])  #注意这里是持仓数目,不是notional          
                print('i_1',i)     
               # print('swap_position_2',Operation_info[i]['swap_position'])
               # print('spot_balance_2',Operation_info[i]['spot_balance'])
                compare_cal=Operation_info[i]['swap_position']+Operation_info[i]['margin_balance']   
                print('compare_cal',compare_cal*spot_present_price)
                #因为一正一负,所以这里用加的
                if Operation_info[i]['margin_balance'] >0:  #开正仓 
                    if compare_cal*spot_present_price>200:       
                        tutu=timestamp +','+i+'现货比永续多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3)    
                    elif compare_cal*spot_present_price<200:       
                        tutu=timestamp +','+i+'永续比现货多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3) 
                elif Operation_info[i]['margin_balance']<0:  #开负仓
                    if compare_cal*spot_present_price>200:       
                        tutu=timestamp +','+i+'永续比现货多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3)    
                    elif compare_cal*spot_present_price<200:       
                        tutu=timestamp +','+i+'现货比永续多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3) 

            if new_record_hour != record_hour:
                # 母帐户U本位,U的总资产
                spot_asset_results=client.get_account()
                time.sleep(0.5)
                spot_price_results=client.get_orderbook_ticker()
                time.sleep(0.5)
                future_account_assets_result=client.futures_account()
                time.sleep(0.5)
                total_spot_asset_price=0

                for i in spot_asset_results['balances']:
                    if float(i['free']) != 0 or float(i['locked']) !=0:
                        if i['asset'] != 'LDUSDT':
                            symbol=i['asset'] +'USDT'
                            for j in spot_price_results:
                                if j['symbol'] == symbol:
                                    asset_price=(float(i['free'])+float(i['locked']))*(float(j['bidPrice'])+float(j['askPrice']))/2              
                                    total_spot_asset_price=total_spot_asset_price+asset_price
                    if i['asset'] == 'USDT':
                        total_USDT_asset=float(i['free']) 

                for i in future_account_assets_result:
                    future_total_balance=float(future_account_assets_result['totalCrossWalletBalance'])                    
                account_asset = total_spot_asset_price+future_total_balance+total_USDT_asset        
                timestamp = datetime.now().isoformat(" ", "seconds")

                #换算成eth
                result = client.futures_orderbook_ticker(symbol='ETHUSDT')
                eth_price = (float(result['askPrice']) + float(result['bidPrice'])) / 2

                new_total_balance_hour = account_asset
                new_total_balance_hour_eth = account_asset/eth_price
                hour_profit = precfloat(new_total_balance_hour - total_balance_hour, 2)
                hour_profit_eth = hour_profit/eth_price
                hour_profit_percent = precfloat(hour_profit / total_balance_hour * 100, 2)

                tutu = timestamp + ",目前总资产为" + str(precfloat(new_total_balance_hour,2)) + 'USDT,等于'+str(precfloat(new_total_balance_hour_eth,2))+'个eth'
                sendmessage(tutu)
                tutu = timestamp + ",每小时获利为" + str(precfloat(hour_profit,2)) + 'USDT,等于'+str(precfloat(hour_profit_eth,2))+'个eth'
                sendmessage(tutu)
                tutu = timestamp + ",每小时获利率为" + str(precfloat(hour_profit_percent,2)) + '%'
                sendmessage(tutu)
                total_balance_hour = new_total_balance_hour

            if new_record_hour == 9 :
                if new_record_minute == 2:
                    if new_record_second < 15:
                        new_total_balance_day = account_asset
                        day_profit = precfloat(new_total_balance_day - total_balance_day, 2)
                        day_profit_eth = day_profit/eth_price
                        day_profit_percent = precfloat(day_profit / total_balance_day * 100, 2)

                        tutu = timestamp + ",每日获利为" + str(precfloat(day_profit,2)) + 'USDT,等于'+str(precfloat(day_profit_eth,2))+'个eth'
                        sendmessage(tutu)

                        tutu = timestamp + ",每日获利率为" + str(precfloat(day_profit_percent,2)) + '%'
                        sendmessage(tutu)

                        mapping = {}
                        kk = []
                        path = '/root/' + Customer_name + '_profit_report.csv'
                        key_list = ['timestamp', '单日获利（usdt）', '单日获利率（%）', '总资产净值（usdt）']
                        for key, value in zip(key_list, [timestamp, day_profit, day_profit_percent, new_total_balance_day]):
                            mapping[key] = value
                        kk.append(eval(json.dumps(mapping)))
                        kkk = pd.DataFrame(kk)
            #            kkk.to_csv(path, mode='a+', header=True, index=False)
                        total_balance_day = new_total_balance_day
                        time.sleep(16)

       
            #每八小时计算一次历史资金费率
            if Nowtime.hour == 8 or Nowtime.hour == 16 or Nowtime.hour == 0:
                if Nowtime.minute == 2:
                    if Nowtime.second < 15:
                        for i in TradingPair:
                            instId_fu, Average_Funding_Rate = funding_recalculate(client,i)
                            Necessary_info[i]['Average_Funding_Rate'] =Average_Funding_Rate
                            instr_fu = instId_fu.split('-')
                            TP = instr_fu[0] + "-" + instr_fu[1]
                            if len(instr_fu) == 3:  # only swap gets funding rate, so 3
                                if MarketQ[TP]["HISTORY_FUNDING"].empty() == True:
                                    MarketQ[TP]["HISTORY_FUNDING"].put(Average_Funding_Rate)
                                elif MarketQ[TP]["HISTORY_FUNDING"].empty() == False:
                                    MarketQ[TP]["HISTORY_FUNDING"].get()
                                    MarketQ[TP]["HISTORY_FUNDING"].put(Average_Funding_Rate)
                        time.sleep(16)
            #划转资金检查
            if new_record_minute != record_minute :              
                position_results=client.futures_position_information()
                time.sleep(0.1)
                for i in position_results:
                    if float(i['positionAmt']) != 0:
                        if float(i['markPrice'])> float(i['liquidationPrice']):
                            #卖空合约
                            if abs(float(i['markPrice'])-float(i['liquidationPrice']))/float(i['liquidationPrice'])<0.2:
                                #从币币全仓转到合约帐户:                                
                                spot_to_umfuture_transfer=client.make_universal_transfer(type="MAIN_UMFUTURE",asset='USDT',amount='50')                                                                                 
                    #        elif abs(float(i['markPrice'])-float(i['liquidationPrice']))/float(i['liquidationPrice'])>0.5:
                                #从合约帐户转到spot帐户
                    #            umfuture_to_spot_transfer=client.make_universal_transfer(type="UMFUTURE_MAIN",asset='USDT',amount='50')             
                        elif float(i['markPrice'])<float(i['liquidationPrice']): #买多合约
                            if abs(float(i['markPrice'])-float(i['liquidationPrice']))/float(i['markPrice'])<0.2:
                                #从币币全仓转到合约帐户
                                spot_to_umfuture_transfer=client.make_universal_transfer(type="MAIN_UMFUTURE",asset='USDT',amount='50')                                                                                 
                      #      elif abs(float(i['markPrice'])-float(i['liquidationPrice']))/float(i['markPrice'])>0.5:
                                #从合约帐户转到spot帐户
                      #          umfuture_to_spot_transfer=client.make_universal_transfer(type="UMFUTURE_MAIN",asset='USDT',amount='50')  
                                    
                 #避免u本位馀额不足
                #print('here')               
                future_account_assets_result=client.futures_account()
                wallet_available_balance=float(future_account_assets_result['availableBalance'])   
                #print('wallet_available_balance_1',wallet_available_balance)
                time.sleep(0.1)
                if wallet_available_balance<150:
                    delta_transfer_amount = 200-wallet_available_balance
                    #从币币全仓转到合约帐户:
                    spot_to_umfuture_transfer=client.make_universal_transfer(type="MAIN_UMFUTURE",asset='USDT',amount= delta_transfer_amount)   
                  #  print('spot_to_umfuture_transfer_result',spot_to_umfuture_transfer)
                elif wallet_available_balance>400:
                     delta_transfer_amount = wallet_available_balance - 300
                     umfuture_to_spot_transfer=client.make_universal_transfer(type="UMFUTURE_MAIN ",asset='USDT',amount=delta_transfer_amount)

                isolated_margin_results=client.get_isolated_margin_account()
                time.sleep(0.1)
                for h in TradingPair:
                    for i in isolated_margin_results['assets']:
                        if i['baseAsset']['asset']==h.split(h[-4:])[0]:
                            margin_level=float(i['marginLevel'])
                            if margin_level<2.5: 
                                usdt_transfer_result=client.transfer_spot_to_isolated_margin(asset='USDT',symbol=h,amount='50')
                               # usdt_transfer_result=client.make_universal_transfer(type="MARGIN_ISOLATEDMARGIN",asset='USDT',amount= 200, toSymbol=h) 

                new_record_minute = record_minute

        except Exception as e:
            print(e)
            pass

 #   ws_USD_Market_data.join()
 #   ws_SPOT_Market_data.join()
 #   ws_USD_Account_data.join()
 #   ws_SPOT_Account_data.join()
 #   ws_Margin_Account_data.join()
 #   ws_trade.join()

