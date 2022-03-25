import okex.Public_api as Public
import okex.Trade_api as Trade
import okex.Account_api as Account
import okex.Market_api as Market
import okex.Funding_api as Funding
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
    if float(spot_size) !=0:
        A = tradeAPI.place_order(instId=spot_instrument_id , tdMode='cross', side='buy', ordType=spot_order_type, sz=spot_size, px=spot_price)
    else:
        A='none'
    # swap下单
    if float(swap_size) != 0:
        # 应该是因为买卖模式的关系,没有posSide的设定
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
    if float(spot_size)!=0:
        A = tradeAPI.place_order(instId=spot_instrument_id , tdMode='cross', side='sell', ordType=spot_order_type, sz=spot_size, px=spot_price)    #这里先没写posSide
    else:
        A='none'
    # swap下单
    if float(swap_size) != 0:
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
        A = tradeAPI.place_order(instId=spot_instrument_id, tdMode='cross', side='sell', ordType=spot_order_type, sz=spot_close_size, px=spot_close_price)
    else:
        A = 'none'
    # swap下单
    if float(swap_close_size) != 0:
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
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
    # spot下单
    if float(spot_close_size) != 0:
        A = tradeAPI.place_order(instId=spot_instrument_id, tdMode='cross', side='buy', ordType=spot_order_type, sz=spot_close_size, px=spot_close_price)
    else:
        A = 'none'
    # swap下单
    if float(swap_close_size) != 0:
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_size, px=swap_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='sell', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
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
        B = tradeAPI.place_order(instId=swap_instrument_id, tdMode='cross', side='buy', ordType=swap_order_type, sz=swap_close_size, px=swap_close_price)
    else:
        B = 'none'
    return B

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

def funding_recalculate(swap_instrument_id):
    A=publicAPI.get_funding_rate(swap_instrument_id)['data']
    time.sleep(0.3)
    #print(past_funding_rate)
  #  day3Afundrate=[]
    instId_fu = None
  #  for i in past_funding_rate:
  #      day3Afundrate.append(precfloat(i['realizedRate'], 6))
  #      instId_fu = i['instId']
  #  Afundrate = np.array(day3Afundrate)
    for i in A:
        instId_fu = i['instId']
        present_funding_rate = float(i['fundingRate'])
        predict_funding_rate = float(i['nextFundingRate'])

    return instId_fu, present_funding_rate, predict_funding_rate

def make_So_Sc(mode_So_Sc, *args, **kwargs):
    maker_commission_spot = mode_So_Sc['maker_commission_spot']
    maker_commission_swap = mode_So_Sc['maker_commission_swap']
    taker_commission_swap = mode_So_Sc['taker_commission_swap']
    taker_commission_spot = mode_So_Sc['taker_commission_spot']
    swap_present_price = mode_So_Sc['swap_present_price']
    spot_index_price = mode_So_Sc['spot_index_price']    #用现货指数

    Cm_swap = maker_commission_spot * swap_present_price
    Cm_spot = maker_commission_swap * swap_present_price
    Ct_swap = taker_commission_swap * spot_index_price
    Ct_spot = taker_commission_spot * spot_index_price

    if swap_present_price > spot_index_price:  #可以开正仓
        So=((swap_present_price-spot_index_price-Cm_swap-Cm_spot)+(swap_present_price-spot_index_price-Cm_swap-Cm_spot)) /2       #So是正数
        Sc=((spot_index_price-swap_present_price-Cm_swap-Cm_spot)+(spot_index_price-swap_present_price-Cm_swap-Cm_spot)) /2     #Sc没必要
    elif spot_index_price>swap_present_price:  #可以开负仓
        So=((swap_present_price-spot_index_price-Cm_swap-Cm_spot)+(swap_present_price-spot_index_price-Cm_swap-Cm_spot)) /2  # So没必要
        Sc=((spot_index_price-swap_present_price-Cm_swap-Cm_spot)+(spot_index_price-swap_present_price-Cm_swap-Cm_spot)) /2  # Sc是正数
    elif swap_present_price == spot_index_price:
        So=0
        Sc=0
    return So, Sc

def open_long_judge(mode_open_long_judge, *args, **kwargs):
    Target_Amount = mode_open_long_judge['Target_Amount']
    spot_balance = mode_open_long_judge['spot_balance']
    swap_position = mode_open_long_judge['swap_position']
    swap_size = mode_open_long_judge['swap_size']
    spot_size = mode_open_long_judge['spot_size']
    contract_val = mode_open_long_judge['contract_value']
    # spot_balance>0, swap_position是short且<0
    open_long_mode = 'off'
    open_long_final_open_mode = 'off'
    open_long_final_close_mode = 'off'
    if Target_Amount - spot_balance > 0 and abs(Target_Amount - spot_balance) > float(spot_size):  # 还大于一个 spot_size,继续建仓
        open_long_mode = 'on'
    elif Target_Amount - spot_balance > 0 and abs(Target_Amount - spot_balance) < float(spot_size):  # spot已经接近Target_Amount,小于一个spot_size,spot不动,看swap
        if spot_balance + swap_position * contract_val > float(swap_size) * contract_val:       #因为posSide是net,所以持仓本身带著正负
            open_long_final_open_mode = 'on'  # swap要建仓 开空
        elif -1*swap_position * contract_val - spot_balance > float(swap_size) * contract_val:
            open_long_final_close_mode = 'on'  # swap要平仓  平空
    return open_long_mode, open_long_final_open_mode, open_long_final_close_mode

def open_short_judge(mode_open_short_judge, *args, **kwargs):
    Target_Amount = mode_open_short_judge['Target_Amount']*-1  #因为是负开仓,所以*-1
    spot_balance = mode_open_short_judge['spot_balance']
    swap_position = mode_open_short_judge['swap_position']
    swap_size = mode_open_short_judge['swap_size']
    spot_size = mode_open_short_judge['spot_size']
    contract_val=mode_open_short_judge['contract_value']

    open_short_mode = 'off'
    open_short_final_open_mode = 'off'
    open_short_final_close_mode = 'off'
    #这边TA是<0,spot_balance<0, swap_position是long且>0
    if Target_Amount - spot_balance < 0 and abs(Target_Amount - spot_balance) > float(spot_size):  # 还大于一个spot_size,继续建仓
        open_short_mode = 'on'
    elif Target_Amount - spot_balance < 0 and abs(Target_Amount - spot_balance) < float(spot_size):  # spot已经接近Target_Amount,小于一个spot_size,spot不动,看swap
        # swap建仓比spot少,swap要建仓 开多
        if  spot_balance + swap_position * contract_val <0 and  abs(spot_balance + swap_position * contract_val) > float(swap_size) * contract_val:
            open_short_final_open_mode = 'on'

        #swap建仓比spot多,swap要平仓 平多
        elif spot_balance + swap_position * contract_val >0 and abs(swap_position * contract_val + spot_balance) > float(swap_size) * contract_val:
            open_short_final_close_mode = 'on'
    return open_short_mode, open_short_final_open_mode, open_short_final_close_mode

def close_long_judge(mode_close_long_judge, *args, **kwargs):
    Target_Amount_Close = mode_close_long_judge['Target_Amount_Close']
    spot_balance = mode_close_long_judge['spot_balance']
    swap_position = mode_close_long_judge['swap_position']
    swap_close_size = mode_close_long_judge['swap_close_size']
    spot_close_size = mode_close_long_judge['spot_close_size']
    Predict_Funding_Rate = mode_close_long_judge['predict_funding']
    Present_Funding_Rate = mode_close_long_judge['present_funding']
    So_Sc_mode = mode_close_long_judge['So_Sc_mode']
    contract_val= mode_close_long_judge['contract_value']


    close_long_mode = 'off'
    close_long_final_open_mode = 'off'
    close_long_final_close_mode = 'off'
    #这里Target_Amount_Close=0,swap是short,<0

    if spot_balance - Target_Amount_Close > 0 and abs(spot_balance - Target_Amount_Close) > float(spot_close_size):  # spot超过TA,要平仓
       # if So_Sc_mode == 'on':         #等有看So_Sc_mode再看资金费率的影响
       #     if Predict_Funding_Rate * Present_Funding_Rate >0:
       #         close_long_mode = 'on'
       #     else:
       #         close_long_mode = 'off'
       # elif So_Sc_mode =='off':     #此时有可能是持仓差过大或大涨需要仓位调整
        close_long_mode = 'on'
    elif spot_balance - Target_Amount_Close > 0 and abs(spot_balance - Target_Amount_Close) < float(spot_close_size):
        if -1*swap_position * contract_val - spot_balance > float(swap_close_size) * contract_val:  # swap多于spot,平空
            close_long_final_close_mode = 'on'
        elif spot_balance + swap_position * contract_val > float(swap_close_size) * contract_val:  # swap少于spot,开空
            close_long_final_open_mode = 'on'
    return close_long_mode, close_long_final_open_mode, close_long_final_close_mode

def close_short_judge(mode_close_short_judge, *args, **kwargs):
    Target_Amount_Close = mode_close_short_judge['Target_Amount_Close']
    spot_balance = mode_close_short_judge['spot_balance']
    swap_position = mode_close_short_judge['swap_position']
    swap_close_size = mode_close_short_judge['swap_close_size']
    spot_close_size = mode_close_short_judge['spot_close_size']
    Predict_Funding_Rate = mode_close_short_judge['predict_funding']
    Present_Funding_Rate = mode_close_short_judge['present_funding']
    So_Sc_mode = mode_close_short_judge['So_Sc_mode']
    contract_val = mode_close_short_judge['contract_value']

    close_short_mode = 'off'
    close_short_final_open_mode = 'off'
    close_short_final_close_mode = 'off'
    #这里spot_balance<0,Target_Amount_close=0
    if spot_balance - Target_Amount_Close < 0 and abs(spot_balance - Target_Amount_Close) > float(spot_close_size):  # spot超过TA,要平仓
       # if So_Sc_mode == 'on':         #等有看So_Sc_mode再看资金费率的影响
       #     if Predict_Funding_Rate * Present_Funding_Rate >0:
       #         close_short_mode = 'on'
       #     else:
       #         close_short_mode = 'off'
       # elif So_Sc_mode =='off':     #此时有可能是持仓差过大或大涨需要仓位调整
        close_short_mode = 'on'
    elif spot_balance - Target_Amount_Close < 0 and abs(spot_balance - Target_Amount_Close) < float(spot_close_size):
        #swap仓位比spot多
        if (swap_position * contract_val + spot_balance )>0 and abs(swap_position * contract_val + spot_balance) > float(swap_close_size) * contract_val:  # swap多于spot,平多
            close_short_final_close_mode = 'on'
        #spot仓位比swap多
        elif (spot_balance + swap_position * contract_val)<0 and abs(spot_balance + swap_position * contract_val) > float(swap_close_size) * contract_val:  # swap少于spot,开多
            close_short_final_open_mode = 'on'
    return close_short_mode, close_short_final_open_mode, close_short_final_close_mode


def get_timestamp():
    now = datetime.now()
    t = now.isoformat("T", "milliseconds")
    return t + "Z"

def precfloat(num,digi):
    return int(round(float(num)*math.pow(10,digi)))/math.pow(10,digi)

def get_server_time():
    url = "https://www.okex.com/api/v5/public/time"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['data'][0]['ts']
    else:
        return ""

def get_local_timestamp():
    return int(time.time())

def login_params(timestamp, api_key, passphrase, secret_key):
    message = timestamp + 'GET' + '/users/self/verify'

    mac = hmac.new(bytes(secret_key, encoding='utf8'), bytes(message, encoding='utf-8'), digestmod='sha256')
    d = mac.digest()
    sign = base64.b64encode(d)

    login_param = {"op": "login", "args": [{"apiKey": api_key,
                                            "passphrase": passphrase,
                                            "timestamp": timestamp,
                                            "sign": sign.decode("utf-8")}]}
    login_str = json.dumps(login_param)
    return login_str

def partial(res):
    data_obj = res['data'][0]
    bids = data_obj['bids']
    asks = data_obj['asks']
    instrument_id = res['arg']['instId']
    # print('全量数据bids为：' + str(bids))
    # print('档数为：' + str(len(bids)))
    # print('全量数据asks为：' + str(asks))
    # print('档数为：' + str(len(asks)))
    return bids, asks, instrument_id

def update_bids(res, bids_p):
    # 获取增量bids数据
    bids_u = res['data'][0]['bids']
    # print('增量数据bids为：' + str(bids_u))
    # print('档数为：' + str(len(bids_u)))
    # bids合并
    for i in bids_u:
        bid_price = i[0]
        for j in bids_p:
            if bid_price == j[0]:
                if i[1] == '0':
                    bids_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                bids_p.append(i)
    else:
        bids_p.sort(key=lambda price: sort_num(price[0]), reverse=True)
        # print('合并后的bids为：' + str(bids_p) + '，档数为：' + str(len(bids_p)))
    return bids_p

def update_asks(res, asks_p):
    # 获取增量asks数据
    asks_u = res['data'][0]['asks']
    # print('增量数据asks为：' + str(asks_u))
    # print('档数为：' + str(len(asks_u)))
    # asks合并
    for i in asks_u:
        ask_price = i[0]
        for j in asks_p:
            if ask_price == j[0]:
                if i[1] == '0':
                    asks_p.remove(j)
                    break
                else:
                    del j[1]
                    j.insert(1, i[1])
                    break
        else:
            if i[1] != "0":
                asks_p.append(i)
    else:
        asks_p.sort(key=lambda price: sort_num(price[0]))
        # print('合并后的asks为：' + str(asks_p) + '，档数为：' + str(len(asks_p)))
    return asks_p

def sort_num(n):
    if n.isdigit():
        return int(n)
    else:
        return float(n)

def check(bids, asks):
    # 获取bid档str
    bids_l = []
    bid_l = []
    count_bid = 1
    while count_bid <= 25:
        if count_bid > len(bids):
            break
        bids_l.append(bids[count_bid-1])
        count_bid += 1
    for j in bids_l:
        str_bid = ':'.join(j[0 : 2])
        bid_l.append(str_bid)
    # 获取ask档str
    asks_l = []
    ask_l = []
    count_ask = 1
    while count_ask <= 25:
        if count_ask > len(asks):
            break
        asks_l.append(asks[count_ask-1])
        count_ask += 1
    for k in asks_l:
        str_ask = ':'.join(k[0 : 2])
        ask_l.append(str_ask)
    # 拼接str
    num = ''
    if len(bid_l) == len(ask_l):
        for m in range(len(bid_l)):
            num += bid_l[m] + ':' + ask_l[m] + ':'
    elif len(bid_l) > len(ask_l):
        # bid档比ask档多
        for n in range(len(ask_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(ask_l), len(bid_l)):
            num += bid_l[l] + ':'
    elif len(bid_l) < len(ask_l):
        # ask档比bid档多
        for n in range(len(bid_l)):
            num += bid_l[n] + ':' + ask_l[n] + ':'
        for l in range(len(bid_l), len(ask_l)):
            num += ask_l[l] + ':'

    new_num = num[:-1]
    int_checksum = zlib.crc32(new_num.encode())
    fina = change(int_checksum)
    return fina

def change(num_old):
    num = pow(2, 31) - 1
    if num_old > num:
        out = num_old - num * 2 - 2
    else:
        out = num_old
    return out

# subscribe channels un_need login
async def subscribe_without_login(url, channels,MQ,TradingPair):
    l = []
    while True:
        try:
            async with websockets.connect(url) as ws:
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)
                await ws.send(sub_str)
               # print(f"send: {sub_str}")

                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            #print(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连……")
                            break

                  #  print(get_timestamp() + res)
                    res = eval(res)
                    if 'event' in res:
                        continue
                    #print('market_res',res)
                    if res['arg']['channel'] == 'books5':
                        instr = res["arg"]["instId"].split("-")
                        TP = instr[0] + "-" + instr[1]
                        if len(instr) == 2:
                            if MQ[TP]["DEPTH5_SPOT"].empty() == True:
                                MQ[TP]["DEPTH5_SPOT"].put(res["data"])
                            elif MQ[TP]["DEPTH5_SPOT"].empty() == False:
                                MQ[TP]["DEPTH5_SPOT"].get()
                                MQ[TP]["DEPTH5_SPOT"].put(res["data"])

                        elif len(instr) == 3:
                            if MQ[TP]["DEPTH5_SWAP"].empty() == True:
                                MQ[TP]["DEPTH5_SWAP"].put(res["data"])
                            elif MQ[TP]["DEPTH5_SWAP"].empty() == False:
                                MQ[TP]["DEPTH5_SWAP"].get()
                                MQ[TP]["DEPTH5_SWAP"].put(res["data"])

                    elif res['arg']['channel'] == 'index-tickers':
                        #spot的指标tickers
                        instr = res["arg"]["instId"].split("-")
                        TP = instr[0] + "-" + instr[1]
                        if len(instr) == 2:
                            if MQ[TP]["INDEX_TICKERS_SPOT"].empty() == True:
                                MQ[TP]["INDEX_TICKERS_SPOT"].put(res["data"])
                            elif MQ[TP]["INDEX_TICKERS_SPOT"].empty() == False:
                                MQ[TP]["INDEX_TICKERS_SPOT"].get()
                                MQ[TP]["INDEX_TICKERS_SPOT"].put(res["data"])
                    
                    elif res['arg']['channel'] == 'mark-price':
                        instr = res["arg"]["instId"].split("-")
                        TP = instr[0] + "-" + instr[1]
                        if len(instr) == 3:
                            if MQ[TP]["MARK_PRICE_SWAP"].empty() == True:
                                MQ[TP]["MARK_PRICE_SWAP"].put(res["data"])
                            elif MQ[TP]["MARK_PRICE_SWAP"].empty() == False:
                                MQ[TP]["MARK_PRICE_SWAP"].get()
                                MQ[TP]["MARK_PRICE_SWAP"].put(res["data"])

                    elif res['arg']['channel'] == 'funding-rate':   #只有swap才有
                        instr = res["arg"]["instId"].split("-")
                        TP = instr[0] + "-" + instr[1]
                        if MQ[TP]["PREDICT_FUNDING"].empty() == True:
                            MQ[TP]["PREDICT_FUNDING"].put(res["data"])
                        elif MQ[TP]["PREDICT_FUNDING"].empty() == False:
                            MQ[TP]["PREDICT_FUNDING"].get()
                            MQ[TP]["PREDICT_FUNDING"].put(res["data"])

        except Exception as e:
            #print("disconneted，connecting MQ……")
            # error存成csv
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

# subscribe channels need login
async def subscribe(url, api_key, passphrase, secret_key, channels,AQ, TradingPair):
    while True:
        try:
            async with websockets.connect(url) as ws:
                # login
                timestamp = str(get_local_timestamp())
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                #print(f"send: {login_str}")
                res = await ws.recv()
               # print(res)
                # subscribe
                sub_param = {"op": "subscribe", "args": channels}
                sub_str = json.dumps(sub_param)

                await ws.send(sub_str)
                #print(f"send: {sub_str}")
                while True:
                    try:
                        res = await asyncio.wait_for(ws.recv(), timeout=25)
                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            #print(res)
                            continue
                        except Exception as e:
                            print("连接关闭，正在重连AQ_login……")
                            break
                    #get account's info
                    res = eval(res)
                    if 'event' in res:
                        continue
                    if res['arg']['channel'] == 'account':  #spot的balance查询
                        res_spot_ccy_list=[]
                        for i in TradingPair:           #从TP中做出做res_spot_ccy_list
                            TP=i.split('-')[0]
                            res_spot_ccy_list.append(TP)

                        stream_spot_list=[]     #从res做res_spot_ccy_list
                        if len(res['data']) != 0:
                            for i in res['data'][0]['details']:
                                if i['ccy'] != 'USDT':
                                    stream_spot_list.append(i['ccy'])                                 
                                
                        #找出tp中跟res中有重合的pair
                        if len(stream_spot_list)!=0:
                            for j in res_spot_ccy_list:
                                if j in stream_spot_list:
                                    for k in res['data'][0]['details']:
                                        if k['ccy']==j:
                                            TP=k['ccy']+'-USDT'  #記得要是okex的格式
                                            if AQ[TP]["POSITION_SPOT"].empty() == True:
                                                AQ[TP]["POSITION_SPOT"].put(k)
                                            elif AQ[TP]["POSITION_SPOT"].empty() == False:
                                                AQ[TP]["POSITION_SPOT"].get()
                                                AQ[TP]["POSITION_SPOT"].put(k)
                                else:
                                    TP=j+'-USDT'
                                    kk = {'availBal': '0', 'availEq': '0', 'cashBal': '0', 'ccy': j, 'crossLiab': '0', 'disEq': '0', 'eq': '0', 'frozenBal': '0',
                                                                'interest': '0', 'isoEq': '0', 'isoLiab': '0', 'liab': '0', 'mgnRatio': '0', 'ordFrozen': '0', 'twap': '0', 'uTime': '0', 'upl': '0'}
                                    if AQ[TP]["POSITION_SPOT"].empty() == True:
                                        AQ[TP]["POSITION_SPOT"].put(kk)
                                    elif AQ[TP]["POSITION_SPOT"].empty() == False:
                                        AQ[TP]["POSITION_SPOT"].get()
                                        AQ[TP]["POSITION_SPOT"].put(kk)
                                        
                    #print('res_here',res)
                    if res['arg']['channel'] == 'positions':
                        if res['arg']['instType'] == 'SWAP':
                            
                            res_swap_ccy_list=[]
                            for i in TradingPair:           #从TP中做出做res_spot_ccy_list
                                res_swap_ccy_list.append(i)

                            stream_swap_list=[]     #从res做res_spot_ccy_list
                            if len(res['data']) != 0:
                                for i in res['data']:
                                    a=i['instId'].split('-SWAP')[0] 
                                    stream_swap_list.append(a) 
                      
                            
                            if len(res_swap_ccy_list) !=0:                  #出现了一开始建仓ws只送[]的情况,所以改成了 res_swap_ccy_list      
                                for j in res_swap_ccy_list:
                                    if j in stream_swap_list:
                                        for k in res['data']:
                                            if k['instId']==j+'-SWAP':
                                                TP=j
                                                if AQ[TP]["POSITION_SWAP"].empty() == True:
                                                    AQ[TP]["POSITION_SWAP"].put(k)
                                                elif AQ[TP]["POSITION_SWAP"].empty() == False:
                                                    AQ[TP]["POSITION_SWAP"].get()
                                                    AQ[TP]["POSITION_SWAP"].put(k)
                                    else:
                                        TP=j
                                        kk = {'pos': '0', 'availPos': '0', 'cashBal': '0', 'instId': j + '-SWAP','instType': 'SWAP'}       
                                        if AQ[TP]["POSITION_SWAP"].empty() == True:
                                            AQ[TP]["POSITION_SWAP"].put(kk)
                                        elif AQ[TP]["POSITION_SWAP"].empty() == False:
                                            AQ[TP]["POSITION_SWAP"].get()
                                            AQ[TP]["POSITION_SWAP"].put(kk)  
                                            

                        if res['arg']['instType'] == 'MARGIN':          #跨币种不用到这个,全部都会放在spot_balance
                           # print('margin_res',res)
                            res_margin_instId_list=[]
                            for i in TradingPair:           #从TP中做出做res_margin_instId_list
                                TP=i.split('-')[0]
                                res_margin_instId_list.append(TP)

                            stream_margin_list=[]     #从res做res_margin_instId_list
                            if len(res['data']) != 0:
                                for i in res['data'][0]['details']:
                                    if i['ccy'] != 'USDT':
                                        stream_margin_list.append(i['ccy'])                                 
                                    
                            #找出tp中跟res中有重合的pair
                            if len(stream_margin_list) !=0:
                                for j in res_margin_instId_list:
                                    if j in stream_margin_list:
                                        for k in res['data'][0]['details']:
                                            if k['ccy']==j:
                                                TP=k['ccy']+'-USDT'  #記得要是okex的格式
                                                if AQ[TP]["POSITION_MARGIN"].empty() == True:
                                                    AQ[TP]["POSITION_MARGIN"].put(k)
                                                elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                                                    AQ[TP]["POSITION_MARGIN"].get()
                                                    AQ[TP]["POSITION_MARGIN"].put(k)
                                    else:
                                        TP=j+'-USDT'
                                        kk = {'availBal': '0', 'availEq': '0', 'cashBal': '0', 'ccy': j, 'crossLiab': '0', 'disEq': '0', 'eq': '0', 'frozenBal': '0',
                                                                    'interest': '0', 'isoEq': '0', 'isoLiab': '0', 'liab': '0', 'mgnRatio': '0', 'ordFrozen': '0', 'twap': '0', 'uTime': '0', 'upl': '0'}
                                        if AQ[TP]["POSITION_MARGIN"].empty() == True:
                                            AQ[TP]["POSITION_MARGIN"].put(kk)
                                        elif AQ[TP]["POSITION_MARGIN"].empty() == False:
                                            AQ[TP]["POSITION_MARGIN"].get()
                                            AQ[TP]["POSITION_MARGIN"].put(kk)                                    
                                                                

                    if res['arg']['channel'] == 'orders':
                        #存spot的未完成订单
                        #print('orders_res', res)
                        for j in TradingPair:

                            if len(res['data']) !=0:
                                for k in res['data']:

                                    instr_fu = k['instId'].split('-')
                                    if res['data'][0]['instType'] == 'SPOT':    #spot

                                        if instr_fu[0] + "-" + instr_fu[1] == j:
                                            TP = instr_fu[0] + "-" + instr_fu[1]
                              #              if k['state'] != 'canceled' or k['state'] != 'filled':
                                            if AQ[TP]["ORDERS_SPOT"].empty() == True:
                                                AQ[TP]["ORDERS_SPOT"].put(k)
                                            elif AQ[TP]["ORDERS_SPOT"].empty() == False:
                                                AQ[TP]["ORDERS_SPOT"].get()
                                                AQ[TP]["ORDERS_SPOT"].put(k)

                                    if res['data'][0]['instType'] == 'SWAP':  # swap
                                        if instr_fu[0] + "-" + instr_fu[1] == j:
                                            TP = instr_fu[0] + "-" + instr_fu[1]

                                            if AQ[TP]["ORDERS_SWAP"].empty() == True:
                                                AQ[TP]["ORDERS_SWAP"].put(k)

                                            elif AQ[TP]["ORDERS_SWAP"].empty() == False:
                                                AQ[TP]["ORDERS_SWAP"].get()
                                                AQ[TP]["ORDERS_SWAP"].put(k)

        except Exception as e:
            print(e)
            print("disconnected，connecting AQ……")
            # error存成csv
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

# trade
async def trade(url, api_key, passphrase, secret_key, MQ,AQ,TradingPair,param_set_list):
    while True:
        try:
           # print('started')

            async with websockets.connect(url) as ws:
                # login
                timestamp = str(get_local_timestamp())
                login_str = login_params(timestamp, api_key, passphrase, secret_key)
                await ws.send(login_str)
                #print(f"send: {login_str}")
                res = await ws.recv()
                #print('res_trade',res)

                # 全部撤掉遗留单,這裡有restAPI,先試用
                beginning_pending_order_result = tradeAPI.get_order_list()
                order_id_list = []
                if beginning_pending_order_result['data'] != 0:
                    for i in beginning_pending_order_result['data']:
                        uncomplete_orderId = i['ordId']
                        uncomplete_order_instId = i['instId']
                        key_list = []
                        value_list = []
                        key_list = ["instId", "ordId"]
                        value_list = [uncomplete_order_instId, uncomplete_orderId]
                        dictionary_uncomplete_order = dict(zip(key_list, value_list))
                        order_id_list.append(dictionary_uncomplete_order)
                if len(order_id_list) != 0:
                    revoke_result = tradeAPI.cancel_multiple_orders(order_id_list)
                    time.sleep(0.1)

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
                print('TradingPair',TradingPair)
                for i in TradingPair:
                    Necessary_info[i] = {}
                    Operation_info[i] = {}

                for i in TradingPair:
                    Necessary_info[i]['swap_instrument_id']=i+'-SWAP'
                    Necessary_info[i]['spot_instrument_id']=i
                    Necessary_info[i]['Total_money'] = TradingPair[i]

                    swap_info = publicAPI.get_instruments('SWAP')['data']
                    time.sleep(0.1)
                    # print(swap_info)
                    for j in swap_info:
                        if j['instId'] == i + '-SWAP':  # need to notice here's insId (SWAP)
                            Necessary_info[i]['swap_tick_size'] = float(j["tickSz"])
                            Necessary_info[i]['swap_tick_digit'] = np.log10(1 / float(j["tickSz"]))
                            Necessary_info[i]['contract_val'] = float(j['ctVal'])
                            Necessary_info[i]['swap_min_size'] = float(j['minSz'])
                                                
                    spot_info = publicAPI.get_instruments('SPOT')['data']
                    time.sleep(0.1)
                    for j in spot_info:
                        if j['instId'] == i :  # need to notice here's insId (SWAP)
                            Necessary_info[i]['spot_tick_size'] = float(j["tickSz"])
                            Necessary_info[i]['spot_tick_digit'] = np.log10(1 / float(j["tickSz"]))
                            Necessary_info[i]['spot_min_size'] = float(j['minSz'])
               
                    # Funding_Rate
                    instId_fu, Necessary_info[i]['Present_Funding_Rate'],Necessary_info[i]['Predict_Funding_Rate'] = funding_recalculate(i + '-SWAP')
                    
                    Operation_info[i]['spot_bids_price5']=[]
                    Operation_info[i]['spot_asks_price5']=[]
                                                            
         #           try:
         #               spot_depth5 = MQ[i]["DEPTH5_SPOT"].get(timeout=1)
                      #  print('我spot_depth5用ws___2')
          #          except:
          #              spot_depth5 = marketAPI.get_orderbook(i , '5')['data']
          #              print('我spot_depth5用restapi___2')
          #              time.sleep(0.1)
                    try:
                        Operation_info[i]['spot_depth5'] = MarketQ[i]["DEPTH5_SPOT"].get(timeout=1)
                    except:                    
                        try:                            
                            Operation_info[i]['spot_depth5'] = Operation_info[i]['spot_depth5']
                        except:
                            Operation_info[i]['spot_depth5'] = marketAPI.get_orderbook(i , '5')['data']
                            print('我spot_depth5用restapi__2')
                            time.sleep(0.1)

                    for j in range(5):
                        Operation_info[i]['spot_bids_price5'].append(float(Operation_info[i]['spot_depth5'][0]['bids'][j][0]))
                        Operation_info[i]['spot_asks_price5'].append(float(Operation_info[i]['spot_depth5'][0]['asks'][j][0]))

                    Operation_info[i]['swap_bids_price5'] = []
                    Operation_info[i]['swap_asks_price5'] = []                
                    #测试    
               #     swap_depth5 = MQ[h]["DEPTH5_SWAP"].get(timeout=1)  
                    try:
                        Operation_info[i]['swap_depth5'] = MQ[i]["DEPTH5_SWAP"].get(timeout=1)
                    except:
                        try:                            
                            Operation_info[i]['swap_depth5']= Operation_info[i]['swap_depth5']
                        except:
                            Operation_info[i]['swap_depth5'] = marketAPI.get_orderbook(i + '-SWAP', '5')['data']
                            time.sleep(0.1)
                            print('我swap_depth5用restapi__2')

                    for j in range(5):
                        Operation_info[i]['swap_bids_price5'].append(float(Operation_info[i]['swap_depth5'][0]['bids'][j][0]))
                        Operation_info[i]['swap_asks_price5'].append(float(Operation_info[i]['swap_depth5'][0]['asks'][j][0]))

                 #   Operation_info[i]['spot_swap_update_mode'] = 'off'
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
                        #time.sleep(0.5)
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

                            Nowtime = datetime.now()

                            #在资金费率开始前半小时跟后两小时机器人不要动,12600秒内不要动
                         #   Total_seconds=0
                         #   if Nowtime.hour in [7,8,15,16,23,0]:
                         #       if Nowtime.hour in [7,15,23] and Nowtime.minute<30:
                         #           pass    
                         #       else:
                         #           if Nowtime.hour in [7,8]:                                         
                         #               T_hour=8-Nowtime.hour
                         #               T_minute=60-Nowtime.minute
                         #               T_second=60-Nowtime.second
                         #           elif Nowtime.hour in [15,16]:                                                   
                         #               T_hour=16-Nowtime.hour
                         #               T_minute=60-Nowtime.minute
                         #               T_second=60-Nowtime.second
                         #           elif Nowtime.hour in [23,0]: 
                         #               if Nowtime.hour == 23:
                         #                   t_hour=-1    
                         #               else:
                         #                   t_hour= Nowtime.hour                             
                         #               T_hour=0-t_hour
                         #               T_minute=60-Nowtime.minute
                         #               T_second=60-Nowtime.second

                         #           Total_seconds=T_hour*3600+T_minute*60+T_second
                         #           print(Total_seconds)
                         #           print('trade loop now for sleep')
                         #           time.sleep(Total_seconds)   

                            #每分钟找predict_funding_rate
                            new_record_minute = Nowtime.minute
                            if new_record_minute != record_minute:
                                #print(new_record_minute)
                                try:
                                    Swap_Funding_Rate = MQ[h]["PREDICT_FUNDING"].get(timeout=0.5)
                                    Necessary_info[h]['Predict_Funding_Rate'] = float(Swap_Funding_Rate[0]['nextFundingRate'])
                                except:
                                    pass
                                # Funding_Rate
                                record_minute = new_record_minute
                                time.sleep(0.3) #避免过度提取ws
                                
                            
                            #每小时更新present_funding_rate
                            new_record_hour = Nowtime.hour
                            if new_record_hour != record_hour:
                                try:
                                    instId_fu, Necessary_info[h]['Present_Funding_Rate'],Necessary_info[h]['Predict_Funding_Rate'] = funding_recalculate(i + '-SWAP')
                                except:
                                    pass
                                time.sleep(0.3) #避免过度提取ws
                                record_hour = new_record_hour  
                                
                                
                            #swap中间价
                            Operation_info[h]['new_swap_bids_price5'] = []
                            Operation_info[h]['new_swap_asks_price5'] = []

                            try:
                                Operation_info[h]['new_swap_depth5'] = MQ[h]["DEPTH5_SWAP"].get(timeout=1)
                            except:
                                try:
                                    Operation_info[h]['new_swap_depth5'] = Operation_info[h]['new_swap_depth5']
                                except:
                                    Operation_info[h]['new_swap_depth5'] = marketAPI.get_orderbook(h + '-SWAP', '5')['data']
                                    time.sleep(0.1)
                            for i in range(5):
                                Operation_info[h]['new_swap_bids_price5'].append(float(Operation_info[h]['new_swap_depth5'][0]['bids'][i][0]))
                                Operation_info[h]['new_swap_asks_price5'].append(float(Operation_info[h]['new_swap_depth5'][0]['asks'][i][0]))
                            new_swap_bid=float(Operation_info[h]['new_swap_bids_price5'][0])
                            new_swap_ask=float(Operation_info[h]['new_swap_asks_price5'][0])
                            swap_present_price = precfloat((new_swap_ask + new_swap_bid)/2,Necessary_info[h]['swap_tick_digit'])    #这里先用swap_present_price,没用new_swap_present_price
                            #new spot中间价
                            Operation_info[h]['new_spot_bids_price5'] = []
                            Operation_info[h]['new_spot_asks_price5'] = []
                            try:
                                Operation_info[h]['new_spot_depth5'] = MQ[h]["DEPTH5_SPOT"].get(timeout=0.5)
                            #    print('我spot_depth5还是用WS___5')
                            except:
                                try:
                                    Operation_info[h]['new_spot_depth5']=Operation_info[h]['new_spot_depth5']
                                except:
                                    Operation_info[h]['new_spot_depth5']=marketAPI.get_orderbook(h, '5')['data']
                                    time.sleep(0.1)
                                    print('h_1',h)
                                    print('我spot_depth5还是用restAPI___5')
                                
                            for i in range(5):
                                Operation_info[h]['new_spot_bids_price5'].append(float(Operation_info[h]['new_spot_depth5'][0]['bids'][i][0]))
                                Operation_info[h]['new_spot_asks_price5'].append(float(Operation_info[h]['new_spot_depth5'][0]['asks'][i][0]))
                            new_spot_bid = float(Operation_info[h]['new_spot_bids_price5'][0])
                            new_spot_ask = float(Operation_info[h]['new_spot_asks_price5'][0])
                            new_spot_present_price = precfloat((new_spot_ask + new_spot_bid)/2,Necessary_info[h]['spot_tick_digit'])
                            #现货指数
                            try:
                                spot_index_tickers = MQ[h]["INDEX_TICKERS_SPOT"].get(timeout=0.5)
                                spot_index_price = float(spot_index_tickers[0]['idxPx'])
                            except:
                                ticker_result = marketAPI.get_index_ticker(instId=h)['data']
                                time.sleep(0.2)
                                spot_index_price = float(ticker_result[0]['idxPx'])

                            #swap标记价格
                            try:
                                swap_mark_prices = MQ[h]["MARK_PRICE_SWAP"].get(timeout=0.5)
                                swap_mark_price = float(swap_mark_prices[0]['markPx'])
                            except:
                                swap_mark_prices = publicAPI.get_mark_price('SWAP')['data']
                                time.sleep(0.2)
                                for i in swap_mark_prices:
                                    instr = i["instId"].split("-")
                                    TP = instr[0] + "-" + instr[1] 
                                    if TP == h:
                                        swap_mark_price=float(i['markPx'])
                                        
                               
                            #swap_size,spot_size
                            if Necessary_info[h]['swap_min_size'] * Necessary_info[h]['contract_val']*swap_present_price > order_limit:      #看usdt swap最少出手的价值,如果超过order_limit就直接是swap_min_size
                                swap_size = Necessary_info[h]['swap_min_size']
                                spot_size = Necessary_info[h]['swap_min_size']*Necessary_info[h]['contract_val']  #考虑到合约可能是张,但是在spot这里要换算成几个币,所以*contract_val
                                swap_close_size = swap_size * close_short_index
                                spot_close_size = spot_size * close_short_index

                            elif Necessary_info[h]['swap_min_size'] * Necessary_info[h]['contract_val']*swap_present_price < order_limit:     #看usdt swap最少出手的价值,如果没超过order_limit就计算
                                swap_size = round(order_limit/(swap_present_price * Necessary_info[h]['contract_val']))
                                spot_size = round(order_limit/new_spot_present_price)
                                swap_close_size = swap_size * close_short_index
                                spot_close_size = spot_size * close_short_index

                            # 处理剩馀swap订单
                            pending_swap_revoke_mode = 'off'
                            if Operation_info[h]['swap_pending_list_left'] == 'on':
                                time.sleep(0.1)
                                Operation_info[h]['swap_pending_order_result'] = tradeAPI.get_order_list(instId=Necessary_info[h]['swap_instrument_id'])['data']
                                #print('swap_left_result',Operation_info[h]['swap_pending_order_result'])
                                if len(Operation_info[h]['swap_pending_order_result']) == 0:  # 表示已经没有未成交单子
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                else:                                  
                                    if len(Operation_info[h]['swap_pending_order_result']) > 2:  # 有两张以上的单,全撤
                                        print('重复下单')  # 这里以后做个钉钉通知
                                        tutu = h + '-SWAP,v5有多馀的挂单没撤掉,现在全撤'
                                       # sendmessage(tutu)
                                        pending_swap_revoke_mode = 'on'

                                    else:  # 一张未完成单,继续
                                        # 做出当下五档差别的判断
                                        Operation_info[h]['new_swap_bids_price5'] = []
                                        Operation_info[h]['new_swap_asks_price5'] = []
                                        try:
                                            Operation_info[h]['new_swap_depth5'] = MQ[h]["DEPTH5_SWAP"].get(timeout=1)
                                            new_swap_depth5=Operation_info[h]['new_swap_depth5']
                                        except:
                                            try:
                                                Operation_info[h]['new_swap_depth5'] = Operation_info[h]['new_swap_depth5'] 
                                                new_swap_depth5=Operation_info[h]['new_swap_depth5']
                                            except:
                                                Operation_info[h]['new_swap_depth5'] = marketAPI.get_orderbook(h + '-SWAP', '5')['data']
                                                new_swap_depth5=Operation_info[h]['new_swap_depth5']
                                                time.sleep(0.1)
                                        for i in range(5):
                                            Operation_info[h]['new_swap_bids_price5'].append(float(new_swap_depth5[0]['bids'][i][0]))
                                            Operation_info[h]['new_swap_asks_price5'].append(float(new_swap_depth5[0]['asks'][i][0]))
                                        # 如果五档变化了，撤单
                                        if Operation_info[h]['swap_bids_price5'][:3] != Operation_info[h]['new_swap_bids_price5'][:3] or Operation_info[h]['swap_asks_price5'][:3] != Operation_info[h]['new_swap_asks_price5'][:3]:
                                            pending_swap_revoke_mode = 'on'
                                        # 如果五档没有变化
                                        elif Operation_info[h]['swap_bids_price5'][:3] == Operation_info[h]['new_swap_bids_price5'][:3] and Operation_info[h]['swap_asks_price5'][:3] == Operation_info[h]['new_swap_asks_price5'][:3]:

                                            if Operation_info[h]['swap_pending_order_result'][0]['side'] == 'sell':
                                                # 看是不是best价
                                                if float(Operation_info[h]['swap_pending_order_result'][0]['px']) == float(new_swap_depth5[0]['asks'][0][0]):
                                                    # 再看是不是唯一单,是就撤单
                                                    if float(new_swap_depth5[0]['asks'][0][3]) == 1:
                                                        pending_swap_revoke_mode = 'on'

                                            #    elif float(Operation_info[h]['swap_pending_order_result'][0]['px']) != float(new_swap_depth5[0]['asks'][0][0]): #不是best价格,但现在不一定一直要挂在best价,所以不必撤单 0827
                                            #        pending_swap_revoke_mode = 'on'

                                            elif Operation_info[h]['swap_pending_order_result'][0]['side'] == 'buy':
                                                if float(Operation_info[h]['swap_pending_order_result'][0]['px']) == float(new_swap_depth5[0]['bids'][0][0]):
                                                    if float(new_swap_depth5[0]['bids'][0][3]) == 1:
                                                        pending_swap_revoke_mode = 'on'

                                        #        elif float(Operation_info[h]['swap_pending_order_result'][0]['px']) != float(new_swap_depth5[0]['bids'][0][0]):
                                        #            pending_swap_revoke_mode = 'on'

                            if pending_swap_revoke_mode == 'on':
                                order_id_list=[]
                                if len(Operation_info[h]['swap_pending_order_result']) != 0:
                                    for i in Operation_info[h]['swap_pending_order_result']:
                                        uncomplete_orderId = i['ordId']
                                        uncomplete_order_instId = i['instId']
                                        key_list = []
                                        value_list = []
                                        key_list = ["instId", "ordId"]
                                        value_list = [uncomplete_order_instId, uncomplete_orderId]
                                        dictionary_uncomplete_order = dict(zip(key_list, value_list))
                                        order_id_list.append(dictionary_uncomplete_order)
                                    if len(order_id_list) != 0:
                                        swap_revoke_result = tradeAPI.cancel_multiple_orders(order_id_list)
                                        #print('swap_revoke_result_1',swap_revoke_result)
                                Operation_info[h]['swap_pending_list_left'] = 'off'
                                # 使旧的swap五档=新的swap五档
                                Operation_info[h]['swap_bids_price5'][:5] = Operation_info[h]['new_swap_bids_price5'][:5]
                                Operation_info[h]['swap_asks_price5'][:5] = Operation_info[h]['new_swap_asks_price5'][:5]

                            # 处理剩馀spot订单
                            pending_spot_revoke_mode = 'off'
                            if Operation_info[h]['spot_pending_list_left'] == 'on':
                                time.sleep(0.1)
                                Operation_info[h]['spot_pending_order_result'] = tradeAPI.get_order_list(instId=Necessary_info[h]['spot_instrument_id'])['data']
                               # print('spot_left_result',Operation_info[h]['spot_pending_order_result'])

                                if len(Operation_info[h]['spot_pending_order_result']) == 0:  # 表示已经没有未成交单子
                                    Operation_info[h]['spot_pending_list_left'] = 'off'
                                else:
                                    if len(Operation_info[h]['spot_pending_order_result']) > 2:  # 有两张以上的单,全撤
                                        tutu = h + '-SPOT,v5有多馀的挂单没撤掉,现在全撤'
                                      #  sendmessage(tutu)
                                        print('spot repeat order')  # 这里以后做个钉钉通知
                                        pending_spot_revoke_mode ='on'


                                    else:  # 一张未完成单,继续
                                        # 做出当下五档差别的判断
                                        Operation_info[h]['new_spot_bids_price5'] = []
                                        Operation_info[h]['new_spot_asks_price5'] = []
                                        try:
                                            Operation_info[h]['new_spot_depth5']= MQ[h]["DEPTH5_SPOT"].get(timeout=0.5)
                                            new_spot_depth5=Operation_info[h]['new_spot_depth5']
                                        #    print('我spot_depth5还是用ws___4')
                                        except:
                                            try:
                                                Operation_info[h]['new_spot_depth5']=Operation_info[h]['new_spot_depth5']
                                                new_spot_depth5=Operation_info[h]['new_spot_depth5']
                                            except:
                                                Operation_info[h]['new_spot_depth5'] = marketAPI.get_orderbook(h, '5')['data']
                                                new_spot_depth5=Operation_info[h]['new_spot_depth5']
                                                print('h_23',h)
                                                print('我spot_depth5还是用restAPI___4')
                                                time.sleep(0.1)
                                        for i in range(5):
                                            Operation_info[h]['new_spot_bids_price5'].append(float(new_spot_depth5[0]['bids'][i][0]))
                                            Operation_info[h]['new_spot_asks_price5'].append(float(new_spot_depth5[0]['asks'][i][0]))
                                        # 如果五档变化了，撤单
                                        if Operation_info[h]['spot_bids_price5'][:3] != Operation_info[h]['new_spot_bids_price5'][:3] or Operation_info[h]['spot_asks_price5'][:3] != Operation_info[h]['new_spot_asks_price5'][:3]:
                                            pending_spot_revoke_mode = 'on'
                                        # 如果五档没有变化
                                        elif Operation_info[h]['spot_bids_price5'][:3] == Operation_info[h]['new_spot_bids_price5'][:3] and Operation_info[h]['spot_asks_price5'][:3] == Operation_info[h]['new_spot_asks_price5'][:3]:
                                            if Operation_info[h]['spot_pending_order_result'][0]['side'] == 'sell':  # 看是不是best价
                                                if float(Operation_info[h]['spot_pending_order_result'][0]['px']) == float(new_spot_depth5[0]['asks'][0][0]):  # 再看是不是唯一单,是就撤单
                                                    if float(new_spot_depth5[0]['asks'][0][3]) == 1:
                                                        pending_spot_revoke_mode = 'on'
                                                        # Operation_info[h]['spot_pending_list_left'] = 'off'
                                       #         elif float(Operation_info[h]['spot_pending_order_result'][0]['px']) != float(new_spot_depth5[0]['asks'][0][0]):    #不是best价格,但现在不一定一直要挂在best价,所以不必撤单 0827
                                       #             pending_spot_revoke_mode = 'on'

                                            elif Operation_info[h]['spot_pending_order_result'][0]['side'] == 'buy':
                                                if float(Operation_info[h]['spot_pending_order_result'][0]['px']) == float(new_spot_depth5[0]['bids'][0][0]):
                                                    if float(new_spot_depth5[0]['bids'][0][3]) == 1:
                                                        pending_spot_revoke_mode = 'on'
                                       #         elif float(Operation_info[h]['spot_pending_order_result'][0]['px']) != float(new_spot_depth5[0]['asks'][0][0]):
                                        #            pending_spot_revoke_mode = 'on'

                            if pending_spot_revoke_mode == 'on':  # 其他mode就不管
                                order_id_list=[]
                                for i in Operation_info[h]['spot_pending_order_result']:
                                    uncomplete_orderId = i['ordId']
                                    uncomplete_order_instId = i['instId']
                                    key_list = []
                                    value_list = []
                                    key_list = ["instId", "ordId"]
                                    value_list = [uncomplete_order_instId, uncomplete_orderId]
                                    dictionary_uncomplete_order = dict(zip(key_list, value_list))
                                    order_id_list.append(dictionary_uncomplete_order)
                                if len(order_id_list) != 0:
                                    spot_revoke_result = tradeAPI.cancel_multiple_orders(order_id_list)
                                    #print('spot_revoke_result_2',spot_revoke_result)
                                Operation_info[h]['spot_pending_list_left'] = 'off'
                                # 使旧的spot五档=新的spot五档
                                Operation_info[h]['spot_bids_price5'][:5] = Operation_info[h]['new_spot_bids_price5'][:5]
                                Operation_info[h]['spot_asks_price5'][:5] = Operation_info[h]['new_spot_asks_price5'][:5]

                            # 如果不撤單,就不用下单
                            if Operation_info[h]['swap_pending_list_left'] == 'on' or Operation_info[h]['spot_pending_list_left'] == 'on':  # 要看有没有剩馀的单
                                no_pending_order='off'  #拿掉大涨跟持仓差的影响
                                So_Sc_mode='off'  #拿掉So_Sc_mode的影响

                            #找出TA
                            Target_Amount = Necessary_info[h]['Total_money']/new_spot_present_price
                            #print('h',h)
                            #print('Target_Amount',Target_Amount)
                       
                            #找spot_balance,swap_position
                            # 找spot_balance
                            try:
                                spot_dic = AQ[h]["POSITION_SPOT"].get(timeout=0.5)
                                Operation_info[h]['spot_balance'] = float(spot_dic['cashBal'])
                                if Operation_info[h]['spot_balance'] == 0:  
                                    Operation_info[h]['spot_balance'] = float(accountAPI.get_account(h.split('-')[0])['data'][0]['details'][0]['cashBal'])
                                    time.sleep(0.5)
                            except:
                                total_spot_dic = accountAPI.get_account()['data'][0]['details']
                                time.sleep(0.5)
                                spot_cc_list=[]
                                if len(total_spot_dic)!=0:
                                    for i in total_spot_dic:
                                        TP=i['ccy']+'-USDT'
                                        spot_cc_list.append(TP)      
                                    if h in spot_cc_list:
                                        instr_fu = h.split('-')        
                                        for i in total_spot_dic:
                                            if i['ccy']==instr_fu[0]:
                                                Operation_info[h]['spot_dic'] = i
                                                spot_dic = Operation_info[h]['spot_dic']
                                                Operation_info[h]['spot_balance'] = float(spot_dic['cashBal'])                  
                                    else:
                                        Operation_info[h]['spot_balance']=0          
                                else:
                                    Operation_info[h]['spot_balance']=0
    
                            # 找swap持仓数                            
                            try:                                  
                                swap_dic = AQ[h]["POSITION_SWAP"].get(timeout=0.5)                                 
                                Operation_info[h]['swap_position'] = float(swap_dic['pos'])
                                if Operation_info[h]['swap_position'] ==0:
                                    Operation_info[h]['swap_position']= float(accountAPI.get_positions(instId=h+'-SWAP')['data'][0]['pos'])
                                    time.sleep(0.5)
                            except:
                                total_swap_dic = accountAPI.get_positions('SWAP')['data']
                                time.sleep(0.5)                              
                                swap_cc_list=[]
                                if len(total_swap_dic)!=0:
                                    for i in total_swap_dic:
                                        TP=i['instId'].split(i['instId'][-5:])[0]
                                        swap_cc_list.append(TP)      
                                    if h in swap_cc_list:
                                        for i in total_swap_dic:
                                            instr_fu = i['instId'].split('-')
                                            if instr_fu[0] + "-" + instr_fu[1] == h:
                                                Operation_info[h]['swap_dic'] = i
                                                swap_dic = Operation_info[h]['swap_dic']
                                                Operation_info[h]['swap_position'] = float(swap_dic['pos'])                   
                                    else:
                                        Operation_info[h]['swap_position']=0          
                                else:
                                    Operation_info[h]['swap_position']=0                            
                                         
                            #print(Operation_info[h]['spot_balance'] * new_spot_present_price > Necessary_info[h]['Total_money'] * 1.05)

                            # 加入大涨调仓判断,大涨后要尽速平仓
                            if no_pending_order == 'on': #先看有没有遗留单,有就不走调仓判断
                                if Operation_info[h]['spot_balance'] > 0 : #这时候是spot开多,swap开空
                                    if Operation_info[h]['spot_balance'] * new_spot_present_price > Necessary_info[h]['Total_money'] * 1.2:
                                        So_Sc_mode = 'off'

        #                               mode_open_long_judge = {'Target_Amount': Target_Amount,
        #                                                        'spot_balance': Operation_info[h]['spot_balance'],
        #                                                        'swap_position': Operation_info[h]['swap_position'],
        #                                                        'swap_size': swap_size,
        #                                                        'spot_size': spot_size,
                                        #                          'contract_value':Necessary_info[h]['contract_val']}

                                        mode_close_long_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                                 'spot_balance': Operation_info[h]['spot_balance'],
                                                                 'swap_position': Operation_info[h]['swap_position'],
                                                                 'swap_close_size': swap_close_size,
                                                                 'spot_close_size': spot_close_size,
                                                                 'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
                                                                 'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                 'So_Sc_mode': So_Sc_mode,
                                                                 'contract_value':Necessary_info[h]['contract_val']}

         #                              open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)
                                        close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)

                                elif Operation_info[h]['spot_balance'] < 0 : #这时候是spot开空,swap开多
                                    if Operation_info[h]['spot_balance'] * new_spot_present_price < Necessary_info[h]['Total_money'] * -1.5:
                                        So_Sc_mode = 'off'
                                        #查询开负仓还有多少可开
              #                          max_avail_size_result = float(accountAPI.get_max_avail_size(h,'cross')['data'][0]['availSell'])
              #                          time.sleep(0.1)
              #                          swap_instrumentId = h + '-SWAP'
              #                          funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
              #                          time.sleep(0.2)
              #                          if abs(float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate']) )< 0.05 / 100:  # 借币日利率
                                        # 小于的话就不要开仓,抵不过借币日利率
              #                              Target_Amount=0
              #                          elif max_avail_size_result < spot_size:
                                            #TA就等于目前的spot_balance然后不要增加
              #                              Target_Amount= abs(Operation_info[h]['spot_balance'])  #因为在def会*-1,所以这里加上abs
              #                          else:
              #                              pass #照常通过,继续开spot负仓


               #                         mode_open_short_judge = {'Target_Amount': Target_Amount,
               #                                                   'spot_balance': Operation_info[h]['spot_balance'],
               #                                                  'swap_position': Operation_info[h]['swap_position'],
               #                                                  'swap_size': swap_size,
               #                                                  'spot_size': spot_size,
                                        #                          'contract_value':Necessary_info[h]['contract_val']}
                                        mode_close_short_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
                                                                  'spot_balance': Operation_info[h]['spot_balance'],
                                                                  'swap_position': Operation_info[h]['swap_position'],
                                                                  'swap_close_size': swap_close_size,
                                                                  'spot_close_size': spot_close_size,
                                                                  'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
                                                                  'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                  'So_Sc_mode': So_Sc_mode,
                                                                  'contract_value':Necessary_info[h]['contract_val']}

                #                        open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                                        close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)

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

                            # 持仓差的调整
                           # print('h_1',h)
                           # print('spot_balance_23',Operation_info[h]['spot_balance'] )
                           # print('swap_position_23',Operation_info[h]['swap_position'] )
                           # print('contract_val_234',Necessary_info[h]['contract_val'])
                           # print(abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])*new_spot_present_price > (tolerate_limit*1.3 ))
                           # print('no_pending_order',no_pending_order)
                            if no_pending_order == 'on':  # 先看有没有遗留单,有就不走持仓差调整
                              
                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])*new_spot_present_price > (tolerate_limit*1.5 ):                                            
                                    So_Sc_mode = 'off'
                                    if swap_mark_price >= spot_index_price:
                                        
                                         #   if Operation_info[h]['spot_balance'] > 0: #现在是开正仓
                                        #评判跟著So_Sc的路线走
                                        if Necessary_info[h]['Present_Funding_Rate'] >= 0: 
                                            mode_open_long_judge = {'Target_Amount': Target_Amount,
                                                                    'spot_balance': Operation_info[h]['spot_balance'],
                                                                    'swap_position': Operation_info[h]['swap_position'],
                                                                    'swap_size': swap_size,
                                                                    'spot_size': spot_size,
                                                                    'contract_value':Necessary_info[h]['contract_val']}
                                            #开正仓
                                            open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)  # 开仓模式
                                            
                                            
                                        elif Necessary_info[h]['Present_Funding_Rate'] < 0:   #虽然<0,还是先选择开正仓
                                            mode_close_short_judge = {'Target_Amount_Close': 0,
                                                                                # Target_Amount_Close這裡設等於0
                                                                                'spot_balance': Operation_info[h]['spot_balance'],
                                                                                'swap_position': Operation_info[h][
                                                                                    'swap_position'],
                                                                                'swap_close_size': swap_close_size,
                                                                                'spot_close_size': spot_close_size,
                                                                                'predict_funding': Necessary_info[h][
                                                                                    'Predict_Funding_Rate'],
                                                                                'present_funding': Necessary_info[h][
                                                                                    'Present_Funding_Rate'],
                                                                                'So_Sc_mode': So_Sc_mode,
                                                                                'contract_value':Necessary_info[h]['contract_val']}
                                            # 平负仓
                                            close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)

                                    elif swap_mark_price < spot_index_price:
                                        if Necessary_info[h]['Present_Funding_Rate'] >= 0: 
                                            mode_close_long_judge = {'Target_Amount_Close': 0,
                                                                                 # Target_Amount_Close這裡設等於0
                                                                                 'spot_balance': Operation_info[h]['spot_balance'],
                                                                                 'swap_position': Operation_info[h][
                                                                                     'swap_position'],
                                                                                 'swap_close_size': swap_close_size,
                                                                                 'spot_close_size': spot_close_size,
                                                                                 'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
                                                                                 'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                                 'So_Sc_mode': So_Sc_mode,
                                                                                 'contract_value':Necessary_info[h]['contract_val']}
                                            # 平正仓
                                            close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)
                                        elif Necessary_info[h]['Present_Funding_Rate'] < 0:
                                            max_avail_size_result = float(accountAPI.get_max_avail_size(h, 'cross')['data'][0]['availSell'])
                                            time.sleep(0.1)
                                            if max_avail_size_result < spot_size:
                                                if abs(Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) > abs(Operation_info[h]['spot_balance']):
                                                # 已经没法继续开负仓,然后永续合约多于现货仓位,TA就等于目前的spot_balance然后不要增加,平swap的仓位
                                                    Target_Amount_Close = abs(Operation_info[h]['spot_balance'])                                                    
                                                    mode_close_short_judge = {'Target_Amount_Close': Target_Amount_Close,  # Target_Amount_Close這裡設等於spot_balance
                                                                        'spot_balance': Operation_info[h]['spot_balance'], 
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_close_size': swap_close_size,
                                                                        'spot_close_size': spot_close_size,
                                                                        'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
                                                                        'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                        'So_Sc_mode': So_Sc_mode,
                                                                        'contract_value':Necessary_info[h]['contract_val']}
                                                    #平负仓
                                                    close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)
                                                elif abs(Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) < abs(Operation_info[h]['spot_balance']):
                                                    # 已经没法继续开负仓,然后现货约多于合约仓位,TA就等于目前的spot_balance然后不要增加,建swap的仓位
                                                    Target_Amount = abs(Operation_info[h]['spot_balance'])
                                                    mode_open_short_judge = {'Target_Amount': Target_Amount,
                                                                        'spot_balance': Operation_info[h]['spot_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'contract_value':Necessary_info[h]['contract_val']}
                                                    # 开负仓
                                                    open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                                            else:#还可以继续开负仓        
                                                mode_open_short_judge = {'Target_Amount': Target_Amount,
                                                                        'spot_balance': Operation_info[h]['spot_balance'],
                                                                        'swap_position': Operation_info[h]['swap_position'],
                                                                        'swap_size': swap_size,
                                                                        'spot_size': spot_size,
                                                                        'contract_value':Necessary_info[h]['contract_val']}
                                                # 开负仓
                                                open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)



                                    
        #                            elif Operation_info[h]['spot_balance'] < 0: #现在是开负仓
        #                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])*new_spot_present_price > (tolerate_limit*1.5):
        #                                    max_avail_size_result = float(accountAPI.get_max_avail_size(h, 'cross')['data'][0]['availSell'])
        #                                    time.sleep(0.1)
        #                                    if max_avail_size_result < spot_size:
        #                                        So_Sc_mode = 'off'
        #                                        if abs(Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) > abs(Operation_info[h]['spot_balance']):
                                                # 已经没法继续开负仓,然后永续合约多于现货仓位,TA就等于目前的spot_balance然后不要增加,平swap的仓位
        #                                            Target_Amount_Close = abs(Operation_info[h]['spot_balance'])                                                    
        #                                            mode_close_short_judge = {'Target_Amount_Close': Target_Amount_Close,  # Target_Amount_Close這裡設等於spot_balance
        #                                                                'spot_balance': Operation_info[h]['spot_balance'],
        #                                                                'swap_position': Operation_info[h]['swap_position'],
        #                                                                'swap_close_size': swap_close_size,
        #                                                                'spot_close_size': spot_close_size,
        #                                                                'predict_funding': Necessary_info[h][
        #                                                                    'Predict_Funding_Rate'],
        #                                                                'present_funding': Necessary_info[h][
        #                                                                    'Present_Funding_Rate'],
        #                                                                'So_Sc_mode': So_Sc_mode,
        #                                                                'contract_value':Necessary_info[h]['contract_val']}
        #                                            close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)
        #                                        elif abs(Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) < abs(Operation_info[h]['spot_balance']):
                                                    # 已经没法继续开负仓,然后现货约多于合约仓位,TA就等于目前的spot_balance然后不要增加,建swap的仓位
        #                                            Target_Amount = abs(Operation_info[h]['spot_balance'])
        #                                            mode_open_short_judge = {'Target_Amount': Target_Amount,
        #                                                                'spot_balance': Operation_info[h]['spot_balance'],
        #                                                                'swap_position': Operation_info[h]['swap_position'],
        #                                                                'swap_size': swap_size,
        #                                                                'spot_size': spot_size,
        #                                                                'contract_value':Necessary_info[h]['contract_val']}
                                                    # 开负仓
        #                                            open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
        #                                    else:#还可以继续开负仓                     
        #                                        So_Sc_mode = 'off'
        #                                        mode_open_short_judge = {'Target_Amount': Target_Amount,
        #                                                                'spot_balance': Operation_info[h]['spot_balance'],
        #                                                                'swap_position': Operation_info[h]['swap_position'],
        #                                                                'swap_size': swap_size,
        #                                                                'spot_size': spot_size,
        #                                                                'contract_value':Necessary_info[h]['contract_val']}
                                                # 开负仓
        #                                        open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)

        #                        elif Operation_info[h]['position_direction']=='close':  #现在是平仓方向
        #                            if Operation_info[h]['spot_balance'] > 0:  # 这时候是spot开多,swap开空
        #                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])*new_spot_present_price > (tolerate_limit*1.5):
                                        #  print('here_check_tilt')
                                            # 持仓差超过tolerate_limit*1.3时启动,跳过So_Sc_mode,可能会有资金不足,所以选择平仓,
        #                                    So_Sc_mode = 'off'         
        #                                    mode_close_long_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
        #                                                            'spot_balance': Operation_info[h]['spot_balance'],
        #                                                            'swap_position': Operation_info[h]['swap_position'],
        #                                                            'swap_close_size': swap_close_size,
        #                                                            'spot_close_size': spot_close_size,
        #                                                            'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
        #                                                            'present_funding': Necessary_info[h]['Present_Funding_Rate'],
        #                                                            'So_Sc_mode': So_Sc_mode,
        #                                                            'contract_value':Necessary_info[h]['contract_val']}

            #                               open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)
        #                                    close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)

        #                            elif Operation_info[h]['spot_balance'] < 0:  # 这时候是spot开空,swap开多
        #                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])*new_spot_present_price> (tolerate_limit*1.5):
                                            # 持仓差超过tolerate_limit*1.3时启动,跳过So_Sc_mode,可能有spot无法继续卖,所以选择平仓模式
        #                                    So_Sc_mode = 'off'                                   
        #                                    mode_close_short_judge = {'Target_Amount_Close': 0,  # Target_Amount_Close這裡設等於0
        #                                                            'spot_balance': Operation_info[h]['spot_balance'],
        #                                                            'swap_position': Operation_info[h]['swap_position'],
        #                                                            'swap_close_size': swap_close_size,
        #                                                            'spot_close_size': spot_close_size,
        #                                                            'predict_funding': Necessary_info[h][
        #                                                                'Predict_Funding_Rate'],
        #                                                            'present_funding': Necessary_info[h][
        #                                                                'Present_Funding_Rate'],
        #                                                            'So_Sc_mode': So_Sc_mode,
        #                                                            'contract_value':Necessary_info[h]['contract_val']}
        #                                    close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)
                            #print('So_Sc_mode_2',So_Sc_mode)
                            #print('open_long_mode_2345',open_long_mode)
                            #print('open_short_mode_2345',open_short_mode)
                            #print('close_long_mode',close_long_mode)
                            #print('close_short_mode',close_short_mode)

                            mode_So_Sc = {'maker_commission_spot': maker_commission_spot,
                                          'taker_commission_spot': taker_commission_spot,
                                          'maker_commission_swap': maker_commission_swap,
                                          'taker_commission_swap': taker_commission_swap,
                                          'swap_present_price': swap_present_price,
                                          'spot_index_price': spot_index_price   #这改用现货指数,不用中间价'spot_present_price': new_spot_present_price
                                          }

                            #So, Sc = make_So_Sc(mode_So_Sc)
                            #print('swap_present_price',swap_present_price)
                            #print('spot_index_price',spot_index_price)
                            #print('So_Sc_mode_3',So_Sc_mode)

                           # swap_present_price=35
                           # spot_index_price=35.6

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

                            #print('h',h)
                            #print('So',So)

                            #加上若盘口（best ask跟bid)之间价差比例 <0.001 就不要下单
                       #     if (new_swap_ask - new_swap_bid)/swap_present_price< 2*Necessary_info[h]['spot_tick_size'] or (new_spot_ask-new_spot_bid)<2*Necessary_info[h]['swap_tick_size']:
                       #         if Necessary_info[h]['spot_tick_size'] / new_spot_present_price <0.0001 or Necessary_info[h]['swap_tick_size'] / swap_present_price <0.0001:
                       #             So_Sc_mode = 'off'

                            #以下是正常流程
                            if no_pending_order == 'on':
                                if So_Sc_mode =='on':
                                    if swap_mark_price > spot_index_price:
                                        if Necessary_info[h]['Present_Funding_Rate'] >= 0:
                                         
                                            if So > Necessary_info[h]['Present_Funding_Rate'] * spot_index_price:
                                                swap_instrumentId = h + '-SWAP'
                                                funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
                                                time.sleep(0.2)

                                                # 如果目前资金费率<0,且加上下期资金费率小于负千一,就不要开正仓
                                                if float(funding_rate_result['fundingRate']) < 0 and float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate']) < 0.001:
                                                    pass
                                                else:
                                                    mode_open_long_judge = {'Target_Amount': Target_Amount,
                                                                            'spot_balance': Operation_info[h]['spot_balance'],
                                                                            'swap_position': Operation_info[h]['swap_position'],
                                                                            'swap_size': swap_size,
                                                                            'spot_size': spot_size,
                                                                            'contract_value':Necessary_info[h]['contract_val']}
                                                    #开正仓
                                                    open_long_mode, open_long_final_open_mode, open_long_final_close_mode = open_long_judge(mode_open_long_judge)  # 开仓模式
                                                  
                                        elif Necessary_info[h]['Present_Funding_Rate'] < 0:
                                            #让平仓增加2倍难度
                                            if So/2 > -1* Necessary_info[h]['Present_Funding_Rate'] * spot_index_price:
                                                swap_instrumentId = h + '-SWAP'
                                                funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
                                                time.sleep(0.2)
                                                if float(funding_rate_result['fundingRate']) < 0 and float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate']) < 0.001:
                                                    pass
                                                else:
                                                    #平均资金费率要大于万三
                                                    if abs(Necessary_info[h]['Present_Funding_Rate']) > 0.0003:
                                                        mode_close_short_judge = {'Target_Amount_Close': 0,
                                                                                  # Target_Amount_Close這裡設等於0
                                                                                  'spot_balance': Operation_info[h]['spot_balance'],
                                                                                  'swap_position': Operation_info[h][
                                                                                      'swap_position'],
                                                                                  'swap_close_size': swap_close_size,
                                                                                  'spot_close_size': spot_close_size,
                                                                                  'predict_funding': Necessary_info[h][
                                                                                      'Predict_Funding_Rate'],
                                                                                  'present_funding': Necessary_info[h][
                                                                                      'Present_Funding_Rate'],
                                                                                  'So_Sc_mode': So_Sc_mode,
                                                                                  'contract_value':Necessary_info[h]['contract_val']}
                                                        # 平负仓
                                                        close_short_mode, close_short_final_open_mode, close_short_final_close_mode = close_short_judge(mode_close_short_judge)

                                    elif swap_mark_price < spot_index_price:
                                        if Necessary_info[h]['Present_Funding_Rate'] >= 0:
                                            #让平仓增加2倍难度
                                            if Sc/2 > Necessary_info[h]['Present_Funding_Rate'] * spot_index_price:
                                                swap_instrumentId = h + '-SWAP'
                                                funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
                                                time.sleep(0.2)

                                             #   print(funding_rate_result['fundingRate'])
                                             #   print(float(funding_rate_result['nextFundingRate']))
                                                
                                                if float(funding_rate_result['fundingRate']) > 0 and float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate']) > 0.001:
                                                    pass
                                                else:
                                                    # 平均资金费率要大于万三
                                                    
                                                    if abs(Necessary_info[h]['Present_Funding_Rate']) > 0.0003:
                                                        mode_close_long_judge = {'Target_Amount_Close': 0,
                                                                                 # Target_Amount_Close這裡設等於0
                                                                                 'spot_balance': Operation_info[h]['spot_balance'],
                                                                                 'swap_position': Operation_info[h][
                                                                                     'swap_position'],
                                                                                 'swap_close_size': swap_close_size,
                                                                                 'spot_close_size': spot_close_size,
                                                                                 'predict_funding': Necessary_info[h]['Predict_Funding_Rate'],
                                                                                 'present_funding': Necessary_info[h]['Present_Funding_Rate'],
                                                                                 'So_Sc_mode': So_Sc_mode,
                                                                                 'contract_value':Necessary_info[h]['contract_val']}
                                                        # 平正仓
                                                        close_long_mode, close_long_final_open_mode, close_long_final_close_mode = close_long_judge(mode_close_long_judge)
                                                       # print('close_long_mode_12',close_long_mode)
                                                       # print('close_long_final_open_mode_12',close_long_final_open_mode)
                                                       # print('close_long_final_close_mode_12',close_long_final_close_mode)

                                        elif Necessary_info[h]['Present_Funding_Rate'] < 0:
                                            if Sc > -1 * Necessary_info[h]['Present_Funding_Rate'] * spot_index_price:
                                                swap_instrumentId = h + '-SWAP'
                                                funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
                                                time.sleep(0.2)
                                         
                                                # 如果目前资金费率>0,且加上下期资金费率大于正千一,就不要开负仓
                                                if float(funding_rate_result['fundingRate']) > 0 and float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate']) > 0.001:
                                                    pass
                                                else:
                                                    #print('xc')
                                                    # 查询开负仓还有多少可开
                                                    max_avail_size_result = float(accountAPI.get_max_avail_size(h, 'cross')['data'][0]['availSell'])
                                                    time.sleep(0.1)
                                                    swap_instrumentId = h + '-SWAP'
                                                    funding_rate_result = publicAPI.get_funding_rate(swap_instrumentId)['data'][0]
                                                    time.sleep(0.2)
                                                    if abs(float(funding_rate_result['fundingRate']) + float(funding_rate_result['nextFundingRate'])) < 0.05 /100 /3:  # 借币日利率假定万五然后除3,因为分三次小时
                                                        # 小于的话就不要开仓,抵不过借币日利率
                                                        Target_Amount = abs(Operation_info[h]['spot_balance'])
                                                    elif max_avail_size_result < spot_size:
                                                        # TA就等于目前的spot_balance然后不要增加
                                                        Target_Amount = abs(Operation_info[h]['spot_balance'])  # 因为在def会*-1,所以这里加上abs
                                                    else:
                                                        pass  # 照常通过,继续开spot负仓
                                                    mode_open_short_judge = {'Target_Amount': Target_Amount,
                                                                             'spot_balance': Operation_info[h]['spot_balance'],
                                                                             'swap_position': Operation_info[h]['swap_position'],
                                                                             'swap_size': swap_size,
                                                                             'spot_size': spot_size,
                                                                             'contract_value':Necessary_info[h]['contract_val']}
                                                  # 开负仓
                                                    open_short_mode, open_short_final_open_mode, open_short_final_close_mode = open_short_judge(mode_open_short_judge)
                                                    #print('here_ck')
                                                    #print('open_short_mode_23',open_short_mode)
                            
                    

                            if open_long_mode =='on' or open_long_final_open_mode=='on' or open_long_final_close_mode=='on' or open_short_mode=='on' or \
                                open_short_final_open_mode =='on' or  open_short_final_close_mode =='on' or close_short_mode =='on' or\
                                    close_short_final_open_mode =='on' or close_short_final_close_mode =='on' or close_long_mode =='on' or \
                                        close_long_final_open_mode =='on' or close_long_final_close_mode =='on' :
                                
                                time.sleep(0.3)
                                #这里准备新的五档数据给下面下单用
                                # 新的swap五档
                                try:
                                    Operation_info[h]['swap_depth5']=MQ[h]["DEPTH5_SWAP"].get(timeout=1)
                            #        swap_depth5 = Operation_info[h]['swap_depth5']
                                except:
                                    try:
                                        Operation_info[h]['swap_depth5']=Operation_info[h]['swap_depth5']
                             #           swap_depth5=Operation_info[h]['swap_depth5']
                                    except:
                                        Operation_info[h]['swap_depth5']=marketAPI.get_orderbook(h + '-SWAP', '5')['data']
                             #           swap_depth5 = Operation_info[h]['swap_depth5']
                                        time.sleep(0.1)
                                # 新的spot五档
                                try:
                                    Operation_info[h]['spot_depth5']= MQ[h]["DEPTH5_SPOT"].get(timeout=1)
                            #        spot_depth5 = Operation_info[h]['spot_depth5']
                                #    print('我spot_depth5还是用ws___#')
                                except:
                                    try:
                                        Operation_info[h]['spot_depth5']= Operation_info[h]['spot_depth5']
                            #            spot_depth5 = Operation_info[h]['spot_depth5']
                                    except:
                                        Operation_info[h]['spot_depth5']= marketAPI.get_orderbook(h, '5')['data']
                             #           spot_depth5 =Operation_info[h]['spot_depth5']
                                        print('h_',h)
                                        print('我spot_depth5还是用restAPI___#')
                                        time.sleep(0.1)

                            if open_long_mode =='on' or open_long_final_open_mode=='on' or open_long_final_close_mode=='on' or open_short_mode=='on' or open_short_final_open_mode =='on' or  open_short_final_close_mode =='on':
                                Operation_info[h]['position_direction']='open'
                            elif close_short_mode =='on' or close_short_final_open_mode =='on' or close_short_final_close_mode =='on' or close_long_mode =='on' or close_long_final_open_mode =='on' or close_long_final_close_mode =='on':
                                Operation_info[h]['position_direction']='close'
                            
                           # if h == 'MINA-USDT':
                            #    print('h', h)
                                #print('swap_position',Operation_info[h]['swap_position'])
                                #print('contract_value',Necessary_info[h]['contract_val'])
                                #print('spot_balance',Operation_info[h]['spot_balance'])
                            #    print('open_long_mode_1',open_long_mode)
                            #    print('open_short_mode_1',open_short_mode)
                            #    print('close_short_mode_1',close_short_mode)
                            #    print('close_long_mode_1',close_long_mode)
                               # print('open_long_final_open_mode',open_long_final_open_mode)
                               # print('open_short_final_open_mode',open_short_final_open_mode)
                               # print('open_long_final_close_mode',open_long_final_close_mode)
                               # print('open_short_final_close_mode',open_short_final_close_mode)
                               # print('close_long_final_close_mode',close_long_final_close_mode)
                               # print('close_short_final_close_mode',close_short_final_close_mode)
                               # print('close_long_final_open_mode',close_long_final_open_mode)
                               # print('close_short_final_open_mode',close_short_final_open_mode)
                                #print('swap_pending_left',Operation_info[h]['swap_pending_list_left'] )
                                #print('spot_pending_left',Operation_info[h]['spot_pending_list_left'])
                            
                            open_short_mode ='off'
                            open_short_final_open_mode ='off'
                            open_short_final_close_mode ='off'
                            close_long_mode ='off'
                            close_long_final_open_mode ='off'
                            close_long_final_close_mode ='off' 
                           # open_long_mode ='off' 
                           # open_long_final_open_mode ='off' 
                           # open_long_final_close_mode ='off' 
                           # close_short_mode ='off' 
                           # close_short_final_open_mode ='off' 
                           # close_short_final_close_mode ='off'                                          

                            if open_long_mode == 'on':

                                if (tolerate_limit/2/swap_present_price) > Operation_info[h]['spot_balance']+ Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > \
                                        (tolerate_limit / 4/ swap_present_price):
                                    # 现货仓位多于合约仓位,合约卖best价,稍微调整现货买2档
                                    
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][1][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}                                

                                elif (tolerate_limit / swap_present_price) > Operation_info[h]['spot_balance']+ Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > \
                                        (tolerate_limit / 2 / swap_present_price):
                                    # 现货仓位多于合约仓位,合约卖中间价,现货买五档
                                    #处理挂单中间价的问题
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': swap_price,
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > \
                                        (tolerate_limit / swap_present_price):
                                    # 现货仓位多于合约仓位,Spot_size=0, swap卖出挂买一价limit, 0607 合约卖中间价
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': swap_price,
                                                       'spot_size': '0',
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif -1*(tolerate_limit / 2 /  swap_present_price) < Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + \
                                        Operation_info[h]['spot_balance'] <-1*(tolerate_limit / 4 / swap_present_price):  
                                    # 合约仓位多于现货仓位,现货买best价,合约稍微调整卖2档                                                                     
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]), Necessary_info[h]['spot_tick_digit']) ,
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][1][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + \
                                        Operation_info[h]['spot_balance'] <-1*(tolerate_limit / 2 / swap_present_price):  
                                    # 合约仓位多于现货仓位,现货买中间价,合约卖五档
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])), Necessary_info[h]['spot_tick_digit'])                                                                          
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': spot_price,
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] < -1*(tolerate_limit / swap_present_price):  
                                    # 合约仓位多于现货仓位,spot买在对手价
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])), Necessary_info[h]['spot_tick_digit']) 
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': spot_price,
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': '0',
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                else:  # 平时设置在Best
                                    mode_take_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                #print('mode_take_long_order',mode_take_long_order)
                            if open_short_mode == 'on':  #swap是long_position,spot_balance<0

                                if -1*(tolerate_limit / 2 / swap_present_price) < Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < -1*(tolerate_limit / 4 / swap_present_price):
                                    # 现货short仓位多于合约long仓位,合约买best价,现货调整卖2档                                   
                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][1][0]),Necessary_info[h]['spot_tick_digit']),
                                                        'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                        'spot_size': spot_size,
                                                        'swap_size': swap_size,
                                                        'spot_order_type': 'post_only',
                                                        'swap_order_type': 'post_only'}

                                elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < -1*(tolerate_limit / 2 / swap_present_price):
                                    # 现货short仓位多于合约long仓位,合约买中间价,现货卖五档
                                    #处理挂单中间价的问题
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                    mode_take_short_order = { 'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                        'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                        'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][4][0]),Necessary_info[h]['spot_tick_digit']),
                                                        'swap_price': swap_price,
                                                        'spot_size': spot_size,
                                                        'swap_size': swap_size,
                                                        'spot_order_type': 'post_only',
                                                        'swap_order_type': 'post_only'}

                                elif Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < -1*(tolerate_limit / swap_present_price):
                                    # 现货仓位多于合约仓位,Spot_size=0, swap买入挂卖一价limit,0607swap挂中间价
                                    
                                    #处理挂单中间价的问题
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': swap_price,
                                                       'spot_size': '0',
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif (tolerate_limit / 2 / swap_present_price) > Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] >(tolerate_limit / 4 / swap_present_price):
                                    # 合约仓位多于现货仓位,现货卖best价,合约调整买2档                                   
                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][1][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif (tolerate_limit / swap_present_price) > Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] >(tolerate_limit / 2 / swap_present_price):
                                    # 合约仓位多于现货仓位,现货卖中间价,合约买五档
                                    #处理挂单中间价的问题
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0])), Necessary_info[h]['spot_tick_digit'])

                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': spot_price,
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                elif Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] > (tolerate_limit / swap_present_price):
                                    # 合约仓位多于现货仓位,spot卖在对手价,0607spot挂中间价
                                    #处理挂单中间价的问题
                                    #如果best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0])), Necessary_info[h]['spot_tick_digit'])
                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': spot_price,
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': '0',
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                else:  # 平时设置在Best,卖spot,买swap,在best价
                                    mode_take_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                       'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                       'spot_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                       'swap_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                       'spot_size': spot_size,
                                                       'swap_size': swap_size,
                                                       'spot_order_type': 'post_only',
                                                       'swap_order_type': 'post_only'}

                                #print('mode_take_short_order',mode_take_short_order)
                            if close_short_mode =='on':  #平负仓，spot平空,swap平多.spot_balance<0,swap_position>0
                                if -1*(tolerate_limit / 2 / swap_present_price) < Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < \
                                        -1*(tolerate_limit / 4 / swap_present_price):  
                                    # 平仓时现货仓位多于合约仓位,现货要加速平空,现货买在best价,合约调整卖在2档                                    
                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][1][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif -1*(tolerate_limit / swap_present_price) < Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < \
                                        -1*(tolerate_limit / 2 / swap_present_price):  
                                    # 平仓时现货仓位多于合约仓位,现货要加速平空,现货买在中间价,合约卖在五档
                                    #处理挂单中间价的问题
                                    #如果spot best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])), Necessary_info[h]['spot_tick_digit'])
                                    
                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': spot_close_price,
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] < \
                                        -1*(tolerate_limit / swap_present_price):  #平负仓，spot平空买入,swap平多卖出.spot_balance<0,swap_position>0
                                    # 平仓时现货仓位多于合约仓位,spot买太慢,现货买在对手价,spot买在中间价
                                    #如果spot best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])), Necessary_info[h]['spot_tick_digit'])

                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': spot_close_price,
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': '0',
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}
                                
                                elif (tolerate_limit / 2 / swap_present_price) > Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + \
                                        Operation_info[h]['spot_balance'] > (tolerate_limit / 4 / swap_present_price):  #平负仓，spot平空买入,swap平多卖出.spot_balance<0,swap_position>0
                                    # 平仓时合约仓位多于现货仓位,合约要加速平多,swap卖在best价,现货调整买在2档
                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][1][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif (tolerate_limit / swap_present_price) > Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + \
                                        Operation_info[h]['spot_balance'] > (tolerate_limit / 2 / swap_present_price):  #平负仓，spot平空买入,swap平多卖出.spot_balance<0,swap_position>0
                                    # 平仓时合约仓位多于现货仓位,合约要加速平多,swap卖在中间价,现货买在五档
                                    #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])
                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': swap_close_price,
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] >\
                                        (tolerate_limit / swap_present_price):  #平负仓，spot平空,swap平多.spot_balance<0,swap_position>0
                                    # 平仓时合约仓位多于现货仓位,合约要加速平多,合约卖在对手价,0607合约卖在中间价
                                    #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': swap_close_price,
                                                             'spot_close_size': '0',
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}
                                   # print('mode_take_close_short_order',mode_take_close_short_order)

                                else:  # 平时设置在best,spot平空买入,swap平多卖出
                                    mode_take_close_short_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                            if close_long_mode =='on':  
                                if (tolerate_limit / 4 / swap_present_price) > Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > (tolerate_limit / 4 / swap_present_price):
                                    #spot平多,swap平空,spot_balance>0,swap_position<0
                                    # 平仓时现货仓位多于合约仓位,现货要加速平多,现货卖best价,合约调整买2档
                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][1][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif (tolerate_limit / swap_present_price) > Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > (tolerate_limit / 2 / swap_present_price):
                                    #spot平多,swap平空,spot_balance>0,swap_position<0
                                    # 平仓时现货仓位多于合约仓位,现货要加速平多,现货卖中间价,合约买五档
                                    #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask               
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0])), Necessary_info[h]['spot_tick_digit'])       
                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': spot_close_price,
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif Operation_info[h]['spot_balance'] +Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] > (tolerate_limit / swap_present_price):
                                    # 平仓时现货仓位多于合约仓位,spot卖太慢,现货卖在对手价,0607spot卖在中间价
                                    if float(Operation_info[h]['spot_depth5'][0]['bids'][0][0]) + Necessary_info[h]['spot_tick_size'] < float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]):
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['spot_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['spot_tick_digit'])
                                    else:
                                        spot_close_price = precfloat((float(Operation_info[h]['spot_depth5'][0]['asks'][0][0])), Necessary_info[h]['spot_tick_digit'])

                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': spot_close_price,
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][4][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': '0',
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}
                                
                                elif -1*tolerate_limit / 2 / swap_present_price < Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] < -1*(tolerate_limit / 4 / swap_present_price):
                                    # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在best价,现货调整卖在2档      
                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][2][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif -1*tolerate_limit / swap_present_price < Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] < -1*(tolerate_limit / 2 / swap_present_price):
                                    # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在中间价,现货卖在五档                                    
                                    #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': swap_close_price,
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                elif Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'] + Operation_info[h]['spot_balance'] < -1*tolerate_limit / swap_present_price:
                                    # 平仓时合约仓位多于现货仓位,合约要加速平空,合约买在对手价 0607 swap买在中间价
                                    #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                    if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                    else:
                                        swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][4][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': swap_close_price,
                                                             'spot_close_size': '0',
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                                else:  # 平时设置在best,spot平多卖出,swap平空买入
                                    mode_take_close_long_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                             'spot_instrument_id': Necessary_info[h]['spot_instrument_id'],
                                                             'spot_close_price': precfloat(float(Operation_info[h]['spot_depth5'][0]['asks'][0][0]), Necessary_info[h]['spot_tick_digit']),
                                                             'swap_close_price': precfloat(float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]), Necessary_info[h]['swap_tick_digit']),
                                                             'spot_close_size': spot_close_size,
                                                             'swap_close_size': swap_close_size,
                                                             'spot_order_type': 'post_only',
                                                             'swap_order_type': 'post_only'}

                            if open_long_final_open_mode == 'on':  # swap继续开仓开空,swap_position<0,spot_balance>0
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_long_final_open_mode,现在相差' + \
                                    str((Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                             #   sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])>= Necessary_info[h]['swap_min_size']* Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_size=int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val'])
                                    else:
                                        swap_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_size=int(Necessary_info[h]['swap_min_size'])
                         
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size']< float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_open_long_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                       'swap_price': swap_price,      #买在best价
                                                                       'swap_size': swap_size,
                                                                        'swap_order_type': 'post_only'}

                            if open_short_final_open_mode == 'on':  # swap继续开仓开多,swap_position>0,spot_balance<0
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_short_final_open_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] +Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                             #   sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                       Necessary_info[h]['contract_val']) >= Necessary_info[h]['swap_min_size'] * Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                               Necessary_info[h]['contract_val']) / Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_size = int(abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                            Necessary_info[h]['contract_val']) / Necessary_info[h]['contract_val'])
                                    else:
                                        swap_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_size = int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著开多在对手价, 0607改成挂在中间价

                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_open_short_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                       'swap_price': swap_price,      #买在best价
                                                                       'swap_size': swap_size,
                                                                        'swap_order_type': 'post_only'}

                            if open_long_final_close_mode == 'on':  # swap仓位多于spot,要平空,swap_position<0,spot_balance>0
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_long_final_close_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] +Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                            #    sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                       Necessary_info[h]['contract_val']) >= Necessary_info[h]['swap_min_size'] * \
                                        Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                               Necessary_info[h]['contract_val']) / Necessary_info[h][
                                               'contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_size = int(abs(
                                            Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                            Necessary_info[h]['contract_val']) / Necessary_info[h]['contract_val'])
                                    else:
                                        swap_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_size = int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著平空/买在对手价, 0607改成挂在中间价

                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_open_long_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                   'swap_price': swap_price,  # 买在best价
                                                                   'swap_size': swap_size,
                                                                   'swap_order_type': 'post_only'}

                            if open_short_final_close_mode == 'on':  # swap仓位多于spot,要平多,swap_position>0,spot_balance<0
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount,swap是open_short_final_close_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] +Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                            #    sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                       Necessary_info[h]['contract_val']) >= Necessary_info[h]['swap_min_size'] * \
                                        Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                               Necessary_info[h]['contract_val']) / Necessary_info[h][
                                               'contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_size = int(abs(
                                            Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] *
                                            Necessary_info[h]['contract_val']) / Necessary_info[h]['contract_val'])
                                    else:
                                        swap_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_size = int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著平多/卖在对手价, 0607改成挂在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_open_short_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                   'swap_price': swap_price,  # 买在best价
                                                                   'swap_size': swap_size,
                                                                   'swap_order_type': 'post_only'}

                            if close_long_final_close_mode == 'on':  # spot平多,swap继续平空a买在对手价,
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_long_final_close_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                            #    sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])>= Necessary_info[h]['swap_min_size']* Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_close_size=int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val'])
                                    else:
                                        swap_close_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_close_size=int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著平空/买在对手价, 0607改成挂在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_close_long_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                    'swap_close_price': swap_price,  # 买在best价
                                                                    'swap_close_size': swap_close_size,
                                                                    'swap_order_type': 'post_only'}


                            if close_short_final_close_mode == 'on':  # swap继续平多,
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_short_final_close_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                            #    sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])>= Necessary_info[h]['swap_min_size']* Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_close_size=int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val'])
                                    else:
                                        swap_close_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_close_size=int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著平多/卖在对手价, 0607改成挂在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_close_short_final_close_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                    'swap_close_price': swap_close_price,  # 买在best价
                                                                    'swap_close_size': swap_close_size,
                                                                    'swap_order_type': 'post_only'}

                            if close_long_final_open_mode == 'on':  # swap继续开空,
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_final_open_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                             #   sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])>= Necessary_info[h]['swap_min_size']* Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_close_size=int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val'])
                                    else:
                                        swap_close_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_close_size=int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著开空/卖在对手价, 0607改成挂在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best ask
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0])), Necessary_info[h]['swap_tick_digit'])

                                mode_take_close_long_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                    'swap_close_price': swap_close_price,  # 买在best价
                                                                    'swap_close_size': swap_close_size,
                                                                    'swap_order_type': 'post_only'}

                            if close_short_final_open_mode == 'on':  # swap开多,
                                tutu = Necessary_info[h]['swap_instrument_id'] + ":" + 'spot已接近Target_Amount_Close,swap是close_final_open_mode,现在相差' + \
                                       str((Operation_info[h]['spot_balance'] + Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val']) * swap_present_price) + '美金'
                            #    sendmessage(tutu)
                                if abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])>= Necessary_info[h]['swap_min_size']* Necessary_info[h]['contract_val']:
                                    if int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val']) > int(Necessary_info[h]['swap_min_size']):
                                        swap_close_size=int(abs(Operation_info[h]['spot_balance']+Operation_info[h]['swap_position'] * Necessary_info[h]['contract_val'])/Necessary_info[h]['contract_val'])
                                    else:
                                        swap_close_size = int(Necessary_info[h]['swap_min_size'])
                                else:
                                    swap_close_size=int(Necessary_info[h]['swap_min_size'])
                                # 因为成交慢,试著开多/买在对手价, 0607改成挂在中间价
                                #如果swap best ask-best bid>1个tick_size,表示可以挂,不然就要挂best bid
                                if float(Operation_info[h]['swap_depth5'][0]['bids'][0][0]) + Necessary_info[h]['swap_tick_size'] < float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]):
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['asks'][0][0]) + float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])) / 2, Necessary_info[h]['swap_tick_digit'])
                                else:
                                    swap_close_price = precfloat((float(Operation_info[h]['swap_depth5'][0]['bids'][0][0])), Necessary_info[h]['swap_tick_digit'])
                                mode_take_close_short_final_open_order = {'swap_instrument_id': Necessary_info[h]['swap_instrument_id'],
                                                                    'swap_close_price': swap_close_price,  # 买在best价
                                                                    'swap_close_size': swap_close_size,
                                                                    'swap_order_type': 'post_only'}
                            #print('funding_rate_result_1',funding_rate_result)

                            if open_long_mode == 'on':
                                # 下单,买spot,卖swap
                                spot_order_result, swap_order_result = take_long_order(mode_take_long_order)
                                time.sleep(0.1)
                                if spot_order_result == 'none':
                                    Operation_info[h]['spot_pending_list_left'] = 'off'
                                elif spot_order_result != 'none':
                                    if int(spot_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['spot_pending_list_left'] = 'on'
                                        #记录买进spot几单了
                                        Operation_info[h]['spot_buy_trading_orders']=Operation_info[h]['spot_buy_trading_orders']+1

                                    else:
                                        Operation_info[h]['spot_pending_list_left'] = 'off'                             
                                
                                if swap_order_result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif swap_order_result !='none':
                                    if int(swap_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['swap_pending_list_left'] = 'on'
                                        #记录卖出swap几单
                                        Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                    else:
                                        Operation_info[h]['swap_pending_list_left'] = 'off'
                         #   if h=='SWRV-USDT':
                         #       print('open_long_spot_order_result',spot_order_result)
                         #       print('open_long_swap_order_result',swap_order_result)
                         #       print('spot_pending_list_left_open_long',Operation_info[h]['spot_pending_list_left'])
                         #       print('swap_pending_list_left_open_long', Operation_info[h]['swap_pending_list_left'])

                            if open_short_mode == 'on':
                                # 下单,买swap,卖spot
                                spot_order_result, swap_order_result = take_short_order(mode_take_short_order)
                                time.sleep(0.1)
                                if spot_order_result == 'none':
                                    Operation_info[h]['spot_pending_list_left'] = 'off'
                                elif spot_order_result != 'none':
                                    if int(spot_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['spot_pending_list_left'] = 'on'
                                        #记录卖出spot几单了
                                        Operation_info[h]['spot_sell_trading_orders']=Operation_info[h]['spot_sell_trading_orders']+1
                                    else:
                                        Operation_info[h]['spot_pending_list_left'] = 'off'                             
                                
                                if swap_order_result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif swap_order_result !='none':
                                    if int(swap_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['swap_pending_list_left'] = 'on'
                                        #记录买进swap几单了
                                        Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                    else:
                                        Operation_info[h]['swap_pending_list_left'] = 'off'

                                #print('open_short_spot_order_result', spot_order_result)
                                #print('open_short_swap_order_result', swap_order_result)
                                #print('spot_pending_list_left_open_long', Operation_info[h]['spot_pending_list_left'])
                                #print('swap_pending_list_left_open_long', Operation_info[h]['swap_pending_list_left'])


                            if close_long_mode == 'on':
                                #卖spot,买swap
                                spot_close_order_result, swap_close_order_result = take_close_long_order(mode_take_close_long_order)
                                time.sleep(0.1)
                                if spot_close_order_result == 'none':
                                    Operation_info[h]['spot_pending_list_left'] = 'off'
                                elif spot_close_order_result != 'none':
                                    if int(spot_close_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['spot_pending_list_left'] = 'on'
                                        #记录卖出spot几单
                                        Operation_info[h]['spot_sell_trading_orders']=Operation_info[h]['spot_sell_trading_orders']+1
                                    else:
                                        Operation_info[h]['spot_pending_list_left'] = 'off'                             
                                
                                if swap_close_order_result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif swap_close_order_result !='none':
                                    if int(swap_close_order_result['data'][0]['sCode'])==0:
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
                                    if int(spot_close_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['spot_pending_list_left'] = 'on'
                                        #记录买进spot几单
                                        Operation_info[h]['spot_buy_trading_orders']=Operation_info[h]['spot_buy_trading_orders']+1
                                    else:
                                        Operation_info[h]['spot_pending_list_left'] = 'off'                             
                                
                                if swap_close_order_result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif swap_close_order_result !='none':
                                    if int(swap_close_order_result['data'][0]['sCode'])==0:
                                        Operation_info[h]['swap_pending_list_left'] = 'on'
                                        #记录卖出swap几单
                                        Operation_info[h]['swap_sell_trading_orders']=Operation_info[h]['swap_sell_trading_orders']+1
                                    else:
                                        Operation_info[h]['swap_pending_list_left'] = 'off'

                                #print('spot_close_short_order_result_334', spot_close_order_result)
                                #print('swap_close_short_order_result_334', swap_close_order_result)
                                #print('spot_pending_list_left_open_long_334', Operation_info[h]['spot_pending_list_left'])
                                #print('swap_pending_list_left_open_long_334', Operation_info[h]['swap_pending_list_left'])


                            if open_long_final_open_mode == 'on':
                                #下單,卖swap
                                result = take_open_long_final_open_order(mode_take_open_long_final_open_order)
                                time.sleep(0.1)
                                #print('mode_take_open_long_final_open_order',mode_take_open_long_final_open_order)
                                if result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif result != 'none':
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
                                        Operation_info[h]['swap_pending_list_left'] = 'on'
                                        #记录买进swap几单
                                        Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                    else:   
                                        Operation_info[h]['swap_pending_list_left'] = 'off'
                                Operation_info[h]['spot_pending_list_left'] = 'off'

                            if open_long_final_close_mode == 'on':
                                # 下单,买swap
                                result = take_open_long_final_close_order(mode_take_open_long_final_close_order)
                                time.sleep(0.1)
                                if result == 'none':
                                    Operation_info[h]['swap_pending_list_left'] = 'off'
                                elif result != 'none':
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
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
                                    if int(result['data'][0]['sCode'])==0:
                                        Operation_info[h]['swap_pending_list_left'] = 'on'
                                        #记录买进swap几单
                                        Operation_info[h]['swap_buy_trading_orders'] = Operation_info[h]['swap_buy_trading_orders']+1
                                    else:   
                                        Operation_info[h]['swap_pending_list_left'] = 'off'
                                Operation_info[h]['spot_pending_list_left'] = 'off'

                          #  print('h12321',h)
                           # print('swap_pending_list_left_final',Operation_info[h]['swap_pending_list_left'])
                           # print('spot_pending_list_left_final',Operation_info[h]['spot_pending_list_left'])
                         #  print('swap_depth5_12',Operation_info[h]['swap_depth5'])

                            # 更新新的swap五档
                            Operation_info[h]['swap_bids_price5'] = []
                            Operation_info[h]['swap_asks_price5'] = []
                            for i in range(5):
                                Operation_info[h]['swap_bids_price5'].append(float(Operation_info[h]['swap_depth5'][0]['bids'][i][0]))
                                Operation_info[h]['swap_asks_price5'].append(float(Operation_info[h]['swap_depth5'][0]['asks'][i][0]))
                            # 更新新的spot五档
                            Operation_info[h]['spot_bids_price5'] = []
                            Operation_info[h]['spot_asks_price5'] = []
                            for i in range(5):
                                Operation_info[h]['spot_bids_price5'].append(float(Operation_info[h]['spot_depth5'][0]['bids'][i][0]))
                                Operation_info[h]['spot_asks_price5'].append(float(Operation_info[h]['spot_depth5'][0]['asks'][i][0]))


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
                                                                    
                                # get_fills_result = tradeAPI.get_fills(limit=100)

                                    for h in TradingPair:
                                        #先找spot成交記錄
                                        spot_fills_result = tradeAPI.get_fills(instId=h)
                                        time.sleep(0.1)
                                        for i in spot_fills_result['data']:
                                            #先算交易费
                                            Operation_info[h]['spot_trading_fee']=Operation_info[h]['spot_trading_fee']+float(i['fee'])*float(i['fillPx'])  #它现货的反佣跟手续费是用它的币,要换算成usdt                                            
                                            timestamp=int(int(i['ts'])/1000)   
                                            #转换成localtime
                                            time_local = time.localtime(timestamp)
                                            #转换成新的时间格式(2016-05-05 20:28:54)
                                            dt =time.strftime("%Y-%m-%d %H:%M:%S",time_local)
                                            ddt=parse(dt)
                                            #先选日期段
                                            if ddt.day== new_record_day-1:
                                                if i['side']=='sell':
                                                    Operation_info[h]['spot_sell_trading_net_amount']=Operation_info[h]['spot_sell_trading_net_amount']+float(i['fillSz'])*float(i['fillPx'])  
                                                    Operation_info[h]['spot_trading_sell_size']=Operation_info[h]['spot_trading_sell_size']+float(i['fillSz']) 

                                                elif i['side']=='buy':
                                                    Operation_info[h]['spot_buy_trading_net_amount']=Operation_info[h]['spot_buy_trading_net_amount']+float(i['fillSz'])*float(i['fillPx'])  
                                                    Operation_info[h]['spot_trading_buy_size']=Operation_info[h]['spot_trading_buy_size']+float(i['fillSz']) 
                                        
                                        #swap成交记录                                    
                                        swap_instrument_id=h+'-SWAP'
                                        swap_fills_result = tradeAPI.get_fills(instId= swap_instrument_id)
                                        time.sleep(0.1)
                                        for i in swap_fills_result['data']:
                                            #先算交易费
                                            Operation_info[h]['swap_trading_fee']=Operation_info[h]['swap_trading_fee']+float(i['fee'])    #它u本位合约的反佣跟手续费是用usdt,直接用
                                            timestamp=int(int(i['ts'])/1000)   
                                            #转换成localtime
                                            time_local = time.localtime(timestamp)
                                            #转换成新的时间格式(2016-05-05 20:28:54)
                                            dt =time.strftime("%Y-%m-%d %H:%M:%S",time_local)
                                            ddt=parse(dt)
                                            #先选日期段
                                            if ddt.day== new_record_day-1:
                                                if i['side']=='sell':
                                                    Operation_info[h]['swap_sell_trading_net_amount']=Operation_info[h]['swap_sell_trading_net_amount']+float(i['fillSz'])*float(i['fillPx'])*Necessary_info[h]['contract_val'] 
                                                    Operation_info[h]['swap_trading_sell_size']=Operation_info[h]['swap_trading_sell_size']+float(i['fillSz'])*Necessary_info[h]['contract_val'] 
                                                elif i['side']=='buy':
                                                    Operation_info[h]['swap_buy_trading_net_amount']=Operation_info[h]['swap_buy_trading_net_amount']+float(i['fillSz'])*float(i['fillPx'])*Necessary_info[h]['contract_val']
                                                    Operation_info[h]['swap_trading_buy_size']=Operation_info[h]['swap_trading_buy_size']+float(i['fillSz'])*Necessary_info[h]['contract_val']            

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


                                # Operation_info[h]['swap_sell_trading_orders']
                                # Operation_info[h]['swap_buy_trading_orders']
                                # Operation_info[h]['spot_sell_trading_orders']
                                # Operation_info[h]['spot_buy_trading_orders']                                                                      
                                    time.sleep(30)                         

                    except (asyncio.TimeoutError, websockets.exceptions.ConnectionClosed) as e:
                        try:
                            await ws.send('ping')
                            res = await ws.recv()
                            print(res)
                            continue
                        except Exception as e:
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


# unsubscribe channels
async def unsubscribe(url, api_key, passphrase, secret_key, channels):
    async with websockets.connect(url) as ws:
        # login
        timestamp = str(get_local_timestamp())
        login_str = login_params(timestamp, api_key, passphrase, secret_key)
        await ws.send(login_str)
        # print(f"send: {login_str}")

        res = await ws.recv()
        print(f"recv: {res}")

        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(f"send: {sub_str}")

        res = await ws.recv()
        print(f"recv: {res}")


# unsubscribe channels
async def unsubscribe_without_login(url, channels):
    async with websockets.connect(url) as ws:
        # unsubscribe
        sub_param = {"op": "unsubscribe", "args": channels}
        sub_str = json.dumps(sub_param)
        await ws.send(sub_str)
        print(f"send: {sub_str}")

        res = await ws.recv()
        print(f"recv: {res}")

def run_ws(api_key,secret_key,passphrase, channels,TT,MQ,AQ,TradingPair,param_set_list):
  #  pd.set_option('display.max_columns', None)  # 显示完整的列
  #  pd.set_option('display.max_rows', None)  # 显示完整的行
  #  pd.set_option('display.expand_frame_repr', False)  # 设置不折叠数据

    flag = '0'

    # WebSocket公共频道
    # 实盘
    url_public = "wss://ws.okex.com:8443/ws/v5/public"
    # 实盘
    url_private = "wss://ws.okex.com:8443/ws/v5/private"
    loop = asyncio.get_event_loop()
    # 公共频道 不需要登录（行情，持仓总量，K线，标记价格，深度，资金费率等）
    if TT=="public":
        loop.run_until_complete(subscribe_without_login(url_public, channels,MQ,TradingPair))
    # 私有频道 需要登录（账户，持仓，订单等）
    if TT=="private":
        loop.run_until_complete(subscribe(url_private, api_key, passphrase, secret_key, channels,AQ,TradingPair))
    # 交易（下单，撤单，改单等）
    if TT== "trade":
        loop.run_until_complete(trade(url_private, api_key, passphrase, secret_key, MQ,AQ,TradingPair,param_set_list))
    loop.close()

if __name__=='__main__':
    #necessary factors for restAPI
    #flag = '1'
    #
    api_key = ""
    secret_key = ""
    passphrase = ""
    
    flag = '0'
    publicAPI = Public.PublicAPI(api_key, secret_key, passphrase, False, flag)
    tradeAPI = Trade.TradeAPI(api_key, secret_key, passphrase, False, flag)
    accountAPI = Account.AccountAPI(api_key, secret_key, passphrase, False, flag)
    marketAPI = Market.MarketAPI(api_key, secret_key, passphrase, False, flag)

    TradingPair ={}
    Customer_name=''
    total_balance_hour = precfloat(accountAPI.get_account()['data'][0]['totalEq'], 3)  #这是u
    #total_balance = precfloat(accountAPI.get_account()['data'][0]['totalEq'], 3)

    #一开始变数设置的位置
    MarketQ = {}
    for i in TradingPair:
        MarketQ[i] = {"DEPTH5_SWAP": "", "DEPTH5_SPOT":"", "PRESENT_FUNDING": "","PREDICT_FUNDING":"","P_INFO_SWAP":'',"P_INFO_SPOT":'','INDEX_TICKERS_SPOT':'','INDEX_TICKERS_SWAP':'','MARK_PRICE_SWAP':''}
        for j in MarketQ[i]:
            MarketQ[i][j] = Queue()

    AccountQ = {}
    for i in TradingPair:
        AccountQ[i] = {"POSITION_SWAP": "", "POSITION_SPOT": "","POSITION_MARGIN": "",'ORDERS_SWAP':"",'ORDERS_SPOT':"",'ORDERS_MARGIN':""}
        for j in AccountQ[i]:
            AccountQ[i][j] = Queue()

    #funding rate
    Nowtime = datetime.now()
    record_minute = Nowtime.minute
    record_hour = Nowtime.hour
    
    Necessary_info = {}
    Operation_info = {}  
    for i in TradingPair:  
        Necessary_info[i] = {}
        Operation_info[i] = {}  

    for i in TradingPair:      
        time.sleep(1)
        instId_fu, Present_Funding_Rate,Predict_Funding_Rate = funding_recalculate(i+'-SWAP')
        instr_fu = instId_fu.split('-')        #instrument_id,利用来编排名称位置
        TP = instr_fu[0] + "-" + instr_fu[1]
        if len(instr_fu)==3:            #only swap gets funding rate, so 3
            if MarketQ[TP]["PRESENT_FUNDING"].empty() == True:
                MarketQ[TP]["PRESENT_FUNDING"].put(Present_Funding_Rate)
            elif MarketQ[TP]["PRESENT_FUNDING"].empty() == False:
                MarketQ[TP]["PRESENT_FUNDING"].get()
                MarketQ[TP]["PRESENT_FUNDING"].put(Present_Funding_Rate)    
       
        swap_info = publicAPI.get_instruments('SWAP')['data']
        time.sleep(0.2)
        # print(swap_info)
        for j in swap_info:
            if j['instId'] == i + '-SWAP':  # need to notice here's insId (SWAP)
                Necessary_info[i]['swap_tick_size'] = float(j["tickSz"])
                Necessary_info[i]['swap_tick_digit'] = np.log10(1 / float(j["tickSz"]))
                Necessary_info[i]['contract_val'] = float(j['ctVal'])
                Necessary_info[i]['swap_min_size'] = float(j['minSz'])
                

        spot_info = publicAPI.get_instruments('SPOT')['data']
        time.sleep(0.2)
        for j in spot_info:
            if j['instId'] == i :  # need to notice here's insId (SWAP)
                Necessary_info[i]['spot_tick_size'] = float(j["tickSz"])
                Necessary_info[i]['spot_tick_digit'] = np.log10(1 / float(j["tickSz"]))
                Necessary_info[i]['spot_min_size'] = float(j['minSz'])
    
    param_set_list = {'maker_commission_spot':-0.00005,
                      'taker_commission_spot':0.0002,
                      'maker_commission_swap':-0.00001,
                      "taker_commission_swap":0.00022,
                      'tolerate_limit':1300,
                      'order_limit':500,       #每次开仓量
                      'close_short_index' : 2   #平仓要几倍的出手量（相对于开仓）
                      }

    channel_private = [{"channel": "positions", "instType": "MARGIN"},
                       {"channel": "positions", "instType": "SWAP"},
                       {"channel": "orders", "instType": "SPOT"},
                       {"channel": "orders", "instType": "MARGIN"},
                       {"channel": "orders", "instType": "SWAP"},
                       {"channel": "account"}
                       ]

    channel_public = []
    for h in TradingPair:
        spot_instrument_id = h
        swap_instrument_id = h + '-SWAP'
        # 订阅spot的指数行情
        key_list = []
        value_list = []
        key_list = ["channel", "instId"]
        value_list = ["index-tickers", spot_instrument_id]
        spot_order_param = dict(zip(key_list, value_list))
        channel_public.append(spot_order_param)
        #订阅spot的深度五档
        key_list = []
        value_list = []
        key_list = ["channel", "instId"]
        value_list = ["books5", spot_instrument_id]
        spot_order_param = dict(zip(key_list, value_list))
        channel_public.append(spot_order_param)
        #订阅Swap的深度五档
        key_list = []
        value_list = []
        key_list = ["channel", "instId"]
        value_list = ["books5", swap_instrument_id]
        swap_order_param = dict(zip(key_list, value_list))
        channel_public.append(swap_order_param)
        # 订阅Swap的资金费率
        key_list = []
        value_list = []
        key_list = ["channel", "instId"]
        value_list = ["funding-rate", swap_instrument_id]
        swap_order_param = dict(zip(key_list, value_list))
        channel_public.append(swap_order_param)

    channel_trade=[]
    ws1 = Process( target=run_ws, args=(api_key,secret_key,passphrase, channel_private,"private",MarketQ,AccountQ,TradingPair,param_set_list))
    ws2 = Process( target=run_ws, args=(api_key,secret_key,passphrase, channel_public,"public",MarketQ,AccountQ,TradingPair,param_set_list))
    wst = Process( target=run_ws, args=(api_key,secret_key,passphrase, channel_trade,"trade",MarketQ,AccountQ,TradingPair,param_set_list))

    ws1.start()
    ws2.start()
    wst.start()

    while True:
       # time.sleep(1)
        try:           
            Nowtime = datetime.now()
            new_record_second = Nowtime.second
            new_record_minute = Nowtime.minute
            new_record_hour = Nowtime.hour
                     

            for i in TradingPair:                           
                Operation_info[i]['spot_bids_price5_forc'] = []
                Operation_info[i]['spot_asks_price5_forc'] = []                 
                try:
                    Operation_info[i]['spot_depth5'] = MarketQ[i]["DEPTH5_SPOT"].get(timeout=1)
                except:                    
                    try:                            
                        Operation_info[i]['spot_depth5'] = Operation_info[i]['spot_depth5']
                    except:
                        Operation_info[i]['spot_depth5'] = marketAPI.get_orderbook(i , '5')['data']
                        print('我spot_depth5用restapi__9')
                        time.sleep(0.1)
                
                for j in range(5):
                    Operation_info[i]['spot_bids_price5_forc'].append(float(Operation_info[i]['spot_depth5'][0]['bids'][j][0]))
                    Operation_info[i]['spot_asks_price5_forc'].append(float(Operation_info[i]['spot_depth5'][0]['asks'][j][0]))
                new_spot_bid=float(Operation_info[i]['spot_bids_price5_forc'][0])
                new_spot_ask=float(Operation_info[i]['spot_asks_price5_forc'][0])
                spot_present_price = precfloat((new_spot_ask + new_spot_bid)/2,Necessary_info[i]['swap_tick_digit'])
                
                instr_fu = i.split('-')
                spot_currency=instr_fu[0]   
                
                try:
                    swap_dic = AccountQ[i]["POSITION_SWAP"].get(timeout=1)
                    Necessary_info[i]['swap_position_result'] = float(swap_dic['pos'])
                    if Necessary_info[i]['swap_position_result'] ==0:  
                        Necessary_info[i]['swap_position_result'] = float(accountAPI.get_positions(instId=i+'-SWAP')['data'][0]['pos'])            
                        time.sleep(0.2)
            #        Operation_info[i]['swap_position'] = float(swap_dic['pos'])
            #    except:
            #        total_swap_dic = accountAPI.get_positions('SWAP')['data']
                except:   
                    try:                  
                        Necessary_info[i]['swap_position_result'] = float(accountAPI.get_positions(instId=i+'-SWAP')['data'][0]['pos'])            
                        time.sleep(0.2)
                    except:
                        Necessary_info[i]['swap_position_result'] = 0
                
                try:
                    spot_dic = AccountQ[i]["POSITION_SPOT"].get(timeout=1)
                   # print('spot_dic_1',spot_dic)
                    Necessary_info[i]['spot_balance_result']  = float(spot_dic['cashBal'])
                    if Necessary_info[i]['spot_balance_result'] ==0:  
                        Necessary_info[i]['spot_balance_result'] = float(accountAPI.get_account(instr_fu[0])['data'][0]['details'][0]['cashBal'])
                        time.sleep(0.5)
                except:
                    total_spot_dic = accountAPI.get_account()['data'][0]['details']
                    time.sleep(0.5)
                    spot_cc_list=[]
                    if len(total_spot_dic)!=0:
                        for j in total_spot_dic:
                            TP=j['ccy']+'-USDT'
                            spot_cc_list.append(TP)      
                        if i in spot_cc_list:
                            instr_fu = i.split('-')        
                            for j in total_spot_dic:
                                if j['ccy']==instr_fu[0]:
                                    Necessary_info[i]['spot_dic'] = j
                                    spot_dic = Necessary_info[i]['spot_dic']
                                    Necessary_info[i]['spot_balance_result'] = float(spot_dic['cashBal'])                  
                        else:
                            Necessary_info[i]['spot_balance_result']=0          
                    else:
                        Necessary_info[i]['spot_balance_result']=0    
                
                compare_cal=Necessary_info[i]['swap_position_result']*Necessary_info[i]['contract_val']+Necessary_info[i]['spot_balance_result']   
                #因为一正一负,所以这里用加的
                if Necessary_info[i]['spot_balance_result'] >0:  #开正仓                    
                    if compare_cal*spot_present_price>500:       
                        tutu=timestamp +','+i+'开正仓,现货比永续多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3)    
                    elif compare_cal*spot_present_price<-500:       
                        tutu=timestamp +','+i+'开正仓,永续比现货多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3)
                elif Necessary_info[i]['spot_balance_result'] <0:  #开负仓 
                    if compare_cal*spot_present_price>500:       
                        tutu=timestamp +','+i+'开负仓,永续比现货多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3)    
                    elif compare_cal*spot_present_price<-500:       
                        tutu=timestamp +','+i+'开负仓,现货比永续多'+ str(precfloat(abs(compare_cal*spot_present_price),3))+'美金'
                        sendmessage(tutu)
                        time.sleep(3) 

            if new_record_hour != record_hour:
                # 母帐户U本位,U的总资产
                account_asset = precfloat(accountAPI.get_account()['data'][0]['totalEq'], 3)
                timestamp = datetime.now().isoformat(" ", "seconds")

                #换算成eth
                result = marketAPI.get_ticker('ETH-USDT')
                eth_price = (float(result['data'][0]['askPx']) + float(result['data'][0]['bidPx'])) / 2

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

                record_hour = new_record_hour


                if new_record_hour == 9 :
                    if new_record_minute == 0:
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
                        time.sleep(60)

                time.sleep(5)                
                #系统重启                
   #             tutu = timestamp + ",机器人重启"
   #             sendmessage(tutu)

            #每半小时重启一次机器人
   #         if new_record_minute == 30:
   #             if new_record_second > 55:

#                    ws1.terminate()
#                    ws2.terminate()
#                    wst.terminate()
#                    restart_program()
#                    record_hour = new_record_hour  #这行可能没用

    

#            if new_record_hour == 9:
#                if new_record_minute == 3:
                    # 母帐户U本位
#                    account_asset = precfloat(accountAPI.get_account()['data'][0]['totalEq'], 3)
#                    timestamp = datetime.now().isoformat(" ", "seconds")

                    # 换算成eth
#                    result = marketAPI.get_ticker('ETH-USDT')
#                    eth_price = (float(result['data'][0]['askPx']) + float(result['data'][0]['bidPx'])) / 2

#                    new_total_balance = account_asset/eth_price
#                    day_profit = precfloat(new_total_balance - total_balance, 2)
#                    day_profit_percent = precfloat(day_profit / total_balance * 100, 2)

#                    tutu = timestamp + ",目前总资产为" + str(new_total_balance) + '个eth'
#                    sendmessage_to_customer(tutu)

#                    tutu = timestamp + ",单日获利为" + str(day_profit) + '个eth'
#                    sendmessage_to_customer(tutu)

#                    tutu = timestamp + ",单日获利率为" + str(day_profit_percent) + '%'
#                    sendmessage_to_customer(tutu)

#                    total_balance = new_total_balance

#                    mapping = {}
#                    kk = []
#                    path = '/root/' + Customer_name + '_profit_report.csv'
#                    key_list = ['timestamp', '单日获利（eth）', '单日获利率（%）', '总资产净值（eth）']
#                    for key, value in zip(key_list, [timestamp, day_profit, day_profit_percent, total_balance]):
#                        mapping[key] = value
#                    kk.append(eval(json.dumps(mapping)))
#                    kkk = pd.DataFrame(kk)
#                    kkk.to_csv(path, mode='a+', header=True, index=False)
#                    time.sleep(60)

     #       if new_record_minute % 10 ==0 :
     #           print(Nowtime)
     #           time.sleep(60)

            if new_record_minute % 20 ==0 :
                timestamp = datetime.now().isoformat(" ", "seconds")
                tutu = timestamp + Customer_name+"机器人还活著"
             #   sendmessage(tutu)
                time.sleep(61)
                #record_minute = new_record_minute


            #每八小时计算一次历史资金费率
            if Nowtime.hour == 8 or Nowtime.hour == 16 or Nowtime.hour == 0:
                if Nowtime.minute == 1:
                    for i in TradingPair:
                        instId_fu, Present_Funding_Rate,Predict_Funding_Rate = funding_recalculate(i+'-SWAP')
                        instr_fu = instId_fu.split('-')
                        TP = instr_fu[0] + "-" + instr_fu[1]
                        if len(instr_fu) == 3:  # only swap gets funding rate, so 3
                            if MarketQ[TP]["PRESENT_FUNDING"].empty() == True:
                                MarketQ[TP]["PRESENT_FUNDING"].put(Present_Funding_Rate)
                            elif MarketQ[TP]["PRESENT_FUNDING"].empty() == False:
                                MarketQ[TP]["PRESENT_FUNDING"].get()
                                MarketQ[TP]["PRESENT_FUNDING"].put(Present_Funding_Rate)
        except:
            pass



#    ws1.join()
#    ws2.join()
#    wst.join()
