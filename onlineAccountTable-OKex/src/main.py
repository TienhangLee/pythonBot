from flask import Flask
from flask import render_template, jsonify
import okex.Public_api as Public
import okex.Trade_api as Trade
import okex.Account_api as Account
import okex.Market_api as Market
import okex.Funding_api as Funding
import time
import math
import datetime
from datetime import datetime,timedelta 
import asyncio



def precfloat(num,digi):
    return int(round(float(num)*math.pow(10,digi)))/math.pow(10,digi)


app = Flask(__name__)  

API_info=[{"Customer_id":'MatrixPort',"api_key" : "","secret_key" : "","passphrase" : ""}]
flag = '0'



@app.route('/data_json_2')
async def data_json():
    Nowtime = datetime.now().replace(microsecond=0)
    Nowtime=Nowtime+timedelta(hours=8)
  #  dummy_data=[]
    flag = '0'
    
    Customer_id=API_info[0]['Customer_id']
    api_key=API_info[0]['api_key']
    secret_key=API_info[0]['secret_key']
    passphrase=API_info[0]['passphrase']
    accountAPI = Account.AccountAPI(api_key, secret_key, passphrase, False, flag)
    fundingAPI = Funding.FundingAPI(api_key, secret_key, passphrase, False, flag)
    publicAPI = Public.PublicAPI(api_key, secret_key, passphrase, False, flag)

    time.sleep(0.5)
    account_result = accountAPI.get_account()
    time.sleep(0.5)
    notionalUsd=precfloat(float(account_result['data'][0]['notionalUsd']),2)    #仓位美金价值
    mgnRatio=precfloat(float(account_result['data'][0]['mgnRatio'])*100,2)   #全仓保证金率＊100

    asset_valuation_result = fundingAPI.get_asset_valuation(ccy = 'USDT')
    time.sleep(0.5)
    totalBal=precfloat(float(asset_valuation_result['data'][0]['totalBal']),2)   #总资产估值

    total_next_swap_earn=0
    total_present_swap_earn=0
    position_result = accountAPI.get_positions('SWAP')
    time.sleep(0.5)
    for j in position_result['data']:
        if j['ccy']=='USDT': #只看U本位
            positon_value=float(j['lever'])*float(j['imr'])     #仓位金额

            funding_rate_result = publicAPI.get_funding_rate(j['instId'])
            time.sleep(0.5)
            next_funding_rate=float(funding_rate_result['data'][0]['nextFundingRate'])
            present_funding_rate=float(funding_rate_result['data'][0]['fundingRate'])

            if float(j['pos'])<0: #正仓
                next_swap_earn=positon_value*next_funding_rate
                present_swap_earn=positon_value*present_funding_rate
            elif float(j['pos'])>0:#负仓
                next_swap_earn=-1*positon_value*next_funding_rate
                present_swap_earn=-1*positon_value*present_funding_rate

            total_next_swap_earn=total_next_swap_earn+next_swap_earn
            total_present_swap_earn=total_present_swap_earn+present_swap_earn

    total_next_swap_earn = precfloat(total_next_swap_earn,2)     #因为资金费率跟仓位的关系
    total_present_swap_earn = precfloat(total_present_swap_earn,2)
    present_earn_percent = precfloat(total_present_swap_earn/totalBal*100,3)

    key_list=["id","总资产估值","建仓美金价值","全仓保证金率",'当期资费收益','当期资费收益率','下期资费预估收益',"Timestamp"]
    value_list=[Customer_id,str(totalBal),str(notionalUsd),str(mgnRatio),str(total_present_swap_earn),str(present_earn_percent), str(total_next_swap_earn),str(Nowtime)]
    mapping = dict(zip(key_list, value_list))
    dummy_data = mapping
  
    return dummy_data

@app.route('/data_json_1')
async def data_json_1():
    Nowtime = datetime.now().replace(microsecond=0)
    Nowtime=Nowtime+timedelta(hours=8)
   # dummy_data=[]
    flag = '0'
    
    Customer_id=API_info[1]['Customer_id']
    api_key=API_info[1]['api_key']
    secret_key=API_info[1]['secret_key']
    passphrase=API_info[1]['passphrase']
    accountAPI = Account.AccountAPI(api_key, secret_key, passphrase, False, flag)
    fundingAPI = Funding.FundingAPI(api_key, secret_key, passphrase, False, flag)
    publicAPI = Public.PublicAPI(api_key, secret_key, passphrase, False, flag)

    time.sleep(0.5)
    account_result = accountAPI.get_account()
    time.sleep(0.5)
    notionalUsd=precfloat(float(account_result['data'][0]['notionalUsd']),2)    #仓位美金价值
    mgnRatio=precfloat(float(account_result['data'][0]['mgnRatio'])*100,2)   #全仓保证金率＊100

    asset_valuation_result = fundingAPI.get_asset_valuation(ccy = 'USDT')
    time.sleep(0.5)
    totalBal=precfloat(float(asset_valuation_result['data'][0]['totalBal']),2)   #总资产估值

    total_next_swap_earn=0
    total_present_swap_earn=0
    position_result = accountAPI.get_positions('SWAP')
    time.sleep(0.5)
    for j in position_result['data']:
        if j['ccy']=='USDT': #只看U本位
            positon_value=float(j['lever'])*float(j['imr'])     #仓位金额

            funding_rate_result = publicAPI.get_funding_rate(j['instId'])
            time.sleep(0.5)
            next_funding_rate=float(funding_rate_result['data'][0]['nextFundingRate'])
            present_funding_rate=float(funding_rate_result['data'][0]['fundingRate'])

            if float(j['pos'])<0: #正仓
                next_swap_earn=positon_value*next_funding_rate
                present_swap_earn=positon_value*present_funding_rate
            elif float(j['pos'])>0:#负仓
                next_swap_earn=-1*positon_value*next_funding_rate
                present_swap_earn=-1*positon_value*present_funding_rate

            total_next_swap_earn=total_next_swap_earn+next_swap_earn
            total_present_swap_earn=total_present_swap_earn+present_swap_earn

    total_next_swap_earn = precfloat(total_next_swap_earn,2)     #因为资金费率跟仓位的关系
    total_present_swap_earn = precfloat(total_present_swap_earn,2)
    present_earn_percent = precfloat(total_present_swap_earn/totalBal*100,3)

    key_list=["id","总资产估值","建仓美金价值","全仓保证金率",'当期资费收益','当期资费收益率','下期资费预估收益',"Timestamp"]
    value_list=[Customer_id,str(totalBal),str(notionalUsd),str(mgnRatio),str(total_present_swap_earn),str(present_earn_percent), str(total_next_swap_earn),str(Nowtime)]
    mapping = dict(zip(key_list, value_list))
    dummy_data_1=mapping
   
    return dummy_data_1  

@app.route('/data_json')
async def main():
    Nowtime = datetime.now().replace(microsecond=0)
    Nowtime=Nowtime+timedelta(hours=8)
    print('start')
    print('now_time',Nowtime)
    print('main_start')
    dummy_data= []
   # dummy_data_1=data_json()
   # dummy_data_2=data_json_1()
    task_list=[data_json(),data_json_1()]
    print('here')
    result= await asyncio.gather(*task_list)
    dummy_data=result
   
  #  for i in result:
   # dummy_data.append(dummy_data_1)
   # dummy_data.append(dummy_data_2)
    #print('dummy_data',dummy_data)
    print('dummy_data',dummy_data)
    #task1 = asyncio.create_task(data_json())        
    #task2 = asyncio.create_task(data_json_1())
    dummy_data_result = jsonify(dummy_data)
    print(dummy_data_result)
    #dummy_data.append(print[])
    #dummy_data.append(task2)
    print('donedone')
    return dummy_data_result


@app.route('/')
async def index():
    dummy_data = asyncio.run(main())
    print('dumdumy_data',dummy_data)
    return render_template(
        'index.html',
        matches=dummy_data.json,
    )
    
	
if __name__ == "__main__":
	app.run()
