from flask import Flask
from flask import current_app as app
#from data_json import data_json
from flask import render_template, jsonify
import okex.Public_api as Public
import okex.Trade_api as Trade
import okex.Account_api as Account
import okex.Market_api as Market
import okex.Funding_api as Funding
import time
import math
import datetime
from multiprocessing import Process, Queue, Pool,Manager
import requests
import psutil
from datetime import datetime,timedelta 
import os,sys
import asyncio
import json
import nest_asyncio
nest_asyncio.apply()

def precfloat(num,digi):
    return int(round(float(num)*math.pow(10,digi)))/math.pow(10,digi)


@app.route('/')
async def index(dummy_data):    #现在应该是AQ这方面的问题
    
    return render_template('index.html',matches=dummy_data.json)