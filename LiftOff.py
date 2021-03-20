# -*- coding: utf-8 -*-
"""
Created on Mon Nov 23 16:20:10 2020

@author: Murali Subramanian
"""

import requests
from datetime import datetime, date, timedelta, timezone
from requests.auth import HTTPBasicAuth
import pandas as pd
import numpy as np
import csv
import logging
import time
logger = logging.getLogger()


user = '78a02376f9'
pwd = 'i6phZ6AsU1NVQRpAfqHL4g=='

yesterday = date.today() - timedelta(1)

#POST Request for generating the report
URL2 = 'https://data.liftoff.io/api/v1/reports'
today = date.today().strftime("%Y-%m-%d")+'T00:00:00.000Z'
yesterday = yesterday.strftime("%Y-%m-%d")+'T00:00:00.000Z'
data = {'start_time': yesterday,'end_time':today}

r = requests.post(URL2, json = data, 
                  headers = {'Content-Type': 'application/json'}, 
                  auth=HTTPBasicAuth(user, pwd))

#Obtaining the post request ID
r_json = r.json()
idRequest = r_json['id']

URL4 = 'https://data.liftoff.io/api/v1/reports/' + idRequest + '/status'
currentStatus = 'queued'
exponentialBackoffCoefficient = 1

while currentStatus == 'queued':
    statusResponse = requests.get(URL4,
                     auth=HTTPBasicAuth(user,pwd))
    currentStatus = statusResponse.json()['state']
    
    if currentStatus == 'queued':
        time.sleep(exponentialBackoffCoefficient)
        exponentialBackoffCoefficient *= 2
    elif currentStatus == 'cancelled':
        logging.info('The report has been cancelled')
        break
    elif currentStatus == 'failed':
        logging.info('The report has failed to generate.')
        break

#GET Request for the report
if currentStatus == 'completed':
    URL3 = 'https://data.liftoff.io/api/v1/reports/' + idRequest + '/data'
    
    output = requests.get(URL3, 
                     headers = {'Content-Type': 'application/json'},
                     auth=HTTPBasicAuth(user,pwd))
    
    #Output comes in a string format. Going through a few transformations on the data
    #to make it into a more readable DataFrame format
    outputSplit = output.text.splitlines()
    numRecords = len(outputSplit)-1
    
    tempList = []
    for i in range(numRecords):
        tempList.append(outputSplit[i+1].split(','))
        
    liftoffDF = pd.DataFrame(tempList, columns = outputSplit[0].split(','))

else:
    logging.info('No report created')