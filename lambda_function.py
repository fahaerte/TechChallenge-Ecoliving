import json
import os 
import boto3 
from botocore.config import Config
from Querymanager import Querymanager

print('Execute Lambada function')


def lambda_handler(event, context):
    
    # connect to Timestream
    session = boto3.Session()
    query_client = session.client('timestream-query')
    query_manager = Querymanager(query_client)
    
    
    # Get last entry as dict
    try:
        data = query_manager.get_last_entry()
        print(data)
        
        # Construct response
    
        allData = []
        rowData = {}
        rowData['Timestamp'] = data['time']
        rowData['CO2_Level'] = int(data['CO2_level'])
        rowData['Humidity'] = float(data['humidity'])
        rowData['Temperature'] = float(data['temperature'])
        allData.append(rowData)
        
        responseData = {}
        responseData['success'] = True
        responseData['data'] = allData
        
    except Exception as e:
        responseData = {}
        responseData['success'] = False
        responseData['Failure'] = str(e)
    
    
    # construct http response object
    responseObject = {}
    responseObject['statusCode'] = 200
    responseObject['headers'] = {}
    responseObject['headers']['Content-Type'] = 'application/json'
    responseObject['body'] = json.dumps(responseData, indent=4)
    
    return responseObject
