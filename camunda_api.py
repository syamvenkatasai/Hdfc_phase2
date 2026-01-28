import os
import argparse
import datetime
import json
import math
import random
import requests
import psutil
import time
import traceback
import shutil
import pandas as pd
import copy
import re
import pytz
tmzone = 'Asia/Kolkata'
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=5)


from flask import Flask, request, jsonify
from datetime import datetime
from flask_cors import CORS
from time import time as tt
from hashlib import sha256
from py_zipkin.zipkin import zipkin_span,ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from pathlib import Path

from db_utils import DB
from ace_logger import Logging
from .case_migration import *
#from producer import produce


from app import app
from PyPDF2 import PdfFileReader

logging = Logging(name='camunda_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def http_transport(encoded_span):
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'})


def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

def insert_into_audit(case_id, data):
    logging.info(f"{data} ##data")
    tenant_id = data.pop('TENANT_ID')
    logging.info(f"{tenant_id} ##data")
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'AUDIT_')
    return True

@app.route('/camunda_api_health_check', methods=['GET'])
def camunda_api_health_check():

    return jsonify({'flag':True})

def get_page_count(file_path):
    with open(file_path, "rb") as f:
        reader = PdfFileReader(f)
        num_pages=reader.numPages
    return num_pages

def extract_date(text):
    try:
        return pd.to_datetime(text, dayfirst=True, errors='raise')
    except:
        match = re.search(r'(\d{4})[./-](\d{2})[./-](\d{2})', text)
        if match:
            try:
                date_str = match.group(0).replace('.', '/').replace('-', '/')
                return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
            except:
                return pd.NaT
    return pd.NaT

def case_processing_cnt(case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    queues_db=DB('queues',**db_config)
    
    qry=f"select party_id,created_date from ocr where case_id='{case_id}'"
    party_id=extraction_db.execute_(qry)['party_id'].tolist()

    if len(party_id)>=1:
        party_id=party_id[0]
        # print(f"party id's are {party_ids}")
        qry=f"select pq.case_id,o.party_id,ql.last_updated from hdfc_queues.process_queue pq join hdfc_queues.queue_list ql on ql.case_id=pq.case_id join hdfc_extraction.ocr o on o.case_id=pq.case_id where o.party_id in '{party_id}' and ql.queue ='waiting_queue'"
        party_ids_df=queues_db.execute_(qry)
        if not party_ids_df.empty:
            party_ids_df.columns = [col.lower() for col in party_ids_df.columns]
            party_ids_df = party_ids_df.loc[:, ~party_ids_df.columns.duplicated()]
            party_ids_df=party_ids_df.dropna()

            # Parse and clean dates
            party_ids_df["parsed_date"] = party_ids_df["last_updated"].apply(extract_date)

            #sort by datetime
            party_ids_df = party_ids_df.sort_values("parsed_date").reset_index(drop=True)

            # # Assign increasing count
            party_ids_df["process_cnt"] = range(1, len(party_ids_df) + 1)

            changed_data=party_ids_df.to_dict(orient='records')
            id_column = "case_id"
            update_column = "process_cnt"
            table_name='process_queue'
            #fomrat the query
            when_then_clauses = "\n".join(
                f"    WHEN '{row[id_column]}' THEN '{row[update_column]}'"
                for row in changed_data
            )
            case_ids = ", ".join(f"'{row[id_column]}'" for row in changed_data)
            #update cases in bulk
            sql_query = f"""
            UPDATE {table_name}
            SET {update_column} = CASE {id_column}
            {when_then_clauses}
            END
            WHERE {id_column} IN ({case_ids})"""
            queues_db.execute_(sql_query)
            logging.info(f"#### SQL Query is {sql_query}")
    else:
        print(f"there is no party id")

    return {"data":"Success","flag":True}

def run_clims_process (case_id,tenant_id,user,approved_date=None):
    try:
        mode = os.environ.get('MODE',None)
        db_config['tenant_id'] = tenant_id
        queues_db = DB('queues', **db_config)
        ext_db = DB('extraction', **db_config)
        query = f"select no_of_retries from ocr where case_id='{case_id}'"
        queues_res = ext_db.execute_(query)
        no_of_retry = queues_res.iloc[0].get('no_of_retries')
        if no_of_retry > 3:
            logging.info(f"Max retries reached for case_id {case_id}. Exiting CLIMS process.")
            return
        fetch_approved_date_query = f"select approved_date from queue_list where case_id='{case_id}'"
        queues_db = DB('queues', **db_config)
        approved_date = queues_db.execute_(fetch_approved_date_query)
        if not approved_date.empty:
            approved_date = approved_date.iloc[0].get('approved_date')
            approved_date = approved_date.strftime("%d-%m-%y %I:%M:%S.%f %p %z")
        clims_request_params = {
            "case_id": case_id,
            "tenant_id":tenant_id,
            "user":user
        }
        host = 'stats'
        port = 443
        route = 'clims_request'
        logging.info(f'Hitting URL: https://{host}:{port}/{route} for clims_request formation')
        headers = {'Content-type': 'application/json; charset=utf-8',
                'Accept': 'application/json'}
        request_data_recieved = requests.post(f'https://{host}:{port}/{route}', json=clims_request_params, headers=headers, stream=True,verify=False)
        if request_data_recieved.status_code == 200:
            request_data_recieved=request_data_recieved.json()
        else :
            logging.info("clims api response is non 200")
            request_data_recieved= None
        logging.info(f" ####### Response Received from clims_api is {request_data_recieved}")
        try:
            if mode == 'UAT' or mode == 'PRE-PROD' or mode == 'PROD' or mode == 'STANDBY':
                if request_data_recieved:
                    try:
                        clims_url = "https://hbentbpuatap.hdfcbankuat.com:9444/com.ofss.fc.cz.hdfc.obp.webservice/StockStatementSummaryRestWrapper/doStockStatementSummary"                    
                        response_data = requests.post(clims_url,json=request_data_recieved,headers=headers,verify=False)
                    except Exception as e:
                        logging.info(f"Exception occured while hitting the clims url {e}")
                        response_data = ''
                    try:
                        if response_data.status_code == 200:
                            response_data = response_data.json()
                            clims_status = (response_data.get('responseString',{}).get('bodyDetails',[{}])[0].get('responseStatus'))
                            logging.info(f"status code:{clims_status}")
                            clims_api_response = str(response_data.get('responseString',{}).get('bodyDetails',[{}])[0].get('responseMessageList',[{}])[0].get('responseMessage',""))
                        else:
                            logging.info(f"Got non 200 code from clims")
                            clims_status = ''
                            clims_api_response = 'Got non 200 code from OBP'
                    except Exception as e:
                        logging.info(f"Exception occured while fetching the clims status {e}")
                        clims_status = ''
                        clims_api_response = ''
                else:
                    logging.info("clims request formation api response is None")
                    clims_status = ''
                    clims_api_response = ''
                if clims_status.lower() == 'success':
                    status_code = 'STP'
                else:
                    status_code = 'NSTP'
            elif mode == 'DEV':
                response_data = {
                    "responseString": {
                        "headerDetails": [{
                            "requestId": "1234567890"
                        }],
                        "bodyDetails": [{
                            "responseStatus": "Success",
                            "partyId": "P12345",
                            "partyName": "John Doe"
                        }]
                    },
                    "status": {
                        "errorCode": 0
                    }
                }
                status_code='NSTP'
                clims_api_response = 'in dev'
            #requestId = response_data.get('responseString',{}).get('headerDetails',[{}])[0].get('requestId')
            requestId = request_data_recieved.get('StockStatementSummaryRequestDTO',{}).get('requestString',{}).get('headerDetails',[{}])[0].get('requestId','')
            error_code = response_data.get('status',{}).get('errorCode',0)
            logging.info(f"error_code is {error_code}")
            logging.info(f"response_data {response_data}")
            try: 
                response_data = json.dumps(response_data).replace("'", "''")
            except Exception as e:
                logging.info(f"Exception occured while converting the response data to string {e}")
                response_data = ''
            no_of_retry = no_of_retry + 1
        except Exception as e:
            print("Error sending request:", e)
            status_code = 'NSTP'
        query = f"update `ocr` set `no_of_retries`='{no_of_retry}',`clims_status`='{status_code}',`clims_response`='{clims_api_response}'  where case_id='{case_id}'"
        ext_db.execute_(query)
        request_data = json.dumps(request_data_recieved).replace("'", "''")
        response_update_query = f"""INSERT INTO clims_audit (clims_request, clims_unique_id, case_id, clims_status, clims_response, error_code, operator, approved_date) VALUES (:request_data, :requestId, :case_id, :status_code, :response_data, :error_code, :operator, TO_TIMESTAMP(:approved_date, 'DD-MM-RR HH:MI:SS.FF6 AM'))"""
        params = {
            'request_data': request_data,
            'requestId': requestId,
            'case_id': case_id,
            'status_code': status_code,
            'response_data': response_data,
            'error_code': error_code,
            'operator': user,
            'approved_date': approved_date
        }
        queues_db.execute_(response_update_query, params=params)
        logging.info(f'insert query executed successfully and query is {response_update_query}')
    except Exception as e:
        logging.exception(f"Exception Occured while inserting the clims audit table {e}")

@app.route('/clims_process', methods=['POST', 'GET'])
def clims_process():
    try:
        data = request.get_json()
        case_id = data.get("case_id")
        tenant_id = data.get("tenant_id")
        user = data.get("user")
        db_config['tenant_id'] = tenant_id
        fetch_approved_date_query = f"select approved_date from clims_audit where case_id='{case_id}' order by approved_date desc limit 1"
        queues_db = DB('queues', **db_config)
        recent_aprroved_date = queues_db.execute_(fetch_approved_date_query)
        if not recent_aprroved_date.empty:
            recent_aprroved_date = recent_aprroved_date.iloc[0].get('approved_date')
            recent_aprroved_date = recent_aprroved_date.strftime("%d-%m-%y %I:%M:%S.%f %p %z")
        else:
            recent_aprroved_date = datetime.now().strftime("%d-%m-%y %I:%M:%S.%f %p %z")
        run_clims_process(case_id, tenant_id, user, recent_aprroved_date)
        return jsonify({"status": "success", "message": "CLIMS process triggered"}), 200

    except Exception as e:
        logging.exception("Error in clims_process endpoint")
        return jsonify({"status": "error", "message": str(e)}), 500
    

def escape_sql(value: str) -> str:
    return value.replace("'", "''")


def get_process_id(task_id):

    # URL for the Camunda REST API
    #ip=os.environ['SERVER_IP']
    ip = 'camundaworkflow'
    url = f"http://{ip}:8080/rest/engine/default/task/{task_id}"
    print(f"URL is:{url}")

    
    # Make the GET request to the API
    response = requests.get(url.format(task_id=task_id))
    try:
        print(f"Response is:{response.json()}")
    except Exception as e:
        logging.error(f"The Exceptions at resposne is:{e}")
        pass
    process_instance_id=''
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Parse the JSON response
        task_data = response.json()
        
        # Extract the processInstanceId from the task data
        process_instance_id = task_data.get("processInstanceId")
        print(f"Process Instance ID: {process_instance_id}")
    else:
        print(f"Error: {response.status_code}")
        print(response.text)

    return process_instance_id

@app.route('/update_queue', methods=['POST', 'GET'])
def update_queue():
    data = request.get_json(force=True)
    logging.info(f"Request Data {data}")

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    # case_id = data["email"]["case_id"]
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    button=data.get('button',None)
    session_id = data.get('session_id', None)
    document_id = data.get('document_id', '')
    safe_task_id=data.get('safe_task_id',None)
    camunda_host = data.get('camunda_host','camundaworkflow')
    camunda_port = data.get('camunda_port','8080')
    file_name=data.get('file_name',None)
    
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':user,'session_id':None})
        user = ui_data.get('user', None)
        session_id = ui_data.get('session_id', None)

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='camunda_api',
        span_name='update_queue',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            flow_list = data.get('flow_list', None)

            if flow_list is None:
                logging.info('###################### Flow list empty, creating new')
                flow_list = []
            
            flow_list_in_json = []
            for i in flow_list:
                flow_list_in_json.append(json.loads(i))

            queue = data.get('queue', None)
            task_id = data.get('task_id', None)
            ui_data = data.get('ui_data',{})
        
            db_config['tenant_id'] = tenant_id

            db = DB('queues', **db_config)
            biz_db=DB('business_rules', **db_config)
            extraction_db = DB('extraction', **db_config)
            duplicate_file=data.get('duplicate_file',None)

            #Unlocking the case for other Button functions to execute
            query = 'UPDATE `process_queue` SET `case_lock`=0 WHERE `case_id`=%s'
            db.execute_(query, params=[case_id])
            

            if queue == 'case_creation':
                escaped_file_name=escape_sql(file_name)
                try:
                    #sql = f"UPDATE `folder_monitor_table` SET `current_status`='Files updated in Case creation queue', `current_queue`='{queue}', `case_id`='{case_id}' WHERE `file_name`='{escaped_file_name}'"
                    sql = f"""
                                UPDATE folder_monitor_table
                                SET current_status = 'Files updated in Case creation queue',
                                    current_queue = '{queue}',
                                    case_id = '{case_id}'
                                WHERE file_name = '{escaped_file_name}'
                                AND created_date = (
                                    SELECT MAX(created_date) FROM folder_monitor_table WHERE file_name = '{escaped_file_name}'
                                )
                           """
                    db.execute_(sql)
                    #insert_query = f"UPDATE `file_data` SET `current_status`='Files updated in Case creation queue', `current_queue`='{queue}', `case_id`='{case_id}' WHERE `final_file_name`='{file_name}'"
                    insert_query= f"""
                                UPDATE file_data
                                SET current_status = 'Files updated in Case creation queue',
                                    current_queue = '{queue}',
                                    case_id = '{case_id}'
                                WHERE final_file_name = '{escaped_file_name}'
                                AND file_received_datetime = (
                                    SELECT MAX(file_received_datetime) FROM file_data WHERE final_file_name = '{escaped_file_name}'
                                )
                           """
                    db.execute_(insert_query)
                except Exception as e:
                    logging.exception(f"Exception occurred while updating status: {e}")
                    pass
            else:
                try:
                    sql = f"UPDATE `folder_monitor_table` SET `current_status`='File is Allocated to Queue', `current_queue`='{queue}' WHERE `case_id`='{case_id}'"
                    db.execute_(sql)
                    insert_query = f"UPDATE `file_data` SET `current_status`='File is Allocated to Queue', `current_queue`='{queue}' WHERE `case_id`='{case_id}'"
                    db.execute_(insert_query)
                except Exception as e:
                    logging.exception(f"Exception occurred while updating status: {e}")
                    pass

            try:
                # when file is moved to rejected queue then status should be auto rejected
                if queue=='rejected_queue' and button=='Move to Reject' and data.get('ui_data', {}).get('variables',{}).get('screen_id')=='waiting_queue_screen_3':
                    query = f"UPDATE `process_queue` SET `status`='Manually Rejected' WHERE `case_id`='{case_id}'"
                    db.execute_(query)
                    # updating the no_of_retries to 0 for clims_api re-request processing
                    update_query = f"update `ocr` set `no_of_retries`=0 where `case_id`='{case_id}'"
                    extraction_db.execute_(update_query)
                elif queue=='rejected_queue' and button=='Move to Reject':
                    query = f"UPDATE `process_queue` SET `status`='Clims Rejected' WHERE `case_id`='{case_id}'"
                    db.execute_(query)
                    # updating the no_of_retries to 0 for clims_api re-request processing
                    update_query = f"update `ocr` set `no_of_retries`=0 where `case_id`='{case_id}'"
                    extraction_db.execute_(update_query)
                elif queue=='rejected_queue':
                    query =f"UPDATE `process_queue` SET `status`='Auto Rejected' WHERE `case_id`='{case_id}'"
                    db.execute_(query)
                else:
                    pass
            except Exception as e:
                logging.exception(f"Exception occured while updating status..{e}")
                pass


            isMaker = (safe_task_id == 'maker_queue' and button == 'Accept')
            isRejected= (queue =='rejected_queue' and duplicate_file is True)

            def async_update_status(case_id, queue, button, data, tenant_id, tmzone):
                try:
                    db_config['tenant_id'] = tenant_id
                    db = DB('queues', **db_config)
                    if queue=='accepted_queue' and button=='Accept':
                        #print(f"Accepted")
                        user = data.get('ui_data', {}).get('user')
                        #print(f"user is:{user}")
                        try:
                            current_ist = datetime.now(pytz.timezone(tmzone))
                            currentTS = current_ist.strftime('%B %d, %Y %I:%M %p')
                            logging.info(f"####currentTS now is {currentTS}")
                            sql = f"""
                                        UPDATE process_queue
                                        SET accepted_time = '{currentTS}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                        try:
                            sql = f"""
                                        UPDATE process_queue
                                        SET accepted_user = '{user}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                    else:
                        pass




                    if queue=='rejected_queue' and button=='Reject':
                        user = data.get('ui_data', {}).get('user')
                        #print(f"user is:{user}")
                        try:
                            current_ist = datetime.now(pytz.timezone(tmzone))
                            currentTS = current_ist.strftime('%B %d, %Y %I:%M %p')
                            logging.info(f"####currentTS now is {currentTS}")
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_time = '{currentTS}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                        try:
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_user = '{user}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                    else:
                        pass



                    if queue=='rejected_queue' and button=='Move to Reject':
                        user = data.get('ui_data', {}).get('user')
                        #print(f"user is:{user}")
                        try:
                            current_ist = datetime.now(pytz.timezone(tmzone))
                            currentTS = current_ist.strftime('%B %d, %Y %I:%M %p')
                            logging.info(f"####currentTS now is {currentTS}")
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_time = '{currentTS}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                        try:
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_user = '{user}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                    else:
                        pass


                    if queue=='rejected_queue' and button=='Accept':
                        #user = data.get('ui_data', {}).get('user')
                        #print(f"user is:{user}")
                        try:
                            current_ist = datetime.now(pytz.timezone(tmzone))
                            currentTS = current_ist.strftime('%B %d, %Y %I:%M %p')
                            logging.info(f"####currentTS now is {currentTS}")
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_time = '{currentTS}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                        try:
                            sql = f"""
                                        UPDATE process_queue
                                        SET rejected_user = 'System'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                    else:
                        pass


                    
                    if queue=='waiting_queue' and button=='Approve':
                        user = data.get('ui_data', {}).get('user')
                        #print(f"user is:{user}")
                        try:
                            current_ist = datetime.now(pytz.timezone(tmzone))
                            currentTS = current_ist.strftime('%B %d, %Y %I:%M %p')
                            logging.info(f"####currentTS now is {currentTS}")
                            sql = f"""
                                        UPDATE process_queue
                                        SET approved_date = '{currentTS}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                        try:
                            sql = f"""
                                        UPDATE process_queue
                                        SET approved_user = '{user}'
                                        WHERE case_id = '{case_id}'
                                """
                            logging.info(f"#### Executing SQL: {sql.strip()}")  # <-- print query
                            result=db.execute_(sql)
                            #print(f"The result is:{result}")
                        except Exception as e:
                            logging.exception(f"Exception occured while updating status..{e}")
                            pass
                    else:
                        pass
                    


                except Exception:
                    logging.exception(f"[ASYNC] Status update failed for {case_id}")


            def async_heavy_logic(case_id, tenant_id, data, safe_task_id, button):
                try:
                    db_config['tenant_id'] = tenant_id
                    db = DB('queues', **db_config)
                    biz_db = DB('business_rules', **db_config)
                    extraction_db = DB('extraction', **db_config)
                    if button=='Re-Apply' or isMaker or isRejected or button== 'Re-Extraction':
                        try:
                            mandatory_fields = {}
                            query = f"SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
                            params = [case_id]
                            result_ = extraction_db.execute_(query, params=params)
                            party_id = result_['PARTY_ID'][0]

                            try:
                                query = f"""
                                    SELECT 
                                        CAST(cm.component_name AS VARCHAR2(4000)) AS component_name,
                                        CAST(cm.component_category AS VARCHAR2(4000)) AS category,
                                        am.age,
                                        am.margin
                                    FROM AGE_MARGIN_WORKING_UAT am
                                    JOIN COMPONENT_MASTER cm
                                        ON CAST(am.component_name AS VARCHAR2(4000)) = CAST(cm.component_name AS VARCHAR2(4000))
                                    WHERE am.party_id = '{party_id}'
                                    AND am.is_active = 1
                                    AND CAST(cm.status AS VARCHAR2(4000)) = 'ACTIVE'
                                """

                                df = extraction_db.execute_(query)
                                CATEGORY_MAP = dict(stock="stocks", stocks="stocks", inventory="inventory",
                                    debtor="debtors", debtors="debtors",
                                    creditor="creditors", creditors="creditors",
                                    receivable="receivable", receivables="receivable",
                                    advance="advances", advances="advances")

                                for _, row in df.iterrows():

                                    component = (row["COMPONENT_NAME"] or "").strip()
                                    raw_category = (row["CATEGORY"] or "").strip().lower()
                                    category = CATEGORY_MAP.get(raw_category, raw_category)
                                    #category  = (row["CATEGORY"] or "").strip().lower()
                                    age       = row.get("AGE") or ""
                                    margin    = row.get("MARGIN") or ""

                                    if category not in mandatory_fields:
                                        mandatory_fields[category] = []
                                    #mandatory_fields.setdefault(category, {})
                                    exists = any(component in item for item in mandatory_fields[category])

                                    if not exists:
                                        mandatory_fields[category].append({
                                            component: {
                                                "age": age,
                                                "margin": margin
                                            }
                                        })
                                def clob_chunks_sql_function(json_str):
                                    if not json_str or json_str.strip() in ['null', 'None', '{}', '[]']:
                                        return "TO_CLOB(NULL)"

                                    escaped = json_str.replace("'", "''")
                                    chunk_size = 3000
                                    chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
                                    return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)


                                mandatory_json = json.dumps(mandatory_fields)
                                clob_expr = clob_chunks_sql_function(mandatory_json)

                                update_qry = f"""
                                    UPDATE ocr
                                    SET mandatory_fields = {clob_expr}
                                    WHERE case_id = '{case_id}'
                                """

                                extraction_db.execute_(update_qry)

                            except Exception as e:
                                logging.error(f"Exception occurred: {e}")
                                mandatory_fields = {}
                        except Exception as e:
                            logging.exception(f"The exception caught is:{e}")
                            pass

                        qry = f"""select * from ocr where case_id='{case_id}'"""
                        ocr_df = extraction_db.execute_(qry)
                        ocr_row = ocr_df.iloc[0] if ocr_df is not None and not ocr_df.empty else {}
                        tab_json_cache={}
                        qr2 = f"""
                            SELECT fields_extracted, fields_edited, not_extracted_fields,
                                extraction, manual, extraction_percentage
                            FROM field_accuracy
                            WHERE case_id='{case_id}'
                        """
                        df2 = db.execute(qr2).to_dict(orient='records')
                        try:
                            if  df2:
                                fa_row = df2[0]

                                try:
                                    fields_extracted = json.loads(fa_row.get("fields_extracted") or "{}")
                                except:
                                    fields_extracted = {}
                                try:
                                    fields_edited = json.loads(fa_row.get("fields_edited") or "{}")
                                except:
                                    fields_edited = {}
                                try:
                                    not_extracted_fields = json.loads(fa_row.get("not_extracted_fields") or "{}")
                                except:
                                    not_extracted_fields = {}

                                extraction = int(fa_row.get("extraction") or 0)
                                manual = int(fa_row.get("manual") or 0)
                                no_of_fields = 0
                                # print(f"first fields_extracted are..............:{fields_extracted}")
                                # print(f"The sfirst fields_edited are................:{fields_edited}")
                                # print(f"The first not_extracted_fields are................:{not_extracted_fields}")
                        except Exception as e:
                            logging.error(f"The exception caugfht is............:{e}")
                            pass
                        def extract_fields_function(rule_text):
                            fields = []
                            i = 0
                            rule_text = rule_text.strip()

                            while i < len(rule_text):
                                end = rule_text.find("()", i)
                                if end == -1:
                                    break

                                start = end - 1
                                while start >= 0 and rule_text[start] not in "+-*/":
                                    start -= 1

                                field = rule_text[start + 1:end].strip()
                                fields.append(field)
                                i = end + 2
                            #print(f"fields are....:{fields}")
                            return fields


                        def apply_ocr_values_function(rule_text, tab_json):
                            updated = rule_text
                            #print(f"tab_json is...............:{tab_json}")

                            for field in extract_fields_function(rule_text):
                                if field not in tab_json:
                                    continue
                                value = tab_json.get(field)
                                if value in ("", None, "null", "None",''):
                                    continue 
                                #print(f"value is.............:{value}")
                                if isinstance(value, dict):
                                    #av = value.get("a.v", {})
                                    #value = list(av.values())[0] if av else 0
                                    av = value.get("a.v")
                                    rv = value.get("r.v")

                                    if isinstance(av, dict) and av:
                                        value = list(av.values())[0]
                                    elif isinstance(rv, dict) and rv:
                                        value = list(rv.values())[0]
                                    else:
                                        value = 0
                                else:
                                    value=value
                                value = str(value).replace(",", "")
                                updated = updated.replace(f"{field}()", f"{field}({value})")

                            return updated
                        def ensure_initial_value(tab_json, field_name):
                            tab_json.setdefault("initial_value", {})

                            # already captured → do nothing
                            #if field_name in tab_json["initial_value"]:
                                

                            if field_name not in tab_json:
                                # tab_json["initial_value"][field_name] = {
                                #     "previous": "",
                                #     "updated": ""
                                # }
                                return


                            init_map = tab_json["initial_value"]

                            if field_name in init_map:
                                prev = init_map[field_name].get("previous", "")
                                upd  = init_map[field_name].get("updated", "")

                                # if updated exists → promote it
                                if upd not in ("", None, "null", "None"):
                                    init_map[field_name] = {
                                        "previous": str(upd),
                                        "updated": ""
                                    }
                                return
                            ocr_val=None
                            field_val = tab_json[field_name]

                            if isinstance(field_val, dict):

                                av = field_val.get("a.v")
                                rv = field_val.get("r.v")

                                if isinstance(av, dict) and av:
                                    ocr_val = list(av.values())[0]
                                elif isinstance(rv, dict) and rv:
                                    ocr_val = list(rv.values())[0]
                                
                            elif isinstance(field_val, (int, float)):
                                ocr_val = field_val

                            elif isinstance(field_val, str):
                                ocr_val = field_val.strip()
                                if ocr_val == "":
                                    ocr_val = None

                            else:
                                ocr_val = None

                            if ocr_val in ("", None, "null", "None"):
                                prev = ""
                            else:
                                prev = str(ocr_val)

                            tab_json["initial_value"][field_name] = {
                                "previous": prev,
                                "updated": ""
                            }


                        def parse_rule_pairs_function(rule_text):
                            pairs = []
                            i = 0
                            n = len(rule_text)
                            #print(f"rule_text is...........:{rule_text}")

                            while i < n:
                                close_idx = rule_text.find(")", i)
                                if close_idx == -1:
                                    break

                                open_idx = rule_text.rfind("(", i, close_idx)
                                if open_idx == -1:
                                    i = close_idx + 1
                                    continue
                                next_open = rule_text.find("(", close_idx + 1)
                                if next_open != -1 and next_open == close_idx + 1:
                                    i = close_idx + 1
                                    continue

                                #value = rule_text[open_idx + 1:close_idx].strip()
                                value = rule_text[open_idx + 1:close_idx].strip()

                                """try:
                                    value = float(value.replace(",", ""))
                                except:
                                    i = close_idx + 1
                                    continue"""
                                # find operator before this field
                                ############################
                                # start = open_idx - 1
                                # while start >= 0 and rule_text[start] not in "+-*/":
                                #     start -= 1

                                # field = rule_text[start + 1:open_idx].strip()
                                ##########################################
                                start = open_idx - 1
                                while start >= 0 and rule_text[start] not in "+-*/":
                                    start -= 1

                                field = rule_text[start + 1:open_idx].strip()

                                #  MINIMAL FIX: handle names like "CREDITORS (PC)()"
                                # If field ends with ')', include the previous bracketed part
                                if field.endswith(")"):
                                    prev_open = field.rfind("(")
                                    if prev_open != -1:
                                        field = field[:].strip()

                                # try:
                                #     value = float(value.replace(",", ""))
                                # except:
                                #     value = ""   

                                # pairs.append((field, value))

                                ##2nd backup
                                # operator = "+"
                                # if start >= 0:
                                #     operator = rule_text[start]

                                # try:
                                #     value = float(value.replace(",", ""))
                                # except:
                                #     value = ""
                                # pairs.append((field, value, operator))
                                operator = "+"
                                if start >= 0:
                                    operator = rule_text[start]

                                try:
                                    value = float(value.replace(",", ""))
                                except:
                                    value = ""

                                #  MINIMAL FIX: negative value should not flip sign again
                                # if isinstance(value, (int, float)) and value < 0:
                                #     operator = "+"

                                pairs.append((field, value, operator))

                                i = close_idx + 1
                            #print(f"pairs are......:{pairs}")
                            return pairs


                        def calculate_from_rule_text_function_bk(rule_text):
                            total = 0.0
                            for _, value in parse_rule_pairs_function(rule_text):
                                try:
                                    total += float(value)
                                except:
                                    pass
                            total=round(total,2)
                            return total
                        def calculate_from_rule_text_function(rule_text, tab_json):
                            total = 0.0
                            has_value = False
                            #print(f"tab_jsom is............:{tab_json}")
                            for field, value,operator  in parse_rule_pairs_function(rule_text):
                                #print(f"rule_text is...........:{rule_text}")
                                #print(f"field value is............:{field,value}")
                                #  DOUBLE CHECK
                                #if value == "" or value=='0' or value=='0.0' and tab_json:
                                if (value in ("", None) or float(value) == 0.0) and tab_json:
                                    #print(f"")
                                    if field not in tab_json:
                                        continue   # DO NOT add or calculate

                                    ocr_val = tab_json.get(field)
                                    if ocr_val in ("", None, "null", "None",''):
                                        continue
                                    #ocr_val = tab_json.get(field, 0)

                                    if isinstance(ocr_val, dict):
                                        #av = ocr_val.get("a.v", {})
                                        #ocr_val = list(av.values())[0] if av else 0
                                        av = ocr_val.get("a.v")
                                        rv = ocr_val.get("r.v")

                                        if isinstance(av, dict) and av:
                                            ocr_val = list(av.values())[0]
                                        elif isinstance(rv, dict) and rv:
                                            ocr_val = list(rv.values())[0]
                                        else:
                                            ocr_val = 0
                                    else:
                                        ocr_val = ocr_val

                                    try:
                                        value = float(str(ocr_val).replace(",", ""))
                                        value=round(value, 2)
                                        has_value = True
                                    except:
                                        value = 0
                                    #rule_text = rule_text.replace(
                                    #    f"{field}()",
                                    #    f"{field}({value})"
                                    #)
                                    rule_text = rule_text.replace(
                                        f"{field}()", f"{field}({value})"
                                    )
                                    rule_text = rule_text.replace(
                                        f"{field}(0)", f"{field}({value})"
                                    )
                                    rule_text = rule_text.replace(
                                        f"{field}(0.0)", f"{field}({value})"
                                    )
                                    rule_text = rule_text.replace(
                                        f"{field}({value})", f"{field}({value})"
                                    )
                                    
                                    tab_json[field] = str(value)

                                # try:
                                #     total += float(value)
                                # except:
                                #     pass
                                #try:
                                #    sign = -1 if f"- {field}(" in rule_text else 1
                                #    total += sign * float(value)
                                #except:
                                #    pass
                                try:
                                    sign = -1 if operator == "-" else 1
                                    total += sign * float(value)
                                    has_value = True 
                                except:
                                    pass
                            if not has_value:
                                return "", tab_json, rule_text
                            #print(f"updated_text is..........:{rule_text}")
                            return round(total, 2), tab_json, rule_text
                            #return total,tab_json,rule_text


                        def update_tab_json_function(tab_json, final_field, final_value):
                            if final_field not in tab_json:
                                return tab_json
                            tab_json[final_field] = str(final_value)
                            tab_json.setdefault("tab_view", {})

                            if "rowData" in tab_json["tab_view"]:
                                for r in tab_json["tab_view"]["rowData"]:
                                    if r.get("fieldName") == final_field:
                                        r["value"] = str(final_value)
                                        break

                            return tab_json


                        def clob_chunks_sql_function(json_str):
                            if not json_str or json_str.strip() in ['null', 'None', '{}']:
                                return "TO_CLOB(NULL)"

                            escaped = json_str.replace("'", "''")
                            chunk_size = 3000
                            chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
                            return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)
                        def replace_field_value_direct(text, field, new_value):
                            key = f"{field}("
                            idx = text.find(key)

                            if idx == -1:
                                return text

                            start = idx + len(key)
                            end = text.find(")", start)

                            if end == -1:
                                return text

                            old = text[idx:end + 1]          # FIELD(oldValue)
                            new = f"{field}({new_value})"    # FIELD(newValue)

                            return text.replace(old, new, 1)

                        # -------- RECOMMENDED CHANGES HELPERS --------

                        def extract_recommended_value(meta):
                            if not isinstance(meta, dict):
                                return None

                            for key in ("Typed", "Cropped", "Converted"):
                                val = meta.get(key)
                                if val not in (None, "", "null"):
                                    return val

                            return None


                        def rebuild_rule_text_function(rule_text, updated_values):
                            pairs = parse_rule_pairs_function(rule_text)
                            rebuilt = rule_text

                            for field, old_val in pairs:
                                if field in updated_values:
                                    rebuilt = rebuilt.replace(
                                        f"{field}({old_val})",
                                        f"{field}({updated_values[field]})"
                                    )

                            return rebuilt

                        def apply_recommended_changes(rule_text_input, recommended_changes):
                            for tab, fields in recommended_changes.items():
                                tab = tab.lower()
                                if tab not in rule_text_input:
                                    continue

                                for field_name, meta in fields.items():
                                    new_val = extract_recommended_value(meta)
                                    if new_val is None:
                                        continue

                                    new_val = str(new_val).replace(",", "").strip()

                                    # for final_field, rule_text in rule_text_input[tab].items():
                                    #     rule_text_input[tab][final_field] = rebuild_rule_text_function(
                                    #         rule_text,
                                    #         {field_name: new_val}
                                    #     )
                                    for rule_id, rules in rule_text_input[tab].items():
                                        for final_field, rule_text in rules.items():
                                            rule_text_input[tab][rule_id][final_field] = rebuild_rule_text_function(
                                                rule_text,
                                                {field_name: new_val}
                                            )

                            return rule_text_input

                        qry = f"SELECT rule_text_input,party_id FROM ocr WHERE case_id='{case_id}'"
                        df = extraction_db.execute_(qry)

                        if df is None or df.empty:
                            pass

                        rule_text_input = df.iloc[0]["rule_text_input"]
                        #print(f"rule_text_input is................:{rule_text_input}")
                        party_id = df.iloc[0]["party_id"]
                        if not rule_text_input:
                            rule_text_input = {}
                            first_time = True
                        else:
                            rule_text_input = json.loads(rule_text_input)
                            first_time=False
                        if first_time:
                            qry = f"""
                                SELECT rule_id, tab_name, rule_text, final_field
                                FROM rule_base_phase_2
                                WHERE party_id='{party_id}'
                            """
                            rules_df = biz_db.execute_(qry)

                            for _, row in rules_df.iterrows():
                                tab = row["tab_name"].lower().strip()
                                #rule_text = row["rule_text"].strip()
                                rule_text = (row["rule_text"] or "").strip()
                                #final_field = row["final_field"].strip()
                                final_field = (row["final_field"] or "").strip()


                                if tab not in tab_json_cache:
                                    raw_tab = ocr_row.get(tab, "{}")
                                    try:
                                        tab_json_cache[tab] = json.loads(raw_tab) if raw_tab else {}
                                    except:
                                        tab_json_cache[tab] = {}

                                tab_json = tab_json_cache[tab]

                                #updated_rule = apply_ocr_values(rule_text, tab_json)

                                #rule_text_input.setdefault(tab, {})
                                #rule_text_input[tab][final_field] = updated_rule
                                rule_id = str(row["rule_id"]).strip()
                                ensure_initial_value(tab_json, final_field)
                                updated_rule = apply_ocr_values_function(rule_text, tab_json)

                                rule_text_input.setdefault(tab, {})
                                rule_text_input[tab].setdefault(rule_id, {})
                                rule_text_input[tab][rule_id][final_field] = {
                                    "previous": updated_rule,   # ORIGINAL DB RULE
                                    "updated": updated_rule  # OCR APPLIED RULE
                                }
                            
                        for tab, rule_map in rule_text_input.items():

                            # raw_tab = ocr_row.get(tab, "{}")
                            # tab_json = json.loads(raw_tab) if raw_tab else {}
                            if tab not in tab_json_cache:
                                raw_tab = ocr_row.get(tab, "{}")
                                try:
                                    tab_json_cache[tab] = json.loads(raw_tab) if raw_tab else {}
                                except:
                                    tab_json_cache[tab] = {}

                            tab_json = tab_json_cache[tab]

                            for rule_id, rules in rule_map.items():
                                for final_field, rule_text in rules.items():
                                    #  normalize rule_text
                                    if isinstance(rule_text, dict):
                                        previous_text = rule_text.get("previous", "")
                                        updated_text = rule_text.get("updated", "")
                                    else:
                                        previous_text = rule_text
                                        updated_text = rule_text

                                    ensure_initial_value(tab_json, final_field)
                                    #  calculate ONLY using updated text
                                    final_value, tab_json, calc_rule_text = calculate_from_rule_text_function(
                                        updated_text,
                                        tab_json
                                    )

                                    #  store both previous & updated (updated = calculated one)
                                    rule_text_input[tab][rule_id][final_field] = {
                                    "previous": calc_rule_text,
                                    "updated":  calc_rule_text
                                    }
                                    if final_field in fields_edited and final_value not  in ("", None, "null", "None",'') :
                                        fields_edited.pop(final_field)
                                        manual = max(manual - 1, 0)

                                    # if field was not extracted earlier → remove
                                    if final_field in not_extracted_fields:
                                        not_extracted_fields.pop(final_field)

                                    # ---- EXTRACTION COUNT HANDLING ----
                                    if final_field not in fields_extracted and final_value not  in ("", None, "null", "None",''):
                                        # FIRST TIME extraction → increase count
                                        extraction += 1

                                    # always update value
                                    if final_value not  in ("", None, "null", "None",''):    
                                        fields_extracted[final_field] = f"E:{final_value}" 
                                    tab_json[final_field] = str(final_value)
                                    for rid, dep_rules in rule_map.items():
                                        for dep_final, dep_text in dep_rules.items():
                                            #print(f"ris is.......:{rid}")
                                            #print(f"rule_id is............:{rule_id}")
                                            #  SKIP the rule that just produced this field
                                            if rid == rule_id and dep_final == final_field:
                                                continue

                                            if isinstance(dep_text, dict):
                                                txt = dep_text.get("updated", "")
                                            else:
                                                txt = dep_text

                                            # replace only dependent references
                                            if f"{final_field}(" in txt:
                                                #new_txt = txt.replace(
                                                #    f"{final_field}(",
                                                #    f"{final_field}({final_value})")
                                                new_txt = replace_field_value_direct(
                                                    txt,
                                                    final_field,
                                                    final_value
                                                )
                                                #print(f"new_txt is..........:{new_txt}")
                                                rule_text_input[tab][rid][dep_final] = {
                                                    "previous": new_txt,
                                                    "updated":  new_txt
                                                }
                                                dep_value, tab_json, calc_txt = calculate_from_rule_text_function(
                                                new_txt,
                                                    tab_json
                                                )

                                                if dep_value in ("", None, "null", "None"):
                                                    tab_json[dep_final] = ""
                                                else:
                                                    dep_value = round(dep_value, 2)
                                                    tab_json[dep_final] = str(dep_value)

                                                rule_text_input[tab][rid][dep_final] = {
                                                    "previous": calc_txt,
                                                    "updated":  calc_txt
                                                }
                    

                                    #final_value=round(final_value,2)
                                    if final_value in ("", None, "null", "None"):
                                        final_value = ""
                                    else:
                                        final_value = round(final_value, 2)

                                    tab_json = update_tab_json_function(tab_json, final_field, final_value)

                            json_str = json.dumps(tab_json)
                            clob_expr = clob_chunks_sql_function(json_str)

                            qry = f"""
                                UPDATE ocr
                                SET {tab} = {clob_expr}
                                WHERE case_id='{case_id}'
                            """
                            extraction_db.execute_(qry)  
                        rule_json = json.dumps(rule_text_input)
                        rule_clob = clob_chunks_sql_function(rule_json)
                        #print(f"rule_clob is.......:{rule_clob}")

                        qry = f"""
                            UPDATE ocr
                            SET rule_text_input = {rule_clob}
                            WHERE case_id='{case_id}'
                        """
                        extraction_db.execute_(qry) 
                        extraction_percentage = round(
                            (extraction / (extraction + manual)) * 100
                            if (extraction + manual) > 0 else 0, 2
                        )
                        manual_percentage = round(
                            (manual / (extraction + manual)) * 100
                            if (extraction + manual) > 0 else 0, 2
                        )
                        
                        no_of_fields = extraction + manual

                        json_str = json.dumps(fields_extracted)
                        clob_expr = clob_chunks_sql_function(json_str)
                        json_str_1 = json.dumps(fields_edited)
                        clob_expr_1 = clob_chunks_sql_function(json_str_1)
                        json_str_2 = json.dumps(not_extracted_fields)
                        clob_expr_2 = clob_chunks_sql_function(json_str_2)
                        # print(f"second fields_extracted are..............:{json_str}")
                        # print(f"The secondd fields_edited are................:{json_str_1}")
                        # print(f"The second not_extracted_fields are................:{json_str_2}")
                        # 6. Update back to DB
                        try:
                            update_qry = f"""
                                UPDATE field_accuracy
                                SET fields_extracted = {clob_expr},
                                    fields_edited={clob_expr_1},
                                    not_extracted_fields={clob_expr_2},
                                    extraction = {extraction},
                                    extraction_percentage = {extraction_percentage},
                                    manual_percentage={manual_percentage},
                                    number_of_fields={no_of_fields}
                                WHERE case_id='{case_id}'
                            """
                            #print(f"Query is:{update_qry}")
                            db.execute(update_qry)
                        except Exception as e:
                            logging.error(f"Failed to update field_accuracy for case_id={case_id}: {e}")




                except Exception:
                    logging.exception(f"[ASYNC] Heavy logic failed for {case_id}")




            try:
                executor.submit(
                    async_update_status,
                    case_id,
                    queue,
                    button,
                    data,
                    tenant_id,
                    tmzone
                )
                logging.info(f"Status update running async for case_id={case_id}")
            except Exception:
                logging.exception("Failed to submit status update async")


            if button=='Re-Apply' or isMaker or isRejected or button== 'Re-Extraction':
                try:
                    executor.submit(
                        async_heavy_logic,
                        case_id,
                        tenant_id,
                        data,
                        safe_task_id,
                        button
                    )
                    logging.info(f"Heavy processing running async for {case_id}")
                except Exception:
                    logging.exception("Failed to submit heavy async job")



            if button == 'Re-Apply':
                try:
                    # 1. Fetch OCR data
                    qry1 = f"""
                        SELECT stocks, debtors, creditors, recivables, total_utilisations
                        FROM ocr
                        WHERE case_id='{case_id}'
                    """
                    df1 = extraction_db.execute(qry1).to_dict(orient='records')
                    if not df1:
                        logging.warning(f"No OCR record found for case_id={case_id}")
                        ocr_row = {}
                    else:
                        ocr_row = df1[0]
                   
                    #print(f"The ocr row is:{ocr_row}")
 
                    # Ensure JSON decode for each column
                    for col in ["stocks", "debtors", "creditors", "recivables", "total_utilisations"]:
                        try:
                            if isinstance(ocr_row.get(col), str):
                                ocr_row[col] = json.loads(ocr_row[col])
                            elif ocr_row.get(col) is None:
                                ocr_row[col] = {}
                        except Exception as e:
                            logging.error(f"Failed to parse JSON for {col}: {e}")
                            ocr_row[col] = {}
 
                    # 2. Get field_accuracy
                    qr2 = f"""
                        SELECT fields_extracted, fields_edited, not_extracted_fields,
                            extraction, manual, extraction_percentage
                        FROM field_accuracy
                        WHERE case_id='{case_id}'
                    """
                    df2 = db.execute(qr2).to_dict(orient='records')
                    try:
                        if  df2:
                            fa_row = df2[0]
 
                            try:
                                fields_extracted = json.loads(fa_row.get("fields_extracted") or "{}")
                            except:
                                fields_extracted = {}
                            try:
                                fields_edited = json.loads(fa_row.get("fields_edited") or "{}")
                            except:
                                fields_edited = {}
                            try:
                                not_extracted_fields = json.loads(fa_row.get("not_extracted_fields") or "{}")
                            except:
                                not_extracted_fields = {}
 
                            extraction = int(fa_row.get("extraction") or 0)
                            manual = int(fa_row.get("manual") or 0)
                            for d in [not_extracted_fields, fields_edited]:
                                keys_to_remove = [k for k, v in d.items() if "total" in k.lower() and (v == "" or v is None)]
                                for key in keys_to_remove:
                                    d.pop(key, None)
                            existing_keys = set()
                            for k in (
                                list(fields_extracted.keys()) +
                                list(fields_edited.keys()) +
                                list(not_extracted_fields.keys())
                            ):
                                k_norm = k.strip().lower()
                                existing_keys.add(k_norm)
 
 
                            # 3. Define mapping
                            mapping = {
                                "stocks": "Total Stock",
                                "debtors": "Total Debtors",
                                "creditors": "Total Creditors",
                                "receivables": "Total Recivables",
                                "total_utilisations": "Total Utilisation"
                            }
 
 
 
                            def extract_value(val, total_field=None):
                                try:
                                    if isinstance(val, dict):
                                        # 1. Exact key or substring match → look inside dict
                                        for k, v in val.items():
                                            if total_field and (total_field.lower() == k.lower() or total_field.lower() in k.lower()):
                                                if isinstance(v, dict):
                                                    if "a.v" in v and v["a.v"]:
                                                        return next(iter(v["a.v"].values()))
                                                    if "r.v" in v and v["r.v"]:
                                                        return next(iter(v["r.v"].values()))
                                                return str(v).strip()
 
                                        # 2. Scan all dicts for a.v / r.v
                                        for v in val.values():
                                            if isinstance(v, dict):
                                                if "a.v" in v and v["a.v"]:
                                                    return next(iter(v["a.v"].values()))
                                                if "r.v" in v and v["r.v"]:
                                                    return next(iter(v["r.v"].values()))
 
                                        # 3. Fallback: first scalar
                                        for v in val.values():
                                            if not isinstance(v, dict):
                                                return str(v).strip()
 
                                    elif isinstance(val, str):
                                        return val.strip()
                                    else:
                                        return str(val).strip()
                                except Exception as e:
                                    logging.error(f"Error extracting value: {e}")
                                return ""
 
 
 
                            # 4. Loop through mapping
                            new_extracted_count = 0
                            #print(f"$$$$$$$$$ exisyting fields is {existing_keys}")
                            for col, total_field in mapping.items():
                                #print(f"$$$$$$$$ col is {col}")
                                #print(f"########## total-fields is {total_field}")
                                try:
                                    total_norm = total_field.lower().strip()
                                    if total_norm in existing_keys:
                                        continue
                                    #print(f"col is:{col}")
                                    #print(f"ocr get col is:{ocr_row.get(col)}")
                                    val = ocr_row.get(col, {})
                                    if not val:
                                        continue
 
                                    extracted_value = extract_value(val, total_field=total_field)
 
                                    if extracted_value:
                                        fields_extracted[total_field] = f"E:{extracted_value}"
                                        new_extracted_count += 1
                                except Exception as e:
                                    logging.error(f"Error processing field {col}: {e}")
 
                            # 5. Update counts
                            extraction += new_extracted_count
                            extraction_percentage = round(
                                (extraction / (extraction + manual)) * 100
                                if (extraction + manual) > 0 else 0, 2
                            )
 
                            # 6. Update back to DB
                            try:
                                update_qry = f"""
                                    UPDATE field_accuracy
                                    SET fields_extracted = '{json.dumps(fields_extracted)}',
                                        extraction = {extraction},
                                        extraction_percentage = {extraction_percentage}
                                    WHERE case_id='{case_id}'
                                """
                                #print(f"Query is:{update_qry}")
                                db.execute(update_qry)
                            except Exception as e:
                                logging.error(f"Failed to update field_accuracy for case_id={case_id}: {e}")
 
                        else:
                            logging.info(f"No field_accuracy record found for case_id={case_id}")
                            pass
 
                    except Exception as e:
                        logging.error(f"Unexpected error in Re-Apply for case_id={case_id}: {e}")
                        pass

                except Exception as e:
                        logging.error(f"Unexpected error in Re-Apply for case_id={case_id}: {e}")
                        pass
 
 
 
 
 


            if queue == 'case_creation':
                escaped_file_name=escape_sql(file_name)
                try:
                    #sql = f"UPDATE `folder_monitor_table` SET `current_status`='Files updated in Case creation queue', `current_queue`='{queue}', `case_id`='{case_id}' WHERE `file_name`='{file_name}'"
                    sql = f"""
                                UPDATE folder_monitor_table
                                SET current_status = 'Files updated in Case creation queue',
                                    current_queue = '{queue}',
                                    case_id = '{case_id}'
                                WHERE file_name = '{escaped_file_name}'
                                AND created_date = (
                                    SELECT MAX(created_date) FROM folder_monitor_table WHERE file_name = '{escaped_file_name}'
                                )
                           """
                    db.execute_(sql)
                except Exception as e:
                    logging.exception(f"Exception occurred while updating status: {e}")
                    pass
            else:
                try:
                    sql = f"UPDATE `folder_monitor_table` SET `current_status`='File is Allocated to Queue', `current_queue`='{queue}' WHERE `case_id`='{case_id}'"
                    db.execute_(sql)
                except Exception as e:
                    logging.exception(f"Exception occurred while updating status: {e}")
                    pass

            try:
                query = f"select task_id from queue_list where case_id = '{case_id}'"
                pre_task_id=db.execute_(query)['task_id'].to_list()[0]
                #print(F"pre_task_id is {pre_task_id}")
            except:
                pre_task_id='0'
                pass


            
            
            update1 = f"update process_queue set `completed_processes` = NULL,`flow_list` = '{json.dumps(flow_list_in_json)}' where case_id = '{case_id}' "
            qs_w_task_id_query = f"select count(*) as count from queue_list where case_id = '{case_id}'"

            qs_w_task_id = int(db.execute_(qs_w_task_id_query)["count"])

            update2=""

            if "previous_queue" in data:
                p_queue = data["previous_queue"]
                if p_queue=="" or p_queue is None:
                    p_queue="default"
            
                if p_queue == "default":
                    update2 = f"""insert into queue_list (case_id, queue, task_id, parallel) values ('{case_id}','{queue}', '{task_id}', 1)"""
                elif p_queue != "default":
                    update2 = f"update queue_list set queue = '{queue}', task_id = '{task_id}' ,pre_task_id = '{pre_task_id}' where case_id = '{case_id}' and queue =  '{p_queue}'"
            

            else:
                if qs_w_task_id == 0:

                    update2 = f"""insert into queue_list (case_id, queue, task_id, parallel) values ('{case_id}','{queue}', '{task_id}',0)"""

                else:
                    update2 = f"update queue_list set queue = '{queue}', task_id = '{task_id}', pre_task_id = '{pre_task_id}' where case_id = '{case_id}' "


            logging.info(f"$$$$$$$$$$$$$$$4 {update1} and {update2}")

            db.execute_(update1) and db.execute_(update2)
        
            response_data = {}
            
            try:
                mode = os.environ.get('MODE',None)
                if (mode == 'UAT' or mode == 'PRE-PROD' or mode == 'PROD' or mode == 'STANDBY' or mode=='DEV') and (queue=='maker_queue' or queue=='rejected_queue' or queue=='case_creation'):
                    file_path=f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{file_name}'
                    pages_got=get_page_count(file_path)
                    query =f"UPDATE `process_queue` SET `page_count`='{pages_got}' WHERE `case_id`='{case_id}'"
                    db.execute_(query)
                    logging.info(f"Page count for case_id {case_id}: {pages_got}")
                    logging.info(f"Queue value: {queue}")
                    if pages_got>101 and queue=='case_creation':
                        query =f"UPDATE `queue_list` SET `case_creation_status` = 'Picked', `count_of_tries`=5 WHERE `case_id`='{case_id}'"
                        db.execute_(query)
                    else:
                        pass
            except Exception as e:
                logging.exception(f"Exception Occured while getting page count .. {e}")
                pages_got=0


            response_data['message']='Successfully updated queue '
            response_data['case_id']=case_id
            
            response_data['camunda_host']=camunda_host
            response_data['camunda_port']=camunda_port
            response_data['updated_queue'] = queue

            if "previous_queue" in data:
                response_data["previous_queue"] = data["queue"]

            
            response_data = {'flag': True,'data':response_data }

            
            try:
                if queue=='waiting_queue':
                    approved_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                ## update the case processing count based on last_updated of queue list when users clicks the Approve button in maker queue
                    case_processing_cnt(case_id,tenant_id)
                    run_clims_process(case_id,tenant_id,user,approved_date)
            except Exception as e:
                logging.exception(f"Exception Occured while updating the processing cnt")


        except Exception as e:
            message = f'error in database connection'
            logging.exception(e)
            response_data = {'flag': False, 'message': message, 'data':{}}
        
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            time_consumed = str(end_time-start_time)
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        
        
        audit_data={
                    "TENANT_ID": tenant_id,
                    "USER_": user,
                    "CASE_ID": case_id,
                    "API_SERVICE": "update_queue",
                    "INGESTED_QUEUE": queue,
                    "SERVICE_CONTAINER": "camunda_api",
                    "CHANGED_DATA": None,
                    "TABLES_INVOLVED": "",
                    "MEMORY_USAGE_GB": str(memory_consumed),
                    "TIME_CONSUMED_SECS": time_consumed,
                    "REQUEST_PAYLOAD": json.dumps(data),
                    "RESPONSE_DATA": json.dumps(response_data['data']),
                    "TRACE_ID": trace_id,
                    "SESSION_ID": session_id,
                    "STATUS": str(response_data['flag'])
                }
        logging.info(f"{audit_data}#####audit_data")

        insert_into_audit(case_id, audit_data)

        return jsonify(response_data)


def normalize_component_name(name):
    
    normalized_name = re.sub(r'\s+', '', name).lower()
    return normalized_name

import math




def clear_aging_margin_fields_code(data, fields_to_clear):
    fields_to_clear_lower = [f.lower().strip() for f in fields_to_clear]
    print(f"fields_to_clear_lower is :{fields_to_clear_lower}")
    if isinstance(data, dict):
        # Check if this dict has a 'fieldName' to match
        field_name = data.get("fieldName")
        if isinstance(field_name, str) and field_name.strip().lower() in fields_to_clear_lower:
            data["aging"] = ""
            data["margin"] = ""

        # Recursively update all dict values
        for k, v in data.items():
            data[k] = clear_aging_margin_fields_code(v, fields_to_clear)
        return data

    elif isinstance(data, list):
        return [clear_aging_margin_fields_code(v, fields_to_clear) for v in data]

    else:
        # Normalize None / NaN / inf
        if data is None:
            return ""
        if isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
            return ""
        return data



def normalize_json_input(data):
    """
    Recursively normalize JSON data:
    - Replace None/null with ""
    - Replace NaN/inf with ""
    """
    if isinstance(data, dict):
        return {k: normalize_json_input(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [normalize_json_input(v) for v in data]
    else:
        if data is None:
            return ""
        if isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
            return ""
        return data




def case_creation_cnt(case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    
    qry=f"select party_id,created_date from ocr where case_id='{case_id}'"
    party_id=extraction_db.execute_(qry)['party_id'].tolist()

    if len(party_id)>=1:
        party_id=party_id[0]
        # print(f"party id's are {party_ids}")
        qry=f"select o.case_id,o.party_id,o.created_date from hdfc_extraction.ocr o join hdfc_queues.queue_list ql on ql.case_id=o.case_id where party_id in '{party_id}' order by created_date"
        party_ids_df=extraction_db.execute_(qry)
        if not party_ids_df.empty:
            party_ids_df.columns = [col.lower() for col in party_ids_df.columns]
            party_ids_df = party_ids_df.loc[:, ~party_ids_df.columns.duplicated()]
            party_ids_df=party_ids_df.dropna()

            # Parse and clean dates
            party_ids_df["parsed_date"] = party_ids_df["created_date"].apply(extract_date)
            # Drop NaT rows for ranking
            # mask = party_ids_df["parsed_date"].notna()
            # party_ids_df.loc[mask, "case_count"] = pd.factorize(party_ids_df.loc[mask, "parsed_date"])[0] + 1
            
            # rank the dates
            #Sort by datetime
            party_ids_df = party_ids_df.sort_values("parsed_date").reset_index(drop=True)

            # # Assign increasing count
            party_ids_df["case_count"] = range(1, len(party_ids_df) + 1)
            
            changed_data=party_ids_df.to_dict(orient='records')
            id_column = "case_id"
            update_column = "case_count"
            table_name='ocr'
            when_then_clauses = "\n".join(
                f"    WHEN '{row[id_column]}' THEN '{row[update_column]}'"
                for row in changed_data
            )
            case_ids = ", ".join(f"'{row[id_column]}'" for row in changed_data)

            sql_query = f"""
            UPDATE {table_name}
            SET {update_column} = CASE {id_column}
            {when_then_clauses}
            END
            WHERE {id_column} IN ({case_ids})"""
            extraction_db.execute_(sql_query)
            logging.info(f"#### SQL Query is {sql_query}")
    else:
        print(f"there is no party id")

    return {"data":"Success","flag":True}



def fetch_camunda_db_xml(tenant_id):

    db_config['tenant_id'] = tenant_id

    queues_db = DB('queues', **db_config)

    #Unlocking the case for other Button functions to execute
    try:
        query = 'select `diagrams` from `camunda_xml`'
        camunda_xml_list=list(queues_db.execute_(query)['diagrams'])

        if len(camunda_xml_list)>0:
            logging.info(f"########### Camunda XML from DB: {camunda_xml_list}")
            camnunda_xml_list = json.loads(camunda_xml_list[0])

            return camnunda_xml_list

        else:
            return []

    except Exception as e:
        logging.info("######### Error in fetching camunda xml from DB")
        logging.exception(e)

@app.route('/deploy_workflow_from_db', methods=['POST', 'GET'])
def deployWorkflow_from_db():
    try:
        request_data = request.get_json(force=True)
        #data = request_data.get("data",None)
        tenant_id = request_data.get("tenant_id",None)

        given_workflow_name = request_data.get("workflow_name","")
        camunda_port = request_data.get("camunda_port","8080")

        os_camundaworkflow_host='camundaworkflow'

        host = os.environ['SERVER_IP']

        endpoint = f'http://{os_camundaworkflow_host}:{camunda_port}/rest/engine/default/deployment/create'
    
        response_json={}
        deployment_flag=True
        deployment_message = "Deployement is Successfull "

        data=fetch_camunda_db_xml(tenant_id)
        # data=db_camunda_xml_list.get("data",None)

        for each_workflow in data:  

            file_type = each_workflow["type"]
            workflow_xml = each_workflow["diagram"]
            workflow_name=each_workflow["name"]

            if given_workflow_name=="":
                pass
            elif given_workflow_name!=workflow_name:
                continue

            logging.info("##############"+str(file_type))
            
            logging.info("##############"+str(workflow_name))


            with open("/temp_folder/"+workflow_name+"."+file_type,"w+", encoding='utf-8') as f:
                f.write(workflow_xml)

            files = {"upload": open("/temp_folder/"+workflow_name+"."+file_type, 'rb')}
            logging.info(f"#############{files}")

            
            response = requests.post(endpoint, files=files)
            if response.status_code==200:
                response_json[workflow_name]=True
                
            else:
                response_json[workflow_name]=False
                deployment_flag=False
                deployment_message = "All/Few deployements were unsucessfull"
        
        
        response_data = {'flag':deployment_flag,'message':deployment_message,'data':response_json}
    
        return jsonify(response_data)

    except Exception as e:
        logging.exception(e)
        message = "Error in Deployment Workflow"
        return jsonify({'flag': False, 'message': message})
                                                         
@app.route('/complete_task', methods=['POST', 'GET'])
def complete_task():

    if request.method == 'GET':

        try:
            camunda_url = request.args.get('camunda_url')
            logging.info(
                f"########### Camunda URL for completion: {camunda_url}")
            response = requests.post(camunda_url, json={})

            if response.status_code == 204:

                return jsonify({'flag': True, 'data': {'message': 'Completed the task Successfully'}})
            else:
                return jsonify({'flag': False, 'message': f'Camunda returned an error{response.content}'})

        except Exception as e:
            logging.info(
                f"############# Error in posting to camunda for Task Completion")
            logging.exception(e)
            return jsonify({'flag': False, 'message': f'Error in posting completion query to Camunda'})


@app.route('/get_bpmn_names', methods=['POST', 'GET'])
def get_bpmn_names():

    try:

        flag, response = get_bpmn_list()

        if flag:
            return(jsonify({"flag": True, "data": {"bpmns": response}}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn list: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn list from camunda"}))


@app.route('/get_bpmn_versions', methods=['POST', 'GET'])
def get_bpmn_versions():

    data = request.get_json(force=True)

    bpmn_name = data.get("bpmn_name", "")

    if bpmn_name == "":
        return jsonify({"flag": False, "message": "please send the bpmn name to get versions"})

    try:

        flag, response = get_bpmn_process_versions(bpmn_name)

        if flag:
            return(jsonify({"flag": True, "data": {"bpmns": response}}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn Versions: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn versions from camunda"}))


@app.route('/get_bpmn_xml_tokens', methods=['POST', 'GET'])
def get_bpmn_xml_tokens():

    data = request.get_json(force=True)

    bpmn_name = data.get("bpmn_version", "")

    if bpmn_name == "":
        return jsonify({"flag": False, "message": "please send the bpmn version to get tokens and xml"})

    try:

        flag, response = get_tokens_xml(bpmn_name)

        if flag:
            return(jsonify({"flag": True, "data": response}))

        else:

            return(jsonify({"flag": False, "message": response}))

    except Exception as e:
        logging.info(f"####### Error in geeting camunda bpmn Versions: {e}")

        return(jsonify({"flag": False, "message": "Could not get the bpmn versions from camunda"}))


@app.route('/case_migration', methods=['POST', 'GET'])
def case_migration():

    data = request.get_json(force=True)

    source_version = data.get("source_version", "")
    dest_version = data.get("dest_version", "")

    flag = data.get("flag", "")

    if source_version == "" or dest_version == "" or source_version == dest_version or flag == "":
        return jsonify({"flag": False, "message": "please send valid bpmn versions to migrate"})

    try:

        if flag == "validate":

            valid_flag, valid_response = validate_migration_plan(
                source_version, dest_version)

            logging.info(f"########## Validation Response: {valid_response}")

            return(jsonify({"flag": valid_flag, "data": valid_response}))

        elif flag == "migrate":

            migrate_flag, migrate_response = migrate_cases(
                source_version, dest_version)

            if migrate_flag:
                return jsonify({"flag": True, "data": {"message": migrate_response}})

            else:
                return jsonify({"flag": False, "message": migrate_response})

    except Exception as e:
        logging.info(f"####### Error in Validating or migration: {e}")

        return(jsonify({"flag": False, "message": "Could not validate or migrate cases"}))


@app.route('/dummy_case_creator', methods=['POST', 'GET'])
def create_addl_cases():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    db_config['tenant_id'] = tenant_id
    host = os.environ.get('HOST_IP', "")
    port = "8080"

    task_id = 'demo'
    api_params = {}

    logging.debug(f'API Params: {api_params}')

    logging.debug(
        'Hitting URL: http://{host}:{8080}/rest/engine/default/process-definition/key/{task_id}/start')
    headers = {'Content-type': 'application/json; charset=utf-8'}
    requests.post(f'http://{host}:{port}/rest/engine/default/process-definition/key/{task_id}/start',
                  json=api_params, headers=headers)
    response_data = {'message': 'Successfully Created Case'}

    return jsonify({'flag': True, 'data': response_data})


@app.route('/moving_case_forcefully', methods=['POST', 'GET'])
def moving_case_forcefully():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.pop('tenant_id', None)
        db_config['tenant_id'] = tenant_id
        host = os.environ.get('HOST_IP', "")
        queue_db = DB('queues', **db_config)
        case_id = data.get('case_id', '')

        # Step1 Getting bpmn version ids
        logging.debug(
            f'Hitting URL: http://{host}:5002/get_bpmn_versions')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"tenant_id": tenant_id,
                      "bpmn_name": "adama_user_workflow"}  # Change the bpmn_name as per named in project
        bpmn_versions_response = requests.post(f'http://{host}:5002/get_bpmn_versions',
                                               json=api_params, headers=headers)
        logging.info(
            f"####### bpmn_versions_response Response: {bpmn_versions_response.text}")
        bpmn_versions_response = bpmn_versions_response.json()

        if len(bpmn_versions_response['data']['bpmns']) == 0:
            return {"flag": False, "message": "Could not find the bpmn version, Check the bpmn name in request"}

        # Step2 Getting processinstance id of the case
        bpmn_versions_response = bpmn_versions_response['data']['bpmns']
        vs = []
        for i in bpmn_versions_response:
            i = i.split(":")
            i[1] = int(i[1])
            vs.append(i[1])
        max_version = max(vs)
        latest_bpmn_version_index = vs.index(max_version)
        latest_bpmn_version = bpmn_versions_response[latest_bpmn_version_index]

        # bpmn_versions_response.sort()
        # latest_bpmn_version = bpmn_versions_response[-1]

        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"processDefinitionId": latest_bpmn_version}
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/task')
        logging.debug(f'api_params: {api_params}')
        instance_details = requests.post(f'http://{host}:8080/rest/engine/default/task',
                                         json=api_params, headers=headers)
        logging.info(
            f"####### instance_details Response: {instance_details.text}")
        instance_details = instance_details.json()
        logging.info(f'Instance details is {instance_details}')

        # Based on caseids(duplicates which are in customerdata) find the Taskid
        if case_id == '':
            return {"flag": False, "message": "Could not move forward case id to find task id is missing"}
        task_id_query = f"select * from queue_list where case_id='{case_id}' and queue='customer_data'"
        task_id_data = queue_db.execute_(task_id_query)
        task_id = task_id_data['task_id'][0]

        # Now search processInstanceId in the step2 response

        instance_details=pd.DataFrame(instance_details)
        instance_details=instance_details[(instance_details.name=='Farmer Data')]
        risk=instance_details[(instance_details.id==task_id)]
        risk.reset_index(inplace=True)
        processInstanceId_l=[]
        process_instance_id=risk['processInstanceId'][0]
        processInstanceId_l.append(process_instance_id)
        
        # Method 2 (The above method or below method both works)

        logging.info(
                        f"###### processInstanceId: {processInstanceId_l}")

        if not processInstanceId_l:
            return {"flag": False, "message": "Could not find the process instance id at stage 2"}
        else:
            processInstanceId = processInstanceId_l[0]

        # Step3 Finding activity instance id
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {}
        activity_instance_details = requests.get(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances',
                                                 json=api_params, headers=headers)
        logging.info(
            f"####### process_instance_details Response: {activity_instance_details.text}")
        activity_instance_details = activity_instance_details.json()

        # in this response collect the id from the childActivityInstances
        childActivityInstances = activity_instance_details['childActivityInstances']
        for i in childActivityInstances:
            for k, v in i.items():
                if 'customer_data:' in v:
                    childActivityInstance = v
                    logging.info(
                        f'###### childActivityInstance: {childActivityInstance}')

        # # Step4 modification api for case movement
        # if the response is 204 r success it is moved
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        
        # priority_queue value is hardcode (which queue will be there in queue_list table)
        api_params = {"instructions": [{"type": "startBeforeActivity", "activityId": "duplicate", "variables":{"priority_queue":{"value":"customer_data","local":True,"type":"String"}}}, {
            "type": "cancel", "activityInstanceId": childActivityInstance}], "annotation": "Modified to resolve an error."}
        modification_details = requests.post(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification',
                                             json=api_params, headers=headers)

        logging.info("####### Done with the modification!!")

        if modification_details.status_code == 204:
            return {'flag': True, 'data': {"message": "Successfully modified case in cockpit to the duplicate queue"}}
        else:
            return {'flag': False, 'data': {"message": "Failed in modifying case in cockpit to the duplicate queue"}}

    except Exception as e:
        logging.info(f"####### Error in moving the case: {e}")
        return {"flag": False, "message": "Could not move the case to previous stage"}



@app.route('/moving_case_forcefully_', methods=['POST', 'GET'])
def moving_case_forcefully_():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.pop('tenant_id', None)
        db_config['tenant_id'] = tenant_id
        host = os.environ.get('HOST_IP', "")
        queue_db = DB('queues', **db_config)
        case_id = data.get('case_id', '')

        # Step1 Getting bpmn version ids
        logging.debug(
            f'Hitting URL: http://{host}:5002/get_bpmn_versions')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"tenant_id": tenant_id,
                      "bpmn_name": "adama_user_workflow"}  # Change the bpmn_name as per named in project
        bpmn_versions_response = requests.post(f'http://{host}:5002/get_bpmn_versions',
                                               json=api_params, headers=headers)
        logging.info(
            f"####### bpmn_versions_response Response: {bpmn_versions_response.text}")
        bpmn_versions_response = bpmn_versions_response.json()

        if len(bpmn_versions_response['data']['bpmns']) == 0:
            return {"flag": False, "message": "Could not find the bpmn version, Check the bpmn name in request"}

        # Step2 Getting processinstance id of the case
        bpmn_versions_response = bpmn_versions_response['data']['bpmns']
        vs = []
        for i in bpmn_versions_response:
            i = i.split(":")
            i[1] = int(i[1])
            vs.append(i[1])
        max_version = max(vs)
        latest_bpmn_version_index = vs.index(max_version)
        latest_bpmn_version = bpmn_versions_response[latest_bpmn_version_index]

        # bpmn_versions_response.sort()
        # latest_bpmn_version = bpmn_versions_response[-1]

        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {"processDefinitionId": latest_bpmn_version}
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/task')
        logging.debug(f'api_params: {api_params}')
        instance_details = requests.post(f'http://{host}:8080/rest/engine/default/task',
                                         json=api_params, headers=headers)
        logging.info(
            f"####### instance_details Response: {instance_details.text}")
        instance_details = instance_details.json()

        # Based on caseids(duplicates which are in customerdata) find the Taskid
        if case_id == '':
            return {"flag": False, "message": "Could not move forward case id to find task id is missing"}
        task_id_query = f"select * from queue_list where case_id='{case_id}' and queue='duplicate'"
        task_id_data = queue_db.execute_(task_id_query)
        task_id = task_id_data['task_id'][0]

        # Now search processInstanceId in the step2 response
        # if id found pick the processinstanceid in the same dict
        processInstanceId_l = []
        for i in instance_details:
            for k, v in i.items():
                if task_id == v:
                    processInstanceId = i['processInstanceId']
                    processInstanceId_l.append(processInstanceId)
                    logging.info(
                        f"###### processInstanceId: {processInstanceId}")

        if not processInstanceId_l:
            return {"flag": False, "message": "Could not find the process instance id at stage 2"}
        else:
            processInstanceId = processInstanceId_l[0]

        # Step3 Finding activity instance id
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        api_params = {}
        activity_instance_details = requests.get(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/activity-instances',
                                                 json=api_params, headers=headers)
        logging.info(
            f"####### process_instance_details Response: {activity_instance_details.text}")
        activity_instance_details = activity_instance_details.json()

        # in this response collect the id from the childActivityInstances
        childActivityInstances = activity_instance_details['childActivityInstances']
        for i in childActivityInstances:
            for k, v in i.items():
                if 'duplicate:' in v:
                    childActivityInstance = v
                    logging.info(
                        f'###### childActivityInstance: {childActivityInstance}')

        # # Step4 modification api for case movement
        # if the response is 204 r success it is moved
        logging.debug(
            f'Hitting URL: http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification')
        headers = {'Content-type': 'application/json; charset=utf-8'}
        
        # priority_queue value is hardcode (which queue will be there in queue_list table)
        api_params = {"instructions": [{"type": "startBeforeActivity", "activityId": "reject", "variables":{"priority_queue":{"value":"duplicate","local":True,"type":"String"}}}, {
            "type": "cancel", "activityInstanceId": childActivityInstance}], "annotation": "Modified to resolve an error."}
        modification_details = requests.post(f'http://{host}:8080/rest/engine/default/process-instance/{processInstanceId}/modification',
                                             json=api_params, headers=headers)

        logging.info("####### Done with the modification!!")

        if modification_details.status_code == 204:
            return {'flag': True, 'data': {"message": "Successfully modified case in cockpit to the reject queue"}}
        else:
            return {'flag': False, 'data': {"message": "Failed in modifying case in cockpit to the reject queue"}}

    except Exception as e:
        logging.info(f"####### Error in moving the case: {e}")
        return {"flag": False, "message": "Could not move the case to previous stage"}
