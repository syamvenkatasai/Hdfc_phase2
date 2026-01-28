import os
import json
import pandas as pd
import ast
import psutil
import requests
import numpy as np

from flask import Flask, request, jsonify
from db_utils import DB
from ace_logger import Logging
from app import app
#
from .BusinessRules import BusinessRules
from ._StaticFunctions import case_creation_cnt
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
import datetime
import pytz
tmzone = 'Asia/Kolkata'

logging = Logging(name='business_rules_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def http_transport(encoded_span):
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body = encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

def insert_into_audit(case_id, data,tenant_id):
    #tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True

def clob_chunks_sql(json_str):
    if not json_str or json_str.strip() in ["null", "None", "{}"]:
        return "TO_CLOB(NULL)"
    escaped = json_str.replace("'", "''")
    size = 3000
    chunks = [escaped[i:i + size] for i in range(0, len(escaped), size)]
    return " || ".join(f"TO_CLOB('{c}')" for c in chunks)


####################### FUNCTIONS ##################
################ Ideally should be in another file ### have to check why imports are not working

from difflib import SequenceMatcher

def partial_match(input_string, matchable_strings, threshold=75):
    """Returns the most similar string to the input_string from a list of strings.
    Args:
        input_string (str) -> the string which we have to compare.
        matchable_strings (list[str]) -> the list of strings from which we have to choose the most similar input_string.
        threshold (float) -> the threshold which the input_string should be similar to a string in matchable_strings.
    Example:
        sat = partial_match('lucif',['chandler','Lucifer','ross geller'])"""
    
    logging.info(f"input_string is {input_string}")
    logging.info(f"matchable_strings got are {matchable_strings}")
    result = {}
    words = matchable_strings
    match_word = input_string
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > 75 and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                logging.info(match_got)
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
            result['flag'] = 'False'
            result['data'] = {'reason':'got wrong input for partial match','error_msg':str(e)}
            return result
    if match_got:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'True'
        result['data'] = {'value':match_got}
    else:
        logging.info(f"match is {match_got} and ratio is {max_ratio}")
        result['flag'] = 'False'
        result['data'] = {'reason':f'no string is partial match morethan {threshold}%','error_msg':'got empty result'}
    return result

def date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy'):
    """Date format change util function
    Args:
        date (str) -> the date string which needs to be converted
        input_format (str) -> the input format in which the given date string is present.
        output_format (str) -> the output format to which we have to convert the data into.
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        
        if flag is True: data contains the key value and value is the converted date.
        if flag is False: data contains the error_msg say why its failed.
    Example:
            x = date_transform('23-03-2020','dd-mmm-yyyy','dd-mm-yy')"""
    
    logging.info(f"got input date is : {date}")
    logging.info(f"got input format is : {input_format}")
    logging.info(f"got expecting output format is : {output_format}")
    result = {}
    date_format_mapping = {'dd-mm-yyyy':'%d-%m-%Y','dd-mm-yy':'%d-%m-%y'}
    try:
        input_format_ = date_format_mapping[input_format]
        output_format_ = date_format_mapping[output_format]
    except:
        input_format_ = '%d-%m-%Y'
        output_format_ = '%d-%m-%Y'
        
    try:
        date_series = pd.Series(date)
        logging.info(f"got series is : {date_series}")
    except Exception as e:
        logging.error("cannnot convert given input to pandas series")
        logging.error(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given input to pandas series','error_msg':str(e)}
        
    try:
        try:
            converted_date = pd.to_datetime(date_series, format=input_format_, errors='coerce').dt.strftime(output_format_)
        except:
            converted_date = pd.to_datetime(date_series, format=input_format_,errors='coerce',utc=True).dt.strftime(output_format_)

        logging.info(f"Got converted date is : {converted_date}")
        result['flag'] = True
        result['data'] = {"value": converted_date[0]}
        
    except Exception as e:
        logging.info("Failed while Converting date into given format")
        logging.info(e)
        result['flag'] = False
        result['data'] = {'reason':'cannnot convert given date to required format','error_msg':str(e)}
    
    return result

def get_data(tenant_id, database, table, case_id, case_id_based=True, view='records'):
    """give the data from database
    Args:
        
    Returns:
        result (dict)
        result is a dict having keys flag, data.Depending on the flag the data changes.
        if flag is True: data contains the key value and value is the data.
        if flag is False: data contains the error_msg say why its failed.
    Example:
            x = get_data('invesco.acelive.ai','extraction','ocr','INV4D15EFC')"""
    result = {}
    db_config['tenant_id'] = tenant_id

    db = DB(database, **db_config)
    try:
        if case_id_based:
            query = f"SELECT * from `{table}` WHERE `case_id` = '{case_id}'"
            try:
                df = db.execute(query)
            except:
                df = db.execute_(query)
            table_data = df.to_dict(orient= view)
            result['flag'] = True
            result['data'] = {"value":table_data}
            
        else:
            query = f"SELECT * from `{table}`"
            df = db.execute(query)
            if not df.empty:
                table_data = df.to_dict(orient = view)
            else:
                table_data = {}
            result['flag'] = True
            result['data'] = {"value":table_data}
    except Exception as e:
        logging.error(f"Failed in getting tables data from database")
        logging.error(e)
        result['flag'] = 'False'
        result['data'] = {'reason':'Failed in getting tables data from database','error_msg':str(e)}
    return result

def save_data(tenant_id, database, table, data, case_id, case_id_based=True, view='records'):
    """Util for saving the data into database
    
    Args:
        tenant_id (str) -> the tenant name for which we have to take the database from. ex.invesco.acelive.ai
        database (str) -> database name. ex.extraction
        table (str) -> table name. ex.ocr
        case_id_based (bool) -> says whether we have to bring in all the data or only the data for a case_id.
        case_id (str) -> case_id for which we have to bring the data from the table.
        data (dict) -> column_value map or a record in the database.
    Returns:
        result (dict)
    Example:
        data1 = {"comments":"testing","assessable_value":1000}
        save_data(tenant_id='deloitte.acelive.ai', database='extraction', table='None', data=data1, case_id='DEL754C18D_test', case_id_based = True, view='records')"""    
    logging.info(f"tenant_id got is : {tenant_id}")
    logging.info(f"database got is : {database}")
    logging.info(f"table name got is : {table}")
    logging.info(f"data got is : {data}")
    logging.info(f"case_id got is : {case_id}")
    result = {}
    
    if case_id_based:
        logging.info(f"data to save is case_id based data.")
        try:
            db_config['tenant_id'] = tenant_id
            connected_db = DB(database, **db_config) # only in ocr or process_queue we are updating
            connected_db.update(table, update=data, where={'case_id':case_id})
        except Exception as e:
            logging.error(f"Cannot update the database")
            logging.error(e)
            result["flag"]=False,
            result['data'] = {'reason':f'Cannot update the database','error_msg':str(e)}
            return result
        result['flag']=True
        result['data']= data
        return result

    else:
        logging.info(f"data to save is master based data.")
        try:
            db_config['tenant_id'] = tenant_id
            connected_db = DB(database, **db_config) # only in ocr or process_queue we are updating
            logging.info('************** have to develop due to where clause condition not getting from data *******')
            connected_db.update(table, update=data, where={'case_id':case_id})
        except Exception as e:
            logging.error(f"Cannot update the database")
            logging.error(e)
            result['flag']=False
            result['data'] = {'reason':f'Cannot update the database','error_msg':str(e)}
        
        result['flag']=True
        result['data']= data  
        return result


@app.route('/index', methods=['POST', 'GET'])
def index():
    return ('Hello world')


@app.route('/get_data', methods=['POST', 'GET'])
def get_data_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    params = request.json
    case_id = params.get('case_id', None)
    tenant_id = params.get('tenant_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_data_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        database = params.get('database', None)
        table = params.get('table', None)
        case_id_based = params.get('case_id_based', True)
        view = params.get('view', 'records')

        if case_id_based == "False":
            case_id_based = False
        else:
            case_id_based = True
        result = get_data(tenant_id, database, table, case_id, case_id_based, view)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(result)

@app.route('/save_data', methods=['POST', 'GET'])
def save_data_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    params = request.json
    tenant_id = params.get('tenant_id', None)
    case_id = params.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='save_data_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
    
        database = params.get('database', None)
        table = params.get('table', None)
        data = params.get('data', None)
        # data = json.dumps(data)
        
        case_id_based = bool(params.get('case_id_based', True))
        view = params.get('view', 'records')
        result = save_data(tenant_id, database, table, data, case_id, case_id_based, view)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(result)

@app.route('/partial_match', methods=['POST', 'GET'])
def partial_match_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    params = request.json
    case_id = params.get('case_id', None)
    tenant_id = params.get('tenant_id', None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='partial_match_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        input_string = params.get('input_string', None)
        matchable_strings = params.get('matchable_strings', [])
        threshold = params.get('threshold', 75)
        result = partial_match(input_string, matchable_strings, threshold=75)

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(result)

@app.route('/date_transform', methods=['POST', 'GET'])
def date_transform_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    params = request.json
    tenant_id = params.get('tenant_id', None)
    case_id = params.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='date_transform_route',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        date = params.get('date', None)
        input_format = params.get('input_format', 'dd-mm-yyyy')
        output_format = params.get('output_format', 'dd-mm-yyyy')
        result = date_transform(date, input_format='dd-mm-yyyy', output_format='dd-mm-yyyy')

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}") 
        return jsonify(result)


@app.route('/assign', methods=['POST', 'GET'])
def assign_route():

    params = request.json    
    key_value_data = params.get('assign_table_data', None)
    key_to_assign = params.get('assign_column', None)
    value_to_assign = params.get('assign_value', None)
    key_value_data[key_to_assign] = value_to_assign

    return jsonify(key_value_data)
    


class Blockly(object):
    def __init__(self):
        self.name = "Blockly"
        self.method_string = ""
        self.retun_var="return_data"

    def function_builder(self,method_string,return_var="return_data"):
        self.method_string=method_string
        self.retun_var=return_var

        def fun():
            try:
                
                exec(self.method_string,globals(),locals())
                logging.info(f"####### local Vars: {locals()}")
                logging.info(f"####### self vars: {locals()['self']}")
                logging.info(f"####### test Vars: {locals()['test']}")
                return_data=locals()[self.retun_var]

                return True,return_data
            except Exception as e:
                logging.info(f"###### Error in executing Python Code")
                logging.exception(e)
                return False,str(e)

        return fun



def print_globals_types():
    for key in globals().keys():
        logging.info(f"######### Key: {key} and type: {type(globals()[key])}")


def function_builder(method_string,return_var="return_data"):
    
    

    def fun():
        try:
            # return_data=return_var

            logging.info(f"####### Function builder calling: {method_string}")
            
            exec(method_string,globals(),globals())
            logging.info(f"####### global keys : {globals().keys()}")

            
            return_dict = {}
            return_list = return_var.split(",")

            for param in return_list:
                return_dict[param] = globals().get(param,"")

            return True,return_dict
        except Exception as e:
            logging.info(f"###### Error in executing Python Code")
            logging.exception(e)
            return False,str(e)

    return fun



@app.route('/execute_business_rules',methods=['POST','GET'])
def execute_business_rules():
     
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id",None)

    if case_id is None:
        rule_id = data.get('rule_id',None)
        trace_id = rule_id
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='business_rules_api',
            span_name='execute_business_rules',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

        username = data.get('user',"")
        flag = data.get('flag',"")
        rule_type = data.get('link_type',"rule")
        rule_name = data.get('rule_name',"")
        message = {'flag':False}

        return_message=""
        return_code = True
        db_config['tenant_id']=tenant_id

        return_data="return_data"

        if rule_type == "rule":
            try:

                string_python = data.get('rule',{}).get('python',"")
                return_param = data.get('return_params',"return_data")
                logging.info(f"############### Globals before execution: {globals().keys()}")
                logging.info(f"############### Locals before execution: {locals().keys()}")
                return_data=test_business_rule(string_python,return_param)
                message = jsonify(return_data)
                #return jsonify(return_data)

            except Exception as e:
                logging.info("######## Error in Executing the given Rule")
                logging.exception(e)
                message = {"flag":False,"message":"Error in executing the given rule"}
                #return jsonify({"flag":False,"message":"Error in executing the given rule"})

        elif rule_type == "chain":
            try:
                rule_seq_list = data.get("group",[])
                if rule_seq_list:
                    rule_seq_list.sort(key=lambda x:x['sequence'])
                    logging.info(f"################ rule sequenced: {rule_seq_list}")

                    # return_data = execute_rule_chain(rule_seq_list)
                    for rule in rule_seq_list:
                        logging.info(f"######### Executing rule id {rule['rule_id']}")

                        fetch_code,rule = get_the_rule_from_db(rule['rule_id'])
                        if not fetch_code:
                            return rule
                        execute_code,return_data=test_business_rule(rule,return_data)
                        if not execute_code:
                            return return_data
                    #return jsonify({'flag':True,'data':return_data})
                    message = {'flag':True,'data':return_data}

                else:
                    message = {"flag":False,"message":"Empty Rule list"}
                    #return jsonify({"flag":False,"message":"Empty Rule list"})
            except Exception as e:
                    logging.info("######## Error in Executing the given Rule CHAIN")
                    logging.exception(e)
                    message = {"flag":False,"message":"Error in executing the given rule chain"}
                    #return jsonify({"flag":False,"message":"Error in executing the given rule chain"})
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(message)


@app.route('/execute_camunda_business_rules',methods=['POST','GET'])
def execute_camunda_business_rules():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id",None)

    if case_id is None:
        trace_id = data.get('rule_id',"")
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='execute_camunda_business_rules',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        db_config['tenant_id']=tenant_id

        return_data="return_data"
        rule_id = data.get('rule_id',"")
        try:
                return_param = data.get('return_param',"return_data")

                

                fetch_code,rule = get_the_rule_from_db(rule_id)
                if fetch_code:
                    return_data = test_business_rule(rule,return_param)
                    logging.info(f"########### Return data: {return_data}")
                    

                else:
                    return_data = {'flag':True,'message':'Error in fetcing rule from db'}
                    

        except Exception as e:
                logging.info("######## Error in Executing the given Rule")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in executing the given rule"}
                
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)
       

def get_the_rule_from_db(rule_id):
    business_db = DB("business_rules",**db_config)


    try:  

        fetch_query = f"select python_code from rule_base where rule_id='{rule_id}'"

        rule_list=business_db.execute_(fetch_query)

        if len(rule_list['python_code'])>0:
            return True,rule_list['python_code'][0]

        else:
            return False,"No Rule for given Rule ID"

    except Exception as e:

        return False,"Error in fetching rule from DB"



def execute_rule_chain(rule_chain):
    try:
        for rule in rule_chain:
            logging.info(f"######### Executing rule id {rule['rule_id']}")

            fetch_code,rule = get_the_rule_from_db(rule['rule_id'])
            if fetch_code:
                execute_code,return_data=test_business_rule(rule,"return_data")
                if not execute_code:
                    return return_data

            else:
                return rule

        return "RAN all rules"


    except Exception as e:
        logging.info("######## Error in Executing the given Rule CHAIN LOOP")
        logging.exception(e)
        return jsonify({"flag":False,"message":"Error in executing the given rule chain LOOP"})


def test_business_rule(string_python,return_var='return_data'):

    return_message=""
    return_code = True


    if string_python != "" and "rm -rf" not in string_python:

        logging.info(f"######### The given code is : {string_python}")

        exec_code = function_builder(string_python,return_var)

        logging.info("##### Calling Python Business Rules")

        return_code,return_message = exec_code()


    else:
        message = "The python block is empty or running a excluded method like rm -rf"
        return_data = {"flag":False,"message":message}
        return return_data

    return_data = {"flag":return_code,"data":return_message}

    logging.info(f"############## Returning data from test business rule: {return_data}")

    return return_data


@app.route('/rule_builder_data',methods=['POST','GET'])
def rule_builder_data():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id", None)
    rule_id = data.get('rule_id',"")
    
    if case_id is None:
        trace_id = rule_id
    else:
        trace_id = case_id
    
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='rule_builder_data',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        username = data.get('user',"")
        flag = data.get('flag',"")
        
        rule_name = data.get('rule_name',"")

        if username == "" or flag == "" or rule_id == "" or tenant_id == "":
            return jsonify({"flag":False,"message":"please send a valid request data"})

        rule_base_table_dict = {}
        rule_base_table_dict['rule_name'] = rule_name
        rule_base_table_dict['description'] = data.get('description',"")
        rule_base_table_dict['xml'] = data.get('rule',{}).get('xml',"")
        rule_base_table_dict['python_code'] = data.get('rule',{}).get('python',"")
        rule_base_table_dict['javascript_code'] = data.get('rule',{}).get('javascript',"")
        rule_base_table_dict['last_modified_by'] = username

        db_config['tenant_id'] = tenant_id
        business_db = DB("business_rules", **db_config)

        if flag == "save":

            rule_base_table_dict['rule_id'] = rule_id
            rule_base_table_dict['created_by'] = username
            try:

                return_state = business_db.insert_dict(table="rule_base",data=rule_base_table_dict)

                if return_state == False:
                    logging.info("######## Duplicate  Rule Id or Error in Saving to Rule Base Table")
                    
                    return_data = {"flag":False,"message":"Duplicate Rule Id or Error in Saving the Rule to DB"}
                    
            except Exception as e:
                logging.info("######## Error in Saving to Rule Base Table")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in Saving the Rule to DB"}
                

        elif flag == 'edit':

            try:

                business_db.update(table="rule_base",update=rule_base_table_dict,where={"rule_id":rule_id})

            except Exception as e:
                logging.info("######## Error in Updating to Rule Base Table")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in Updating the Rule to DB"}
                


        elif flag == 'fetch':

            try:

                fetch_query = f"select * from rule_base where rule_id = '{rule_id}'"
                rule_dict = business_db.execute(fetch_query).to_dict(orient="records")

                if len(rule_dict)>0:

                    rule_dict[0]['rule']={}
                    rule_dict[0]['rule']['xml'] = rule_dict[0]['xml']
                    rule_dict[0].pop('xml')

                    rule_dict[0]['rule']['javascript'] = rule_dict[0]['javascript_code']
                    rule_dict[0].pop('javascript_code')
                    rule_dict[0]['rule']['python'] = rule_dict[0]['python_code']
                    rule_dict[0].pop('python_code')


                    logging.info(f"############# Fetch rule for {rule_id} is {rule_dict[0]}")

                    return_data = {"flag":True,"data":rule_dict[0]}
                    

                else:
                    logging.info("######## Empty Data from Rule Base Table")
                    return_data = {"flag":True,"data":{}}
                    


            except Exception as e:
                logging.info("######## Error in Fetching to Rule Base Table")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in Fetching the Rule to DB"}
                
        elif flag == 'execute':
            try:

                string_python = data.get('rule',{}).get('python',"")
                return_param = data.get('return_param',"return_data")
                return_data = test_business_rule(string_python,return_param)
                


            except Exception as e:
                logging.info("######## Error in Executing the given Rule")
                logging.exception(e)
                return_data = {"flag":False,"message":"Error in executing the given rule"}
                
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)


def insert_or_update_chain_linker(database,table,data_dict):

    try:
        logging.info("########### Tyring to insert the data")

        insert_status = database.insert_dict(table=table,data=data_dict)

        if insert_status == False or insert_status is None:
            logging.info("############# Failed to insert ")
            logging.info("############# Trying to Update the Data")

            

            where_dict = {
                'group_id': data_dict['group_id'],
                'rule_id': data_dict['rule_id'],
                'sequence': data_dict['sequence'],
                'link_type': data_dict['link_type'],
            }
            
            data_dict.pop('created_by')

            update_status = database.update(table='chain_linker',update=data_dict,where=where_dict)

        return True

    except Exception as e:
        logging.info("############ Error in Insert/Update to Chain linker table")
        logging.exception(e)
        return False


def chain_linker_db_logic(request_data,database):

    tenant_id = request_data.get("tenant_id","")

    username = request_data.get('user',"")
    flag = request_data.get('flag',"")
    group_id = request_data.get('group_id',"")
    
    group_list = request_data.get('group',[])

    chain_link_data_dict = {'group_id': group_id, 'last_modified_by': username}

    try:
        logging.info(f"########## Trying to clear for group_id {group_id}")
        delete_group_query = f"delete from chain_linker where group_id='{group_id}'"
        database.execute(delete_group_query)

    except Exception as e:
        logging.info("############## Error in deleting the links for group")
        logging.exception(e)


    
    if group_list:
        for link in group_list:
            chain_link_data_dict['rule_id']=link['rule_id']
            chain_link_data_dict['sequence']=link['sequence']
            chain_link_data_dict['link_type']=link['link_type']
            chain_link_data_dict['created_by']=username
            save_check = insert_or_update_chain_linker(database=database,table="chain_linker",data_dict=chain_link_data_dict)

            if not save_check:
                return jsonify({'flag':False,'message':"Group List/Data is Empty"})


    else:
        return jsonify({'flag':False,'message':"Group List/Data is Empty"})

    return jsonify({'flag':True,'message':"Successfully Saved to Chain Linker Table"})


def check_if_id_exists(column,value,database,table):

    check_query =  f"select count(*) from {table} where {column}='{value}' "

    try:
        check_df = database.execute_(check_query)['count']

        if len(check_df)>0:
            return True
        else:
            return False

    except Exception as e:
        logging.info(f"########## Error while check {column} is existence")
        logging.exception(e)
        return None
        


@app.route('/get_rules_data',methods=['GET','POST'])
def get_routes():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_routes',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        db_config['tenant_id'] = tenant_id

        business_rules_db = DB('business_rules',**db_config)

        try:
            fetch_query = f"select rule_id,rule_name,description as rule_description from rule_base"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            return_data = {'flag':True,'data':rule_list}
            

        except Exception as e:
            logging.info("######## Error in fetching all rules")
            logging.exception(e)
            return_data = {"flag":False,"message":"Error in fetching rules"}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)


    
@app.route('/get_rule_from_id',methods=['GET','POST'])
def get_rule_from_id():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='get_rule_from_id',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        rule_id = data.get('rule_id',"")
        db_config['tenant_id'] = tenant_id
        business_rules_db = DB('business_rules',**db_config)

        try:
            fetch_query = f"select rule_id,rule_name,xml,description as rule_description from rule_base where rule_id='{rule_id}'"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            
            return_data = {'flag':True,'data':rule_list}

        except Exception as e:
            logging.info("######## Error in fetching all rules")
            logging.exception(e)
            return_data = {"flag":False,"message":"Error in fetching rules"}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)


def block_db(database,statement):


    

    db_obj = DB(database, **db_config)

    try:

        return_data2=db_obj.execute_(statement).to_dict(orient='records')

        
        return return_data2


    except Exception as e:
        logging.info("######### Error in Running test function")
        logging.info(e)

    
def block_get_var(var_name):

    return globals().get(var_name,"")


@app.route('/check_function_builder',methods=['GET','POST'])
def check_function_builder():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='check_function_builder',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        test_function = data.get('function',"logging.info('Hello World')")

        exec_code = function_builder(test_function)

        exec_code()

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify({"flag":True,"message":"message"})


def get_data_sources(business_rules_db, case_id, column_name, master_data_columns={}, master=False):
    """Helper to get all the required table data for the businesss rules to apply
    """
    get_datasources_query = "SELECT * from `data_sources`"
    # db_config['tenant_id'] = tenant_id
    # business_rules_db = DB('business_rules', **db_config)
    data_sources = business_rules_db.execute_(get_datasources_query)

    # sources
    sources={}
    # if data_sources[column_name] != "":
    sources = json.loads(list(data_sources[column_name])[0])
    logging.info(f"sources: {sources}")
    
    data = {}
    data_frames={}
    
    if sources: ### Process only if sources has some data
        for database, tables in sources.items():
            
            db = DB(database, **db_config)
            for table in tables:
                if master:
                    if master_data_columns:
                        for table_name, columns_list in master_data_columns.items():
                            if table == table_name:
                                columns_list = ', '. join(columns_list)
                                query = f"SELECT {columns_list} from `{table}`"
                                df = db.execute_(query)
                                data_frames[table]=df
                    else:
                        query = f"SELECT * from `{table}`"
                        df = db.execute_(query)
                        data_frames[table]=df
                else:
                    query = f"SELECT * from `{table}` WHERE case_id = %s"
                    params = [case_id]
                    df = db.execute_(query, params=params)
                    #logging.info(f"got data is {df}")
                    if not df.empty:
                        data[table] = df.to_dict(orient='records')[0]
                    else:
                        data[table] = {}
            
            
            
        # case_id_based_sources = json.loads(list(data_sources['case_id_based'])[0])
    
    return data, sources,data_frames


def function_check(tenant_id, case_id, rule_list, data_tables, update_table_sources, return_vars):
    if len(rule_list)>0:
        BR = BusinessRules(case_id, rule_list, data_tables)
        BR.tenant_id = tenant_id
        BR.return_vars=return_vars

        return_data = BR.evaluate_rule(rule_list)
            
        if BR.changed_fields:
            updates = BR.changed_fields
            BR.update_tables(updates, data_tables, update_table_sources)
        logging.info(f"###### Return Data {return_data}")
        
    else:
        logging.info(f"############ Rules are empty, please check {rule_list}")

def code_to_function_with_try(body_code: str, func_name_: str):
    """
    Converts a block of Python code into a callable function with a try-except block.
    The function's result is stored in the 'results' dictionary.
    
    :param body_code: A string containing the body of the function.
    :param func_name: The name of the dynamically created function.
    :param results: A dictionary to store function results (success or exception).
    :return: None (stores result in 'results').
    """
    func_name=func_name_.split()
    func_name= "_".join(func_name)
    # Wrap the code into a function definition
    func_def = f"def {func_name}(results):\n"
    
    # Add a try-except block around the body inside the string itself
    try_block = "    try:\n"
    
    # Properly indent the body code (2 levels of indentation)
    indented_body = "\n".join([f"        # {line}" if "global" in line else f"        {line}" for line in body_code.splitlines()])
    # indented_body = "\n".join([f"        {line}" for line in body_code.splitlines()])
    
    # Add exception handling, and store the result in results within the code string itself
    except_block = f"""
    except Exception as e:
        results['{func_name}'] = f'Error: {{str(e)}}'
    else:
        results['{func_name}'] = BR.each_out_come
        BR.each_out_come="Sucess But No Output"
        
    """
    
    # Combine everything: function definition, try-except block, and body code
    complete_function_code = func_def + try_block + indented_body + except_block+ f"\n{func_name}(BR.results)\n"
    # print(complete_function_code)
    
    return complete_function_code

session_storage={}
        

@app.route('/run_business_rule', methods=['POST', 'GET'])
def run_business_rule():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json

    logging.info(f"######## Running business with the data: {data}")

    case_id = data.get("case_id",None)
    rule_id = data.get("rule_id","")
    do_save = data.get("do_save",False)
    tenant_id = data.get("tenant_id",None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    optimize = data.get("optimize", "")
    do_update= data.get("do_update",False)
    dynamic_rules = data.get("dynamic_rules", False)
    button = data.get("button","")

    trace_id = rule_id if case_id is None else case_id
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':None,'session_id':None})
        user = ui_data['user']
        session_id = ui_data['session_id']

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='business_rules_api',
        span_name='run_business_rule',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        return_vars = data.get("return_vars","")
        ui_data=data.get("ui_data",{})
        field_changes = ui_data.get('field_changes', [])
        master_data_require = data.get('master_data_require', 'False')
        master_data_columns = data.get('master_data_columns', {})
        if master_data_columns:
            master_data_columns = ast.literal_eval(master_data_columns)

        if case_id=="" or tenant_id=="" or rule_id=="":
            return jsonify({"flag":False,"message":"Please provide valid case_id,tenant_id and rule_id "})

        db_config['tenant_id'] = tenant_id
        business_rules_db = DB('business_rules', **db_config)
        extraction_db = DB('extraction', **db_config)
        # try:
        #     print(F"optimize is {optimize}")
        #     if optimize == "true":
        #         rule_id = ast.literal_eval(rule_id)
        #         rule_id=list(rule_id.values())
        #         print(F"rule_id here is {rule_id}")
        #         rule_id = [item for sublist in rule_id for item in sublist]
        #         print(F"rule_id here is {rule_id}")
        #     else:
        #         rule_id = ast.literal_eval(rule_id)
        # except:
        #     rule_id = rule_id

        try:
            rule_id = ast.literal_eval(rule_id)
        except:
            rule_id = rule_id

        if return_vars=='':
            if len(rule_id)==1:
                rule_list = [rule_id[0]]
            else:
                rule_list = list(rule_id)
        else:
            rule_list = [rule_id]

        # print(f"rule_list is {rule_list}")
        case_id_data_tables, case_id_sources,_ = get_data_sources(business_rules_db, case_id, 'case_id_based')
        # if not data_sr_req:
        if master_data_require == 'True':
            master_data_tables, master_data_sources,master_data_frames = get_data_sources(business_rules_db, case_id, 'master', master_data_columns, master=True)
        else:
            master_data_tables, master_data_sources,master_data_frames = {},{},{}
        # consolidate the data into data_tables
        #logging.info(f"Type of case_id_data_tables: {type(case_id_data_tables)}")
        logging.info(f"upto case_id_data_tables ")
        # logging.info(f"case_id_sources: {case_id_sources}")
        # logging.info(f"master_data_tables: {master_data_tables}")
        # logging.info(f"master_data_sources: {master_data_sources}")

        data_tables = {**case_id_data_tables, **master_data_tables}
        # data_frame=copy.deepcopy(data_tables)
        try:
            
            if len(rule_list)==1:
                query = f"select python_code,rule_name from `rule_base` where rule_id = '{rule_list[0]}'"
                rule_list = business_rules_db.execute_(query)
                print(f"rule_list is {rule_list}")
            else:
                query = f"select python_code,rule_name from `rule_base` where rule_id in {tuple(rule_list)}"
                rule_list = business_rules_db.execute_(query)
                print(f"rule_list is else is {rule_list}")

            # if button.lower() == 're-apply' or dynamic_rules: 
            #     try:
            #         if case_id:
            #             party_id_query = f""" SELECT PARTY_ID FROM OCR WHERE CASE_ID = '{case_id}'"""
            #             result_df = extraction_db.execute_(party_id_query)
            #             party_id = result_df['PARTY_ID'].tolist()[0] if not result_df.empty else None
            #             query = f"select python_code, rule_name from rule_base_phase_2 where PARTY_ID = '{party_id}'"
            #             dynamic_rules_list = business_rules_db.execute_(query)
            #             print(f"dynamic_rules_list------------------{dynamic_rules_list}")

            #     except Exception as e:
            #         logging.info(f'exception as {e}')
            #         dynamic_rules_list = []

            #     if not dynamic_rules_list.empty:
            #         rule_list = pd.concat([rule_list, dynamic_rules_list], ignore_index=True)
            #         logging.info(f'rule_list after adding dynamic rules-----{rule_list}')


            # Final debug output
            # data_tables=data_frame
            #logging.info(f"data_tables:  {type(data_tables)}")

            ######## handling data sources for master tables also
            try:
                logging.info("Comes to if block")
                update_table_sources = {}

                for key in case_id_sources:
                    if key in master_data_sources:
                        update_table_sources[key] = case_id_sources[key] + master_data_sources[key]
                    else:
                        update_table_sources[key] = case_id_sources[key]

                for key in master_data_sources:
                    if key not in update_table_sources:
                        update_table_sources[key] = master_data_sources[key]
            except:
                logging.info(f"Comes to except block")
                update_table_sources = {**case_id_sources, **master_data_sources}

            if return_vars!='':

                logging.info(rule_list)
                for i in range(len(rule_list)):
                    BR = BusinessRules(case_id, rule_list['python_code'][i], data_tables)
                    BR.tenant_id = tenant_id
                    BR.return_vars=return_vars
                    BR.field_changes=field_changes
                    BR.results={}
                    BR.data_frame=data_tables
                    BR.master_data_frames=master_data_frames
                    BR.each_out_come=''
                    BR.cus_table=''
                    return_data = BR.evaluate_rule(rule_list['python_code'][i])
                    logging.info(return_data)        
                    if BR.changed_fields:
                        updates = BR.changed_fields
                        logging.info(f"updates: {updates}")
                        BR.update_tables(updates, data_tables, update_table_sources)

                    logging.info(f"###### Return Data {return_data}")
                        #return jsonify({'flag':True,'data':return_data})

                        #return jsonify({'flag':True,'data':{'message':'Error in executing the business rules'}})
                    
                response_data = {
                    "flag": True,
                    "data": return_data
                }
                #return jsonify(response_data)
            else:
                #logging.info(f"update_table_sources: {update_table_sources}")
                #rules_start=time.time()
                # print(F"start time is {time.time()}")
                #logging.info(rule_list)

                all_rules_python_code=''
                for i in range(len(rule_list)):
                    all_rules_python_code=all_rules_python_code+'\n'
                    all_rules_python_code=all_rules_python_code+code_to_function_with_try(rule_list['python_code'][i], rule_list['rule_name'][i])

                logging.info(f"all_rules_python_code is {all_rules_python_code}")
                data_tables=data_tables
                BR = BusinessRules(case_id, all_rules_python_code, data_tables)
                print(f'BR data is  {BR}')
                BR.tenant_id = tenant_id
                BR.return_vars=return_vars
                BR.field_changes=field_changes
                BR.results={}
                BR.data_frame=data_tables
                BR.master_data_frames=master_data_frames
                BR.each_out_come=''
                BR.cus_table=''
                return_data = BR.evaluate_rule(all_rules_python_code)
                print(f'return data of all rules python code {return_data}')

                print(BR.results,"BR.results")  

                if BR.changed_fields:
                    updates = BR.changed_fields
                    print(f"updates: {updates}")
                    BR.update_tables(updates, data_tables, update_table_sources)

                    #logging.info(f"###### Return Data {return_data}")

            
            logging.info(f"###### do_save {do_save}")
            if not do_save and do_update == True:
                update_to_db(case_id,data_tables,case_id_sources)
            # try:
            #     if 'date_stat' in field_changes:
            #         result=case_creation_cnt(case_id, tenant_id)
            #     else:
            #         pass
            # except Exception as e:
            #     logging.exception(f"Exception occured while updating the case creation cnt!!")
            #     pass
            

            if return_vars=='':
                return_data={"message": "Successfully executed business rules"}
                response_data = {
                    "flag": True,
                    "data": return_data
                }
            else:
                response_data = {
                    "flag": True,
                    "data": return_data
                }


        except Exception as e:
            logging.info(e)
            response_data={
                'flag': False,
                'message': 'Something went wrong in executing business rules',
                'data':{}
            }
        #if table:
        #   response_data['data']['c_dict']=table

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
            response_data['data']['time_consumed']=time_consumed
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        # insert audit
        """audit_data = {"tenant_id": tenant_id, "user": user, "case_id": case_id, 
                        "api_service": "run_business_rule", "service_container": "business_rules_api", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)

        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")"""
        return jsonify(response_data)

def update_to_db(case_id,data_frame,case_id_sources):
    



    for table, colum_values in data_frame.items(): 
        for data_base, inside_tables in case_id_sources.items():
            logging.info(f"data_base: {data_base}")
            logging.info(f"inside_tables: {inside_tables}")
            if table in inside_tables:
                db_connection = data_base
                logging.info(f"db_connection: {db_connection}")
                logging.info(f"db_config: {db_config}")
                respective_table_db = DB(db_connection, **db_config)
                logging.info(f"inside_tables: {colum_values}")
                # Normalize keys to lowercase
                normalized_data = {}
                seen_keys = set()  # Track normalized keys to avoid duplicates

                for key, value in colum_values.items():
                    lower_key = key.lower()  # Convert to lowercase for comparison
                    if lower_key not in seen_keys:
                        normalized_data[key] = value  # Preserve original casing for the first occurrence
                        seen_keys.add(lower_key)
                logging.info(f"final column values: {normalized_data}")
                if 'cluster' in normalized_data :
                    del normalized_data['cluster']
                logging.info(f"final column values: {normalized_data}")
                

                respective_table_db.update(table, update=normalized_data, where={'case_id':case_id})
                break

@app.route('/get_ui_rules', methods=['POST', 'GET'])
def get_ui_rules():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    case_id = data.get('case_id', None)
    tenant_id = data.get("tenant_id", None)
    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='business_rules_api',
            span_name='get_ui_rules',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

    

        db_config['tenant_id'] =tenant_id

        business_rules_db = DB('business_rules', **db_config)

        try:

            
            fetch_query = "SELECT RULE_ID, JAVASCRIPT_CODE  FROM RULE_BASE"
            rule_list = business_rules_db.execute_(fetch_query).to_dict(orient='records')

            rules_dict = {row['RULE_ID']: row['JAVASCRIPT_CODE'] for row in rule_list}
            return_data = {'flag':True,'data':rules_dict}

                   

        except Exception as e:
            logging.execption("####### Error in fetching UI business rules",e)
            return_data = {'flag':False,'message':'Error in fetchign UI business rules'}
            
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        logging.info(f"## BR Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify(return_data)

@app.route('/business_rules_api_health_check', methods=['POST', 'GET'])
def business_rules_api_health_check():
    return jsonify({'flag':True})



def match_key(target, data):
    # Normalize text to match ignoring spaces, underscores, case
    norm = lambda s: s.lower().replace(" ", "").replace("_", "")
    target_norm = norm(target)
    for k in data.keys():
        if norm(k) == target_norm:
            return k
    return None

@app.route('/test_dynamic_rules', methods=['POST', 'GET'])
def test_dynamic_rules():
    """
    Executes or applies a dynamic rule.

    TEST:
    - Calculates total from rule_text (no DB write)

    APPLY:
    - Updates OCR only if final_field already exists
    - Updates field_accuracy (existing logic preserved)
    """
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    case_id = data.get("case_id",None)
    tenant_id = data.get("tenant_id",None)
    changed_data = data.get("changed_data", {})
    flag = data.get("flag", "").lower()

    final_field = changed_data.get("final_field",None)
    tab_name = changed_data.get("tab_name",None)
    rule_text = changed_data.get("rule_text",None)
    final_value = changed_data.get("final_value",None)


    # ---------------- ZIPKIN ----------------
    trace_id = case_id or generate_random_64bit_string()
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name="business_rules_api",
        span_name="test_dynamic_rules",
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5
    ):
        try:
            # ---------------- DB CONTEXT ----------------
            db_config["tenant_id"] = tenant_id
            queues_db = DB("queues", **db_config)
            extraction_db = DB("extraction", **db_config)
            group_access_db=DB("group_access", **db_config)
            query = f"SELECT `party_id` from  `ocr` where `case_id` = '{case_id}'"
            party_id = extraction_db.execute_(query)['party_id'].to_list()[0]
            user = data.get('user', '')
            role_query = f"select role from active_directory where username='{user}'"
            role = group_access_db.execute_(role_query).iloc[0]['role'].lower()
            update_col = "maker_id" if role == "maker" else "checker_id"
            time_col = "maker_datetime" if role == "maker" else "checker_datetime"
            changed_fields_dict = {
                "rule_name": final_field,
                "rule_text":rule_text,
                "tab": tab_name
            }

            changed_fields_str = json.dumps(changed_fields_dict)
            
            # ---------------- VALIDATION ----------------
            if not case_id or not tab_name or not final_field:
                return {
                    "flag": False,
                    "message": "Missing required parameters"
                }

            tab_name = tab_name.lower().strip()

            # HELPER: PARSE LAST (value) — NO REGEX
        
            def parse_term(term):
                term = term.strip()
                if "(" not in term or ")" not in term:
                    return None, None

                open_idx = term.rfind("(")
                close_idx = term.rfind(")")

                if close_idx < open_idx:
                    return None, None

                field_name = term[:open_idx].strip()
                value_str = term[open_idx + 1:close_idx].strip()

                try:
                    value = float(value_str.replace(",", ""))
                except:
                    value = None

                return field_name, value

            # TEST MODE — CALCULATE FROM rule_text
            if flag == "test":
                if not rule_text:
                    return {
                        "flag": False,
                        "message": "rule_text missing for test"
                    } 
                
                total = 0.0
                current = ""
                paren_depth = 0

                for ch in rule_text:
                    if ch == "(":
                        paren_depth += 1
                    elif ch == ")":
                        paren_depth -= 1

                    if ch in "+-" and paren_depth == 0:
                        if current.strip():
                            sign = -1 if current.strip().startswith("-") else 1
                            _, val = parse_term(current)
                            if val is not None:
                                total += sign * val
                        current = ch
                    else:
                        current += ch

                # last term
                if current.strip():
                    sign = -1 if current.strip().startswith("-") else 1
                    _, val = parse_term(current)
                    if val is not None:
                        total += sign * val


                rounded = round(total, 2)

                # if it's a whole number, return int
                if rounded.is_integer():
                    rounded = int(rounded)

                  
                return {
                    "flag": True,
                    "total_value": rounded,
                    "message": "Final value calculated successfully!"
                }


            # APPLY MODE — UPDATE OCR
            if flag == "apply":

                # ---------- FETCH OCR TAB ----------
                qry = f"SELECT {tab_name} FROM ocr WHERE case_id='{case_id}'"
                df = extraction_db.execute_(qry)

                if df is None or df.empty:
                    raise Exception("OCR record not found")

                tab_json = json.loads(df.iloc[0][tab_name] or "{}")

                # ❗ Do NOT create new fields
                if final_field not in tab_json:
                    return {
                        "flag": False,
                        "message": f"Field '{final_field}' not present in OCR. Update skipped."
                    }
                try:
                    #print(f"entereddddddddddddddddddddddddd heree")
                    qry = f"select CAST(edited_fields AS VARCHAR2(4000)) as edited_fields from ocr where case_id='{case_id}'"
                    df = extraction_db.execute_(qry)

                    edited_fields_raw = df.iloc[0]['edited_fields'] or "[]"

                    try:
                        edited_fields_list = json.loads(edited_fields_raw)
                    except:
                        edited_fields_list = []

                    # Ensure it's a list
                    if not isinstance(edited_fields_list, list):
                        edited_fields_list = []

                    # Remove final_field if present
                    if final_field in edited_fields_list:
                        edited_fields_list.remove(final_field)

                    # Save back to DB
                    clob_val_edited = clob_chunks_sql(json.dumps(edited_fields_list))
                    #print(f"before updationnnnnnnnnnnnnnnnnn")

                    qry = f"""
                        UPDATE ocr
                        SET edited_fields = {clob_val_edited}
                        WHERE case_id='{case_id}'
                    """
                    extraction_db.execute_(qry)
                except Exception as e:
                    logging.exception(f"The Exception Caught is:{e}")
                    pass

                # ---------- UPDATE FIELD ----------
                try:
                    final_value = round(float(final_value), 2)

                    # smart formatting (same as test)
                    if final_value.is_integer():
                        final_value = int(final_value)
                except:
                    return {
                        "flag": False,
                        "message": "Invalid final_value"
                    }

                tab_json[final_field] = str(final_value)

                if "tab_view" in tab_json and "rowData" in tab_json["tab_view"]:
                    for row in tab_json["tab_view"]["rowData"]:
                        if row.get("fieldName") == final_field:
                            row["value"] = tab_json[final_field]

                # ---------- CLOB HELPER ----------
                clob_val = clob_chunks_sql(json.dumps(tab_json))

                qry = f"""
                    UPDATE ocr
                    SET {tab_name} = {clob_val}
                    WHERE case_id='{case_id}'
                """
                extraction_db.execute_(qry)

                # FIELD ACCURACY — EXISTING BEHAVIOR
                qry = f"""
                    SELECT fields_extracted, fields_edited, not_extracted_fields,
                           extraction, manual, not_used_fields
                    FROM field_accuracy
                    WHERE case_id='{case_id}'
                """
                fa_df = queues_db.execute_(qry)

                if not fa_df.empty:
                    row = fa_df.iloc[0]

                    fields_extracted = json.loads(row["fields_extracted"] or "{}")
                    fields_edited = json.loads(row["fields_edited"] or "{}")
                    not_extracted_fields = json.loads(row["not_extracted_fields"] or "{}")

                    extraction = int(row["extraction"] or 0)
                    manual = int(row["manual"] or 0)
                    not_used_fields = int(row["not_used_fields"] or 0)

                    fields_extracted[final_field] = f"E:{final_value}"
                    extraction += 1

                    extraction_percentage = round(
                        (extraction / (extraction + manual)) * 100
                        if (extraction + manual) > 0 else 0, 2
                    )
                    manual_percentage = round(
                        (manual / (extraction + manual)) * 100
                        if (extraction + manual) > 0 else 0, 2
                    )

                    qry = f"""
                        UPDATE field_accuracy
                        SET
                            fields_extracted={clob_chunks_sql(json.dumps(fields_extracted))},
                            not_extracted_fields={clob_chunks_sql(json.dumps(not_extracted_fields))},
                            fields_edited={clob_chunks_sql(json.dumps(fields_edited))},
                            extraction='{extraction}',
                            manual='{manual}',
                            extraction_percentage='{extraction_percentage}',
                            manual_percentage='{manual_percentage}',
                            number_of_fields='{extraction + manual}',
                            not_used_fields='{not_used_fields}'
                        WHERE case_id='{case_id}'
                    """
                    queues_db.execute_(qry)

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
                    "USER_": user,
                    "CASE_ID": case_id,
                    "API_SERVICE": "test_dynamic_rules",
                    "INGESTED_QUEUE": None,
                    "SERVICE_CONTAINER": "business_rules_api",
                    "CHANGED_DATA": changed_fields_str,
                    "TABLES_INVOLVED": "",
                    "MEMORY_USAGE_GB": str(memory_consumed),
                    "TIME_CONSUMED_SECS": time_consumed,
                    "REQUEST_PAYLOAD": None,
                    "RESPONSE_DATA": None,
                    "TRACE_ID": trace_id,
                    "SESSION_ID": None
                }
                logging.info(f"{audit_data}#####audit_data")

                insert_into_audit(case_id, audit_data,tenant_id)



                return {
                    "flag": True,
                    "message": "Successfully Applied The Rule for This Case"
                }

            return {
                "flag": False,
                "message": "Invalid flag value"
            }

        except Exception as e:
            logging.exception("Error in test_dynamic_rules")
            return {
                "flag": False,
                "message": str(e)
            }
