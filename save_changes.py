import json
import os
import requests
import ast
import re
import math
import psutil
from concurrent.futures import ThreadPoolExecutor

from app.db_utils import DB
from time import time as tt
from ace_logger import Logging
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from dateutil import parser
from app import app
from datetime import datetime
from datetime import date
import calendar
import pandas as pd
import numpy as np

# app = Flask(__name__)
import pytz
tmzone = 'Asia/Kolkata'

logging = Logging(name='save_changes')
from concurrent.futures import ThreadPoolExecutor

# Global thread pool — reuse across all A() calls
executor = ThreadPoolExecutor(max_workers=10)

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
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

def insert_into_audit(case_id, data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True

def update_table(db, case_id, file_name,data):
    logging.info('Updating table...')

    query = f"SELECT `fields_changed` from `field_accuracy` WHERE case_id='{case_id}'"
    fields_json_string_df = db.execute_(query)
    if not fields_json_string_df.empty:
        update_field_accuracy(
            fields_json_string_df,case_id, db,data
        )
    else:
        insert_field_accuracy(case_id, file_name, db,data)
    return "UPDATED TABLE"


def insert_field_accuracy(case_id, file_name, db,data):
    extraction_db = DB('extraction', **db_config)
    fields=data['fields']
    fields_changed=data['field_changes']
    field_changes={}
    for field in fields_changed:
        temp={}
        temp_main={}
        value_changed=fields[field]
        temp['changed_value']=value_changed
        query=f"select `{field}` from `ocr` where case_id='{case_id}'"
        actual_value=extraction_db.execute_(query)[field].to_list()[0]
        temp['actual_value']=actual_value
        temp_main[field]=temp
        field_changes.update(temp_main)
    total_fields_count=len(fields)
    logging.info(f"count of total fields are {total_fields_count}")
    percentage = len(fields_changed)/total_fields_count
    logging.info(f"percentage calculated {percentage}")
    query = f"INSERT INTO `field_accuracy` (`id`, `case_id`, `file_name`, `fields_changed`, `percentage`) VALUES (NULL,'{case_id}','{file_name}','{json.dumps(field_changes)}','{percentage}')"
    db.execute(query)
    logging.info(f"Inserted into field accuracy for case `{case_id}`")


def update_field_accuracy(fields_json_string_df,case_id,db, data):
    #logging.info(f"in update field accuracy function")
    fields_json_string = fields_json_string_df['fields_changed'][0]
    fields_json = json.loads(fields_json_string)
    logging.info(f"fields changed are {fields_json}")
    extraction_db = DB('extraction', **db_config)
    fields=data['fields']
    fields_changed=data['field_changes']
    for field in fields_changed:
        temp={}
        temp_main={}
        value_changed=fields[field]
        temp['changed_value']=value_changed
        query=f"select `{field}` from `ocr` where case_id='{case_id}'"
        actual_value=extraction_db.execute_(query)[field].to_list()[0]
        temp['actual_value']=actual_value
        temp_main[field]=temp
        fields_json.update(temp_main)

    total_fields_count=len(fields)
    logging.info(f"count of total fields are {total_fields_count}")
    fields_json=fields_json
    #logging.debug(f"Fields JSON after: {fields_json}")
    percentage = len(fields_json)/total_fields_count
    logging.info(f"percentage calculated for {case_id} is {percentage}")
    query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(fields_json)}', `percentage`= '{percentage}'  WHERE case_id='{case_id}'"
    db.execute(query)
    logging.info(f"Updated field accuracy table for case `{case_id}`")


def dict_split_wise_table(orgi_dict):
    '''
    changes:- added new function for save changes to work on multiple tables
    author:- Amara Sai Krishna Kumar
    '''
    orgi_dict = orgi_dict
    tx_master_db = DB('tx_master', **db_config)

    query = f"select * from master_upload_tables where purpose != ''"
    master_upload_tables_df = tx_master_db.execute_(query)
    l=[]
    parent_dict = orgi_dict
    for each in master_upload_tables_df.index:
        total_table_name = master_upload_tables_df['table_name'][each]
        purpose = master_upload_tables_df['purpose'][each]       
        #logging.info(f"##orgi_dict is {orgi_dict}")
        logging.info(f"total_table_name: {total_table_name}, purpose:{purpose}")
        temp={}
        temp1={}
        for k,v in orgi_dict.items():
            test = k.split('_')[0]
            if test.lower() == purpose.lower():
                temp[k]=v
            else:
                continue
            temp1[total_table_name] = temp
        l.append(temp1)
    
    for i in range(len(l)):
        for k,v in l[i].items():
            child_dict = v
            for key in child_dict.keys():
                if key in parent_dict:
                    parent_dict.pop(key)
    l.append({'extraction.ocr':parent_dict})
    logging.info(f"fields are  {l}")
    return l


def count_different_pairs(ui_dict1, db_dict2):
    changed={}
    not_ext=0
    for key in ui_dict1.keys():
        if key not in db_dict2:
            temp={}
            temp['changed_value']=ui_dict1[key]
            temp['actual_value']=''
            changed[key]= temp
            not_ext+=1
        elif key in db_dict2 and ui_dict1[key] != db_dict2[key]:
            if not db_dict2[key]:
                temp={}
                temp['changed_value']=ui_dict1[key]
                temp['actual_value']=db_dict2[key]
                changed[key]= temp
                not_ext+=1
            else:
                temp={}
                temp['changed_value']=ui_dict1[key]
                temp['actual_value']=db_dict2[key]
                changed[key]= temp
    return changed,not_ext

def field_accuracy_(ui_data,tenant_id,case_id):
    fields=ui_data['fields']
    changes_fields=ui_data['field_changes']
    changed_all={}
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    queue_db = DB('queues', **db_config)
    not_extracted_all=0
    #these are hard codded for now need to create a db table and get it from there 
    custom_data=['STOCK STATEMENT_OCR','DEBITORS STATEMENT_OCR','CREDITORS_OCR','SECURITY DEPOSIT_OCR','ADVANCES SUPPLIERS_OCR','BANK OS_OCR','ADVANCES DEBTORS_OCR','SALES_OCR','ADVANCES_OCR','PURCHASES_OCR']
    other_fields=['date','customer_name']
    for key,value in fields.items():
        if key in custom_data and key in changes_fields:
            query = f"select `{key}` from `ocr` where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)[key].to_list()
            if query_data:
                value=json.loads(value)
                extracted=json.loads(query_data[0])
                changed,not_ext=count_different_pairs(value, extracted)
                not_extracted_all+=not_ext
                changed_all.update(changed)
                logging.info(f"changed fields are {changed_all}")
        elif key in changes_fields and key in other_fields:
            query = f"select `{key}` from `ocr` where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)[key].to_list()
            if query_data:
                extracted=query_data[0]
                if extracted!=value:
                    temp={}
                    temp['changed_value']=value
                    temp['actual_value']=extracted
                    changed_all[key]= temp
                else:
                    temp={}
                    temp['changed_value']=value
                    temp['actual_value']=extracted
                    changed_all[key]= temp

    query = f"SELECT * from `field_accuracy` WHERE case_id='{case_id}'"
    field_accuracy = queue_db.execute_(query).to_dict(orient='records')[0]
    if field_accuracy:
        extarcted=int(field_accuracy['extraction'])
        logging.info(f"extraction count is {extarcted}")
        if field_accuracy['fields_changed']:
            fields_changed = json.loads(field_accuracy['fields_changed'])
            fields_changed.update(changed_all)
            changed=len(fields_changed)
            total_fileds=int(field_accuracy['total_fields'])+not_extracted_all
            accuracy = (extarcted-changed) / (total_fileds) * 100
            query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(fields_changed)}', `fields_modified`= '{len(fields_changed)}',`percentage`='{accuracy}' WHERE case_id='{case_id}'"
            queue_db.execute(query)
        else:
            changed=len(changed_all)
            total_fileds=int(field_accuracy['total_fields'])+not_extracted_all
            accuracy = ((extarcted-changed) / (total_fileds)) * 100
            query = f"UPDATE `field_accuracy` SET `fields_changed` = '{json.dumps(changed_all)}', `fields_modified`= '{len(changed_all)}' ,`percentage`='{accuracy}' WHERE case_id='{case_id}'"
            queue_db.execute(query)

    return ''

def compare_dicts(dict1, dict2):
    if set(dict1.keys()) != set(dict2.keys()):
        return False
    for key in dict1:
        if dict1[key] != dict2[key]:
            return False
    return True

def calculate_distance(point1, point2):
    # logging.info(point1,point2)
    x1, y1 = point1['left'], point1['top']
    x2, y2 = point2['left'], point2['top']
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return distance

def calculate_value_distance(point1, point2):
    # logging.info(point1,point2)
    x1 = (point1['right']+point1['left'])/2
    x2= (point2['right']+ point2['left'])/2
    distance = abs(x1-x2)
    return distance


def from_cor_dict(v,inc=0):
    
    img_width = v['width']
    
    temp_dic={}
    temp_dic['word']=v['word']
    temp_dic['width']= v['area']['width'] * (670 / img_width)
    temp_dic['height']= v['area']['height'] * (670 / img_width)
    temp_dic['pg_no']= v['area']['page']
    temp_dic['bottom']= v['area']['y'] + v['area']['height']+inc
    temp_dic['top']= v['area']['y']-inc
    temp_dic['right']= v['area']['x'] + v['area']['width']+inc
    temp_dic['left']= v['area']['x']-inc
    
    return temp_dic


def get_value_dict(v,ocr_data,inc):
    
    if inc>100:
        return {}

    re_val_=from_cor_dict(v,inc)
    re_word=re_val_.get('word','')
    re_val={}
    for word in ocr_data:
        if word['top']>=re_val_['top'] and word['bottom']<=re_val_['bottom']:
            if word['left']>=re_val_['left'] and word['right']<=re_val_['right']:
                if re_word==word['word']:
                    re_val=word
                    break
                
    if not re_val:
        re_val=get_value_dict(v,ocr_data,inc+10)

    return re_val



def get_cont(row_top,sorted_words,width,col_f,api=''):
    cont={}
    logging.info(F"cont is {cont} and width is {width} and row_top is {row_top} and {col_f}")
    if width<0:
        return cont
    
    for word in sorted_words:
        if not api:
            if word['top']<row_top:
                # logging.info(F"here is {word} and {col_f}")
                if int(abs(word['right']-word['left']))>width:
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='above'
                    cont=word
                    break
        else:
            if word['top']>row_top:
                # logging.info(F"here is {word} and {col_f}")
                if int(abs(word['right']-word['left']))>width:
                    word['hypth']=calculate_distance(word, col_f)
                    word['context_width']=width
                    word['position']='below'
                    cont=word
                    break
    if not cont:
        return get_cont(row_top,sorted_words,width-100,col_f,api)
    else:
        return cont


def is_alphanumeric(word):
    return any(char.isalpha() for char in word) and any(char.isdigit() for char in word)


def line_wise_ocr_data(words):
    #ocr_word=[]
    sorted_words = sorted(words, key=lambda x: x["top"])

    # Group words on the same horizontal line
    line_groups = []
    current_line = []
    for word in sorted_words:
        if not current_line:
            current_line.append(word)
        else:
            diff=abs(word["top"] - current_line[-1]["top"])
            if diff < 5:
                # Word is on the same line as the previous word
                current_line.append(word)
            else:
                # Word is on a new line
                line_groups.append(current_line)
                current_line = [word]

        
    if current_line:
        line_groups.append(current_line)

                
    return line_groups   

def get_col(sorted_words,re_val):

    if len(sorted_words)==0:
        return '',{},0
    
    next_values=[]
    k_left=re_val["left"]
    min_dif=10000
    
    
    for line in sorted_words:
        
        if line:
            diff_ver=abs(abs((line[0]["top"]+line[0]["bottom"])/2) - abs((re_val["top"]+re_val["bottom"])/2))
           
            if diff_ver<min_dif:
                #logging.info(f"diff_ver is {diff_ver} for line {line}")
                next_values=[]
                found_line=[]
                min_dif=diff_ver
                for word in line:
                    found_line.append(word)
                    if k_left>word['left']:
                        next_values.append(word)
    new_sorted_words=[]
    for line in sorted_words:
        tem=[]
        for word in line:
            if word not in found_line:
                tem.append(word)
        new_sorted_words.append(tem)

    logging.info(F"next_values for value for col_head are is {next_values,  min_dif} ")
    
    sorted_list = sorted(next_values, key=lambda x: x['right'])
    col_head=''
    
    for item in sorted_list:
        t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
        t_num=re.sub(r'[^a-zA-Z]', '', item['word'])
        if (not t or len(t)<5) and len(t_num) < 4:
            continue
        if t.isalpha():
            col_head = item['word']
            col_f=item
            break       
    if not col_head:
        #logging.info(F"sorted_list is {sorted_list}")
        for item in sorted_list:
            t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
            t_num=re.sub(r'[^a-zA-Z]', '', item['word'])
            if not t or len(t)<5 or len(t_num)<5:
                continue
            t_fl=False
            numeric_pattern = re.compile(r'^[\d,]+(\.\d+)?$')
            l_i=item['word'].split()
            for i in l_i:
                if numeric_pattern.match(i):
                    t_fl=True
                    break
            if not t_fl or len(t)>10:
                col_head = item['word']
                col_f=item
                break

    if not col_head:
        return get_col(new_sorted_words,re_val)
    else:
        return col_head,col_f,min_dif


def get_top_header(sorted_list_head,col_f,flag=False):

    row_head_list=[]

    for item in sorted_list_head:
        logging.info(F" item is {item}")
        #i_cen=abs(item['right']+item['left']/2)
        t=re.sub(r'[^a-zA-Z0-9]', '', item['word'])
        #t_alp=re.sub(r'[^a-zA-Z]', '', item['word'])
        if not t:
            continue
        numeric_headers=['90','0','180','91','120','121','150','151','181','270','271','365']
        if not row_head_list:
            row_top=item['top']
        if t and len(t)>=4 and t.isalpha():
            #logging.info(F" item is {item}")
            if abs(item['bottom']-row_top)<=(2*abs(item['top']-item['bottom'])):
                #logging.info(abs(item['bottom']-row_top),abs(item['top']-item['bottom']))
                if col_f['right']<item['left']:
                    row_head_list.append(item['word'])
                    row_top=item['top']       
        else:
            numeric=re.sub(r'[^a-zA-Z0-9]', ' ', item['word'])
            list_i=numeric.split()
            t_fl=False
            for i in list_i:
                i_numeric=re.sub(r'[^a-zA-Z]', '', i)
                if i not in numeric_headers and not i_numeric:
                    t_fl=True
                    break
            if not t_fl:
                if abs(item['bottom']-row_top)<=(2*abs(item['top']-item['bottom'])):
                    if col_f['right']<item['left']:
                        row_head_list.append(item['word'])
                        row_top=item['top']
        #logging.info(F"row_head_l is {row_head_list}")

    return row_head_list,row_top



def get_headers(act_val,re_val_,case_id,tenant_id):

    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    ocr_data_all=json.loads(document_id_df_all)
    ocr_data=ocr_data_all[int(re_val_['pg_no'])]

    re_li=act_val.get('ocr_data',[])
    if len(re_li) == 1:
        re_val=re_li[0]
    elif len(re_li) == 0:
        return '','',{}
    else:
        re_val=get_value_dict(act_val,ocr_data,0)
    if not re_val:
        re_val=re_val_
    logging.info(F"re_val got is {re_val}")
    #next_values=[]
    return_list=[]
    
    #k_left=re_val["left"]
    centorid=(re_val['left']+re_val['right'])/2
    sorted_words=line_wise_ocr_data(ocr_data)

    for line in sorted_words:
            
            flag=False
            for word in line:
                #nned to do based in center point top -bottom
                if (word['left']<=centorid<=word['right'] or word['left']<=re_val['right']<=word['right'] or word['left']<=re_val['left']<=word['right'] )and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['right']<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-10<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-20<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-30<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
                elif (word['left']<=re_val['left']-40<=word['right'] ) and word['top'] < re_val['top']:
                    return_list.append(word)
                    flag=True
            if flag == False:
                sorted_left = sorted(line, key=lambda x: x["left"],reverse=True)
                for wo in sorted_left:
                    if wo['right']<re_val['left'] and abs(wo['right']-re_val['left'])<100 and wo['top'] < re_val['top']:
                        if return_list and return_list[-1]['bottom']<wo['top']:
                            return_list.append(wo)
                            break
                        else:
                            return_list.append(wo)
                            break
            
    col_head=''
    row_head_list=[]
    row_head=''
    cont={}
    

    col_head,col_f,diff_col=get_col(sorted_words,re_val)

    logging.info(F"return_list for value for row_head are is {return_list} ")

    #sort in reverse order
    sorted_list_head = sorted(return_list, key=lambda x: x['top'] ,reverse=True)

    row_head_list,row_top=get_top_header(sorted_list_head,col_f)

    if not row_head_list:
        row_head_list,row_top=get_top_header(sorted_list_head,col_f,True)

    #logging.info(F"final row_head_list  are {row_head_list} and {col_head} ")
    for head in row_head_list:
        row_head=row_head+' '+head

    logging.info(F"final row_head and col_head are {row_head} and {col_head} ")
    sorted_words = sorted(ocr_data, key=lambda x: x["top"], reverse=True)
    cont=get_cont(row_top,sorted_words,1000,col_f)
    #logging.info(cont)
    if not cont:
        cont=get_cont(row_top,sorted_words,1000,col_f,True)
    #logging.info(F"final cont is {cont} ")
    
    #logging.info(F"row_head col_head is {row_head, col_head  } ")
    return col_head,row_head,cont,diff_col


def get_training_data(case_id, tenant_id):
    db_config['tenant_id'] = tenant_id
    template_db = DB('template_db', **db_config)
    extraction=DB('extraction',**db_config)
    query = f"SELECT `party_id` from  `ocr` where `case_id` = '{case_id}'"
    party_id = extraction.execute_(query)['party_id'].to_list()[0]
    logging.info(f"party_id got is {party_id}")


    PRIORITY_DATA = {}
    PRIORITY_CONTEXT = {}
    PRIORITY_FORMULA = {}

    if  party_id:
        
        try:
            #print(f"he party_id is::::::::{party_id}")
            query = f"""
                SELECT 
                    `PRIORITY_DATA`,
                    `PRIORITY_CONTEXT`,
                    `PRIORITY_FORMULA`
                FROM `trained_info`
                WHERE `party_id` = '{party_id}'
            """
            data = template_db.execute_(query)
            logging.info(f"Training data fetched for party_id {party_id}: {data}")

            # --- PRIORITY_DATA ---
            try:
                PRIORITY_DATA_LIST = data['PRIORITY_DATA'].to_list()
                PRIORITY_DATA = json.loads(PRIORITY_DATA_LIST[0]) if PRIORITY_DATA_LIST else {}
            except Exception as e:
                logging.info(f"Error parsing PRIORITY_DATA for party_id {party_id}: {e}")
                PRIORITY_DATA = {}

            # --- PRIORITY_CONTEXT ---
            try:
                PRIORITY_CONTEXT_LIST = data['PRIORITY_CONTEXT'].to_list()
                PRIORITY_CONTEXT = json.loads(PRIORITY_CONTEXT_LIST[0]) if PRIORITY_CONTEXT_LIST else {}
            except Exception as e:
                logging.info(f"Error parsing PRIORITY_CONTEXT for party_id {party_id}: {e}")
                PRIORITY_CONTEXT = {}

            # --- PRIORITY_FORMULA ---
            try:
                PRIORITY_FORMULA = data['PRIORITY_FORMULA'].to_list()
                if PRIORITY_FORMULA:
                    try:
                        PRIORITY_FORMULA = json.loads(PRIORITY_FORMULA[0])
                        if not PRIORITY_FORMULA or PRIORITY_FORMULA is None:
                            PRIORITY_FORMULA = {}
                    except Exception:
                        PRIORITY_FORMULA = {}
                else:
                    PRIORITY_FORMULA = {}
            except Exception as e:
                logging.info(f"Error parsing PRIORITY_FORMULA for party_id {party_id}: {e}")
                PRIORITY_FORMULA = {}
        except Exception as e:
            logging.error(f"the issue is:::{e}")
    return PRIORITY_CONTEXT, PRIORITY_DATA, PRIORITY_FORMULA

# ##update menas codee xinted13_10_2025
# def get_training_data(case_id,tenant_id):
#     db_config['tenant_id']=tenant_id
#     temaplate_db=DB('template_db',**db_config)
#     extraction=DB('extraction',**db_config)
#     query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
#     CUSTOMER_NAME = extraction.execute_(query)['customer_name'].to_list()[0]
#     logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
#     PRIORITY_DATA={}
#     PRIORITY_CONTEXT={}
#     PRIORITY_FORMULA={}
#     if CUSTOMER_NAME:
#         CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
#         query = f"SELECT `PRIORITY_DATA`,`PRIORITY_CONTEXT`,`PRIORITY_FORMULA` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
#         data = temaplate_db.execute_(query)
#         logging.info(f"CUSTOMER_NAME got is {data}")
#         try:
#             PRIORITY_DATA=data['PRIORITY_DATA'].to_list()
#             if PRIORITY_DATA:
#                 PRIORITY_DATA=json.loads(PRIORITY_DATA[0])
#             else:
#                 PRIORITY_DATA={}
#             PRIORITY_CONTEXT=data['PRIORITY_CONTEXT'].to_list()
#             if PRIORITY_CONTEXT:
#                 PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT[0])
#             else:
#                 PRIORITY_CONTEXT={}
#             PRIORITY_FORMULA=data['PRIORITY_FORMULA'].to_list()
#             if PRIORITY_FORMULA:
#                 try:
#                     PRIORITY_FORMULA=json.loads(PRIORITY_FORMULA[0])
#                     if not PRIORITY_FORMULA[0] or PRIORITY_FORMULA[0] == None:
#                         PRIORITY_FORMULA={}
#                 except:
#                     PRIORITY_FORMULA={}
#             else:
#                 PRIORITY_FORMULA={}
#         except Exception as e:
#             logging.exception(f"exception occured ..{e}")
#             pass

#     return PRIORITY_CONTEXT,PRIORITY_DATA,PRIORITY_FORMULA

# ##update code menasn as oer 13_10_2025
# def update_tarining_data(PRIORITY_DATA,PRIORITY_CONTEXT,PRIORITY_FORMULA,case_id,tenant_id):
#     db_config['tenant_id']=tenant_id
#     temaplate_db=DB('template_db',**db_config)
#     extraction_db=DB('extraction',**db_config)
#     query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
#     CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
#     logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
#     if CUSTOMER_NAME:
#         CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
#         insert_data = {
#             'PRIORITY_DATA':json.dumps(PRIORITY_DATA),
#             'PRIORITY_CONTEXT':json.dumps(PRIORITY_CONTEXT),
#             'PRIORITY_FORMULA':json.dumps(PRIORITY_FORMULA)
#         }
#         temaplate_db.update('trained_info', update=insert_data, where={'CUSTOMER_NAME': CUSTOMER_NAME})
#     return True

def update_tarining_data(PRIORITY_DATA, PRIORITY_CONTEXT, PRIORITY_FORMULA, case_id, tenant_id):
    db_config['tenant_id'] = tenant_id
    template_db = DB('template_db', **db_config)
    extraction_db=DB('extraction',**db_config)
    query = f"SELECT `party_id` from  `ocr` where `case_id` = '{case_id}'"
    party_id = extraction_db.execute_(query)['party_id'].to_list()[0]
    logging.info(f"party_id got is {party_id}")

    if  party_id:
        try:
            # Prepare the update data
            insert_data = {
                'PRIORITY_DATA': json.dumps(PRIORITY_DATA),
                'PRIORITY_CONTEXT': json.dumps(PRIORITY_CONTEXT),
                'PRIORITY_FORMULA': json.dumps(PRIORITY_FORMULA)
            }
                # Record exists — update it
            logging.info(f"Updating existing training data for party_id {party_id}.")
            template_db.update('trained_info', update=insert_data, where={'party_id': party_id})
        except Exception as e:
            logging.error(f"the isueeu is:::::{e}")
    return True

def generate_formula(input_formula,mapping):
    parts = []
    current_part = ''
    for char in input_formula:
        if char in ['+', '-', '*', '/']:
            parts.append(current_part)
            parts.append(char)
            current_part = ''
        else:
            current_part += char
    parts.append(current_part)
    output_parts = [mapping.get(part, part) for part in parts]
    output_formula = ''.join(output_parts)
    return output_formula

# def generate_tab_view(data):
#     """
#     Generate tab_view structure from data dictionary
#     """
#     tabular_data = {
#         "header": ["fieldName", "value", "aging", "margin", "formula"],
#         "rowData": []
#     }
#     print(f"the data is::::::::::{data}")
    
#     for field, value in data.items():
#         if field == 'tab_view':  # Skip self field to avoid recursion
#             continue
        
#         # Extract value from nested structure
#         if isinstance(value, dict) and 'a.v' in value:
#             value = list(value['a.v'].values())[0] if value['a.v'] else ''
#         elif not isinstance(value, str):
#             value = str(value) if value else ''
        
#         row = {
#             "fieldName": field, 
#             "value": value, 
#             "aging": "", 
#             "margin": "", 
#             "formula": ""
#         }
#         tabular_data["rowData"].append(row)

#     return tabular_data
def generate_tab_view(data):
    tabular_data = {
        "header": ["fieldName", "value", "aging", "margin", "formula"],
        "rowData": []
    }

    #print(f"the data is::::::::::{data}")

    # If existing tab_view exists, use those values
    old_tab = data.get("tab_view", {}).get("rowData", [])

    old_map = {row["fieldName"]: row for row in old_tab}

    for field, value in data.items():

        if field == 'tab_view':
            continue

        # Extract value
        if isinstance(value, dict) and 'a.v' in value:
            value = list(value['a.v'].values())[0] if value['a.v'] else ''
        elif not isinstance(value, str):
            value = str(value) if value else ''

        # Preserve old aging/margin/formula if it existed
        old = old_map.get(field, {})
        #print(f"the old is::::{old}")
        row = {
            "fieldName": field,
            "value": value,
            "aging": old.get("aging", ""),
            "margin": old.get("margin", ""),
            "formula": old.get("formula", "")
        }

        tabular_data["rowData"].append(row)

    return tabular_data

# def filter_active_components_only(recommended_changes, party_id, extraction_db):
#     """
#     Filter recommended_changes to only include components that are active 
#     in both AGE_MARGIN_WORKING_UAT and COMPONENT_MASTER
#     """
#     filtered_changes = {}
    
#     for col, rec in recommended_changes.items():
#         filtered_changes[col] = {}
        
#         for main_label, lab_changes in rec.items():
#             try:
#                 # Check AGE_MARGIN_WORKING_UAT
#                 age_query = f"""
#                     SELECT component_name
#                     FROM AGE_MARGIN_WORKING_UAT
#                     WHERE party_id = '{party_id}'
#                     AND component_name = '{main_label}'
#                     AND is_active = 1
#                 """
#                 age_result = extraction_db.execute_(age_query).to_dict(orient='records')
                
#                 if not age_result:
#                     print(f"Skipping '{main_label}' - not active in AGE_MARGIN_WORKING_UAT")
#                     continue
                
#                 # Check COMPONENT_MASTER
#                 comp_query = f"""
#                     SELECT component_name
#                     FROM COMPONENT_MASTER
#                     WHERE CAST(component_name AS VARCHAR2(4000)) = '{main_label}'
#                     AND CAST(status AS VARCHAR2(4000)) = 'ACTIVE'
#                 """
#                 comp_result = extraction_db.execute_(comp_query).to_dict(orient='records')
                
#                 if not comp_result:
#                     print(f" Skipping '{main_label}' - not active in COMPONENT_MASTER")
#                     continue
                
#                 # Component is active in both - include it
#                 filtered_changes[col][main_label] = lab_changes
#                 print(f" Component '{main_label}' is active - including")
                
#             except Exception as e:
#                 print(f"Error checking '{main_label}': {e} - skipping")
#                 continue
        
#         # Remove empty columns
#         if not filtered_changes[col]:
#             del filtered_changes[col]
#     print(f"the finltered changes are::{filtered_changes}")
#     return filtered_changes

def save_recommended(recommended_changes,cropped_data,formula_data,case_id,extraction_db,tenant_id):
    # print(f"the recommendded changes are:::::::{recommended_changes}")
    # print(f"the cropped_data are:::::::{cropped_data}")
    # print(f"the formula_data are:::::::{formula_data}")
    qry = f"SELECT party_id FROM ocr WHERE case_id='{case_id}'"
    PARTY_ID = extraction_db.execute_(qry).to_dict(orient='records')
    #print(f"the party id us:::::::::{PARTY_ID}")
     # *** ADD THIS SINGLE LINE ***
    # recommended_changes = filter_active_components_only(recommended_changes, PARTY_ID, extraction_db)
    
    PRIORITY_CONTEXT,PRIORITY_DATA,PRIORITY_FORMULA=get_training_data(case_id,tenant_id)
    try:
        try:
            PRIORITY_CONTEXT=json.loads(json.loads(PRIORITY_CONTEXT))
        except:
            PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT)
    except:
        PRIORITY_CONTEXT=PRIORITY_CONTEXT
    try:
        PRIORITY_DATA=json.loads(PRIORITY_DATA)
    except:
        PRIORITY_DATA=PRIORITY_DATA
    try:
        PRIORITY_FORMULA=json.loads(PRIORITY_FORMULA)
    except:
        PRIORITY_FORMULA=PRIORITY_FORMULA

    logging.info(F"before changes PRIORITY_DATA is {PRIORITY_DATA} ")
    logging.info(F"before changes PRIORITY_FORMULA is {PRIORITY_FORMULA} ")
    logging.info(F"before changes PRIORITY_CONTEXT is {PRIORITY_CONTEXT} ")
    final_dict={}
    #data=request.json
    for col,rec in recommended_changes.items():
        query = f"select `{col}` from `ocr` where case_id ='{case_id}'"
        #print(f"the query is::::::::{query}")
        ac = extraction_db.execute_(query)[col].to_list()[0]
        
        try:
            ac=json.loads(ac)
            #print(f"the ac is:::::::{ac}")
        except:
            ac=ac
        for_temp={}
        #logging.info(rec)

        for main_label,lab_changes in rec.items():
            for label,changed_val in lab_changes.items():
                if 'Typed' in label:
                    if col in formula_data and main_label in formula_data[col]:
                        ac[main_label]=formula_data[col][main_label]['final_value']
                    else:
                        ac[main_label]=changed_val
                elif 'Converted' in label:
                    # if data.get('group') == "Re-Extraction":
                    continue  
                    # else:
                    #     ac[main_label] = changed_val
                elif 'Cropped' in label:
                    if main_label in cropped_data:
                        try:
                            #logging.info(F"changes is {changed_val}")
                            crop_cor=from_cor_dict(cropped_data[main_label])
                            col_head,row_head,context,diff_val_col=get_headers(cropped_data[main_label],crop_cor,case_id,tenant_id)
                            if col_head:
                                if main_label not in PRIORITY_DATA:
                                    PRIORITY_DATA[main_label]={}
                                PRIORITY_DATA[main_label]={col_head+"@"+str(diff_val_col)+"_crop":row_head}
                                try:
                                    PRIORITY_CONTEXT=json.loads(PRIORITY_CONTEXT)
                                except:
                                    PRIORITY_CONTEXT=PRIORITY_CONTEXT
                                if main_label not in PRIORITY_CONTEXT:
                                    PRIORITY_CONTEXT[main_label]={}
                                PRIORITY_CONTEXT[main_label][col_head+"@"+str(diff_val_col)+row_head]=context
                            if  col in formula_data and main_label in formula_data[col]:
                                ac[main_label]=formula_data[col][main_label]['final_value']
                            else:
                                ac[main_label]=changed_val
                            for_temp[main_label]=col_head+"_"+row_head
                        except Exception as e:
                            logging.info(F"################# there is a issue here {e}")
                            ac[main_label]=changed_val
                else:
                    if main_label in PRIORITY_DATA:
                        if '_crop' not in PRIORITY_DATA[main_label]:
                            tem=label.split('_')
                            if len(tem)==2:
                                PRIORITY_DATA[main_label]={tem[0]:tem[1]}
                    if main_label in ac and 'a.v' in ac[main_label]:
                        if  col in formula_data and main_label in formula_data[col]:
                                ac[main_label]=formula_data[col][main_label]['final_value']
                        else:
                            try:
                                tem_a=ac[main_label]['a.v']
                                ac[main_label]['a.v']={label:changed_val}
                                rem=[]
                                for key in ac[main_label]['r.v']:
                                    if key in changed_val:
                                        rem.append(key)
                                for ke in rem:
                                    ac[main_label]['r.v'].pop(ke) 
                                if 'r.v' in ac[main_label]:
                                    ac[main_label]['r.v'].update(tem_a) 
                            except:
                                ac[main_label]=changed_val
                    else:
                        ac[main_label]=changed_val
            logging.info(F"after changes col is {col} ")
            if col not in PRIORITY_FORMULA:
                PRIORITY_FORMULA[col]=[] 
            if col in formula_data:
                output_formula =generate_formula(formula_data[col][main_label]['formula'],for_temp)
                logging.info(F" ##### update formula is {output_formula}")
                PRIORITY_FORMULA[col].append(output_formula)
            final_dict[col]=ac

    logging.info(F"after changes PRIORITY_DATA is {PRIORITY_DATA} ") 
    if PRIORITY_DATA:
        update_tarining_data(PRIORITY_DATA,PRIORITY_CONTEXT,PRIORITY_FORMULA,case_id,tenant_id)
    #print(f"the final dicts is::::::::::{final_dict}")
    for col,value in final_dict.items():
        # REGENERATE TAB VIEW HERE
        try:
            #print("###########")
            value["tab_view"] = generate_tab_view(value)
        except Exception as e:
            logging.error(f"the issue is::::::{e}")
    
        chunk_size = 4000 
        value=json.dumps(value)
        #print(f"Updated JSON data: {value}")

        chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


        sql = f"UPDATE ocr SET {col} = "

        # Append each chunk to the SQL query
        for chunk in chunks:
            safe_chunk = chunk.replace("'", "''")
            sql += "TO_CLOB('" + safe_chunk + "') || "

        # Remove the last ' || ' and add the WHERE clause to specify the case_id
        sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
        
        extraction_db.execute_(sql)
        
    return True




def combine_dicts(dicts):
    if not dicts:
        return {}

    #this function is used for combining ocr dicts into one

    try:
        combined_dict = {
            "word": ' '.join([d["word"] for d in dicts]),
            "height": max([d["height"] for d in dicts]),
            "top": min([d["top"] for d in dicts]),
            "left": min([d["left"] for d in dicts]),
            "bottom": max([d["bottom"] for d in dicts]),
            "right": max([d["right"] for d in dicts]),
            "width": abs(max([d["right"] for d in dicts])-min([d["left"] for d in dicts])),
            "confidence": max([d["confidence"] for d in dicts]),
            "pg_no": dicts[0]["pg_no"],# Assuming they all have the same pg_no
            "sen_no":None
        }

        confidance=100
        for d in dicts:
            if 'confidance' in d and confidance>d['confidance']:
                confidance=d['confidance']

        combined_dict['confidance']=confidance
    

        return combined_dict
    
    except:
        return {}


def from_high_cor(words):
    try:
        temp = {}
        # temp['word'] = words['word']
        temp['top'] = words['top']
        temp['left'] = words['left']
        temp['bottom'] = words['bottom']
        temp['right'] = words['right']
        temp['page'] = words['pg_no']-1
        temp['x'] = words['left']
        temp['y'] = words['top']

        max_height = temp['bottom'] - temp['top']
        total_width = temp['right'] - temp['left']
        temp['height'] = max_height
        temp['width'] = total_width

        return  temp
    except:
        return {}


def update_tarining_date(date, unsubcribed_fields, user_trained_data, case_id, tenant_id):
    #print(f"the user trained data is:{user_trained_data}")
    db_config['tenant_id'] = tenant_id
    template_db = DB('template_db', **db_config)
    extraction_db = DB('extraction', **db_config)
    quer=f"select party_id from ocr where case_id = '{case_id}'"
    result = extraction_db.execute_(quer)

    party_id = ""
    if not result.empty:
        party_id = result['party_id'].iloc[0] if not pd.isna(result['party_id'].iloc[0]) else ""
    else:
        party_id = ""
    try:
        #print(f"try date:::")
        if party_id:
            check_query = f"SELECT * FROM trained_info WHERE party_id = '{party_id}'"
            existing_data = template_db.execute_(check_query)

            # Step 3: If no record found, insert new one
            if existing_data.empty:
                insert_data = {}
                logging.info(f"No record found for party_id {party_id}. Inserting new training data.")
                insert_data['party_id'] = party_id  # Ensure party_id included in insert
                res = template_db.insert_dict(insert_data, 'trained_info')
            else:
                logging.info(f"Record already exists for party_id {party_id}. Skipping insert.")
        else:
            logging.warning(f"No party_id found for case_id {case_id}. Skipping training insert.")    
    except Exception as e:
        logging.error(f"the isssue is::::{e}")


    # Log the party_id instead of CUSTOMER_NAME
    logging.info(f"Processing for PARTY_ID: {party_id}")

    # Fetch OCR words data for this case
    queue_db = DB('queues', **db_config)
    query = f"SELECT `ocr_word` FROM `ocr_info` WHERE `case_id` = '{case_id}'"
    document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
    ocr_data_all = json.loads(document_id_df_all)

    word = ''
    word_dict = {}
    try:
        # Extract date word details if provided
        if date:
            if isinstance(date, str):
                date = json.loads(date.replace("'", '"'))
            #print(f"the date is:::{date}")
            top_values = [item['top'] for item in date]
            bottom_values = [item['bottom'] for item in date]

            min_top = min(top_values) - 2
            max_bottom = max(bottom_values) + 2
                    # keep your existing min_top / max_bottom above this block
            try:
                #print("try:::::::::")
                # original line (attempt it first)
                ocr_data = ocr_data_all[int(date[0]['pg_no']) - 1]
            except Exception as e:
                # log the issue but do NOT raise or return
                logging.warning(
                    f"Failed to access ocr_data for pg_no={date[0].get('pg_no')} | "
                    f"total_pages={len(ocr_data_all)} | error={e}"
                )
                # fallback: try last page if available, otherwise use empty list
                if len(ocr_data_all) > 0:
                    ocr_data = ocr_data_all[-1]
                else:
                    ocr_data = []  # safe empty list so later code can continue


            # ocr_data = ocr_data_all[int(date[0]['pg_no']) - 1]

            words = [word['word'] for word in ocr_data if word['top'] >= min_top and word['bottom'] <= max_bottom]
            word = " ".join(words)
            word_dict = {"word": word, "bottom": max_bottom, "top": min_top}

        trained_data = {}
        saved_data = {}

        if user_trained_data:
            USER_TRAINED_HIGHLIGHTS = {}

            try:
                # Fetch any previously saved data for this party_id
                query = f"SELECT `USER_TRAINED_FIELDS` FROM `trained_info` WHERE `party_id` = '{party_id}'"
                saved_data = template_db.execute_(query)['USER_TRAINED_FIELDS'].to_list()
                
                if saved_data and saved_data[0]:
                    saved_data = json.loads(saved_data[0])
                else:
                    saved_data = {}

                logging.info(f"Saved data for PARTY_ID {party_id}: {saved_data}")

                # Iterate through new trained data
                for key, value in user_trained_data.items():
                    if value == 'context_clear':
                        saved_data.pop(key, None)
                        continue

                    USER_TRAINED_HIGHLIGHTS[key] = []

                    if "word" in value:
                        value_box = combine_dicts(value['word'])
                        USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(value_box))

                    if "header" in value:
                        header_box = combine_dicts(value['header'])
                        USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(header_box))

                    if "context" in value:
                        context_box = combine_dicts(value['context'])
                        USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(context_box))

                    logging.info(f"USER_TRAINED_HIGHLIGHTS: {USER_TRAINED_HIGHLIGHTS}")

                    if "word" in value and "header" in value and 'context' in value:
                        trained_data[key] = {}
                        temp = []
                        trained_header = []
                        trained_context = []

                        page_no = value['word'][0]['pg_no'] - 1
                        ocr_data = ocr_data_all[page_no]

                        value_box = combine_dicts(value['word'])
                        header_box = combine_dicts(value['header'])
                        context_box = combine_dicts(value['context'])

                        diff_head = calculate_distance(header_box, context_box)
                        diff_value_head = calculate_value_distance(header_box, value_box)

                        trained_header = [header_box['word']]

                        trained_context = context_box
                        trained_context['context_width'] = trained_context['width']

                        columm_box = combine_dicts(value['header'])
                        diff_col = abs(abs((columm_box["top"] + columm_box["bottom"]) / 2) - abs((value_box["top"] + value_box["bottom"]) / 2))
                        col_head = columm_box['word']
                        col_f = columm_box

                        trained_context['hypth'] = calculate_distance(col_f, trained_context)
                        trained_context['position'] = 'above' if context_box['top'] < col_f['top'] else 'below'

                        temp = [diff_col, diff_head, col_head, trained_header, trained_context, diff_value_head, True]
                        trained_data[key] = temp

                # Update OCR table
                query = f"UPDATE OCR SET USER_TRAINED_HIGHLIGHTS = '{json.dumps(USER_TRAINED_HIGHLIGHTS)}' WHERE case_id = '{case_id}'"
                query2 = f"UPDATE OCR SET user_trained_data = '{json.dumps(user_trained_data)}' WHERE case_id = '{case_id}'"
                extraction_db.execute_(query)
                extraction_db.execute_(query2)

            except Exception as e:
                saved_data = {}
                logging.info(f"######## Error in training update for PARTY_ID {party_id}: {e}")
                pass

        # Save back to trained_info by party_id
        if party_id:
            #print(f"final party_id::::{party_id}")
            saved_data.update(trained_data)
            insert_data = {}

            if saved_data:
                #print(f"the save_data is:::::{saved_data}")
                insert_data["USER_TRAINED_FIELDS"] = json.dumps(saved_data)
            if word_dict:
                insert_data["TRAINED_DATE"] = json.dumps(word_dict)
            if unsubcribed_fields:
                #print(f"the unsubsrvbed ields are::::::{unsubcribed_fields}")
                insert_data["UNSUBCRIBED_FIELDS"] = json.dumps(unsubcribed_fields)

            if insert_data:
                
                #print(f"the insert data is:::::{insert_data}")
                template_db.update('trained_info', update=insert_data, where={'party_id': party_id})
    except Exception as e:
        logging.error(f"the isue is update date trainig is:::::::{e}")
    return True


# ##this is for already presnet13_10_2025
# def update_tarining_date(date,unsubcribed_fields,user_tarined_data,case_id,tenant_id):
#     db_config['tenant_id']=tenant_id
#     temaplate_db=DB('template_db',**db_config)
#     extraction_db=DB('extraction',**db_config)

#     query = f"SELECT `customer_name` from  `ocr` where `case_id` = '{case_id}'"
#     CUSTOMER_NAME = extraction_db.execute_(query)['customer_name'].to_list()[0]
#     logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")
#     if CUSTOMER_NAME:
#         CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()

#     queue_db = DB('queues', **db_config)
#     query = f"SELECT `ocr_word` from  `ocr_info` where `case_id` = '{case_id}'"
#     document_id_df_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
#     ocr_data_all=json.loads(document_id_df_all)

#     word=''
#     word_dict={}
#     if date:
#         top_values = [item['top'] for item in date]
#         bottom_values = [item['bottom'] for item in date]

#         min_top = min(top_values)
#         min_top=(min_top-2)
#         max_bottom = max(bottom_values)
#         max_bottom=(max_bottom+2)

#         ocr_data=ocr_data_all[int(date[0]['pg_no'])-1]

#         words=[]
#         for word in ocr_data:
#             if word['top']>=min_top and word['bottom']<=max_bottom:
#                 words.append(word['word'])

#         word=" ".join(item for item in words)

#         word_dict={"word":word,"bottom":max_bottom,"top":min_top}

#     trained_data={}
#     saved_data={}
#     if user_tarined_data:

#         USER_TRAINED_HIGHLIGHTS={}

#         try:
#             query = f"SELECT `USER_TRAINED_FIELDS` from  `trained_info` where `CUSTOMER_NAME` = '{CUSTOMER_NAME}'"
#             saved_data = temaplate_db.execute_(query)['USER_TRAINED_FIELDS'].to_list()
#             # if saved_data:
#             if saved_data and saved_data[0]:  # Ensures it's not None or an empty list
#                 saved_data=json.loads(saved_data[0])
#             else:
#                 saved_data={}

#             logging.info(F"saved_data is {saved_data}")
#             for key,value in user_tarined_data.items():

#                 if value=='context_clear':
#                     saved_data.pop(key)
#                     continue

#                 USER_TRAINED_HIGHLIGHTS[key]=[]
             
#                 if "word" in value:
#                     value_box=combine_dicts(value['word'])
#                     #logging.info(value_box)
#                     USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(value_box))
                  
#                 if "header" in value:
#                     header_box=combine_dicts(value['header'])
#                     #logging.info(header_box)
#                     USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(header_box))
                  
#                 if "context" in value:
#                     context_box=combine_dicts(value['context'])
#                     #logging.info(context_box)
#                     USER_TRAINED_HIGHLIGHTS[key].append(from_high_cor(context_box))
                  
#                 logging.info(F"USER_TRAINED_HIGHLIGHTS is {USER_TRAINED_HIGHLIGHTS}")
                
#                 if "word" in value and "header" in value and 'context' in value:
#                     trained_data[key]={}
#                     temp={}
#                     trained_header=[]
#                     trained_context=[]
#                     page_no=value['word'][0]['pg_no']-1
#                     ocr_data=ocr_data_all[page_no]
#                     value_box=combine_dicts(value['word'])
#                     header_box=combine_dicts(value['header'])
#                     context_box=combine_dicts(value['context'])

#                     diff_head=calculate_distance(header_box,context_box)
#                     diff_value_head=calculate_value_distance(header_box,value_box)

#                     # sorted_words=line_wise_ocr_data(ocr_data)
#                     # col_head,col_f,diff_col=get_col(sorted_words,value_box)

#                     # max_width=0
#                     # for word in ocr_data:
#                         # if word["left"]>=header_box['left']-10 and  word["right"]<=header_box['right']+10:
#                         #     if  word["top"]>=header_box['top']-10 and  word["bottom"]<=header_box['bottom']+10:
#                         #         trained_header.append(word['word'])
#                         # if word["left"]>=context_box['left'] and  word["right"]<=context_box['right']:
#                         #     width=word["right"]-word["left"]
#                         #     if  word["top"]>=context_box['top'] and  word["bottom"]<=context_box['bottom']:
#                         #         if max_width<width:
#                         #             word['context_width']=width
#                         #             max_width=width
#                         #             trained_context=
                        
#                     trained_header=[header_box['word']]
                    
#                     trained_context=context_box
#                     trained_context['context_width']=trained_context['width']

#                     columm_box=combine_dicts(value['header'])
#                     diff_col=abs(abs((columm_box["top"]+columm_box["bottom"])/2) - abs((value_box["top"]+value_box["bottom"])/2))
#                     col_head=columm_box['word']
#                     col_f=columm_box

#                     trained_context['hypth']=calculate_distance(col_f,trained_context)
#                     if context_box['top']<col_f['top']:
#                         trained_context['position']='above'
#                     else:
#                         trained_context['position']='below'

#                     temp=[diff_col,diff_head,col_head,trained_header,trained_context,diff_value_head,True]
#                     trained_data[key]=temp
               

#             query=f"UPDATE OCR SET USER_TRAINED_HIGHLIGHTS = '{json.dumps(USER_TRAINED_HIGHLIGHTS)}' WHERE case_id='{case_id}'"
#             query2=f"UPDATE OCR SET user_trained_data = '{json.dumps(user_tarined_data)}' WHERE case_id='{case_id}'"
#             extraction_db.execute_(query)
#             extraction_db.execute_(query2)

#         except Exception as e:
#             saved_data={}
#             logging.info(F" ######## error is {e}")
#             pass
            
#     if CUSTOMER_NAME:
#         saved_data.update(trained_data)
#         insert_data={}
#         if saved_data:
#             insert_data["USER_TRAINED_FIELDS"]=json.dumps(saved_data)
#         if word_dict:
#             insert_data["TRAINED_DATE"]=json.dumps(word_dict)
#         if unsubcribed_fields:
#             insert_data["UNSUBCRIBED_FIELDS"]=json.dumps(unsubcribed_fields)
#         if insert_data:
#             temaplate_db.update('trained_info', update=insert_data, where={'CUSTOMER_NAME': CUSTOMER_NAME})

#     return True

def case_creation_cnt(case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    qry=f"select party_id,date_stat from ocr where case_id='{case_id}'"
    party_id=extraction_db.execute_(qry)['party_id'].tolist()

    if len(party_id)>=1:
        party_id=party_id[0]
        # print(f"party id's are {party_ids}")
        qry=f"select case_id,party_id,date_stat from ocr where party_id in '{party_id}' order by date_stat"
        party_ids_df=extraction_db.execute_(qry)
        if not party_ids_df.empty:
            party_ids_df.columns = [col.lower() for col in party_ids_df.columns]
            party_ids_df = party_ids_df.loc[:, ~party_ids_df.columns.duplicated()]
            party_ids_df=party_ids_df.dropna()

            # Parse and clean dates
            party_ids_df["parsed_date"] = party_ids_df["date_stat"].apply(extract_date)
            # Drop NaT rows for ranking
            # mask = party_ids_df["parsed_date"].notna()
            # party_ids_df.loc[mask, "case_count"] = pd.factorize(party_ids_df.loc[mask, "parsed_date"])[0] + 1
            
            # rank the dates
            party_ids_df["case_count"] = (party_ids_df["parsed_date"].rank(method="dense", ascending=True).astype("Int64"))

            # Optional: Fill NaNs with 0 or leave them
            party_ids_df["case_count"] = party_ids_df["case_count"].fillna("")
             # --- Update customer_name in bulk ---
            
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
        logging.warning(f"there is no party id")

    return {"data":"Success","flag":True}



def date_out(parameters):
    logging.info(f"parameters are {parameters}")
    d_month_full = ['january', 'february', 'march', 'april', 'may', 'june',
                    'july', 'august', 'september', 'october', 'november', 'december']
    d_month_abbr = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    d_month = d_month_full + d_month_abbr
    ordinal_suffixes = ['st', 'nd', 'rd', 'th']

    value = parameters['date_format']
    logging.info(f'value-------{value}')
    date_cop = value

    # Replace unwanted characters with space but KEEP commas
    value = re.sub(r'[^a-zA-Z0-9 /\-.,]', ' ', value)
    value = value.split('(')[0]

    # === Handle DD.MM.YYYY and YYYY-MM-DD formats ===
    date_patterns = [
        r'\b(\d{2})[.\-/](\d{2})[.\-/](\d{4})\b',
        r'\b(\d{4})[.\-/](\d{2})[.\-/](\d{2})\b',
    ]

    for pattern in date_patterns:
        match = re.search(pattern, value)
        if match:
            parts = list(map(int, match.groups()))
            if len(parts) == 3:
                if pattern.startswith(r'\b(\d{2})'):
                    day, month, year = parts
                else:
                    year, month, day = parts
                try:
                    logging.info(f"Extracted date: {year}-{month}-{day}")
                    date(year, month, day)
                    return f"{year}-{month:02d}-{day:02d}"
                except ValueError:
                    continue
    # === End pattern handling ===

    tokens = re.split(r'[\s]', value)
    potential_dates = []
    current_chunk = []
    for token in tokens:
        if re.search(r'[a-zA-Z]', token) or re.search(r'\d', token):
            current_chunk.append(token)
        else:
            if current_chunk:
                potential_dates.append(" ".join(current_chunk))
                current_chunk = []
    if current_chunk:
        potential_dates.append(" ".join(current_chunk))

    for date_str in potential_dates:
        parts = re.split(r'[\s/\-.,]', date_str)
        out = []

        for word in parts:
            if not word:
                continue
            temp = re.sub(r'[^a-zA-Z0-9]', '', word)
            for suffix in ordinal_suffixes:
                if temp.endswith(suffix) and temp[:-2].isdigit():
                    temp = temp[:-2]

            if temp.lower() in d_month:
                out.append(temp)
            else:
                try:
                    int(temp)
                    out.append(temp)
                except:
                    alphabetic_part = re.sub(r'[^a-zA-Z]', '', word)
                    numeric_part = re.sub(r'[^0-9]', '', word)
                    if numeric_part in d_month or alphabetic_part.lower() in d_month:
                        a_pos = word.find(alphabetic_part)
                        n_pos = word.find(numeric_part)
                        if a_pos > n_pos:
                            out.append(numeric_part)
                            out.append(alphabetic_part.lower())
                        else:
                            out.append(alphabetic_part.lower())
                            out.append(numeric_part)

        if len(out) == 2:
            month_part = ''
            year_part = ''
            for part in out:
                part_clean = re.sub(r'[^a-zA-Z0-9]', '', part)
                if part_clean.lower() in d_month:
                    month_part = part_clean.lower()
                elif part_clean.isdigit():
                    year_part = part_clean

            if month_part and year_part:
                month_num = (d_month_abbr.index(month_part)
                             if month_part in d_month_abbr
                             else d_month_full.index(month_part)) + 1
                if len(year_part) == 2:
                    year = int('20' + year_part) if int(year_part) < 50 else int('19' + year_part)
                else:
                    year = int(year_part)

                last_day = calendar.monthrange(year, month_num)[1]
                return f"{year}-{month_num:02d}-{last_day:02d}"

        elif len(out) == 3:
            day = month = year = None
            for part in out:
                part_clean = re.sub(r'[^a-zA-Z0-9]', '', part)
                if part_clean.lower() in d_month:
                    month = d_month_abbr.index(part_clean.lower()) + 1 if part_clean.lower() in d_month_abbr else d_month_full.index(part_clean.lower()) + 1
                elif len(part_clean) == 4 and part_clean.isdigit():
                    year = int(part_clean)
                elif len(part_clean) <= 2 and part_clean.isdigit():
                    day = int(part_clean)
            if day and month and year:
                return f"{year}-{month:02d}-{day:02d}"

    return value

def doDateTransform(self, parameters):
    """ Takes date as input and converts it into required format
        'parameters':{
            'input_date' : {"source":"input_config","table":"ocr","column":"Stock Date"},
            'output_format' : '%d-%b-%y'
            'output_type': {"source":"input","value":"object"}//can be object or string
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = parameters['input_date']
    output_format = parameters['output_format']
    if input_date != "":
        try:
            output_type = self.get_param_value(parameters['output_type'])
        except:
            output_type = parameters['output_type']

        date_series = pd.Series(input_date)
        try:
            if output_type == "object":
                converted_date = pd.to_datetime(date_series,dayfirst=True,errors='coerce').dt.strftime(output_format)
                #converted_date = datetime.strptime(converted_date[0],output_format).date()
                logging.debug(f"######converted date: {converted_date[0]}")
                return converted_date[0]
            else:
                converted_date = pd.to_datetime(date_series,dayfirst=True,errors='coerce').dt.strftime(output_format)
                if np.isnan(converted_date[0]):
                    converted_date = input_date
                else:
                    converted_date = converted_date[0]
                return converted_date
        except Exception as e:
            logging.error("cannot convert date to the given format")
            logging.error(e)
            return input_date
    else:
        logging.info(f"Input_date is empty")
        return input_date
    
def get_month_last_date(parameters):
    logging.info(f"Parameters received: {parameters}")
    input_date = parameters.get('input')
    logging.info(f"Input date: {input_date}")

    try:
        # Attempt to parse the input date in expected format
        parsed_date = datetime.strptime(input_date, "%d-%m-%Y")
        logging.info(f"Parsed date: {parsed_date}")

        year = parsed_date.year
        month = parsed_date.month
        logging.info(f"Year: {year}, Month: {month}")

        last_day = calendar.monthrange(year, month)[1]
        logging.info(f"Last day of the month: {last_day}")

        result_date = datetime(year, month, last_day)
        logging.info(f"Resulting date: {result_date}")

        return result_date.strftime("%Y-%m-%d")  # Return in a standard string format
    except Exception as e:
        logging.warning("An error occurred while parsing the date. Returning original input.")
        logging.error(e)
        return input_date 

def save_changes_extraction_report(case_id, data, tenant_id, queue_id, message_data={}):
    fields = data.get('fields', {})
    extraction_db = DB('extraction', **db_config)
    queues_db = DB('queues', **db_config)
    field_changes = data.get('field_changes', [])
    recommended_changes = data.get('recommended_changes', {})
    field_removal=data.get('field_removal',{})
    cropped_data = data.get('cropped_data', {})
    try:
        fields_edited_query = f"select fields_edited, fields_extracted, not_extracted_fields from field_accuracy where case_id='{case_id}'"
        fields_edited = queues_db.execute_(fields_edited_query)
        existed_fields = fields_edited['fields_edited'].to_list()[0]
        extracted_fields = json.loads(fields_edited['fields_extracted'].to_list()[0] or '{}')
        not_extracted_fields = json.loads(fields_edited['not_extracted_fields'].to_list()[0] or '{}')
        if existed_fields:
            existed_fields = json.loads(existed_fields)
        else:
            existed_fields = {}

        final_updates = {}
        #print(f"field changes are:{field_changes}")
        for field in field_changes:
        #    print(f"enetred fir loop")
        #    print(f"field is:{field}")
            if field in recommended_changes:
        #        print(f"entered if condition")
                # Extract key-value pairs from nested recommended_changes
                for sub_key, sub_val in recommended_changes[field].items():
        #            print(f"sub val is:{sub_val}")
                    if isinstance(sub_val, dict):
        #                print(f"entered sub val is diuct")
                        # if "Typed" in sub_val:
                        #     logging.info(f'in typed  cond for key {sub_key} and value is {sub_val}')
                        #     final_updates[sub_key] = f"M: {sub_val['Typed']}"
                        typed_val = sub_val.get("Typed", "")
        #                print(f"typed val is:{typed_val}")
                        if typed_val != "":
                            logging.info(f'in typed cond for key {sub_key} and value is {sub_val}')
                            final_updates[sub_key] = f"M: {typed_val}"
        #                elif typed_val in ("", None, "null", "NULL"):
        #                    print(f"Extracted fiuelds before tyeps= empty:{extracted_fields}")
        #                    print(f"Existed fiuelds before tyeps= empty:{existed_fields}")
        #                    extracted_fields.pop(sub_key, None)
        #                    existed_fields.pop(sub_key, None)
        #                    print(f"Extracted fiuelds after tyeps= empty:{extracted_fields}")
        #                    print(f"Existedd fiuelds after tyeps= empty:{existed_fields}")

                        #elif "Converted" in sub_val:
                        #    converted_val = sub_val["Converted"]
                        #    if sub_key in extracted_fields:
                        #        extracted_fields[sub_key] = f"E:{converted_val}"
                        #    elif sub_key in existed_fields:
                        #        existed_fields[sub_key] = f"M:{converted_val}"
                        elif "Converted" in sub_val:
        #                   print(f"entered converted")
                            converted_val = sub_val["Converted"]
                            if converted_val not in ("", None, "null", "NULL"):
                                if sub_key in extracted_fields:
                                    extracted_fields[sub_key] = f"E:{converted_val}"
                                elif sub_key in existed_fields:
                                    existed_fields[sub_key] = f"M:{converted_val}"
                            else:
                                extracted_fields.pop(sub_key, None)
                                existed_fields.pop(sub_key, None)


                        elif "Cropped" in sub_val:
                            existed_fields[sub_key] = f"M:{sub_val['Cropped']}"

                        elif typed_val in ("", None, "null", "NULL"):
        #                    print(f"Extracted fiuelds before tyeps= empty:{extracted_fields}")
        #                    print(f"Existed fiuelds before tyeps= empty:{existed_fields}")
                            extracted_fields.pop(sub_key, None)
                            existed_fields.pop(sub_key, None)
        #                    print(f"Extracted fiuelds after tyeps= empty:{extracted_fields}")
        #                    print(f"Existedd fiuelds after tyeps= empty:{existed_fields}")
                        #elif len(sub_val) == 1:
                        #    dropdown_key, dropdown_val = list(sub_val.items())[0]
                        #    extracted_fields[sub_key] = f"E:{dropdown_val}"
                        elif len(sub_val) == 1:
        #                    print(f"entered len==1")
                            dropdown_key, dropdown_val = list(sub_val.items())[0]
                            if dropdown_val not in ("", None, "null", "NULL"):
                                extracted_fields[sub_key] = f"E:{dropdown_val}"
                            else:
                                extracted_fields.pop(sub_key, None)
                                existed_fields.pop(sub_key, None)

            #elif field in fields:
                # if "customer_name" in field:
                #     customer_name_value = fields["customer_name"]
                #     existed_fields["customer_name"] = f"M: {customer_name_value}"
                

        for field, sub_fields in recommended_changes.items():
            if field not in field_changes:
                for sub_key, sub_val in sub_fields.items():
                    #if isinstance(sub_val, dict) and len(sub_val) == 1:
                    #    print(f"enetred in thsi loop")
                    #    dropdown_key, dropdown_val = list(sub_val.items())[0]
                    #    extracted_fields[sub_key] = f"E:{dropdown_val}"

                    if isinstance(sub_val, dict) and len(sub_val) == 1:
        #                print("entered in this loop")
                        dropdown_key, dropdown_val = list(sub_val.items())[0]

                        if dropdown_val not in ("", None, "null", "NULL"):
                            extracted_fields[sub_key] = f"E:{dropdown_val}"
                        else:
                            # remove the field if dropdown is empty
        #                    print(f"Removing {sub_key} because dropdown_val is empty/null")
                            extracted_fields.pop(sub_key, None)
                            existed_fields.pop(sub_key, None)
        for field, sub_fields in field_removal.items():
            for sub_key, sub_val in sub_fields.items():
            # Case 1: single-key dictionary
                if isinstance(sub_val, dict) and len(sub_val) == 1:
                    action_key, action_val = list(sub_val.items())[0]
                    if action_key == "field_removal":
                        # Remove from extracted_fields and existed_fields
                        extracted_fields.pop(sub_key, None)
                        existed_fields.pop(sub_key, None)

        # if "date_stat" in fields and "date_stat" in field_changes:
        #     logging.info(f'if date_stat in field')
        #     date_stat_value = fields["date_stat"]
        #     parameters = {'date_format': date_stat_value}
        #     logging.info(f'parameters for date_out are {parameters}')
        #     formatted_date = date_out(parameters)
        #     logging.info(f'formatted_date----------{formatted_date}')
        #     params = {"output_format": "%d-%m-%Y", "output_type": "object", "input_date": formatted_date}
        #     transformed_date = doDateTransform(formatted_date, params)
        #     logging.info(f'transformed_date------------{transformed_date}')
        #     params = {"input": transformed_date}
        #     transformed_date_new = get_month_last_date(params)
        #     params = {"output_format": "%d-%m-%Y", "output_type": "object", "input_date": transformed_date_new}
        #     date_value = doDateTransform(transformed_date_new, params)
        #     logging.info(f'date_value----------{date_value}')
        #     date_obj = datetime.strptime(date_value , "%d-%m-%Y")
        #     converted_date = date_obj.strftime("%d/%m/%Y")
        #     existed_fields["date_stat"] = f"M: {converted_date}"
        #     extracted_fields.pop("date_stat", None)
        
        # elif "date_stat" in fields and "date_stat" not in field_changes:
        #     logging.info(f'if date_stat in field')
        #     date_stat_value = fields["date_stat"]
        #     parameters = {'date_format': date_stat_value}
        #     logging.info(f'parameters for date_out are {parameters}')
        #     formatted_date = date_out(parameters)
        #     logging.info(f'formatted_date----------{formatted_date}')
        #     params = {"output_format": "%d-%m-%Y", "output_type": "object", "input_date": formatted_date}
        #     transformed_date = doDateTransform(formatted_date, params)
        #     logging.info(f'transformed_date------------{transformed_date}')
        #     params = {"input": transformed_date}
        #     transformed_date_new = get_month_last_date(params)
        #     params = {"output_format": "%d-%m-%Y", "output_type": "object", "input_date": transformed_date_new}
        #     date_value = doDateTransform(transformed_date_new, params)
        #     logging.info(f'date_value----------{date_value}')
        #     date_obj = datetime.strptime(date_value , "%d-%m-%Y")
        #     converted_date = date_obj.strftime("%d/%m/%Y")
        #     extracted_fields["date_stat"] = f"E: {converted_date}"

        if "date_stat" in fields:
            logging.info("Processing date_stat field")

            date_stat_value = fields["date_stat"]
            parameters = {'date_format': date_stat_value}
            logging.info(f'parameters for date_out are {parameters}')

            # Step 1: Convert with date_out
            formatted_date = date_out(parameters)
            logging.info(f'formatted_date: {formatted_date}')

            # Step 2: First transform
            params = {"output_format": "%d-%m-%Y", "output_type": "object", "input_date": formatted_date}
            transformed_date = doDateTransform(formatted_date, params)
            logging.info(f'transformed_date: {transformed_date}')

            # Step 3: Get month end date
            transformed_date_new = get_month_last_date({"input": transformed_date})

            # Step 4: Final transform
            params["input_date"] = transformed_date_new
            date_value = doDateTransform(transformed_date_new, params)
            logging.info(f'date_value: {date_value}')

            # Step 5: Convert to desired format
            date_obj = datetime.strptime(date_value, "%d-%m-%Y")
            converted_date = date_obj.strftime("%d/%m/%Y")
            if "date_stat" in field_changes and "date_stat" in cropped_data:
                existed_fields["date_stat"] = f"M: {converted_date}"
                extracted_fields.pop("date_stat", None)
            elif "date_stat" in field_changes:
                existed_fields["date_stat"] = f"M: {converted_date}"
                extracted_fields.pop("date_stat", None)
            else:
                #check for already modified or not in field_accuracy table
                query = f"SELECT fields_edited FROM field_accuracy WHERE case_id = '{case_id}'"
                query_data = queues_db.execute_(query)['fields_edited'].to_list()[0]
                if query_data:
                    query_data=json.loads(query_data)
                    if "date_stat" in query_data:
                        existed_fields["date_stat"] = f"M: {converted_date}"
                        extracted_fields.pop("date_stat", None)
                    else:
                        extracted_fields["date_stat"] = f"E: {converted_date}"

        for key, value in final_updates.items():
            existed_fields[key] = value

        logging.info(f"final_updates are {existed_fields}")
        try:
            for field in field_changes:
                if field in recommended_changes:
                    for sub_key, sub_val in recommended_changes[field].items():
                        sub_key = sub_key.strip()
                        if isinstance(sub_val, dict):
                            typed_val = sub_val.get("Typed", "")
                            if 'Typed' in sub_val:
                                if typed_val != "":
                                    extracted_fields.pop(sub_key, None)
                                    not_extracted_fields.pop(sub_key, None)
                                elif typed_val in ("", None, "null", "NULL"):
                                    existed_fields.pop(sub_key, None)
                                    extracted_fields.pop(sub_key, None)
                            elif 'Cropped' in sub_val:
                                extracted_fields.pop(sub_key, None)
                                not_extracted_fields.pop(sub_key, None)
                elif field in fields:
                    extracted_fields.pop(field, None)
                    not_extracted_fields.pop(field, None)
        except Exception as e:
            logging.error(f"Error while removing fields from extracted_fields and not_extracted_fields: {e}")
        logging.info(f"extracted_fields are {extracted_fields}")
        logging.info(f"not_extracted_fields are {not_extracted_fields}")
        edited_count = len(existed_fields)
        extraction_count = len(extracted_fields)
        not_used_count = len(not_extracted_fields)

        extraction_percentage = round((extraction_count / (extraction_count + edited_count)) * 100 if (extraction_count + edited_count) > 0 else 0,2)
        manual_percentage = round((edited_count / (extraction_count + edited_count)) * 100 if (extraction_count + edited_count) > 0 else 0,2)
        logging.info(f"extraction_percentage is {extraction_percentage}")
        logging.info(f"manual_percentage is {manual_percentage}")
        def clob_chunks_sql(json_str):
            if not json_str or json_str.strip() in ['null', 'None', '{}', '[]', '""']:
                return "TO_CLOB(NULL)"
            escaped = json_str.replace("'", "''")
            #escaped = escaped.replace("{", "{{").replace("}", "}}")

            chunk_size = 3000
            chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
            return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)
        fe_val = clob_chunks_sql(json.dumps(extracted_fields))
        ne_val = clob_chunks_sql(json.dumps(not_extracted_fields))
        ed_val = clob_chunks_sql(json.dumps(existed_fields))
        update_query = f"""
                UPDATE field_accuracy 
                SET fields_edited = {ed_val},
                    manual = {edited_count},
                    fields_extracted = {fe_val},
                    not_extracted_fields = {ne_val},
                    extraction = {extraction_count},
                    not_used_fields = {not_used_count},
                    extraction_percentage = {extraction_percentage},
                    NUMBER_OF_FIELDS = {extraction_count + edited_count},
                    manual_percentage = {manual_percentage}
                WHERE case_id = '{case_id}'
            """

        queues_db.execute_(update_query)

        return True

    except Exception as e:
        logging.error(f"Error while processing changed_fields: {e}")
        return False

def save_changes_m(case_id, data, tenant_id, queue_id, message_data={}):
    attr = ZipkinAttrs(
        trace_id=case_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id if tenant_id is not None else ''
    )

    with zipkin_span(
            service_name='save_changes',
            zipkin_attrs=attr,
            span_name='save_changes',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            logging.info(f'Data recieved for save changes: {data}')

            db_config['tenant_id'] = tenant_id

            fields = data.get('fields', {})
            #total_fields=fields.keys()

            column_dict = {}    

            if "" in fields:
                fields.pop("")

            try:
                extraction_db = DB('extraction', **db_config)
                queues_db = DB('queues', **db_config)
                edited_fields= data.get('editedFields', [])
                logging.info(f"edited_fields are {edited_fields}")
                selectedUnitsValue= data.get('selectedUnitsValue', '')
                selectedUnitsValue = json.dumps(selectedUnitsValue)
                query = f"SELECT EDITED_FIELDS FROM ocr WHERE case_id = '{case_id}'"
                query_data = extraction_db.execute_(query)['edited_fields'].to_list()[0]
                if query_data:
                    query_data=json.loads(query_data)
                    query_data.extend(edited_fields)
                    query_data = list(set(query_data))
                    query_data=json.dumps(query_data)
                    query=f"UPDATE OCR SET EDITED_FIELDS = '{query_data}' WHERE case_id='{case_id}'"
                    extraction_db.execute_(query)
                else:
                    edited_fields=json.dumps(edited_fields)
                    query=f"UPDATE OCR SET EDITED_FIELDS = '{edited_fields}' WHERE case_id='{case_id}'"
                    extraction_db.execute_(query)

                
                query=f"UPDATE OCR SET SELECTED_UNITS = '{selectedUnitsValue}' WHERE case_id='{case_id}' "
                extraction_db.execute_(query)
            
            except:
                pass

            field_changes = data.get('field_changes', [])
            cropped_data = data.get('cropped_data', [])
            group=data.get('group',None)
            try:
                if group=='Approve':
                    pass
                else:
                    executor.submit(save_changes_extraction_report, case_id, data,tenant_id, case_id, message_data)
                #report_changes = save_changes_extraction_report(case_id, data, tenant_id, queue_id, message_data)
            except Exception as e:
                logging.error(f"Error while processing save_changes_extraction_report: {e}")
                report_changes = False
            #verify_operator = data.get('user', None)
            logging.debug(f"Case id is {case_id} Fields going to save in database are {fields}")
           
            '''
            changes:- standardizing the function to work on multiple table (previouslt restricted on ocr itself)
            author:- Amara Sai Krishna Kumar
            '''
            try:
                logging.info(f"in try block for updating table")
                field_accuracy_(data,tenant_id,case_id)
                # update_table(queue_db, case_id, "", data)
            except Exception as e:
                logging.info(f"in exception {e}")
                pass

            if len(field_changes) != 0:
                #print("here field changes is not zero")
                changed_fields = {k: fields[k]
                                    for k in field_changes if k in fields}
                # manipulate highlight data with the received UI request data (crooped_data)
                list_of_dicto = dict_split_wise_table(changed_fields)
                for i in range(len(list_of_dicto)):
                    for total_table_name,column_dict in list_of_dicto[i].items():
                        data_base,table = total_table_name.split('.')
                        db_conn = DB(data_base, **db_config)
                        custom_table=['STOCK STATEMENT','DEBITORS STATEMENT','CREDITORS']
                        for column in custom_table:
                            if column in changed_fields:
                                table='custom_table'
                                column_dict=changed_fields
                        logging.info(f'##table {table} #column_dict {column_dict}')        
                        if table !='ocr':
                            db_conn.update(table, update= column_dict, where={'case_id': case_id})
                            
                        else:
                            query = f"select case_id,highlight from {table} where case_id='{case_id}'"
                            case_data = db_conn.execute_(
                                query).to_dict(orient='records')
                            logging.info(f"case_data--{case_data}")
                            if not case_data[0]['highlight']:
                                case_data[0]['highlight']='{}'
                            highlight = ast.literal_eval(case_data[0]['highlight'])
                            
                            try:
                                if len(column_dict) != 0:
                                    if len(cropped_data) != 0:
                                        keys_ = list(highlight.keys())
                                        for k_, v_ in cropped_data.items():
                                            #logging.info(v_)
                                            for k,v in v_.items():
                                                # logging.info(k)
                                                img_width = v['width']
                                                new_dict = {'x': v['area']['x'] * (670 / img_width), 'y': v['area']['y'] * (670 / img_width), 'width': v['area']['width'] * (670 / img_width), 'height': v['area']['height'] * (670 / img_width), 'page': v['area']['page'], 'bottom': v['area']['y'] + v['area']
                                                            ['height'], 'top': v['area']['y'], 'right': v['area']['x'] + v['area']['width'], 'left': v['area']['x'], 'status': 'Updated Highlight' if k in keys_ else 'New Highlight'}
                                                # Updating data to highlight
                                                highlight[k] = new_dict
                                        data.pop('cropped_data', '')
                                        column_dict['highlight'] = json.dumps(highlight)
                                        logging.info(f"column dict iss--{column_dict}")
                                    db_conn.update(table, update=column_dict, where={'case_id': case_id})       
                            except:
                                if len(column_dict) != 0:
                                
                                    if len(cropped_data) != 0:
                                        keys_ = list(highlight.keys())
                                        for k, v in cropped_data.items():
                                            img_width = v['width']
                                            new_dict = {'x': v['area']['x'] * (670 / img_width), 'y': v['area']['y'] * (670 / img_width), 'width': v['area']['width'] * (670 / img_width), 'height': v['area']['height'] * (670 / img_width), 'page': v['area']['page'], 'bottom': v['area']['y'] + v['area']
                                                        ['height'], 'top': v['area']['y'], 'right': v['area']['x'] + v['area']['width'], 'left': v['area']['x'], 'status': 'Updated Highlight' if k in keys_ else 'New Highlight'}
                                            # Updating data to highlight
                                            highlight[k] = new_dict
                                        data.pop('cropped_data', '')
                                        column_dict['highlight'] = json.dumps(highlight)
                                        logging.info(f"column dict in expection iss--{column_dict}")
                                    db_conn.update(table, update=column_dict, where={'case_id': case_id})
            recommended_changes = data.get('recommended_changes', {})
            formula_data = data.get('formula_data', {})
            if recommended_changes:
                #print("$$$$")
                save_recommended(recommended_changes,cropped_data,formula_data,case_id,extraction_db,tenant_id)
            date_cropped = data.get('date_cropped', {})
            unsubcribed_fields = data.get('unsubscribed_fields', [])
            user_tarined_data = data.get('user_trained_data', [])
            if date_cropped or unsubcribed_fields or user_tarined_data:
                # print(f"The cropped data length is: {len(date_cropped)}")
                # print(f"the unsubsrived fields are:::::::::{unsubcribed_fields}")
                # print(f"The unsubscribed fields length is: {len(unsubcribed_fields)}")
                # print(f"The user trained data length is: {len(user_tarined_data)}")
                # print(f"the user_Trained_fieds are:::::{user_tarined_data}")
                try:
                    update_tarining_date(date_cropped,unsubcribed_fields,user_tarined_data,case_id,tenant_id)
                except Exception as e:
                    logging.error(f"the issue is in update trind date:{e}")
            return {'flag':True,'message': 'Saved changes.','data':column_dict}
        except:
            logging.exception('Something went wrong saving changes. Check trace.')
            return {'flag':False,'message': 'Something went wrong saving changes. Check logs.','data':{}}

# backup code
@app.route('/save_changes', methods=['POST', 'GET'])
def save_changes_route():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    #print("request_data",request.data)
    all_data = request.get_json(force=True)
    #print("all_data",all_data)
    #Parsing the data from execute function(UI) request data
    try:
        data = json.loads(all_data['ui_data'])
    except:
        data = all_data['ui_data']
    logging.info(f'Message UI Data: {data}')

    user = data.get('user', None)
    session_id = data.get('session_id', None)
    #variables_check = True
    template_db = DB('template_db', **db_config)
    case_id = data.get('case_id', None)
    if case_id is None:
        case_id=all_data.get("case_id",None)
    # functions = data['functions']
    tenant_id = data.get("tenant_id",None)
    if tenant_id is None:
        tenant_id=all_data.get("tenant_id",None)

    queue_id = data.get('queue_id',None)
    if queue_id is None:
        queue_id=all_data.get("queue_id",None)

    if case_id is None or tenant_id is None or queue_id is None:
        logging.warning(f'Either Case id or tenant id is None please check. UI data: [{data}]')
        return jsonify({'flag': False, 'message': 'Either Case id or tenant id is None please check'})

    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    group_access_db=DB('group_access', **db_config)
    template_db = DB('template_db', **db_config)
    biz_db = DB('business_rules', **db_config)

    fields=data.get('fields',{})
    party_id=fields.get("party_id",'')
    field_changes = data.get('field_changes',[])
    logging.info(f"#######fields is {fields}")
    recommended_changes=data.get("recommended_changes",{})
    button = all_data.get('button',None)
    safe_task_id = all_data.get('safe_task_id',None)

    column_names=[]
    new_fields = data.get("new_fields", {})
    #print(f"the new fields is:::::{new_fields}")
    # Convert to JSON string
    json_value = json.dumps(new_fields)

    chunk_size = 4000
    chunks = [json_value[i:i+chunk_size] for i in range(0, len(json_value), chunk_size)]
    #print("##########")
    
    if party_id:
        
        check_query = f"SELECT * FROM trained_info WHERE party_id = '{party_id}'"
        existing_data = template_db.execute_(check_query)

        if existing_data.empty:
            # INSERT NEW
            logging.info(f"No record found for party_id {party_id}. Inserting new.")
            insert_data = {
                # "id": next_id,
                "party_id": party_id,
                "new_add_fields": json_value   # or clob_value if chunked
            }
            template_db.insert_dict(insert_data, 'trained_info')

        else:
            #print("not emptyyyyyyyy:::::")
            update_data = {
                'new_add_fields': json_value
            }

            #print(f"Existing record found for party_id {party_id}. Updating training data.")
            template_db.update(
                'trained_info',
                update=update_data,
                where={'party_id': party_id}
            )

            #print("new_fields successfully stored into DB.")
           
    try:
        extra_params = data.get("extra_params", {})
        component_dict = extra_params.get("component_dict", {})

        # Extract categories from input
        component_fields   = {k: v.get("component_fields", []) for k, v in component_dict.items()}
        duplicated_fields  = {k: v.get("duplicated_fields", []) for k, v in component_dict.items()}
        dynamic_fields     = {k: v.get("dynamic_fields", []) for k, v in component_dict.items()}
        removed_fields     = {k: v.get("removed_fields", {"duplicate_fields": [], "custom_fields": []})
                            for k, v in component_dict.items()}

        any_changes = component_fields or duplicated_fields or any(removed_fields.values())
        if not any_changes:
            return  # nothing to update

        # FETCH EXISTING DB RECORD
        query = f"SELECT * FROM dynamic_fields WHERE party_id = '{party_id}'"
        existing_data = extraction_db.execute_(query).to_dict(orient="records")

        def safe_load(val):
            try:
                return ast.literal_eval(val or "{}")
            except:
                return {}

        component_fields_db  = {}
        duplicated_fields_db = {}
        dynamic_fields_db    = {}

        if existing_data:
            existing_row = existing_data[0]
            component_fields_db  = safe_load(existing_row.get("component_fields"))
            duplicated_fields_db = safe_load(existing_row.get("duplicated_fields"))
            dynamic_fields_db    = safe_load(existing_row.get("dynamic_fields"))

            
            # APPLY REMOVE OPERATIONS
            for key, rm in removed_fields.items():
                rm_custom = rm.get("custom_fields", [])
                rm_dup    = rm.get("duplicate_fields", [])

                component_fields_db[key]  = [v for v in component_fields_db.get(key, []) if v not in rm_custom]
                duplicated_fields_db[key] = [v for v in duplicated_fields_db.get(key, []) if v not in rm_dup]
                dynamic_fields_db[key]    = [v for v in dynamic_fields_db.get(key, []) if v not in rm_custom]

                # Remove from INPUT as well
                component_fields[key]  = [v for v in component_fields.get(key, []) if v not in rm_custom]
                duplicated_fields[key] = [v for v in duplicated_fields.get(key, []) if v not in rm_dup]

            # MERGE FUNCTION
            def merge(existing, new):
                seen = set()
                merged = []

                for v in existing or []:
                    if v not in seen:
                        merged.append(v); seen.add(v)

                for v in new or []:
                    if v not in seen:
                        merged.append(v); seen.add(v)

                return merged

            # MERGE ALL CATEGORIES
            all_keys = set(component_fields_db) | set(component_fields)
            for key in all_keys:
                component_fields_db[key] = merge(component_fields_db.get(key, []),
                                                component_fields.get(key, []))

            all_keys = set(duplicated_fields_db) | set(duplicated_fields)
            for key in all_keys:
                duplicated_fields_db[key] = merge(duplicated_fields_db.get(key, []),
                                                duplicated_fields.get(key, []))

            all_keys = set(dynamic_fields_db) | set(dynamic_fields)
            for key in all_keys:
                dynamic_fields_db[key] = merge(dynamic_fields_db.get(key, []),
                                            dynamic_fields.get(key, []))

            # LENGTH DICTS
            component_fields_len  = {k: len(v) for k, v in component_fields_db.items()}
            duplicated_fields_len = {k: len(v) for k, v in duplicated_fields_db.items()}
            dynamic_fields_len    = {k: len(v) for k, v in dynamic_fields_db.items()}

            last_modified = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # UPDATE QUERY
            query = f"""
                UPDATE dynamic_fields SET
                    component_fields      = '{json.dumps(component_fields_db)}',
                    duplicated_fields     = '{json.dumps(duplicated_fields_db)}',
                    dynamic_fields        = '{json.dumps(dynamic_fields_db)}',
                    component_fields_len  = '{json.dumps(component_fields_len)}',
                    duplicated_fields_len = '{json.dumps(duplicated_fields_len)}',
                    dynamic_fields_len    = '{json.dumps(dynamic_fields_len)}',
                    last_modified_date    = TO_TIMESTAMP('{last_modified}', 'YYYY-MM-DD HH24:MI:SS')
                WHERE party_id = '{party_id}'
            """

        else:
            # INSERT NEW RECORD
            component_fields_len  = {k: len(v) for k, v in component_fields.items()}
            duplicated_fields_len = {k: len(v) for k, v in duplicated_fields.items()}
            dynamic_fields_len    = {k: len(v) for k, v in dynamic_fields.items()}

            last_modified = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            query = f"""
                INSERT INTO dynamic_fields (
                    party_id, component_fields, duplicated_fields, dynamic_fields,
                    component_fields_len, duplicated_fields_len, dynamic_fields_len,
                    last_modified_date
                ) VALUES (
                    '{party_id}',
                    '{json.dumps(component_fields)}',
                    '{json.dumps(duplicated_fields)}',
                    '{json.dumps(dynamic_fields)}',
                    '{json.dumps(component_fields_len)}',
                    '{json.dumps(duplicated_fields_len)}',
                    '{json.dumps(dynamic_fields_len)}',
                    TO_TIMESTAMP('{last_modified}', 'YYYY-MM-DD HH24:MI:SS')
                )
            """

        extraction_db.execute_(query)

    except Exception as e:
        logging.error(f"Error updating dynamic fields: {e}")





    def extract_payload_values(data):
        results = {}
        for field_name, field_value in data.items():
            column_names.append(field_name)
            if isinstance(field_value, str):
                try:
                    
                    field_value = json.loads(field_value)
                except json.JSONDecodeError:
                    pass  
            if isinstance(field_value, dict):  

                tab = field_value
                for key, value in tab.items():
                    if isinstance(value, dict):
                        logging.info("value is", value)
                        nested_data = value
                        for sub_key, sub_data in nested_data.items():
                            if sub_key == 'a.v':
                                results[key] = next(iter(sub_data.values()))
                    else:
                        # Store the value for other keys
                        results[key] = value
            else:
                results[field_name] = field_value

        return results

    
    if fields:
        payload_values=extract_payload_values(fields)

    #logging.info(f"###payload_values is {payload_values}")
    

    query=f"select * from ocr where case_id='{case_id}'"
    ocr_data=extraction_db.execute_(query).to_dict(orient='records')[0]
    # logging.info(f"####ocr_data is {ocr_data}")
    old_value={}
    new_value={}
    changed_fileds=[]

    tabs=['creditors','stocks','debtors','advances','term','working_capital_ocr','dp_statement']
    database_values={}

    try:
        # STEP 1: Build database_values
        for column in column_names:
            db_value = ocr_data.get(column)

            # TAB TYPE FIELDS
            if column in tabs and isinstance(db_value, str):
                try:
                    db_value = json.loads(db_value)
                except json.JSONDecodeError:
                    pass

                if isinstance(db_value, dict):
                    for key, value in db_value.items():
                        if key == "tab_view":
                            continue

                        if isinstance(value, dict) and "a.v" in value:
                            database_values[key] = list(value["a.v"].values())[0]
                        else:
                            database_values[key] = value
            else:
                database_values[column] = db_value

        logging.info(f"##database_values is {database_values}")

        
        # STEP 2: Compare payload vs DB → find changes
        
        ignored_keys = {
            'script_rating_ocr','customer_category','script_rating',
            'date_stat','comments','unhold_comments'
        }

        for key in payload_values:
            db_val = database_values.get(key)
            payload_val = payload_values.get(key)
            def normalize(val):
                if val is None:
                    return ""
                return str(val).strip()

            db_norm = normalize(db_val)
            payload_norm = normalize(payload_val)
            # BASIC DIFF CHECK
            if key not in ignored_keys and db_norm != payload_norm :
                old_value[key] = "" if db_val is None else db_val
                new_value[key] = payload_val
                changed_fileds.append(key)
                continue

            # SPECIAL HANDLING 1 → customer_category
            if key == 'customer_category' and db_val and db_val != payload_val:
                old_value[key] = db_val
                new_value[key] = payload_val
                changed_fileds.append(key)

            # SPECIAL HANDLING 2 → date_stat
            if key == 'date_stat' and 'date_stat' in field_changes:
                old_value[key] = db_val or ""
                new_value[key] = payload_val
                changed_fileds.append(key)

            # SPECIAL HANDLING 3 → comments
            if key == 'comments':
                try:
                    db_comment = json.loads(db_val)[0]["comment"] if db_val else ""
                    py_comment = payload_val[0]["comment"] if payload_val else ""

                    if db_comment != py_comment:
                        old_value['hold_comments'] = db_comment
                        new_value['hold_comments'] = py_comment
                        changed_fileds.append('hold_comments')

                except Exception:
                    logging.info("###Error at the hold comments...")

            # SPECIAL HANDLING 4 → unhold_comments
            if key == 'unhold_comments':
                try:
                    db_comment = json.loads(db_val)[0]["comment"] if db_val else ""
                    py_comment = payload_val[0]["comment"] if payload_val else ""

                    if db_comment != py_comment:
                        old_value[key] = db_comment
                        new_value[key] = py_comment
                        changed_fileds.append(key)

                except Exception:
                    logging.info("###Error at the unhold comments...")

        logging.info(f"#####changed_fileds is {changed_fileds}")

        
        # Build formatted audit text
        
        new_values_str = ", ".join(f"{k.replace('_',' ').title()}: {v}" for k, v in new_value.items())
        old_values_str = ", ".join(f"{k.replace('_',' ').title()}: {v}" for k, v in old_value.items())
        changed_fields_str = ", ".join(v.replace("_", " ").title() for v in changed_fileds)

        formatted_datetime = datetime.now(pytz.timezone(tmzone)).strftime('%Y-%m-%d %H:%M:%S')

        logging.info(f"newvalues----------------- {new_values_str}")
        logging.info(f"oldvalues=============== {old_values_str}")
        logging.info(f"changedfields0000000 {changed_fields_str}")

        
        # USER ROLE & AUDIT INSERT
        
        user = data.get('user', '')
        role_query = f"select role from active_directory where username='{user}'"
        role = group_access_db.execute_(role_query).iloc[0]['role'].lower()

        update_col = "maker_id" if role == "maker" else "checker_id"
        time_col = "maker_datetime" if role == "maker" else "checker_datetime"

        if new_value:
            audit_query = f"""
                INSERT INTO audit_report_case 
                (party_id, changed_fields, old_values, new_values, {update_col}, case_id, {time_col}, category)
                VALUES (
                    '{party_id}', '{changed_fields_str}', '{old_values_str}', '{new_values_str}',
                    '{user}', '{case_id}',
                    TO_TIMESTAMP('{formatted_datetime}', 'YYYY-MM-DD HH24:MI:SS'),
                    'Change in Transaction'
                )
            """
            logging.info(f"####query is {audit_query}")
            extraction_db.execute_(audit_query)

        
        # Save reapply master-field snapshot
        
        try:
            reapply_cols = ["customer_name","stock_doc_month","stock_doc_year",
                            "customer_category","bank_share","due_date"]

            snap_data = {f: payload_values.get(f, "") for f in reapply_cols}
            snap_json = json.dumps(snap_data)

            update_snap = f"""
                UPDATE ocr 
                SET BIZ_RULES_LIST = '{snap_json}' 
                WHERE case_id='{case_id}'
            """
            extraction_db.execute_(update_snap)

        except Exception:
            logging.exception("####----Re-apply not working---")

    except:
        logging.exception("----Data is not inserted into the audit table----")


    executor = ThreadPoolExecutor(max_workers=5)

    # Call the function   
    """try:
        logging.debug('Submitting save_changes_m to background thread')

        def bg_task(case_id, data, tenant_id, queue_id):
            try:
                save_changes_m(case_id, data, tenant_id, queue_id)
                logging.info(f"save_changes_m completed for case_id={case_id}")
            except Exception:
                logging.exception(f"save_changes_m failed for case_id={case_id}")

        executor.submit(bg_task, case_id, data, tenant_id, queue_id)

        # Return immediately
        result = {'data': {}}
        return_data = {'flag': True, 'data': {'message': 'Saving started in background'}}

    except Exception:
        logging.exception("Failed to submit async task")
        result = {'data': {}}
        return_data = {'flag': False, 'message': 'Error starting Save Changes'}"""
    try:
        logging.debug('Calling function `save_changes`')
        result = save_changes_m(case_id, data, tenant_id, queue_id)

        if not result['flag']:
            return jsonify({'flag': False, 'message': 'Error in Save Changes'})
        logging.info(f'response of save changes function {result}')
        return_data = {'flag': True, 'data': {'message':'Saving Data Sucess'}}
    except Exception as e:
        result = {'data':{}}
        return_data =  {'flag': False, 'message': 'Error in Save Changes'}

    isMaker = (safe_task_id == 'maker_queue' and button == 'Save')
    #isMaker = (safe_task_id == 'maker_queue' and button == 'Save')
    if isMaker:
        #print(f"data is......................:{data}")
        #initial_value_dict=data.get('initial_value',{})
        #print(f"initial_value_dict is..............:{initial_value_dict}")
        tab_json_cache={}
        #print(f"enetred here.........")

        def extract_fields(rule_text):
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

            return fields

        def ensure_initial_value(tab_json, field_name):
            initial_value_dict=data.get('initial_value',{})
            #print(f"initial_value_dict is..............:{initial_value_dict}")
            tab_json.setdefault("initial_value", {})
            #print(f"entered for initial_value")
            # already captured → do nothing
            # if field_name in tab_json["initial_value"]:
            #     return
            init_map = tab_json["initial_value"]
            prev=""
            #print(f"init_map is...............:{init_map}")
            if field_name in init_map:
                #print(f"field_name is...........:{field_name}")

                prev = init_map[field_name].get("previous", "")
                #print(f"prev is....:{prev}")
            if field_name not in tab_json:
                # tab_json["initial_value"][field_name] = {
                #     "previous": "",
                #     "updated": ""
                # }
                return
            ocr_val=None
            field_val = tab_json[field_name]
            #print(f"field_val is...........:{field_val}")

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
            #print(f"ocr_val is..................:{ocr_val}")
            if ocr_val in ("", None, "null", "None"):
                update = ""
            else:
                update = str(ocr_val)
            #print(f"update is...............:{update}")
            #tab_json["initial_value"][field_name] = {
            #    "previous": prev,
            #    "updated": update
            #}
            # print("type:", type(initial_value_dict))
            # print("bool:", bool(initial_value_dict))
            # print("repr:", repr(initial_value_dict))
            if isinstance(initial_value_dict, str):
                try:
                    initial_value_dict = json.loads(initial_value_dict)
                except:
                    initial_value_dict = {}
            if initial_value_dict :
                #print(f"enetred here,..............")
                # print(f"field_name is......:{field_name}")
                # print(f"tab_json is......:{tab_json}")
                if field_name in tab_json and field_name in initial_value_dict:
                    #print(f"field_name is......:{field_name}")
                    #print(f"tab_json is......:{tab_json}")
                    previous_val= initial_value_dict.get(field_name,{}).get('previous','')
                    #print(f"previous val is..:{previous_val}")
                    tab_json["initial_value"][field_name] = {
                        "previous": previous_val,
                        "updated": update
                    }
                    #print(f"tab_json[intital_value][field] name is.........:{tab_json['initial_value'][field_name]}")
                else:
                    #print(f"enetreddddddddd elseeeeeeeee")
                    tab_json["initial_value"][field_name] = {
                        "previous": prev,
                        "updated": update
                    }
                    #print(f"prev is.............:{prev}")
                    #print(f"updated is.......:{update}")
            else:
                tab_json["initial_value"][field_name] = {
                    "previous": prev,
                    "updated": update
                }

        def apply_ocr_values(rule_text, tab_json):
            updated = rule_text

            for field in extract_fields(rule_text):
                if field not in tab_json:
                    continue   # DO NOT inject 0

                value = tab_json.get(field)
                if value in ("", None, "null", "None",''):
                    continue
                #value = tab_json.get(field, 0)

                if isinstance(value, dict):
                    av = value.get("a.v", {})
                    value = list(av.values())[0] if av else 0

                value = str(value).replace(",", "")
                updated = updated.replace(f"{field}()", f"{field}({value})")

            return updated


        # -------- EXACT PARSER (NO REGEX) --------
        def parse_rule_pairs(rule_text):
            pairs = []
            i = 0
            n = len(rule_text)

            while i < n:
                close_idx = rule_text.find(")", i)
                if close_idx == -1:
                    break

                open_idx = rule_text.rfind("(", i, close_idx)
                if open_idx == -1:
                    i = close_idx + 1
                    continue

                #value = rule_text[open_idx + 1:close_idx].strip()
                value = rule_text[open_idx + 1:close_idx].strip()
                try:
                    value = float(value.replace(",", ""))
                except:
                    i = close_idx + 1
                    continue

                # find operator before this field
                start = open_idx - 1
                while start >= 0 and rule_text[start] not in "+-*/":
                    start -= 1

                field = rule_text[start + 1:open_idx].strip()

                pairs.append((field, value))
                i = close_idx + 1

            return pairs


        def calculate_from_rule_text(rule_text):
            total = 0.0
            for _, value in parse_rule_pairs(rule_text):
                try:
                    total += float(value)
                except:
                    pass
            return total




        def update_tab_json(tab_json, final_field, final_value):
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


        def clob_chunks_sql(json_str):
            if not json_str or json_str.strip() in ['null', 'None', '{}']:
                return "TO_CLOB(NULL)"

            escaped = json_str.replace("'", "''")
            chunk_size = 3000
            chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
            #return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)
            #return ' || '.join(f"TO_CLOB('{chunk.replace(\"'\", \"''\")}')" for chunk in chunks )
            return " || ".join("TO_CLOB('{}')".format(chunk.replace("'", "''")) for chunk in chunks)




        def extract_recommended_value(meta):
            if not isinstance(meta, dict):
                return None

            for key in ("Typed", "Cropped", "Converted"):
                val = meta.get(key)
                if val == "":
                    val=0
                    return val
                if val not in (None,"null"):
                    return val

            return None


        def rebuild_rule_text(rule_text, updated_values):
            rebuilt = rule_text
            #print(f"rule_built beofre ..:{rebuilt}")
            for field, new_val in updated_values.items():
                new_val = str(new_val).replace(",", "").strip()

                rebuilt = re.sub(
                    rf"{re.escape(field)}\s*\([^)]*\)",
                    f"{field}({new_val})",
                    rebuilt
                )
            #print(f"rebuilt is...............................:{rebuilt}")
            return rebuilt




        def apply_recommended_changes(rule_text_input, recommended_changes,ocr_row):
            #print(f"recommended_changes is............:{recommended_changes}")
            for tab, fields in recommended_changes.items():
                tab = tab.lower()
                if tab not in rule_text_input:
                    continue

                for field_name, meta in fields.items():

                    # -------- INITIAL VALUE STORAGE (ADD ONLY) --------
                    source = None
                    src_val = None

                    if meta.get("Cropped") not in ("", None, "null"):
                        src_val = meta.get("Cropped")
                        source = "Cropped"
                    elif meta.get("Converted") not in ("", None, "null"):
                        src_val = meta.get("Converted")
                        source = "Converted"
                    else:
                        source = "Typed"

                    if source in ("Cropped", "Converted","Typed"):
                        if tab not in tab_json_cache:
                            raw_tab = ocr_row.get(tab, "{}")
                            try:
                                tab_json_cache[tab] = json.loads(raw_tab) if raw_tab else {}
                            except:
                                tab_json_cache[tab] = {}

                        tab_json = tab_json_cache[tab]
                        ensure_initial_value(tab_json, field_name)
                        #print(f"tab_json are................in recommended :{tab_json}")



                    new_val = extract_recommended_value(meta)
                    if new_val is None:
                        continue

                    new_val = str(new_val).replace(",", "").strip()

                    for rule_id, rules in rule_text_input[tab].items():
                        for final_field, rule_text in rules.items():

                            #old_text = rule_text  #  store previous
                            if isinstance(rule_text, dict):
                                rule_text_to_rebuild = rule_text.get("updated") or rule_text.get("previous")
                            else:
                                rule_text_to_rebuild = rule_text
                            old_text = rule_text_to_rebuild
                            new_text = rebuild_rule_text(
                                rule_text_to_rebuild,
                                {field_name: new_val}
                            )
                            if new_text != old_text:
                                rule_text_input[tab][rule_id][final_field] = {
                                    "previous": old_text,
                                    "updated": new_text
                                }
                            else:
                                rule_text_input[tab][rule_id][final_field] = old_text

            return rule_text_input



        def process_dynamic_rules(case_id, extraction_db, biz_db, recommended_changes):
            #print(f"enteredd calling programmmm")
            qry = f"SELECT * FROM ocr WHERE case_id='{case_id}'"
            ocr_df = extraction_db.execute_(qry)

            if ocr_df is None or ocr_df.empty:
                return

            ocr_row = ocr_df.iloc[0]
            party_id = ocr_row.get("party_id")
            rule_text_input_raw = ocr_row.get("rule_text_input")
            #print(f"rule_text_input_raw is............:{rule_text_input_raw}")
            if not rule_text_input_raw:
                rule_text_input = {}
                first_time = True
            else:
                rule_text_input = json.loads(rule_text_input_raw)
                first_time = False

            # -------- FIRST TIME BUILD --------
            if first_time:
                qry = f"""
                    SELECT rule_id, tab_name, rule_text, final_field
                    FROM rule_base_phase_2
                    WHERE party_id='{party_id}'
                """
                rules_df = biz_db.execute_(qry)

                for _, row in rules_df.iterrows():
                    tab = row["tab_name"].lower().strip()
                    rule_text = row["rule_text"].strip()
                    final_field = row["final_field"].strip()

                    raw_tab = ocr_row.get(tab, "{}")
                    try:
                        tab_json = json.loads(raw_tab) if isinstance(raw_tab, str) else raw_tab or {}
                    except:
                        tab_json = {}

                    rule_id = str(row["rule_id"]).strip()
                    updated_rule = apply_ocr_values(rule_text, tab_json)

                    rule_text_input.setdefault(tab, {})
                    rule_text_input[tab].setdefault(rule_id, {})
                    rule_text_input[tab][rule_id][final_field] = updated_rule


            # -------- APPLY RECOMMENDED CHANGES --------
            if recommended_changes:
                rule_text_input  = apply_recommended_changes(
                    rule_text_input,
                    recommended_changes,
                    ocr_row
                )
                #print(f"rule_text_input is.........:{rule_text_input}")

            # -------- SAVE RULE_TEXT_INPUT --------
            rule_json = json.dumps(rule_text_input)
            rule_clob = clob_chunks_sql(rule_json)
            #print(f"rule_clob is.......:{rule_clob}")
            qry = f"""
                UPDATE ocr
                SET rule_text_input = {rule_clob}
                WHERE case_id='{case_id}'
            """
            extraction_db.execute_(qry)
            for tab, tab_json in tab_json_cache.items():
                json_str = json.dumps(tab_json)
                #print(f"json_str is.............:{json_str}")
                clob_expr = clob_chunks_sql(json_str)

                qry = f"""
                    UPDATE ocr
                    SET {tab} = {clob_expr}
                    WHERE case_id='{case_id}'
                """
                extraction_db.execute_(qry)


        # --------------------------------------------------
        # ---------------- EXECUTION ------------------------
        # --------------------------------------------------
        #print(f"calling process_dynamic_rules")
        process_dynamic_rules(
            case_id=case_id,
            extraction_db=extraction_db,
            biz_db=biz_db,
            recommended_changes=recommended_changes
        )


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

    
    
    audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id, 
                        "api_service": "save_changes", "service_container": "save_changes", "changed_data": json.dumps(result['data']),
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                        "response_data": json.dumps(return_data), "trace_id": case_id, "session_id": session_id,"status":str(return_data['flag'])}
    insert_into_audit(case_id, audit_data)

    return jsonify(return_data)


