import argparse
import ast
import base64
import json
import requests
import traceback
import warnings
import os
import pysftp
import shutil
import pandas as pd
import math
import sqlalchemy
from sqlalchemy import create_engine, exc,text
from sqlalchemy.engine import create_engine
import psutil
from pathlib import Path
from multiprocessing import Pool
from functools import partial

from datetime import datetime, timedelta
from app.db_utils import DB
from flask import Flask, request, jsonify
from flask_cors import CORS
from pandas import Series, Timedelta, to_timedelta
from time import time
from itertools import chain, repeat, islice, combinations
from io import BytesIO,StringIO
from app.elasticsearch_utils import elasticsearch_search
from py_zipkin.util import generate_random_64bit_string
from py_zipkin import storage
from collections import defaultdict
from sqlalchemy.orm import sessionmaker
from elasticsearch import Elasticsearch
import re
import numpy as np
from datetime import datetime
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
import pytz
tmzone = 'Asia/Kolkata'

from ace_logger import Logging

from app import app
from app import cache
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from cryptography.fernet import Fernet


import io

es_dns = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_DNS','')
es_port = os.environ.get('ELASTIC_SEARCH_FULL_PORT', '')
es_scheme = os.environ.get('ELASTIC_SEARCH_FULL_SEARCH_SCHEME','')


es = Elasticsearch(
    [f'{es_dns}'],
    http_auth=('elastic','MagicWord'),
    scheme=f"https",
    port=es_port,
    ca_certs="/usr/share/elastic-auth/elasticsearch-ca.pem",
)

logging = Logging(name='master_upload')

db_config = {
    'host': os.environ['HOST_IP'],
    'port': os.environ['LOCAL_DB_PORT'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD']
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


def insert_into_audit(data):
    tenant_id = "hdfc"
    db_config['tenant_id'] = tenant_id
    groupdb = DB('group_access', **db_config)
    groupdb.insert_dict(data, 'hdfc_audit')
    return True

def generate_multiple_insert_query(data, table_name):
    values_list = []
    for row in data:
        values_list_element = []
        for column, value in row.items():
            values_list_element.append(f"'{value}'")
        values_list.append('(' + ', '.join(values_list_element) + ')')
    values_list = ', '.join(values_list)
    columns_list = ', '.join([f"`{x}`" for x in list(data[0].keys())])
    query = f"INSERT INTO `{table_name}` ({columns_list}) VALUES {values_list}"
    
    return query

def create_index(tenant_ids, sources=[]):
    body = {
        "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase"
                        ]
                    }
                }
            }
        },
        "mappings": {
            "date_detection": "false",
            "numeric_detection": "false"
        }
    }
    
    body_with_date = {
            "settings": {
            "analysis": {
                "analyzer": {
                    "default": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                          "lowercase"
                        ]
                    }
                }
            }
            },
        "mappings": {
            "properties": {
                "@timestamp": {
                    "type": "date"
                },
                "@version": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },
                "ocr": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            "type": "date"
                        }
                    }
                },
                "process_queue": {
                    "properties": {
                        "created_date": {
                            "type": "date"
                        },
                        "last_updated": {
                            "type": "date"
                        },
                        "freeze": {
                            "type": "boolean"
                        },
                        "case_lock":{
                            "type": "boolean"
                        }
                    }
                }
            },
            "date_detection": "false",
            "numeric_detection": "false"
        }
        }
    indexes = []
    
    for tenant_id in tenant_ids:
        indexes.extend(get_search_indexes(sources, tenant_id))

    logging.info(f"####indexes are {indexes}")
    for ind in indexes:
        es.indices.delete(index=ind, ignore=[400, 404])
        if 'processqueue' in ind:
            es.indices.create(index=ind, body=body_with_date, ignore=400)
        else:
            logging.info(f"###Creating index in progress...")
            es.indices.create(index=ind, body=body, ignore=400)
            logging.info(f"###Index created")

def get_search_indexes(sources, temp_tenant_id=''):
    """
    Author : Akshat Goyal
    :param sources:
    :return:
    """
    tenant_id = temp_tenant_id.replace('.', '')
    if not sources:
        return '_all'
    indexes = []
    if isinstance(sources, list):
        for source in sources:
            new_source = tenant_id + source if tenant_id else source
            new_source = new_source.replace('.', '').replace('_', '')
            indexes.append(new_source)
    elif isinstance(sources, str):
        new_source = tenant_id + '_' + sources if tenant_id else sources
        new_source = new_source.replace('.', '').replace('_', '')
        indexes.append(new_source)

    return indexes

def dataframe_to_blob(data_frame):
    
    chunk_size = 10000

    bio = BytesIO()


    writer = pd.ExcelWriter(bio, engine='xlsxwriter')

    # Write the DataFrame to the Excel file in chunks
    for i in range(0, len(data_frame), chunk_size):
        data_frame_chunk = data_frame.iloc[i:i+chunk_size]
        data_frame_chunk.to_excel(writer, index=False, startrow=i)

    # Close the ExcelWriter to flush data to the file
    writer.save()

    
    bio.seek(0)

    blob_data = base64.b64encode(bio.read())

    # Return the Base64 encoded blob data
    return blob_data

def fix_json_decode_error(data_frame):
    for column in data_frame.columns:
        if isinstance(data_frame.loc[0, column], (pd._libs.tslibs.timedeltas.Timedelta, pd._libs.tslibs.timestamps.Timestamp)):       
            data_frame[column] = data_frame[column].astype(str)
    return data_frame

def get_user_groups(tenant_id):
    db_config['tenant_id'] = tenant_id
    group_db = DB('group_access', **db_config)
    queue_db = DB('queues', **db_config)

    query = "SELECT id, username from active_directory"
    user_list = group_db.execute(query).username.to_dict()

    query = "SELECT * from user_organisation_mapping where type = 'user'"
    user_details = group_db.execute(query).to_dict(orient='records')

    query = "SELECT * from organisation_attributes"
    attributes_df=group_db.execute_(query)
    attributes = group_db.execute(query, index = 'att_id').to_dict()

    query = "SELECT * from organisation_hierarchy"
    hierarchy = group_db.execute(query).set_index('h_group').to_dict()['h_order']
    
    query = "SELECT id,group_definition from group_definition"
    group_definition = group_db.execute(query).group_definition.to_dict()
    
    user_sequence = {}
    for user_detail in user_details:
        try:
            user_sequence[user_detail['sequence_id']].append(user_detail)
        except:
            user_sequence[user_detail['sequence_id']] = [user_detail]
            
    attribute_dropdown = group_db.get_all('attribute_dropdown_definition')   
    
    attribute_dropdown['attribute_id'] = attribute_dropdown['attribute_id'].apply(lambda x: attributes_df['attribute'])
               
    user_info = defaultdict(dict)
    for k, v in user_sequence.items():
        for user_detail in v:
            name = user_list[user_detail['user_id']]
            index = user_detail['organisation_attribute']
            attribute_name = attributes['attribute'][index]
            attribute_value = user_detail['value']             
            try:
                if attribute_name in user_info[k][name]:
                    user_info[k][name][attribute_name] = ','.join(set(user_info[k][name][attribute_name].split(',') + [attribute_value]))
                else:
                    user_info[k][name][attribute_name] = attribute_value
            except:
                user_info[k][name] = {attribute_name: attribute_value}
                               
            for key, val in hierarchy.items():
                if attribute_name in val.split(','):
                    attribute_loop = val.split(attribute_name+',')[1].split(',') if len(val.split(attribute_name+',')) > 1 else val
                    for child_attribute in attribute_loop:
                        condition = (attribute_dropdown['parent_attribute_value'] == attribute_value) & (attribute_dropdown['attribute_id'] == child_attribute)
                        query_result = attribute_dropdown[condition]
                        if not query_result.empty:
                            child_attribute_value = list(query_result.value.unique())
                            user_info[k][name][child_attribute] =  ','.join(child_attribute_value)
                    
    # Optimize below   
    group_dict = defaultdict(dict)
    for key_, val_ in user_info.items():
        for k,v in val_.items():
            group_list = []
            for key, val in v.items():
                subset = []
                val = val.split(',')
                for i in val:
                    for group, attribute in group_definition.items(): 
                        attribute = json.loads(attribute)
                        for x,y in attribute.items():
                            if key == x and i == y:
                               subset.append(group)
                if subset!= []:
                    group_list.append(subset)
            group_dict[key_][k] = group_list
            
    classify_users = defaultdict(dict)
    for key, val in group_dict.items():           
        for user, value in val.items():
            if value and len(value) > 1:
                classify_users[key][user] = list(set.intersection(*map(set,value)))
            else:
                if len(value) > 0:
                    classify_users[key][user] = value[0]
                else:
                    pass

    return classify_users

def get_group_ids(user, db):
    print(f'Getting group IDs for user `{user}`')

    query = 'SELECT organisation_attributes.attribute, user_organisation_mapping.value \
            FROM `user_organisation_mapping`, `active_directory`, `organisation_attributes` \
            WHERE active_directory.username=%s AND \
            active_directory.id=user_organisation_mapping.user_id AND \
            organisation_attributes.id=user_organisation_mapping.organisation_attribute'

    user_group = db.execute_(query, params=[user])

    if user_group.empty:
        logging.error(f'No user organisation mapping for user `{user}`')
        return

    user_group_dict = dict(zip(user_group.attribute, user_group.value))
    user_group_dict = {key: [value] for key, value in user_group_dict.items()}
    group_def_df = db.get_all('group_definition')

    if group_def_df.empty:
        logging.debug(f'Groups not defined in `group_definition`')
        return

    group_def = group_def_df.to_dict(orient='index')
    group_ids = []
    for index, group in group_def.items():
        logging.debug(f'Index: {index}')
        logging.debug(f'Group: {group}')

        try:
            group_dict = json.loads(group['group_definition'])
        except:
            logging.error('Could not load group definition dict.')
            break

        # Check if all key-value from group definition is there in the user group
        if group_dict.items() == user_group_dict.items():
            group_ids.append(index)

    print(f'Group IDs: {group_ids}')
    return group_ids



def structure_to_excel(table_name, db, tenant, database):
    try:
        query = 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name ="'+table_name+'" AND TABLE_SCHEMA LIKE "'+tenant+'_'+database+'"'
        df =db.execute_(query)
        print(f"THIS IS THE SELECT STATEMENT {query}")
        print(f"THIS IS THE OUTPUT {df}")
        df1= df.rename({'TABLE_SCHEMA':'database', 'TABLE_NAME':'table_name', 'COLUMN_NAME':'columns','COLUMN_TYPE':'COLUMN_TYPE', 'CHARACTER_MAXIMUM_LENGTH':'length','IS_NULLABLE':'index'}, axis=1)
        print(df1.columns)
        df1=df1[['database', 'table_name', 'columns','COLUMN_TYPE','index','COLUMN_KEY','EXTRA']]
        df1['datatype'] = df1['COLUMN_TYPE'].str.split('(').str[0]
        df1['length'] = df1['COLUMN_TYPE'].str.split('(').str[1]
        df1['length'] = df1['length'].str.split(')').str[0]
        df1=df1[['database', 'table_name', 'columns','datatype','length','index','COLUMN_KEY','EXTRA']]
        df2=df1.replace({'index': r'^N.$'}, {'index': 'not null'}, regex=True)
        df3=df2.replace({'index': r'^YE.$'}, {'index': 'null'}, regex=True)
        df4= df3.replace({'length' : { 'None' : 1}})
        df5=df4.replace(to_replace ="UNI", value ="unique key") 
        df6=df5.replace(to_replace ="PRI", value ="primary key")
        df7=df6[['database', 'table_name', 'columns','datatype','length','index','COLUMN_KEY','EXTRA']]

        blob_data = dataframe_to_blob(df7)
        
        message = 'Successfuly converted structure to excel.'
        return {"flag": True, "message" : message, "blob_data": blob_data}
    except:
        traceback.print_exc()
        message= f"Something went wrong while converting the structure to excel"
        return {"flag": True, "message": message}



@app.route('/folder_monitor_sftp', methods=['POST', 'GET'])
def folder_monitor_sftp():
    data = request.json
    print(f'Request data: {data}')
    tenant_id = data.get('tenant_id', None)
    try:
        input_path_str = "/var/www/master_upload/app/master_input/"
        output_path = "/var/www/master_upload/app/master_output/"
        file_list = os.listdir(input_path_str)
        if len(file_list):
            files = [file_list[0]]
        else:
            files = []
        reponse_data = {}
        file_names = []
        for file_ in files:
            logging.debug(f'move from: {file_}')
            reponse_data['move_from'] = str(file_)
            filename = file_
            file_name = input_path_str+'/'+f'{file_}'
            try:
                shutil.copy2(Path(file_name),Path(output_path))
                os.remove(file_name)
            except Exception as e:
                out_path = output_path+"/"+f'{file_}'
                destination_path = Path(out_path)
                os.remove(destination_path)
                shutil.copy2(Path(file_name),Path(output_path))
                os.remove(file_name)
            file_names.append({'file_name': filename})
        logging.debug(f'Files: {file_names}')
        reponse_data['files'] = file_names
        reponse_data['workflow'] = 0

        final_response_data = {"flag": True, "data": reponse_data}
        return jsonify(final_response_data)
    except:
        logging.exception(
                'Something went wrong watching folder. Check trace.')
        final_response_data = {'flag': False, 'message': 'System error! Please contact your system administrator.'}
        return jsonify(final_response_data)

# Decrypt the password
def decrypt_password(encrypted_password):
    key=os.environ['CRE_KEY']
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password  


def decrypt_password_sftp(encrypted_password):
    key=os.environ['CADPRO_CRE_KEY']
    #print(f"Key is............... :{key}")
    fernet = Fernet(key)
    #print(f"Fernet is:{fernet}")
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    #print(f"Decrypted Password is:{decrypted_password}")
    return decrypted_password


@app.route('/get_files_from_sftp_masters', methods = ['GET', 'POST'])
# def get_files_from_sftp_masters():
#     try:
#         memory_before = measure_memory_usage()
#         start_time = tt()
#     except Exception:
#         logging.warning("Failed to start ram and time calc")
#         pass

#     trace_id = generate_random_64bit_string()
#     tenant_id = os.environ.get('TENANT_ID',None)

#     attr = ZipkinAttrs(
#         trace_id=trace_id,
#         span_id=generate_random_64bit_string(),
#         parent_span_id=None,
#         flags=None,
#         is_sampled=False,
#         tenant_id=tenant_id
#     )

#     with zipkin_span(
#             service_name='get_files_from_sftp_masters',
#             span_name='get_files_from_sftp_masters',
#             transport_handler=http_transport,
#             zipkin_attrs=attr,
#             port=5010,
#             sample_rate=0.5):

#         MODE = os.environ.get('MODE',None)
#         hostname = os.environ.get('CRE_HOSTNAME',None)
#         username = os.environ.get('CRE_USERNAME',None)
#         source_file_path = os.environ.get('CRE_FILE_PATH',None)

#         destination_file_path = '/var/www/master_upload/app/master_input'
#         output_path = "/var/www/master_upload/app/master_output/"
#         read_files_list = os.listdir(output_path)
#         print(f"Files which are already read are {read_files_list}")

#         print(f"### MODE is {MODE} Connecting to SFTP server: {hostname} with username: {username}")

#         if MODE == 'UAT' or MODE == 'PRE-PROD' or MODE == 'PROD' or MODE == 'STANDBY':
#             enc_password = os.environ.get('CRE_PASSWORD',None)
#             password=decrypt_password(enc_password)
#             port = int(os.environ.get('CRE_PORT',None))

#             try:
#                 source_pdf_files = []
#                 cnopts = pysftp.CnOpts()
#                 cnopts.hostkeys = None

#                 with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
#                     print("Connected to SFTP server")
#                     sftp.cwd(source_file_path)
#                     files = sftp.listdir()
#                     source_pdf_files = [file for file in files if file.endswith('.csv') or file.endswith('.xlsx')]
#                     print(f"List of PDF files found in source path: {source_pdf_files}")

#                     for filename in source_pdf_files:
#                         source_file_path_ = f"{source_file_path}/{filename}"
#                         destination_file_path_ = f"{destination_file_path}/{filename}"
#                         print(f"Copying file from {source_file_path_} to {destination_file_path_}")
#                         sftp.chmod(destination_file_path,0o777)
#                         if filename not in read_files_list:
#                             sftp.get(source_file_path_, destination_file_path_)
#                             print(f"Copied {filename} from {source_file_path_} to {destination_file_path_}")
#                         # sftp.remove(source_file_path_)
#                         else:
#                             print(f"{filename} Already Read ")

#                 response_data = {"flag": True,"data":{"message":"Copied file from SFTP server"}}
#                 print("Successfully copied files from SFTP server")

#             except Exception as e:
#                 logging.exception(ManageException(e).resolve())
#                 print("## Exception: Something went wrong in getting SFTP files", e)
#                 response_data = {"flag": False,"message":f"Error at Copied file from SFTP server","data":{}}

#             return response_data

def get_files_from_sftp_masters():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time calc")
        pass

    trace_id = generate_random_64bit_string()
    tenant_id = os.environ.get('TENANT_ID',None)

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='get_files_from_sftp_masters',
            span_name='get_files_from_sftp_masters',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

        MODE = os.environ.get('MODE',None)
        hostname = os.environ.get('CRE_HOSTNAME',None)
        username = os.environ.get('CRE_USERNAME',None)
        source_file_path = os.environ.get('CRE_FILE_PATH',None)
        result1 = {"flag": False}
        result2 = {"flag": False}
        destination_file_path = '/var/www/master_upload/app/master_input'
        output_path = "/var/www/master_upload/app/master_output/"
        read_files_list = os.listdir(output_path)
        #print(f"Files which are already read are {read_files_list}")

        #print(f"### MODE is {MODE} Connecting to SFTP server: {hostname} with username: {username}")

        if MODE == 'UAT' or MODE == 'PRE-PROD' or MODE == 'PROD' or MODE == 'STANDBY':
            enc_password = os.environ.get('CRE_PASSWORD',None)
            password=decrypt_password(enc_password)
            port = int(os.environ.get('CRE_PORT',None))

            try:
                source_pdf_files = []
                cnopts = pysftp.CnOpts()
                cnopts.hostkeys = None

                with pysftp.Connection(host=hostname, username=username, password=password, port=port, cnopts=cnopts) as sftp:
                    #print("Connected to SFTP server")
                    sftp.cwd(source_file_path)
                    files = sftp.listdir()
                    source_pdf_files = [file for file in files if file.endswith('.csv') or file.endswith('.xlsx')]
                    #print(f"List of PDF files found in source path: {source_pdf_files}")

                    for filename in source_pdf_files:
                        source_file_path_ = f"{source_file_path}/{filename}"
                        destination_file_path_ = f"{destination_file_path}/{filename}"
                        #print(f"Copying file from {source_file_path_} to {destination_file_path_}")
                        sftp.chmod(destination_file_path,0o777)
                        if filename not in read_files_list:
                            sftp.get(source_file_path_, destination_file_path_)
                            #print(f"Copied {filename} from {source_file_path_} to {destination_file_path_}")
                        # sftp.remove(source_file_path_)
                        else:
                            logging.info(f"{filename} Already Read ")

                result1 = {"flag": True,"data":{"message":"Copied file from SFTP server"}}
                #print("Successfully copied files from SFTP server")

            except Exception as e:
                logging.error("## Exception: Something went wrong in getting SFTP files", e)
                result1 = {"flag": False,"message":f"Error at Copied file from SFTP server","data":{}}

            try:
                hostname_sftp = os.environ['SFTP_SERVER']
                #print(f"!!!!!!!!! hostname_sftp is {hostname_sftp}")
                username_sftp = os.environ['SFTP_USERNAME']
                #print(f"@@@@@@@@2 username_sftp is {username_sftp}")
                enc_password = os.environ['CADPRO_PASSWORD']
                #print(f"@@@@@@@@@@@@@ enc_password is {enc_password}")
                password = decrypt_password_sftp(enc_password)
                #print(f"Password is:{password}")
                source_file_path = os.environ['SFTP_SOURCE_FILEPATH']
                #print(f"@@@@@@@@@@@@@ SFTP soirce path is{source_file_path}")
                destination_file_path = '/var/www/master_upload/app/master_input'
                output_path = "/var/www/master_upload/app/master_output/"
                read_files_list_sftp = os.listdir(output_path)

                if hostname_sftp:
                    cnopts = pysftp.CnOpts()
                    cnopts.hostkeys = None

                    with pysftp.Connection(hostname_sftp, username=username_sftp, password=password, cnopts=cnopts) as sftp:
                        sftp.cwd(source_file_path)
                        files = sftp.listdir()
                        source_files = [f for f in files if f.endswith('.csv')]

                        for filename in source_files:
                            source_file_path_ = f"{source_file_path}/{filename}"
                            destination_file_path_ = f"{destination_file_path}/{filename}"
                            sftp.chmod(destination_file_path, 0o777)
                            if filename not in read_files_list_sftp:
                                sftp.get(source_file_path_, destination_file_path_)
                            else:
                                logging.info(f"{filename} Already Read")

                    result2 = {"flag": True, "data": {"message": "Copied from SFTP 2"}}
            except Exception as e:
                logging.info("Error in SFTP 2:", e)
                result2 = {"flag": False, "data": {}, "message": str(e)}
            
            final_flag = result1["flag"] or result2["flag"]

            response_data = {
                "flag": final_flag,
                "message": "Copied files from one or both SFTP servers" if final_flag else "Failed to copy files from both SFTP servers",
                "data": {
                    "sftp1": result1,
                    "sftp2": result2
                }
            }
            return response_data





@app.route('/upload_master_blob_sftp', methods = ['GET', 'POST'])
def upload_master_blob_sftp():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    print(f'Request data: {data}')
    tenant_id = data.pop('tenant_id', None)
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    queues_db= DB('queues', **db_config)
    #file_name = data.pop('file', '')
    #file_name = file_name['file_name']
    file_name=data.get('file_name',None)
    #sys_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
                service_name='master_upload',
                zipkin_attrs=attr,
                span_name='upload_master_blob',
                transport_handler=http_transport,
                sample_rate=0.5):
        try:
            table_name = file_name.split(".")[0].lower()
            #Converting table name to the suitable table name formate
            if 'partymaster' in table_name or 'Partymaster' in table_name or 'Party_Master' in table_name or 'party_master' in table_name:
                table_name = 'party_master'
            #elif 'citystate' in table_name or 'Citystate' in table_name or 'RMMaster' in table_name or 'rm_master' in table_name or 'rmmaster' in table_name or 'RM_Master' in table_name:
            elif 'citystate' in table_name or 'Citystate' in table_name or 'RMMaster' in table_name or 'rm_master' in table_name or 'rmmaster' in table_name or 'RM_Master' in table_name or 'hubcode' in table_name or 'HUBCODE' in table_name or 'Hubcode' in table_name or 'hubcode_master' in table_name: 
                # table_name = 'city_state' or table_name ='RM_Master'
                table_name ='RM_Master'
            elif 'age_margin_master' in table_name or 'Age_Margin_Master' in table_name or 'agemarginmaster' in table_name or 'AgeMarginMaster' in table_name:
                table_name = 'age_margin_working_uat'
            elif 'component_master' in table_name or 'Component_Master' in table_name:
                table_name = 'component_master'
            elif 'wbo_region_master' in table_name or 'WBO_Region_Master' in table_name or 'wboregionmaster' in table_name or 'WBORegionMaster' in table_name:
                table_name = 'wbo_region'
    
            table_name=table_name.lower()
            logging.info(f'#####table name is ---> {table_name}')
            file_name = f'/var/www/master_upload/app/master_output/{file_name}'
            ext=Path(file_name).suffix
            excel_data=''
            if ext=='.csv':
                # Extended list of possible delimiters
                possible_delimiters = ['\t','~', ';', '|', '$', ',', ':', ' ', '^', '&', '%', '#', '@', '!', '*', '?', '/']
                
                for delim in possible_delimiters:
                    try:
                        excel_data = pd.read_csv(file_name, delimiter=delim,dtype=str)
                        logging.info(f"Successfully read CSV file using delimiter: {delim}")
                        break
                    except pd.errors.ParserError as e:
                        logging.warning(f"Delimiter {delim} failed: {e}")
                else:
                    logging.info(f"CSV file could not be read with any known delimiters")
                
            elif ext=='.xlsx':
                excel_data = pd.read_excel(file_name,engine='openpyxl',dtype=str)
            else:
                message = "Not Supported Format"
                data = {'message': message}
                return_data = {'flag': True, 'data': data}
                return jsonify(return_data)
            
            excel_data.columns = excel_data.columns.str.lower()
            if table_name == 'party_master':
                # Convert all column names to lowercase
                excel_data.columns = excel_data.columns.str.lower()
                # Rename columns
                excel_data.rename(
                    columns={
                        'partyid': 'party_id',
                        'rmcode': 'relation_mgr_emp_code',
                        'rmname': 'relation_mgr',
                        'partyname': 'party_name'
                    },
                    inplace=True
                )
                # Drop cid column if exists
                if 'cid' in excel_data.columns:
                    excel_data.drop(columns=['cid'], inplace=True)
                #print(f"!!!!!!!!11 excel_data is:\n{excel_data}")

            if table_name == 'rm_master':
                # Convert all column names to lowercase
                excel_data.columns = excel_data.columns.str.lower()
                # Rename columns
                excel_data.rename(
                    columns={
                        'emp code': 'emp_code',
                        'emp name': 'emp_name',
                        'branch code': 'branch_code',
                        'branch location': 'branch_location',
                        'hrms region': 'hrms_region',
                        'rm state': 'rm_state',
                        'rm city': 'rm_city',
                        'rm region': 'rm_region',
                        'wbo region': 'wbo_region',
                        'hub code': 'hub_code'
                    },
                    inplace=True
                )
                # Drop cid column if exists
                if 'cid' in excel_data.columns:
                     excel_data.drop(columns=['cid'], inplace=True)
                #print(f"!!!!!!!!11 excel_data is:\n{excel_data}")
            #print(f"!!!!!!!!11 excel_data is {excel_data}")

            if 'id' in excel_data.columns:
                excel_data = excel_data.drop(columns=['id'])
            if 'last_updated' in excel_data.columns:
                excel_data = excel_data.drop(columns=['last_updated'])
            if 'last_updated_by' in excel_data.columns:
                excel_data = excel_data.drop(columns=['last_updated_by'])
            try:
                excel_data.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
            except:
                logging.info(f"#######NONE, NAN and null values are not removing while uploading")

            columns = excel_data.columns
            columns=columns.to_list()
            column = []
            for i in columns:    
                i = i+" VARCHAR2(255)"
                column.append(i)
            query = 'SELECT table_name FROM user_tables'
            df = extraction_db.execute_(query)
            table_names_list=df['table_name'].to_list()
            table = table_name.upper()
            data = excel_data
            data = data.astype(str)
            #print(f'Data is: {data}')
            table_name=table_name.lower()

            if table_name == 'age_margin_working_uat':
                try:
                    data['COMPOSITE_KEY'] = data['PARTY_ID'].astype(str) + data['COMPONENT_NAME'].astype(str) + data['AGE'].astype(str)
                except:
                    data['COMPOSITE_KEY'] = data['party_id'].astype(str) + data['component_name'].astype(str) + data['age'].astype(str)
                data = data.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                data.reset_index(drop=True, inplace=True)
                data = data.drop(columns=['COMPOSITE_KEY'])
                logging.info(f"Original 'age' column dtype before numeric conversion: {data['age'].dtype}")
                logging.info(f"Sample 'age' values before numeric conversion: {data['age'].head().tolist()}")

                # Convert 'age' column to numeric, coercing any errors (like 'nan' strings) to actual NaN values
                data['age'] = pd.to_numeric(data['age'], errors='coerce')

                # Fill any NaN values with 0 and then convert the column to integer type.
                # Adjust '0' if a different default integer is needed for missing 'age' values.
                data['age'] = data['age'].fillna(0).astype(int)

                logging.info(f"Final 'age' column dtype after numeric conversion: {data['age'].dtype}")
                logging.info(f"Sample 'age' values after numeric conversion: {data['age'].head().tolist()}")

            try:
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                logging.info(f"####currentTS now is {currentTS}")
                data['LAST_UPDATED'] = currentTS
            except:
                pass

            ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
            os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
            str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
            engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
            connection = engine.raw_connection()
            cursor = connection.cursor()
            if table_name == 'party_master':
                # Get existing DB records
                qry = """
                    SELECT party_id, party_name, relation_mgr_emp_code, relation_mgr
                    FROM party_master
                """
                df_db = extraction_db.execute_(qry)

                # Keep needed columns
                required_cols = ['party_id', 'party_name', 'relation_mgr_emp_code', 'relation_mgr']
                data = data[required_cols].copy()

                # Ensure string type
                df_db = df_db.astype(str)
                data = data.astype(str)

                # Merge based on party_id
                merged = data.merge(
                    df_db,
                    on='party_id',
                    how='left',
                    suffixes=('_new', '_old')
                )

                # ============================
                # FIND UPDATED ROWS
                # ============================
                update_mask = (
                    (merged['party_name_new'] != merged['party_name_old']) |
                    (merged['relation_mgr_emp_code_new'] != merged['relation_mgr_emp_code_old']) |
                    (merged['relation_mgr_new'] != merged['relation_mgr_old'])
                ) & merged['party_name_old'].notna()

                update_records = merged[update_mask][[
                    'party_id',
                    'party_name_new',
                    'relation_mgr_emp_code_new',
                    'relation_mgr_new'
                ]]

                update_records.columns = required_cols

                logging.info("### UPDATE RECORDS ###")
                logging.info(update_records)

                # ============================
                # FIND INSERT ROWS
                # ============================
                insert_mask = merged['party_name_old'].isna()

                insert_records = merged[insert_mask][[
                    'party_id',
                    'party_name_new',
                    'relation_mgr_emp_code_new',
                    'relation_mgr_new'
                ]]

                insert_records.columns = required_cols

                logging.info("### INSERT RECORDS ###")
                logging.info(insert_records)
                from datetime import datetime
                sys_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # ============================
                # PERFORM UPDATE
                # ============================
                for _, row in update_records.iterrows():
                    upd_qry = f"""
                        UPDATE party_master
                        SET 
                            party_name = '{row['party_name']}',
                            relation_mgr_emp_code = '{row['relation_mgr_emp_code']}',
                            relation_mgr = '{row['relation_mgr']}',
                            last_updated = TO_DATE('{sys_date}', 'YYYY-MM-DD HH24:MI:SS')
                        WHERE CAST(party_id as VARCHAR2(4000)) = '{row['party_id']}'
                    """
                    extraction_db.execute_(upd_qry)

                # ============================
                # PERFORM INSERT
                # ============================
                for _, row in insert_records.iterrows():
                    ins_qry = f"""
                        INSERT INTO party_master 
                        (party_id, party_name, relation_mgr_emp_code, relation_mgr, last_updated)
                        VALUES (
                            '{row['party_id']}',
                            '{row['party_name']}',
                            '{row['relation_mgr_emp_code']}',
                            '{row['relation_mgr']}',
                            TO_DATE('{sys_date}', 'YYYY-MM-DD HH24:MI:SS')
                        )
                    """
                    extraction_db.execute_(ins_qry)

                logging.info("### PARTY MASTER SYNC COMPLETED ###")
            if table_name == 'rm_master':
                # ----------------------------------------
                # Load DB records
                # ----------------------------------------
                qry = """
                    SELECT emp_code, emp_name, branch_code, branch_location,
                        hrms_region, rm_state, rm_city, rm_region,
                        wbo_region, hub_code
                    FROM rm_master
                """
                df_db = extraction_db.execute_(qry)

                required_cols = [
                    'emp_code','emp_name','branch_code','branch_location',
                    'hrms_region','rm_state','rm_city','rm_region',
                    'wbo_region','hub_code'
                ]
                from datetime import datetime
                sys_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                # Keep only required columns from Excel
                data = data[required_cols].copy()

                # Convert both to strings
                df_db = df_db.astype(str)
                data = data.astype(str)

                # ----------------------------------------
                # Merge Excel vs DB using emp_code
                # ----------------------------------------
                merged = data.merge(
                    df_db,
                    on='emp_code',
                    how='left',
                    suffixes=('_new', '_old')
                )

                # ----------------------------------------
                # Identify UPDATED records
                # ----------------------------------------
                update_mask = (
                    (merged['emp_name_new'] != merged['emp_name_old']) |
                    (merged['branch_code_new'] != merged['branch_code_old']) |
                    (merged['branch_location_new'] != merged['branch_location_old']) |
                    (merged['hrms_region_new'] != merged['hrms_region_old']) |
                    (merged['rm_state_new'] != merged['rm_state_old']) |
                    (merged['rm_city_new'] != merged['rm_city_old']) |
                    (merged['rm_region_new'] != merged['rm_region_old']) |
                    (merged['wbo_region_new'] != merged['wbo_region_old']) |
                    (merged['hub_code_new'] != merged['hub_code_old'])
                ) & merged['emp_name_old'].notna()

                update_records = merged[update_mask][[
                    'emp_code',
                    'emp_name_new',
                    'branch_code_new',
                    'branch_location_new',
                    'hrms_region_new',
                    'rm_state_new',
                    'rm_city_new',
                    'rm_region_new',
                    'wbo_region_new',
                    'hub_code_new'
                ]]

                update_records.columns = required_cols

                logging.info("### UPDATE RECORDS ###")
                logging.info(update_records)

                # ----------------------------------------
                # Identify NEW records (INSERT)
                # ----------------------------------------
                insert_mask = merged['emp_name_old'].isna()

                insert_records = merged[insert_mask][[
                    'emp_code',
                    'emp_name_new',
                    'branch_code_new',
                    'branch_location_new',
                    'hrms_region_new',
                    'rm_state_new',
                    'rm_city_new',
                    'rm_region_new',
                    'wbo_region_new',
                    'hub_code_new'
                ]]

                insert_records.columns = required_cols

                logging.info("### INSERT RECORDS ###")
                logging.info(insert_records)

                # ----------------------------------------
                # Get Python SYSDATE value
                # ----------------------------------------
                from datetime import datetime
                sys_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # ----------------------------------------
                # Perform UPDATE
                # ----------------------------------------
                for _, row in update_records.iterrows():
                    upd_qry = f"""
                        UPDATE rm_master
                        SET 
                            emp_name = '{row['emp_name']}',
                            branch_code = '{row['branch_code']}',
                            branch_location = '{row['branch_location']}',
                            hrms_region = '{row['hrms_region']}',
                            rm_state = '{row['rm_state']}',
                            rm_city = '{row['rm_city']}',
                            rm_region = '{row['rm_region']}',
                            wbo_region = '{row['wbo_region']}',
                            hub_code = '{row['hub_code']}',
                            last_updated = TO_DATE('{sys_date}', 'YYYY-MM-DD HH24:MI:SS')
                        WHERE emp_code = '{row['emp_code']}'
                    """
                    extraction_db.execute_(upd_qry)

                # ----------------------------------------
                # Perform INSERT
                # ----------------------------------------
                for _, row in insert_records.iterrows():
                    ins_qry = f"""
                        INSERT INTO rm_master 
                        (emp_code, emp_name, branch_code, branch_location,
                        hrms_region, rm_state, rm_city, rm_region,
                        wbo_region, hub_code, last_updated)
                        VALUES (
                            '{row['emp_code']}',
                            '{row['emp_name']}',
                            '{row['branch_code']}',
                            '{row['branch_location']}',
                            '{row['hrms_region']}',
                            '{row['rm_state']}',
                            '{row['rm_city']}',
                            '{row['rm_region']}',
                            '{row['wbo_region']}',
                            '{row['hub_code']}',
                            TO_DATE('{sys_date}', 'YYYY-MM-DD HH24:MI:SS')
                        )
                    """
                    extraction_db.execute_(ins_qry)

                logging.info("### RM MASTER SYNC COMPLETED ###")


            # try:
            #     trunc_query = f'TRUNCATE TABLE {table_name}'
            #     extraction_db.execute_(trunc_query)
            #     logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
            # except Exception as e:
            #     logging.info(f"## Exception occured while truncation data ..{e}")
            #     pass
            # data_tuples = [tuple(row) for row in data.to_numpy()]
            # columns = ','.join(data.columns)
            # logging.info(f"### columns string: {columns}")
            # placeholders = ','.join([':' + str(i+1) for i in range(len(data.columns))])
            # logging.info(f"### Placeholders string: {placeholders}")
            # sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            # logging.info(f"### Query is {sql} for {table_name}")
            # cursor.executemany(sql, data_tuples)
            # connection.commit()
            # cursor.close()
            # connection.close()
            # create_index(tenant_ids= [tenant_id], sources= [table_name])
            logging.info('Created Index')
            from datetime import datetime
            party_query = """
                SELECT last_updated 
                FROM party_master 
                ORDER BY last_updated DESC 
                FETCH FIRST 1 ROW ONLY
            """
            party_result = extraction_db.execute_(party_query)
            #print("entered paaprty")

            # Step 2: Fetch last_updated from rm_master
            rm_query = """
                SELECT last_updated 
                FROM rm_master 
                ORDER BY last_updated DESC 
                FETCH FIRST 1 ROW ONLY
            """
            rm_result = extraction_db.execute_(rm_query)
            #print("entered rm_master")

            # Step 3: Initialize flags
            party_flag = False
            rm_flag = False

            # Step 4: Compare with today's date
            current_ist_today = datetime.now(pytz.timezone(tmzone)).date()
            #print(f"current ist is:{current_ist_today}")
            #current_ist_for_hub = current_ist_today.strftime('%Y-%m-%d')
            #print(f"current ist for hub is:{current_ist_for_hub}")

            # Check party_master
            if not party_result.empty:
                party_date = pd.to_datetime(party_result.iloc[0]['last_updated']).date()
                party_flag = (party_date == current_ist_today)

            # Check rm_master
            if not rm_result.empty:
                rm_date = pd.to_datetime(rm_result.iloc[0]['last_updated']).date()
                #print(f"RM date is :{rm_date}")
                rm_flag = (rm_date == current_ist_today)
                #print(f"RM_FLAG is :{rm_flag}")
            #print("compared both timings")
            def execute_update(engine, query):
                try:
                    with engine.begin() as conn:  # auto-commit on exit
                        result = conn.execute(text(query))
                        #print(f"result is ...............................:{result}")
                        return result.rowcount
                except Exception as e:
                    logging.exception(f"Error executing update: {e}")
                    return 0

            
            if rm_flag or party_flag:
                #print("enbtered if")
                try:
                    #print("entered try")


                    # Step 1: Fetch party_id → relation_mgr_emp_code from party_master
                    party_master_query = """
                        SELECT DISTINCT CAST(PARTY_ID AS VARCHAR2(4000)) as party_id, CAST(RELATION_MGR_EMP_CODE AS VARCHAR2(4000)) as relation_mgr_emp_code
                        FROM party_master
                    """
                    party_df = extraction_db.execute_(party_master_query)

                    #print(f"Before step 2") 
                    # Step 2: Get unique relation_mgr_emp_code values
                    unique_emp_codes = party_df['relation_mgr_emp_code'].dropna().unique().tolist()
                    #print(f"The unique emp codes are:{unique_emp_codes}")
                    if not unique_emp_codes:
                        logging.warning("No valid relation_mgr_emp_code found in party_master.")
                        return

                    emp_code_str = "', '".join(unique_emp_codes)
                    #print(f"Before step 3")

                    batch_size = 200  # below Oracle limit
                    rm_dfs = []

                    for start in range(0, len(unique_emp_codes), batch_size):
                        subset = unique_emp_codes[start:start+batch_size]
                        emp_code_str = "', '".join(subset)
                        rm_master_query = f"""
                            SELECT emp_code, hub_code, branch_code, branch_location
                            FROM rm_master
                            WHERE emp_code IN ('{emp_code_str}')
                        """
                        rm_dfs.append(extraction_db.execute_(rm_master_query))

                    rm_df = pd.concat(rm_dfs, ignore_index=True) if rm_dfs else pd.DataFrame()


                    #print("brofre if rm_df is empty")

                    if rm_df.empty:
                        logging.warning(" No matching emp_code found in rm_master.")
                        return

                    # Step 4: Merge both to get: party_id → hub_code
                    merged_df = pd.merge(party_df, rm_df, left_on='relation_mgr_emp_code', right_on='emp_code', how='inner')
                    party_hub_df = merged_df[['party_id', 'hub_code','branch_code','branch_location']].dropna()
                    #print(f"LENGTH IS >>>>>>>>>>>>>>>>:{len(party_hub_df)}")
                    party_ids = party_hub_df['party_id'].unique().tolist()
                    ocr_map = {}
                    batch_size = 500

                    for i in range(0, len(party_ids), batch_size):
                        batch = party_ids[i:i+batch_size]
                        ids_str = "','".join(batch)

                        ocr_query=f"""SELECT party_id, hub_code
                                        FROM (
                                        SELECT o.party_id, pq.hub_code,
                                        ROW_NUMBER() OVER (PARTITION BY o.party_id ORDER BY pq.case_id DESC) AS rn
                                        FROM hdfc_extraction.ocr o
                                        JOIN hdfc_queues.process_queue pq 
                                        ON pq.case_id = o.case_id
                                        WHERE o.party_id IN ('{ids_str}')
                                    )
                                    WHERE rn = 1"""



                        ocr_df = extraction_db.execute_(ocr_query)
                        #print(f"The length of OCR DFFF is:{len(ocr_df)}")

                        if not ocr_df.empty:
                            temp_dict = ocr_df.set_index('party_id')[['hub_code']].to_dict('index')
                            ocr_map.update(temp_dict)

                    logging.info(f" OCR fetch complete. Records fetched: {len(ocr_map)}")

                    #  Filter rows that need update
                    def need_update(row):
                        pid = row['party_id']
                        if pid not in ocr_map:
                            return True

                        old = ocr_map[pid]
                        return old.get('hub_code') != row['hub_code']

                    before = len(party_hub_df)
                    party_hub_df = party_hub_df[party_hub_df.apply(need_update, axis=1)].reset_index(drop=True)
                    after = len(party_hub_df)

                    logging.info(f" Update filter: {before} → {after} rows will be updated")

                    
                    
                    if not party_hub_df.empty:
                        batch_size = 200  # keep queries small enough

                        for start in range(0, len(party_hub_df), batch_size):
                            chunk = party_hub_df.iloc[start:start+batch_size]
                            values_clause = "\nUNION ALL\n".join(
                                f"SELECT '{pid}' AS party_id, '{hub}' AS hub_code FROM DUAL"
                                for pid, hub in zip(chunk['party_id'], chunk['hub_code'])
                            )

                            update_sql = f"""
                                UPDATE hdfc_queues.process_queue pq
                                SET pq.hub_code = (
                                    SELECT tmp.hub_code
                                    FROM (
                                        {values_clause}
                                    ) tmp
                                    JOIN OCR o ON o.party_id = tmp.party_id
                                    WHERE o.case_id = pq.case_id
                                    FETCH FIRST 1 ROWS ONLY
                                )
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM (
                                        {values_clause}
                                    ) tmp
                                    JOIN OCR o ON o.party_id = tmp.party_id
                                    WHERE o.case_id = pq.case_id
                                )   
                            """
                            #print(f"Updating region batch {start}-{start+len(chunk)}")
                            rows_affected =execute_update(engine,update_sql)
                            #print(f"Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")

                        #print(f"update sql is..........................:{update_sql}")
                        #rows_affected = execute_update(engine, update_sql)
                        #print(f"Step 1 | Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")
                    else:
                        logging.info(f"No Hub code is Updated")
                        pass

                except Exception as e:
                    logging.error(f"Error while Updating the Hub code:{e}")
                    pass
            logging.info(f"hub code successfully updated")
            party_query = """
                SELECT last_updated 
                FROM party_master 
                ORDER BY last_updated DESC 
                FETCH FIRST 1 ROW ONLY
            """
            party_result = extraction_db.execute_(party_query)
            #print("entered paaprty")

            # Step 2: Fetch last_updated from rm_master
            rm_query = """
                SELECT last_updated 
                FROM rm_master 
                ORDER BY last_updated DESC 
                FETCH FIRST 1 ROW ONLY
            """
            rm_result = extraction_db.execute_(rm_query)
            #print("entered rm_master")

            # Step 3: Initialize flags
            party_flag = False
            rm_flag = False

            # Step 4: Compare with today's date
            current_ist_today = datetime.now(pytz.timezone(tmzone)).date()
            #print(f"current ist is:{current_ist_today}")
         

            # Check party_master
            if not party_result.empty:
                party_date = pd.to_datetime(party_result.iloc[0]['last_updated']).date()
                party_flag = (party_date == current_ist_today)

            # Check rm_master
            if not rm_result.empty:
                rm_date = pd.to_datetime(rm_result.iloc[0]['last_updated']).date()
                #print(f"RM date is :{rm_date}")
                rm_flag = (rm_date == current_ist_today)
                #print(f"RM_FLAG is :{rm_flag}")
            #print("compared both timings")




            #print(f"rm_flag is :{rm_flag}")
            if rm_flag or party_flag:

                #print(f"entered for updation.......................................")
                # Step 2: Fetch party → emp_code mapping
                party_master_query = """
                    SELECT DISTINCT 
                        CAST(PARTY_ID AS VARCHAR2(4000)) as party_id,
                        CAST(RELATION_MGR_EMP_CODE AS VARCHAR2(4000)) as relation_mgr_emp_code,
                        CAST(party_name AS VARCHAR2(4000)) as customer_name
                    FROM party_master
                    WHERE relation_mgr_emp_code IS NOT NULL
                """
                party_df = extraction_db.execute_(party_master_query)

                if party_df.empty:
                    logging.info(" No party_id found with relation_mgr_emp_code. Exit.")
                    return

                # Fetch RM Name & State using emp_code
                unique_emp_codes = party_df['relation_mgr_emp_code'].dropna().unique().tolist()
                batch_size = 500
                rm_dfs = []

                for start in range(0, len(unique_emp_codes), batch_size):
                    subset = unique_emp_codes[start:start+batch_size]
                    emp_code_str = "', '".join(subset)

                    rm_master_query = f"""
                        SELECT emp_code, EMP_NAME, rm_state
                        FROM rm_master
                        WHERE emp_code IN ('{emp_code_str}')
                    """
                    rm_dfs.append(extraction_db.execute_(rm_master_query))

                rm_df = pd.concat(rm_dfs, ignore_index=True) if rm_dfs else pd.DataFrame()

                if rm_df.empty:
                    logging.info(" No matching emp_code found in rm_master. Exit.")
                    return

                # Merge to map party_id → RM Info
                merged_df = pd.merge(
                    party_df, rm_df,
                    left_on='relation_mgr_emp_code',
                    right_on='emp_code',
                    how='inner'
                )

                party_rm_df = merged_df[['party_id', 'relation_mgr_emp_code','customer_name', 'emp_name', 'rm_state']].dropna().drop_duplicates(subset=['party_id'])
                
                party_ids = party_rm_df['party_id'].unique().tolist()
                ocr_map = {}
                batch_size = 500

                for i in range(0, len(party_ids), batch_size):
                    batch = party_ids[i:i+batch_size]
                    #ids_str = "','".join(batch)
                    #ids_str = ",".join([f"'{i}'" for i in ids])
                    ids_str = ",".join([f"'{pid}'" for pid in batch])


                    #ocr_query = f"""
                    #    SELECT party_id, relation_mgrname, rm_state,customer_name
                    #    FROM ocr
                    #    WHERE party_id IN ('{ids_str}')
                    #"""
                    ocr_query = f"""
                            SELECT party_id, relation_mgrname, rm_state, customer_name
                            FROM (
                                SELECT o.party_id,
                                o.relation_mgrname,
                                o.rm_state,
                                o.customer_name,
                                ROW_NUMBER() OVER (PARTITION BY o.party_id ORDER BY ql.case_id DESC) AS rn
                                FROM hdfc_extraction.ocr o
                                JOIN hdfc_queues.queue_list ql
                                ON ql.case_id = o.case_id
                                WHERE o.party_id IN ({ids_str})
                                )
                                WHERE rn = 1 """

                    ocr_df = extraction_db.execute_(ocr_query)

                    if not ocr_df.empty:
                        temp_dict = ocr_df.set_index('party_id')[['relation_mgrname','rm_state','customer_name']].to_dict('index')
                        ocr_map.update(temp_dict)

                logging.info(f" OCR fetch complete. Records fetched: {len(ocr_map)}")

                #  Filter rows that need update
                def need_update(row):
                    pid = row['party_id']
                    if pid not in ocr_map:
                        return True

                    old = ocr_map[pid]
                    return old.get('relation_mgrname') != row['emp_name'] or old.get('rm_state') != row['rm_state'] or old.get('customer_name') != row['customer_name']                
                before = len(party_rm_df)
                party_rm_df = party_rm_df[party_rm_df.apply(need_update, axis=1)].reset_index(drop=True)
                after = len(party_rm_df)

                logging.info(f" Update filter: {before} → {after} rows will be updated")

                if not party_rm_df.empty:

                    #print(" Party to RM mapping prepared.")
                    batch_size=200
                    ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                    os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                    str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                    engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                    connection = engine.raw_connection()
                    cursor = connection.cursor()
                    
                    # Step 4a: Update process_queue relation_mgr_name
                    for start in range(0, len(party_rm_df), batch_size):
                        engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                        chunk = party_rm_df.iloc[start:start+batch_size]

                        values_clause = "\nUNION ALL\n".join(
                            f"SELECT '{relation_mgr_emp_code}' AS emp_code, '{relation_mgr_name}' AS emp_name, '{party_id}' AS party_id FROM DUAL"
                            for party_id, relation_mgr_emp_code, relation_mgr_name in zip(
                                chunk['party_id'], chunk['relation_mgr_emp_code'], chunk['emp_name']

                            )
                        )

                        update_sql = f"""
                            UPDATE OCR o
                            SET o.relation_mgrname = (
                                SELECT tmp.emp_name
                                FROM (
                                    {values_clause}
                                ) tmp
                                WHERE tmp.party_id = o.party_id
                                FETCH FIRST 1 ROWS ONLY
                            )
                            WHERE EXISTS (
                                SELECT 1
                                FROM (
                                    {values_clause}
                                )tmp
                                WHERE tmp.party_id = o.party_id
                            )
                            """
                        #print(f"Update sql is................ :{update_sql}")
                        rows_affected = execute_update(engine, update_sql)
                        #print(f"Step 1 | Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")

                    #print(f"after rm name")
                    connection = engine.raw_connection()
                    # Step 4b: Update process_queue rm_state
                    for start in range(0, len(party_rm_df), batch_size):
                        chunk = party_rm_df.iloc[start:start+batch_size]

                        values_clause = "\nUNION ALL\n".join(
                            f"SELECT '{relation_mgr_emp_code}' AS emp_code, '{rm_state}' AS rm_state, '{party_id}' AS party_id FROM DUAL"
                            for party_id, relation_mgr_emp_code, rm_state in zip(
                                chunk['party_id'], chunk['relation_mgr_emp_code'], chunk['rm_state']
                            )
                        )

                        update_sql = f"""
                            UPDATE OCR o
                            SET o.rm_state = (
                                SELECT tmp.rm_state
                                FROM (
                                    {values_clause}
                                ) tmp
                                WHERE tmp.party_id = o.party_id
                                FETCH FIRST 1 ROWS ONLY
                            )
                            WHERE EXISTS (
                                SELECT 1
                                FROM (
                                    {values_clause}
                                )tmp
                                WHERE tmp.party_id = o.party_id
                            )
                        """

                        #print(f"Update sql is................ :{update_sql}")
                        rows_affected = execute_update(engine, update_sql)
                        #print(f"Step 1 | Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")
                    #print(" Step 2b Complete: Updated pq.rm_state")
                    connection.commit()
                    #cursor.close()
                    connection.close()
                    connection = engine.raw_connection()

                    for start in range(0, len(party_rm_df), batch_size):
                        chunk = party_rm_df.iloc[start:start+batch_size]

                        values_clause = "\nUNION ALL\n".join(
                            f"SELECT '{customer_name}' AS customer_name,  '{party_id}' AS party_id FROM DUAL"
                            for party_id, customer_name in zip(
                                chunk['party_id'], chunk['customer_name']
                            )
                        )

                        update_sql = f"""
                            UPDATE OCR o
                            SET o.customer_name = (
                                SELECT tmp.customer_name
                                FROM (
                                    {values_clause}
                                ) tmp
                                WHERE tmp.party_id = o.party_id
                                FETCH FIRST 1 ROWS ONLY
                            )
                            WHERE EXISTS (
                                SELECT 1
                                FROM (
                                    {values_clause}
                                )tmp
                                WHERE tmp.party_id = o.party_id
                            )
                        """

                        #print(f"Update sql is................ :{update_sql}")
                        rows_affected = execute_update(engine, update_sql)
                        #print(f"Step 1 | Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")
                    #print(" Step 3b Complete: Updated pq.customer_name")
                    connection.commit()
                    connection.close()
                    connection = engine.raw_connection()
                    # Step 5: Update process_queue.region based on rm_state

                    #case_ids = queues_db.execute_("SELECT case_id FROM queue_list where queue!='case_creation'").iloc[:,0].tolist()

                    #for start in range(0, len(case_ids), batch_size):
                    #    batch = case_ids[start:start+batch_size]
                    #    case_ids_str = "', '".join(batch)

                    #    update_sql = f"""
                    #       UPDATE hdfc_queues.process_queue pq
                    #            SET pq.region = (
                    #                SELECT wr.wbo_region
                #                 FROM OCR o
                    #                JOIN rm_master rm
                    #                ON o.rm_state = CAST(rm.rm_state as VARCHAR2(4000))
                    #                JOIN wbo_region wr
                    #                ON CAST(rm.rm_state as VARCHAR2(4000)) = wr.rm_state
                    #                WHERE o.case_id = pq.case_id  
                    #                FETCH FIRST 1 ROWS ONLY
                    #            )
                    #            WHERE pq.case_id IN ('{case_ids_str}')
                    #    """
                    unique_rm_state = party_rm_df['rm_state'].dropna().unique().tolist()
                    batch_size = 200
                    region_dfs = []

                    for start in range(0, len(unique_rm_state), batch_size):
                        subset = unique_rm_state[start:start+batch_size]
                        rm_state_str = "', '".join(subset)

                        region_query = f"""
                            SELECT rm_state, wbo_region
                            FROM wbo_region
                            WHERE rm_state IN ('{rm_state_str}')
                        """
                        region_dfs.append(extraction_db.execute_(region_query))

                    region_df = pd.concat(region_dfs, ignore_index=True) if region_dfs else pd.DataFrame()

                    if region_df.empty:
                        logging.info(" No matching rm_state found in wbo_region. Exit.")
                        return

                    # Merge party_rm_df with region_df to get party_id → region
                    party_region_df = pd.merge(
                        party_rm_df, region_df,
                        left_on='rm_state',
                        right_on='rm_state',
                        how='inner'
                    )

                    party_region_df = party_region_df[['party_id', 'wbo_region']].dropna().drop_duplicates(subset=['party_id'])

                    # -----------------------------
                    # Step 6: Update process_queue.region in batches
                    # -----------------------------
                    batch_size = 100
                    for start in range(0, len(party_region_df), batch_size):
                        chunk = party_region_df.iloc[start:start+batch_size]

                        values_clause = "\nUNION ALL\n".join(
                            f"SELECT '{party_id}' AS party_id, '{wbo_region}' AS region FROM DUAL"
                            for party_id, wbo_region in zip(chunk['party_id'], chunk['wbo_region'])
                        )

                        #update_sql = f"""
                        #    UPDATE hdfc_queues.process_queue pq
                        #    SET pq.region = (
                        #        SELECT tmp.region
                        #        FROM (
                        #            {values_clause}
                        #        ) tmp
                        #        JOIN OCR o ON o.party_id = tmp.party_id
                        #        WHERE o.case_id = pq.case_id
                    #         FETCH FIRST 1 ROWS ONLY
                        #    )
                        #    WHERE pq.case_id IN (
                        #        SELECT o.case_id
                        #        FROM OCR o
                        #        JOIN (
                        #        {values_clause}
                        #        ) tmp ON o.party_id = tmp.party_id
                        #    )
                    # """
                        update_sql = f"""
                            UPDATE hdfc_queues.process_queue pq
                                SET pq.region = (
                                    SELECT tmp.region
                                    FROM (
                                        {values_clause}
                                    ) tmp
                                    JOIN OCR o ON o.party_id = tmp.party_id
                                    WHERE o.case_id = pq.case_id
                                    FETCH FIRST 1 ROWS ONLY
                                )
                                WHERE EXISTS (
                                    SELECT 1
                                    FROM (
                                        {values_clause}
                                    ) tmp
                                    JOIN OCR o ON o.party_id = tmp.party_id
                                    WHERE o.case_id = pq.case_id
                                )
                            """
                        #print(f"Updating region batch {start}-{start+len(chunk)}")
                        rows_affected =execute_update(engine,update_sql)
                        #print(f"Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")




                        #print(f"update sql is :{update_sql}")
                        #queues_db.execute_(update_sql)
                        rows_affected = execute_update(engine, update_sql)
                        #print(f"Step 1 | Batch {start}-{start+len(chunk)} committed. Rows affected: {rows_affected}")
                
                else:
                    logging.info(f"Nothing is Updated ")
                    pass
                #print(" Step 4 Complete: Updated pq.region for relevant case_ids")
                connection.commit()
                connection.close()
                party_flag = False
                rm_flag = False


            else:
                logging.warning(" Skipped all data : both party_master and rm_master must be uploaded.")





            message = "Successfully Updated"
            data = {'message': message}
            return_data = {'flag': True, 'data': data}
        except Exception as e:
            traceback.print_exc()
            message = f"Could not update {table_name}. Please check the template/data."
            data = {'message': message}
            return_data = {'flag': True, 'data': data}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return jsonify(return_data)



def escape_sql(value: str) -> str:
    return str(value).replace("'", "''")



@app.route('/upload_master_blob', methods = ['GET', 'POST'])
def upload_master_blob():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json

    tenant_id = data.pop('tenant_id', None)
    duplicate_check = data.pop('duplicate_check', False)

    last_updated_by = data.get('user', None)
    insert_flag = data.pop('insert_flag', 'append')
    file_name=data.get("file_name",None)

    if 'age_margin_master' in file_name.lower():
        file_name = file_name.replace('Age_Margin_master', 'age_margin_working_uat')


    file_without_extension,extension=os.path.splitext(file_name)

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
                service_name='master_upload',
                zipkin_attrs=attr,
                span_name='upload_master_blob',
                transport_handler=http_transport,
                sample_rate=0.5):

        try:
            master_table_name = data.pop('master_table_name')
        except:
            traceback.print_exc()
            message = "Master table name not provided"
            return jsonify({"flag": False, "message" : message})

        try:
            database, table_name = master_table_name.split('.')
        except:
            traceback.print_exc()
            message = "Master table name is in incorrect format. Check configuration."
            return jsonify({"flag": False, "message": message})

        try:
            blob_data = data.pop('blob')
        except:
            traceback.print_exc()
            message = "Blob data not provided"
            return jsonify({"flag": False, "message" : message})

        db_config['tenant_id'] = tenant_id
        table_db = DB(database, **db_config)
        extraction_db = DB('extraction', **db_config)
        duplicate_blob = None

        logging.debug('trying to convert blob to dataframe')
        try:
            blob_data = blob_data.split(",", 1)
            # extension = blob_data[0].split("/", 1)[1].split(";")[0].strip().lower()
            file_blob_data=blob_data[1]
            # Padding
            file_blob_data += '='*(-len(file_blob_data)%4)
            file_stream = BytesIO(base64.b64decode(file_blob_data))
            # filename = blob_data[0].split(";")[0].split("/")[-1].strip()
            

            # Allow only .csv's and xlsx's and xls to be upload from the Frontend
            if extension not in ('.csv','.xlsx'):
                message = "Uploaded File Format is not Supported , Upload CSV and XLSX"
                return jsonify({"flag": False, "message": message})

            # File Name Validation
            if table_name.lower() not in file_without_extension.lower():
                message = f"Change file name as {table_name} and Upload"
                return jsonify({"flag": False, "message": message})
            
            
            ## Handling both csv and excel files from front end upload
            if extension=='.csv':
                data_frame = pd.read_csv(file_stream)  
            else:
                data_frame = pd.read_excel(file_stream,engine='openpyxl')
        


            try:
                if table_name.lower() == 'age_margin_working_uat':

                    #Extract data from DB
                    query = 'SELECT * FROM age_margin_working_uat'
                    data___ = extraction_db.execute_(query).to_dict(orient='records')

                    df = pd.DataFrame(data___)

                    #Drop unnecessary or audit columns (case-insensitive)
                    columns_to_drop = ['id', 'last_updated', 'last_updated_by', 'last_modified', 'last_modified_by']
                    df = df.drop(columns=[col for col in df.columns if col.lower() in columns_to_drop], errors='ignore')
                    data_frame = data_frame.drop(columns=[col for col in data_frame.columns if col.lower() in columns_to_drop], errors='ignore')
                    
                    #Get common columns and align
                    common_columns = list(set(df.columns).intersection(set(data_frame.columns)))
                    if not common_columns:
                        logging.error("No common columns found for comparison.")


                    df_common = df[common_columns].copy()
                    data_frame_common = data_frame[common_columns].copy()
                    

                    #Normalize for safe comparison (stringify, lowercase, strip)
                    for col in common_columns:
                        df_common[col] = df_common[col].astype(str).str.strip().str.lower()
                        data_frame_common[col] = data_frame_common[col].astype(str).str.strip().str.lower()

                    #Remove duplicates, sort, and reset index
                    df_common = df_common.drop_duplicates().sort_values(by=common_columns).reset_index(drop=True)
                    data_frame_common = data_frame_common.drop_duplicates().sort_values(by=common_columns).reset_index(drop=True)

                    #Compare DB → Incoming: rows in DB but not in source
                    db_not_in_source = df_common.merge(data_frame_common, how='left', indicator=True)
                    db_only = db_not_in_source[db_not_in_source['_merge'] == 'left_only'].drop(columns=['_merge'])

                    #Compare Incoming → DB: rows in source but not in DB
                    source_not_in_db = data_frame_common.merge(df_common, how='left', indicator=True)
                    source_only = source_not_in_db[source_not_in_db['_merge'] == 'left_only'].drop(columns=['_merge'])

                    

                    if not db_only.empty:
                        logging.info(" Rows present in DB but missing from source:")
                        logging.info(db_only.to_string(index=False))

                    if not source_only.empty:
                        logging.info(" Rows present in source but missing from DB:")
                        logging.info(source_only.to_string(index=False))
                        

                    #Insert new records from source_only into audit_report_case
                    if not source_only.empty:
                        logging.info(" Inserting new records into audit_report_case:")
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        insert_lines=[]
                        for _, row in source_only.iterrows():
                            #new_values_dict = row.to_dict()
                            # Escape single quotes in new_values and replace with double quotes
                            #new_values_str = str(new_values_dict).replace("'", '"')
                            #insert_query = """
                            #INSERT INTO audit_report_case (category, maker_name, new_values,maker_id,maker_datetime)
                            #VALUES (:1, :2, :3,:4,:5)
                            #"""
                            #values = (
                            #    'Upload in Age Margin Master',
                            #    'Age Margin Master',
                            #    f'({new_values_str})',
                            #    last_updated_by,
                            #    currentTS
                            #)

                            #try:
                            #    extraction_db.execute_(insert_query,params=values)
                            #except Exception as e:
                            #    logging.info(f" Failed to insert records: {e}")
                            #    logging.exception(f"Failed to insert audit records: {e}")
                            try:
                                new_values_dict = row.to_dict()
                                new_values_str = str(new_values_dict).replace("'", '"')
                                escaped_values = escape_sql(f'({new_values_str})')

                                line = (
                                    f"  INTO audit_report_case (category, maker_name, new_values, maker_id, maker_datetime) "
                                    f"VALUES ('Upload in Age Margin Master', 'Age Margin Master', '{escaped_values}', "
                                    f"'{escape_sql(last_updated_by)}', '{currentTS}')"
                                )
                                insert_lines.append(line)
                            except Exception as e:
                                logging.warning(f"Skipping insert for record due to error: {e}")

                        #if insert_lines:
                        #    bulk_query = f"INSERT ALL\n{chr(10).join(insert_lines)}\nSELECT * FROM dual"
                        #    try:
                        #        extraction_db.execute_(bulk_query)
                        #        logging.info(f"Inserted {len(insert_lines)} records into audit_report_case")
                        #    except Exception as e:
                        #        logging.error(f"Failed bulk insert: {e}")

                        batch_size = 500
                        total_records = len(insert_lines)
                        num_batches = math.ceil(total_records / batch_size)

                        for i in range(num_batches):
                            batch = insert_lines[i * batch_size : (i + 1) * batch_size]
                            bulk_query = f"INSERT ALL\n{chr(10).join(batch)}\nSELECT * FROM dual"

                            try:
                                extraction_db.execute_(bulk_query)
                                logging.info(f"Inserted {len(batch)} records into audit_report_case")
                            except Exception as e:
                                logging.error(f"Failed bulk insert for batch {i+1}/{num_batches}: {e}")
                                logging.exception(e)


                        
                        logging.info(f" Completed inserting {len(source_only)} records into audit_report_case\n")
                    if db_only.empty and source_only.empty:
                        logging.info(" No differences found. DB and source data match exactly.\n")


            except Exception as e:
                pass                    


            data_frame['last_updated_by'] = last_updated_by
            data_frame.fillna(value= '', inplace=True)

            if table_name.lower() == 'component_master':
                data_frame['_original_order'] = range(len(data_frame))
            columns_ = list(data_frame.columns.values)
            # Remove original order helper column
            if '_original_order' in data_frame.columns:
                data_frame.drop(columns=['_original_order'], inplace=True)
            logging.info(f"columns names are : {columns_}")
            if '_original_order' in columns_:
                columns_.remove('_original_order')
            try:
                columns_.remove('id') #ALL COLUMNS EXCEPT ID 
            except Exception:
                pass
            if 'last_updated' in columns_:
                columns_.remove('last_updated') #ALL COLUMNS EXCEPT ID 
                
            if 'last_updated_by' in columns_:
                columns_.remove('last_updated_by')

            try:
                date_column = [col for col in data_frame.columns if 'date' in col.lower()]
                for col in date_column:
                    data_frame[col] = data_frame[col].str.replace('/', '-')
            except:
                pass
        except:
            message = "Could not convert blob to dataframe"
            logging.exception(message)
            return jsonify({"flag": False, "message" : message})

        try:
            master_df = extraction_db.master_execute_(f"SELECT * FROM `{table_name}`")
            column_names = master_df.columns.tolist()

            columns = []
            values_to_drop = ['id','last_updated','last_updated_by']

            column_names = list(filter(lambda x: x not in values_to_drop, column_names))
            
            if set (columns_ ) == set (column_names):
                logging.info("checking the comparision of both sheets, columns are same")
            else:
                logging.info("please check both sheets columns names should match")
                return {'flag': False, 'message': "Columns do not match. Download the excel and get the columns"}
        except:
            logging.info("Error while uploading sheet")
            return_data = ({"flag": False, "message" : "error uploading xl sheet"})
        logging.info(f"checking for the values of duplicate check and insert flag {duplicate_check},{insert_flag}")
        
        #remove NONE, NAN and null values
        try:
            data_frame.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
        except:
            logging.info(f"#######NONE, NAN and null values are not removing while uploading")

        if duplicate_check == True and insert_flag == 'append':
            try:
                master_df = extraction_db.execute_(f"SELECT * FROM `{table_name}`")
                

                master_df.replace(to_replace= 'None', value= '', inplace= True)
                master_df.fillna(value= '', inplace= True)

                data_frame.replace(to_replace= 'nan', value= '', inplace= True)
                data_frame.fillna(value= '', inplace= True)

                
                columns = columns_


                records_to_upload = len(data_frame)

                data_frame.drop_duplicates(subset= columns_, keep = 'first', inplace = True)

                master_df = master_df.astype(str)
                data_frame = data_frame.astype(str)
                df_all = data_frame.merge(master_df[columns], how = 'left', on= columns, indicator = True)

                unique_df = df_all[df_all['_merge'] == 'left_only'].drop(columns = ['_merge'])
                duplicates_df = df_all[df_all['_merge'] == 'both'].drop(columns = ['_merge']) #NEED THIS FOR KARVY

                records_uploaded = len(unique_df)
                
                no_of_duplicates = records_to_upload - records_uploaded

                if len(unique_df)==0:
                    
                    message = f"There were no unique values to insert"
                    return_data = {"flag": True, "message" : message}
                    return jsonify(return_data)
                if 'last_updated' in unique_df.columns and 'id' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['id','last_updated'])
                elif 'id' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['id'])
                elif 'last_updated' in unique_df.columns:
                    df_to_insert = unique_df.drop(columns = ['last_updated'])

                df_to_insert.drop(columns=['last_updated_by'], inplace=True)
                if table_name == 'age_margin_working_uat':
                    logging.info(f"### table is age_margin_working_uat")
                    current_ist = datetime.now(pytz.timezone(tmzone))
                    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                    logging.info(f"####currentTS now is {currentTS}")
                    df_to_insert['LAST_UPDATED'] = currentTS
                    cols = [col.lower() for col in res.columns]
                    if 'is_active' in cols:
                        actual_col = res.columns[cols.index('is_active')]
                        res[actual_col] = '1'
                    if 'inactive_reason' in cols:
                        actual_col = res.columns[cols.index('inactive_reason')]
                        res[actual_col] = ''

                    try:
                        def remove_decimals(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        df_to_insert['age'] = df_to_insert['age'].apply(remove_decimals)
                    except:
                        logging.info(f"##error in the age conversion")
                        pass
                if table_name == 'party_master':

                    master_df = master_df.drop_duplicates(subset='party_id')
                elif table_name == 'city_state' or table_name =='RM_Master':
                    master_df = master_df.drop_duplicates(subset='rm_mgr_code')


                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                data_tuples = [tuple(row) for row in df_to_insert.to_numpy()]
                columns = ','.join(df_to_insert.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(df_to_insert.columns))])
                
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                logging.info(f"###  Query is {sql} \n INSERTION  SUCCESSFUL FOR {table_name}")
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                
                duplicates_df = duplicates_df.astype(str)
                duplicates_df.replace(to_replace= 'None', value= '', inplace= True)
                duplicate_blob = dataframe_to_blob(duplicates_df)


                ##### creating elastic search index of data
                create_index(tenant_ids= [tenant_id], sources= [table_name])
                logging.info('Created Index')

                #updating last updated by
                query = f"update `master_upload_tables` set last_updated_by = 'NULL' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                last_updated = extraction_db.execute_(time_query).to_dict()

                message = f"Successfully updated {table_name} in {database}. {no_of_duplicates} duplicates ignored out of total {records_to_upload} records."
                return_data = {'flag': True, 'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message, 'blob' : duplicate_blob.decode('utf-8')}

            except Exception:
                traceback.print_exc()
                message = f"Could not append data to {table_name}"
                return_data = {"flag": False, "message" : message}

        elif duplicate_check == False and insert_flag == 'append':
            try:
                if 'last_updated' in data_frame.columns and 'id' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['id','last_updated'])
                elif 'id' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['id'])
                elif 'last_updated' in data_frame.columns:
                    data_frame = data_frame.drop(columns = ['last_updated'])
                data_frame.drop(columns=['last_updated_by'], inplace=True)
                if table_name == 'age_margin_working_uat':
                    logging.info(f"### table is age_margin_working_uat")
                    current_ist = datetime.now(pytz.timezone(tmzone))
                    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                    logging.info(f"####currentTS now is {currentTS}")
                    data_frame['LAST_UPDATED'] = currentTS
                    # HDFC requirement: set all rows active and reset inactive_reason
                    cols = [col.lower() for col in data_frame.columns]
                    if 'is_active' in cols:
                        actual_col = data_frame.columns[cols.index('is_active')]
                        data_frame[actual_col] = '1'
                    if 'inactive_reason' in cols:
                        actual_col = data_frame.columns[cols.index('inactive_reason')]
                        data_frame[actual_col] = ''


                    try:
                        def remove_decimals(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        data_frame['age'] = data_frame['age'].apply(remove_decimals)
                    except:
                        logging.info(f"##error in the age conversion")
                        pass
                # Restore original row order for component_master
                if table_name.lower() == 'component_master' and '_original_order' in data_frame.columns:
                    data_frame = data_frame.sort_values('_original_order').drop(columns=['_original_order'])
    
                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                data_tuples = [tuple(row) for row in data_frame.to_numpy()]
                columns = ','.join(data_frame.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(data_frame.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                logging.info(sql)
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                
                create_index(tenant_ids= [tenant_id], sources= [master_table_name])
                logging.info('Created Index')

                #updating last updated by
                query = f"update `master_upload_tables` set last_updated_by = 'NULL' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                inserting = extraction_db.execute_(query)
                time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                last_updated = extraction_db.execute_(time_query).to_dict()

                message = f"Successfully updated in {database}."
                return_data = {'flag': True, 'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}

            except:
                traceback.print_exc()
                message = f"Could not update {table_name}"
                return_data = {"flag": False, "message" : message}

        elif insert_flag == 'overwrite':
            try:
                if table_name == 'age_margin_working_uat':
                    query = f'SELECT * from age_margin_working_uat'
                    res = extraction_db.execute_(query)
                    res = res.fillna('')
                    res.replace('null', '', inplace=True)
                    res.replace('NULL', '', inplace=True)
                    res.replace('Null', '', inplace=True)
                    res.replace('None', '', inplace=True)
                    res.replace('NONE', '', inplace=True)
                    res.replace('none', '', inplace=True)
                    res.replace('nan', '', inplace=True)

                    res = res.drop(columns=[ 'id','last_updated', 'PARTY_ID', 'BANKING_TYPE','MARGIN_TYPE', 'DP_MARGIN_CONSIDERATION', 'STOCK_STATEMENT_TYPE','BANKING_SHARE', 'COMPONENT_NAME', 'AGE', 'MARGIN', 'ID','LAST_UPDATED'])
                    try:
                        res['age'] = res['age'].astype(float)
                        res['age'] = res['age'].astype(int)
                        res['age'] = res['age'].astype(str)
                    except:
                        pass


                    if 'id' in data_frame.columns:
                        data_frame = data_frame.drop(columns = ['id'])

                    # Do not drop ID column for component_master
                    if table_name.lower() != 'component_master':
                        if 'id' in data_frame.columns:
                            data_frame = data_frame.drop(columns=['id'])

                    data_frame = data_frame.astype(str)
                    if 'last_updated_by' in data_frame.columns:
                        data_frame.drop(columns=['last_updated_by'], inplace=True)
                    data_frame = data_frame.fillna('')
                    data_frame.replace('null', '', inplace=True)
                    data_frame.replace('NULL', '', inplace=True)
                    data_frame.replace('Null', '', inplace=True)
                    data_frame.replace('None', '', inplace=True)
                    data_frame.replace('NONE', '', inplace=True)
                    data_frame.replace('none', '', inplace=True)
                    data_frame.replace('nan', '', inplace=True)

                    try:
                        data_frame['age'] = data_frame['age'].astype(float)
                        data_frame['age'] = data_frame['age'].astype(int)
                        data_frame['age'] = data_frame['age'].astype(str)
                    except:
                        pass

                    result = pd.concat([res, data_frame], ignore_index=True)
                    data_frame = result
                    
                    logging.info(f'Result is: {data_frame}')
                    cols = [col.lower() for col in data_frame.columns]
                    if 'is_active' in cols:
                        actual_col = data_frame.columns[cols.index('is_active')]
                        data_frame[actual_col] = '1'
                    if 'inactive_reason' in cols:
                        actual_col = data_frame.columns[cols.index('inactive_reason')]
                        data_frame[actual_col] = ''

                    try:
                        # Remove rows where PARTY_ID is missing
                        if 'PARTY_ID' in data_frame.columns:
                            data_frame = data_frame[data_frame['PARTY_ID'].notna() & (data_frame['PARTY_ID'].astype(str).str.strip() != '')]

                        # If PARTY_ID is lowercase, handle accordingly
                        if 'party_id' in data_frame.columns:
                            data_frame = data_frame[data_frame['party_id'].notna() & (data_frame['party_id'].astype(str).str.strip() != '')]
                    except:
                        logging.warning(f"#### removing empty party id columns removing failed")


                    try:
                        def remove_decimals2(age):
                            if age:
                                return age.split('.')[0]
                            return age
                        data_frame['age'] = data_frame['age'].apply(remove_decimals2)
                    except:
                        logging.info(f"#####")
                        pass
                    
                    try:
                        data_frame['COMPOSITE_KEY'] = data_frame['PARTY_ID'].astype(str) + data_frame['COMPONENT_NAME'].astype(str) + data_frame['AGE'].astype(str)
                    except:
                        data_frame['COMPOSITE_KEY'] = data_frame['party_id'].astype(str) + data_frame['component_name'].astype(str) + data_frame['age'].astype(str)
                    df_unique = data_frame.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                    df_unique.reset_index(drop=True, inplace=True)
                    df_unique = df_unique.drop(columns=['COMPOSITE_KEY'])

                    if table_name == 'age_margin_working_uat':
                        logging.info(f"### table is age_margin_working_uat")
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        df_unique['LAST_UPDATED'] = currentTS
                        try:
                            def remove_decimals(age):
                                if age:
                                    return age.split('.')[0]
                                return age
                            df_unique['age'] = df_unique['age'].apply(remove_decimals)
                        except:
                            logging.info(f"#####")
                            pass
               
            
                    ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                    os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                    str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                    engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                    connection = engine.raw_connection()
                    cursor = connection.cursor()
                    try:
                        trunc_query = f'TRUNCATE TABLE {table_name}'
                        extraction_db.execute_(trunc_query)
                        logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                    except Exception as e:
                        logging.info(f"## Exception occured while truncation data ..{e}")
                        pass
                    
                    logging.info(f"####age values ----> {df_unique['age']}")
                    data_tuples = [tuple(row) for row in df_unique.to_numpy()]
                    #If the table is age_margin_working_uat, you exclude IS_ACTIVE and INACTIVE_REASON columns.
                    if table_name == 'age_margin_working_uat':
                        columns = list(df_unique.columns)
                        columns.remove('IS_ACTIVE')
                        columns.remove('INACTIVE_REASON')
                    else:
                        columns = list(df_unique.columns)
                    columns = ','.join(columns)
                    df_unique = df_unique.loc[:, ~df_unique.columns.str.lower().duplicated()]
                    placeholders = ','.join([':' + str(i+1) for i in range(len(df_unique.columns))])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    logging.info(sql)
                    data_tuples = [tuple(x) for x in df_unique.to_numpy()]
                    cursor.executemany(sql, data_tuples)
                    connection.commit()
                    cursor.close()
                    connection.close()

                    create_index(tenant_ids= [tenant_id], sources= [table_name])
                    logging.info('Created Index')

                    #updating last updated by

                    query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                    inserting = extraction_db.execute_(query)
                    time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                    last_updated = extraction_db.execute_(time_query).to_dict()

                    message = f"Successfully updated in {database}."
                    return_data = {'flag': True,'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}


                else:
                    logging.info(f'table name is {table_name} and dataframe is {data_frame}')
                    
                    if 'id' in data_frame.columns:
                        data_frame = data_frame.drop(columns = ['id'])
                    data_frame = data_frame.astype(str)
                    data_frame.drop(columns=['last_updated_by'], inplace=True)
                    if table_name == 'age_margin_working_uat':
                        logging.info(f"### table is age_margin_working_uat")
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        data_frame['LAST_UPDATED'] = currentTS

                        try:
                            def remove_decimals(age):
                                if age:
                                    return age.split('.')[0]
                                return age
                            data_frame['age'] = data_frame['age'].apply(remove_decimals)
                        except:
                            logging.info(f"##error in the age conversion")
                            pass
                    ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                    os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                    str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                    engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                    connection = engine.raw_connection()
                    cursor = connection.cursor()
                    try:
                        trunc_query = f'TRUNCATE TABLE {table_name}'
                        extraction_db.execute_(trunc_query)
                        logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                    except Exception as e:
                        logging.info(f"## Exception occured while truncation data ..{e}")
                        pass
                    data_tuples = [tuple(row) for row in data_frame.to_numpy()]
                    columns = ','.join(data_frame.columns)
                    placeholders = ','.join([':' + str(i+1) for i in range(len(data_frame.columns))])
                    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    logging.info(sql)
                    cursor.executemany(sql, data_tuples)
                    connection.commit()
                    cursor.close()
                    connection.close()

                    ##### creating elastic search index of data          
                    create_index(tenant_ids= [tenant_id], sources= [table_name])
                    logging.info('Created Index')

                    #updating last updated by

                    query = f"update `master_upload_tables` set last_updated_by = '{last_updated_by}' WHERE `table_name` = '{master_table_name}'"
                    inserting = extraction_db.execute_(query)
                    time_query = f"select `last_updated` from `master_upload_tables` where `table_name` = '{master_table_name}' limit 1"
                    last_updated = extraction_db.execute_(time_query).to_dict()

                    message = f"Successfully updated in {database}."
                    return_data = {'flag': True,'last_updated_by': last_updated_by,'last_updated': last_updated, 'message': message}

            except:
                traceback.print_exc()
                message = f"Could not update {table_name}. Please check the template/data."
                return_data = {"flag": False, "message" : message}
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
        logging.info(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return return_data

        
@app.route('/download_master_blob', methods = ['GET', 'POST'])
def download_master_blob():
    data = request.json
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    tenant_id = data.pop('tenant_id', None)
    tenant_id = tenant_id.replace('.acelive.ai','')
    download_type = data.pop('dowload_type', 'Data')
    last_updated_by=data.get('user','')
    file_type=data.get('file_type','xlsx')

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id)

    with zipkin_span(
        service_name='download_master_blob',
        zipkin_attrs=attr,
        span_name='download_master_blob',
        transport_handler=http_transport,
        sample_rate=0.5):

        try:
            master_table_name = data.pop('master_table_name')
        except:
            traceback.print_exc()
            message = f"Master table name not provided"
            return jsonify({"flag": False, "message" : message})

        try:
            database, table_name = master_table_name.split('.')
        except:
            traceback.print_exc()
            message = f"Master table name is in incorrect format. Check configuration."
            return jsonify({"flag": False, "message": message})

        db_config['tenant_id'] = tenant_id
        table_db = DB(database,**db_config)
        extraction_db=DB('extraction',**db_config)

        #Storing the download details for Audit report 
        if table_name=='age_margin_working_uat':
                        
            current_ist = datetime.now(pytz.timezone(tmzone))
            currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

            #INSERT statement for logging the audit record          
            insert_query = f"""
            INSERT INTO audit_report_case (category, maker_name,maker_id,maker_datetime)
            VALUES ('Download in Age Margin Master','Age Margin Master','{last_updated_by}','{currentTS}')
            """
            try:
                extraction_db.execute_(insert_query,)
                logging.info('successsfully inserted')
            except Exception as e:
                logging.info(f" Failed to insert records: {e}")
            
        if download_type == 'Data':
            try:
                data_frame = table_db.master_execute_(f"SELECT * FROM `{table_name}`")
                data_frame.fillna(value= '')
                data_frame = data_frame.astype(str)
                try:
                    data_frame.replace(re.compile(r'^(n[ou]*ll|n[oa]*ne|nan)$', re.IGNORECASE), '', inplace=True)
                except:
                    logging.info(f"#######NONE, NAN and null values are not removing while uploading")
                
                try:
                    data_frame = data_frame.drop(columns = ['last_updated'])

                    temp_dir=Path('/app/master_download')
                    if file_type=="csv":
                        temp_file=Path(f'{table_name}.csv')
                        logging.info(f"### Writing df to excel file ....")
                        data_frame.to_csv(temp_dir / temp_file,index=False)
                        return jsonify({'flag': True, 'file_name' : str(temp_file)})
                    else:
                        # temp_file=Path(f'{table_name}.xlsx')
                        # logging.info(f"### Writing df to excel file ....")
                        # data_frame.to_excel(temp_dir / temp_file,index=False)
                        # return jsonify({'flag': True, 'file_name' : str(temp_file)})


                        ######### testing same as in UAT###############
                        # ILLEGAL_CHARACTERS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]')
                        # data_frame = data_frame.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
                        # temp_file = temp_dir / f'{table_name}.xlsx'
                        # chunk_size = 10000  
                        # logging.info(f"### Writing df to Excel file in chunks of {chunk_size} ....")

                        # try:
                        #     with pd.ExcelWriter(temp_file, engine='openpyxl') as writer:
                        #         for i in range(0, len(data_frame), chunk_size):
                        #             chunk = data_frame.iloc[i:i + chunk_size]  
                        #             chunk.to_excel(writer, index=False, startrow=i, header=(i == 0)) 

                        #     logging.info(f"### File saved successfully as {temp_file}")
                        #     return jsonify({'flag': True, 'file_name': temp_file.name})

                        # except Exception as e:
                        #     logging.error(f"Error while writing Excel file: {e}")
                        #     return jsonify({'flag': False, 'message': 'Error while writing the file. Please try again later.'})
                        chunk_size = 10000
                        ILLEGAL_CHARACTERS_RE = re.compile(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F-\x9F]')
                        data_frame = data_frame.applymap(lambda x: ILLEGAL_CHARACTERS_RE.sub('', x) if isinstance(x, str) else x)
                        output = BytesIO()
                        temp_file = temp_dir / f'{table_name}.xlsx'
                        start_time = tt()
                        try:
                           
                            logging.info(f"### Writing df to Excel file in chunks of {chunk_size} ....")
                            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                                for i in range(0, len(data_frame), chunk_size):
                                    chunk = data_frame.iloc[i:i + chunk_size]
                                    chunk.to_excel(writer, index=False, startrow=i, header=True if i == 0 else False)
                                    #chunk.to_excel(writer, index=False, startrow=i, header=(i == 0)) 
                                    #chunk.to_excel(writer, index=False, startrow=(i if i == 0 else i + chunk_size), header=(i == 0))
                            output.seek(0)
                            with open(temp_file, 'wb') as f:
                                f.write(output.read())
                            end_time = tt()
                            time_consumed = str(round(end_time-start_time,3))

                            logging.info(f"### File written successfully. End time: {end_time}. Time taken: {time_consumed} seconds.")
                            
                            return jsonify({'flag': True, 'file_name': temp_file.name})
                        except Exception as e:
                            logging.error(f"Error while writing Excel file: {e}")
                            return jsonify({'flag': False, 'message': 'Error while writing the file. Please try again later.'})


                except Exception as e:
                    logging.info(f"## Error OcCuured {e} ")
                
            except:
                traceback.print_exc()
                message = f"Could not load from {master_table_name}"
                return jsonify({"flag": False, "message" : message})
        elif download_type == 'template':
            try:
                data_frame = table_db.master_execute_(f"SELECT * FROM `{table_name}` LIMIT 0")
                data_frame = data_frame.astype(str)
                data_frame.replace(to_replace= 'None', value= '', inplace= True)
                blob_data = dataframe_to_blob(data_frame)
            except:
                traceback.print_exc()
                message = f"Could not load from {master_table_name}"
                return jsonify({"flag": False, "message" : message})
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return jsonify({'flag': True, 'blob': blob_data.decode('utf-8'), 'file_name' : master_table_name + '.xlsx'})
       
def remove_duplicates(dict_list):
    # A set to keep track of unique dictionaries (excluding 'id' key)
    seen = []
    unique_list = []
    # print(f" #### DICT LIST IS {dict_list}")
    for d in dict_list:
        # Create a new dictionary excluding the 'id' key
        #dict_without_id = tuple((k, v) for k, v in d.items() if k != 'ID')
        dict_without_id = {k: v for k, v in d.items() if k.lower() != 'id' and k.lower()!='last_updated'}
        #dict_tuple = tuple(sorted(dict_without_id.items())
        # If this dictionary (excluding 'id') hasn't been seen, add it to the result
        if dict_without_id not in seen:
            seen.append(dict_without_id)
            unique_list.append(d)
    print(f"### UNQUE LEN IS {len(unique_list)}")

    return unique_list





def master_search(tenant_id, text, table_name, start_point, offset, columns_list, header_name):
    elastic_input = {}
    
    print(f'#######{tenant_id}')
    print(f'#######{table_name}')
    print(f'#######{start_point}')
    print(f'#######{offset}')
    print(f'#######{columns_list}')
    print(f'#######{header_name}')
    elastic_input['columns'] = columns_list
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    if header_name:
        header_name=header_name.upper()
        elastic_input['filter'] = [{'field': header_name, 'value': "*" + text + "*"}]
    else:
        elastic_input['text'] = text
    elastic_input['source'] = table_name
    elastic_input['tenant_id'] = tenant_id
    print(f"output of the elastic_input---------{elastic_input}")
    files, total = elasticsearch_search(elastic_input)
    ## to handle duplicate data if there is any
    logging.info(f"###files are {files}")
    
    print(f"output----------{files,total}")
    return files, total
    
@app.route('/elastic_search_test', methods= ['GET', 'POST'])
def elastic_search_test():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    print(f'Request data: {data}')
    try:
        tenant_id = data['tenant_id']
    except:
        traceback.print_exc()
        message = f"tenant_id not provided"
        return jsonify({"flag": False, "message" : message})
    
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='ace_template_training',
            zipkin_attrs=attr,
            span_name='train',
            transport_handler=http_transport,
            sample_rate=0.5
    ):    
        try:
            text = data['data'].pop('search_word')
            table_name = data['data'].pop('table_name')
            start_point = data['data']['start'] - 1
            end_point = data['data']['end']
            header_name = data['data'].get('column', None)
            offset = end_point - start_point
        except:
            traceback.print_exc()
            message = f"Input data is missing "
            return jsonify({"flag": False, "message" : message})
        
        db_config['tenant_id'] = tenant_id
        extraction = DB('extraction', **db_config)
        columns_list = list(extraction.execute_(f"SHOW COLUMNS FROM `{table_name}`")['Field'])
    
        files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = 0, offset = 10, columns_list = columns_list, header_name=header_name)
        
        if end_point > total:
            end_point = total
        if start_point == 1:
            pass
        else:
            start_point += 1
        
        pagination = {"start": start_point, "end": end_point, "total": total}
        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            memory_consumed = f"{memory_consumed:.10f}"
            print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
            time_consumed = str(round(end_time-start_time,3))
            
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass
        print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        
        return jsonify({"flag": True, "data": files, "pagination":pagination})
        
def button_options(extraction_db):
    try:   
        button_options_query=f"SELECT * FROM `button_options`"
        button_options_query_df=extraction_db.execute_(button_options_query)
        button_options_query_df.to_dict(orient='records')
        for idx, row in button_options_query_df.iterrows():
            if row['type'] == 'dropdown':
                button_options_query_df.at[idx, 'options'] = json.loads(row['options'])
        button_list=[]
        options_data={}
        for row in button_options_query_df['button'].unique():
            button_list=[]
            button_options_df =button_options_query_df[button_options_query_df['button']==row]
            for display_name in button_options_df['display_name'].unique():
                button_option_df=button_options_query_df[button_options_query_df['display_name']==display_name]
                button_df=button_option_df.to_dict(orient='records')
                button_list.append({"display_name":display_name,"options":button_df})
            options_data[row]=button_list
        return {"flag":True,"options_data":options_data }
    except:
        traceback.print_exc()
        message = f"something went wrong while generating button_options data"
        return {"flag": False, "message" : message} 
    

def check_if_record_exists(table_db,table_name,data_dict):
    
    where_clause = []
    where_value_list = []
    cols_query = f"SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'"
    db_cols = set(table_db.execute_(cols_query)["COLUMN_NAME"].str.lower())


    for where_column, where_value in data_dict.items():
        if where_column.lower() not in db_cols:
            continue
        if where_value==None or where_value=='NULL' or where_value=='null' or where_column=='None' or type(where_value)==int or where_value==False or where_value==True:
            where_clause.append(f"{where_column}=%s")
        elif len(where_value)>4000 or where_column=='last_updated':
            where_clause.append(f"{where_column}=%s")
        else:
            where_clause.append(f"TO_CHAR({where_column})=%s")
        where_value_list.append(where_value)
    where_clause_string = ' AND '.join(where_clause)

    select_query=f"select count(*) from {table_name} where {where_clause_string}"

    result = list(table_db.execute_(select_query,params=where_value_list)["COUNT(*)"])[0]

    if result>0:
        return True
    else:
        return False



def find_changes(modified_data,original_data):
    modified_data_list = modified_data
    old_data_list = original_data

    fields_changed = []
    new_values = []
    old_values = []

    # Iterate through each pair of modified and old data
    for modified_data, old_data in zip(modified_data_list, old_data_list):
        change_details = {
            "id": modified_data.get("id"),
            "fields_changed": [],
            "new_values": {},
            "old_values": {}
        }
        
        # Iterate over the fields in old_data
        for key in old_data:
            old_value = old_data.get(key)
            new_value = modified_data.get(key)

            # Check if the value has changed
            if old_value != new_value:
                change_details["fields_changed"].append(key)
                change_details["new_values"][key] = new_value
                change_details["old_values"][key] = old_value

        if change_details["fields_changed"]:
            fields_changed.append({change_details["id"]: change_details["fields_changed"]})
            new_values.append({change_details["id"]: change_details["new_values"]})
            old_values.append({change_details["id"]: change_details["old_values"]})

    return fields_changed, new_values, old_values

@app.route('/update_master_data', methods = ['GET', 'POST'])
def update_master_data():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    try:
        data = request.json
        print(f'Request data: {data}')
        tenant_id = data.get('tenant_id', None)
        
        attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
            )
        with zipkin_span(
            service_name='master_upload',
            span_name='update_master_data',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
                
           
            master_table_name = data.get('master_table',"")
            username = data.get('user',"")
            modified_data = data.get('modified_data',[])
            if isinstance(modified_data, list):
                modified_data = [
                    {key.lower(): value for key, value in record.items()}
                    for record in modified_data
                ]
                logging.info(f"modified data is : {modified_data}")
            original_data = data.get('original_data',[])
            if isinstance(modified_data, list):
                original_data = [
                    {key.lower(): value for key, value in record.items()}
                    for record in original_data
                ]
                logging.info(f"original data is : {original_data}")
            
            if master_table_name=="":
                message="############ Master table is not defined"
                logging.info(message)
                return jsonify({"flag":False,"message":message})

            try:
                database, table_name = master_table_name.split('.')
                logging.info(f"The database name is {database}")
                logging.info(f"The table name is {table_name}")
            except:
                traceback.print_exc()
                message = "Master table name is in incorrect format. Check configuration."
                logging.info(f"############{message}")
                return jsonify({"flag": False, "message": message})
            
            db_config['tenant_id'] = tenant_id
            table_db = DB(database, **db_config)
            extraction_db = DB('extraction', **db_config)
            group_access_db = DB('group_access', **db_config)
            role_query = """select role from active_directory where username=%s"""
            params = [username]
            result = group_access_db.execute_(role_query, params=params)
            role = result['ROLE'][0]
            print(f"The role of user is {role}")
            column_name = "id"
            print(f"The Column Name is {column_name}")
            main_audit_records = []
            for record in modified_data:
                logging.info(f"###in for loop record is {record}")
                print(f"The Record is {record}")
                record_type = record.pop("__type","")
                if record_type=="":
                    continue
                elif record_type=="new":
                    logging.info(f"###in for loop new")
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    if not duplicate_check:
                        try:
                            del record['last_updated']
                        except:
                            pass
                        try:
                            del record['LAST_UPDATED']
                        except:
                            pass
                        try:
                            del record['id']
                        except:
                            pass
                        try:
                            del record['ID']
                        except:
                            pass
                        #Data base triggers are not correct timestamp for last_updated
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        record['LAST_UPDATED'] = currentTS
                        print(f'Record is: {record}')
                        print(f'Table is : {table_name}')
                        valid_cols_query = f"SELECT COLUMN_NAME FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{table_name.upper()}'"
                        valid_cols = set(table_db.execute_(valid_cols_query)["COLUMN_NAME"].str.lower())
                        record = {k: v for k, v in record.items() if k.lower() in valid_cols}

                        #table_db.insert_dict(record, table_name)
                        table_db.insert_dict(record,table_name)
                    # Deepak Code
                    try:
                        if record != {}:
                            PARTY_ID = record.pop("party_id", "") if 'party_id' in record else ""
                            logging.info(f"The Party ID is {PARTY_ID}")
                            changed_fields = [i for i in record.keys()]
                            logging.info(f"The Changed Fields are {changed_fields}")
                            keys_to_remove = [field for field in record if record[field] == "" or record[field] is None or field == "LAST_UPDATED"]
                            for field in keys_to_remove:
                                record.pop(field)
                            for field in record:
                                if table_name == "party_master" or table_name == "age_margin_working_uat":
                                    if role == "Maker":
                                        if table_name == "age_margin_working_uat":
                                            table_name_ = "Age Margin Master"
                                        else:
                                            table_name_ = table_name
                                        value_query = ""
                                        value_query += f"INSERT INTO audit_report_case (PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, MAKER_ID, MAKER_NAME, MAKER_DATETIME, CATEGORY) VALUES ('{PARTY_ID}', '{field}', '{''}', '{record[field]}', '{username}', '{table_name_}', '{currentTS}', 'Change in Master Data')"
                                        insert_result = extraction_db.execute_(value_query)
                                        print(f"The value query for maker is {value_query}")
                                        logging.info(f"The result for maker is {insert_result}")
                                    if role == "Checker":
                                        if table_name == "age_margin_working_uat":
                                            table_name_ = "Age Margin Master"
                                        else:
                                            table_name_ = table_name
                                        value_query = ""
                                        value_query += f"INSERT INTO audit_report_case (PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, CHECKER_ID, MAKER_NAME, CHECKER_DATETIME, CATEGORY) VALUES ('{PARTY_ID}', '{field}', '{''}', '{record[field]}', '{username}', '{table_name_}', '{currentTS}', 'Change in Master Data')"
                                        insert_result = extraction_db.execute_(value_query)
                                        print(f"The value query for checker is {value_query}")
                                        logging.info(f"The result for checker is {insert_result}")
                                else:
                                    if role == "Maker":
                                        value_query = ""
                                        value_query += f"INSERT INTO audit_report_case (CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, MAKER_ID, MAKER_NAME, MAKER_DATETIME, CATEGORY) VALUES ('{field}', '{''}', '{record[field]}', '{username}', '{table_name}', '{currentTS}', 'Change in Master Data')"
                                        insert_result = extraction_db.execute_(value_query)
                                        print(f"The value query for maker is {value_query}")
                                        logging.info(f"The result for maker is {insert_result}")
                                    if role == "Checker":
                                        value_query = ""
                                        value_query += f"INSERT INTO audit_report_case (CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, CHECKER_ID, MAKER_NAME, CHECKER_DATETIME, CATEGORY) VALUES ('{field}', '{''}', '{record[field]}', '{username}', '{table_name}', '{currentTS}', 'Change in Master Data')"
                                        insert_result = extraction_db.execute_(value_query)
                                        print(f"The value query for checker is {value_query}")
                                        logging.info(f"The result for checker is {insert_result}")
                    except Exception as e:
                        logging.error(f"Error occured with Exception {e}")
                        
                elif record_type=="update":
                    logging.info(f"###in for loop update")
                    # Checking for Duplicate Records
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    # Checking for Duplicate Records are completed
                    if not duplicate_check:
                        # Code for audit report generation master
                        try:
                            # Deepak code
                            audit_records = []
                            modified_column_value = record.get(column_name)
                            logging.info(f"The modified column name is {modified_column_value}")
                            category = "Masters"
                            for original_record in original_data:
                                original_column_value = original_record.get(column_name)
                                if modified_column_value == original_column_value:
                                    logging.info(f"The modified column result is True, the modified column value and original record value is {modified_column_value} and {original_column_value}")
                                    margin_prefix = original_record.get("component_name","")
                                    logging.info(f"The Margin Prefix is: {margin_prefix}")
                                    for field, value in record.items():
                                        if field in ["id", "__type"]:
                                            logging.info(f"The modified value is with ID (or) TYPE Flag")
                                            continue
                                        if isinstance(value, str) and re.fullmatch(r"^\s*", value):
                                            old_value = original_record.get(field, "")
                                            new_value = value
                                            # If old value is non-empty and new value is spaces, accept and continue processing
                                            if old_value and not new_value.strip():
                                                logging.info(f"The new value is full of EMPTY SPACES, but old value is non-empty. Proceeding with the update.")
                                                continue
                                            logging.info(f"The value is full of EMPTY SPACES")
                                            continue
                                        else:
                                            old_value = original_record.get(field,"")
                                            logging.info(f"The Old Value is {old_value}")
                                            new_value = record.get(field,"")
                                            logging.info(f"The New Value is {new_value}")
                                            if old_value != new_value:
                                                if table_name == "age_margin_working_uat":
                                                    party_id = record.get("party_id") or record.get("PARTY_ID")
                                                    logging.info(f"The party_id is {party_id}")
                                                    audit_record = {"CHANGED_FIELDS": str(margin_prefix) + ": " + str(field), "OLD_VALUES": old_value, "NEW_VALUES": new_value, "PARTY_ID": party_id}
                                                    logging.info(f"The record is {audit_record}")
                                                    main_audit_records.append(audit_record)
                                                elif table_name == "party_master":
                                                    party_id = record.get("party_id") or record.get("PARTY_ID")
                                                    logging.info(f"The party_id is {party_id}")
                                                    audit_record = {"CHANGED_FIELDS": field, "OLD_VALUES": old_value, "NEW_VALUES": new_value, "PARTY_ID": party_id}
                                                    logging.info(f"The record is {audit_record}")
                                                    main_audit_records.append(audit_record)
                                                else:
                                                    audit_record = {"CHANGED_FIELDS": field, "OLD_VALUES": old_value, "NEW_VALUES": new_value}
                                                    audit_records.append(audit_record)
                                                    logging.info(f"The record is {audit_record}")
                                                    main_audit_records.append(audit_record)
                            logging.info(f"The Audit Data is {audit_records}")
                            logging.info(f"The Main Audit Data is {main_audit_records}")
                            # Insert into `audit_report_case` Table
                        except Exception as e:
                            logging.info(f"Error occured with Exception {e}")
                            logging.info(f"The Audit Data is not Available {audit_records}")
                            logging.info(f"The Main Audit Data is not Available {main_audit_records}")

                        logging.info(f"##in updating loop")
                        record_id = None
                        if 'id' in record:
                            record_id=record.pop("id")
                        elif 'ID' in record:
                            record_id=record.pop("ID")
                        else:
                            logging.info("id and ID not present in the request data from the UI")
                        print(f"The Record ID is: {record_id}")
                        # except Exception as e:
                        #     print(f"Error occured with exception {e}")
                        #     print("Record ID")
                        try:
                            if record_id is None or record_id == '':
                                record_id=record.pop("ID")
                            print(f"The Record ID is: {record_id}")
                        except Exception as e:
                            print(f"Error occured with exception {e}")
                            print("Record ID")
                        try:
                            record.pop("last_updated")
                        except:
                            pass
                        try:
                            record.pop("LAST_UPDATED")
                        except:
                            pass
                        #Data base triggers are not correct timestamp for last_updated
                        current_ist = datetime.now(pytz.timezone(tmzone))
                        currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                        logging.info(f"####currentTS now is {currentTS}")
                        record['LAST_UPDATED'] = currentTS
                        logging.info(f"###record is {record} , record_id is {record_id}")
                        table_db.update(table_name,update=record,where={"id":record_id})
                        print(record)

                    
                elif record_type=="remove":
                    logging.info(f"###in for loop remove")
                    duplicate_check = check_if_record_exists(table_db,table_name,record)
                    if duplicate_check:
                        try:
                            record_id=record.pop("id")
                        except:
                            pass
                        try:
                            record_id=record.pop("ID")
                        except:
                            pass
                        delete_query = f"DELETE from {table_name} where id={record_id}"

                        
                        table_db.execute_(delete_query)
                        
            #logging.info(f"The Final Audit Data is {audit_records}")
            logging.info(f"The Final Main Audit Data is {main_audit_records}")
            
            #insert into `audit_report_case` updated
            
            if main_audit_records:
                for record in main_audit_records:
                    for key,value in record.items():
                        if isinstance(value, str):  # Check if the value is a string
                            value=value.replace("_"," ")
                        record[key]=value
                    # inserting into audit report table 
                    if table_name == "party_master" or table_name == "age_margin_working_uat":
                        if role == "Maker":
                            if table_name == "age_margin_working_uat":
                                table_name_ = "Age Margin Master"
                            else:
                                table_name_ = table_name
                            value_query = ""
                            value_query += f"INSERT INTO audit_report_case (PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, MAKER_ID, MAKER_NAME, MAKER_DATETIME, CATEGORY) VALUES ('{record["PARTY_ID"]}', '{record['CHANGED_FIELDS']}', '{record['OLD_VALUES']}', '{record['NEW_VALUES']}', '{username}', '{table_name_}', '{currentTS}', 'Change in Master Data')"
                            insert_result = extraction_db.execute_(value_query)
                            print(f"The value query for maker is {value_query}")
                            logging.info(f"The result for maker is {insert_result}")
                        if role == "Checker":
                            if table_name == "age_margin_working_uat":
                                table_name_ = "Age Margin Master"
                            else:
                                table_name_ = table_name
                            value_query = ""
                            value_query += f"INSERT INTO audit_report_case (PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, CHECKER_ID, MAKER_NAME, CHECKER_DATETIME, CATEGORY) VALUES ('{record["PARTY_ID"]}', '{record['CHANGED_FIELDS']}', '{record['OLD_VALUES']}', '{record['NEW_VALUES']}', '{username}', '{table_name_}', '{currentTS}', 'Change in Master Data')"
                            insert_result = extraction_db.execute_(value_query)
                            print(f"The value query for checker is {value_query}")
                            logging.info(f"The result for checker is {insert_result}")
                    else:
                        if role == "Maker":
                            value_query = ""
                            value_query += f"INSERT INTO audit_report_case (CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, MAKER_ID, MAKER_NAME, MAKER_DATETIME, CATEGORY) VALUES ('{record['CHANGED_FIELDS']}', '{record['OLD_VALUES']}', '{record['NEW_VALUES']}', '{username}', '{table_name}', '{currentTS}', 'Change in Master Data')"
                            insert_result = extraction_db.execute_(value_query)
                            print(f"The value query for maker is {value_query}")
                            logging.info(f"The result for maker is {insert_result}")
                        if role == "Checker":
                            value_query = ""
                            value_query += f"INSERT INTO audit_report_case (CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, CHECKER_ID, MAKER_NAME, CHECKER_DATETIME, CATEGORY) VALUES ('{record['CHANGED_FIELDS']}', '{record['OLD_VALUES']}', '{record['NEW_VALUES']}', '{username}', '{table_name}', '{currentTS}', 'Change in Master Data')"
                            insert_result = extraction_db.execute_(value_query)
                            print(f"The value query for checker is {value_query}")
                            logging.info(f"The result for checker is {insert_result}")
                
            if table_name == 'age_margin_working_uat':

                query = f'SELECT * from age_margin_working_uat order by last_updated ASC'
                logging.info(f"####query is {query}")
                res = extraction_db.execute_(query)
                res = res.fillna('')
                res.replace('null', '', inplace=True)
                res.replace('NULL', '', inplace=True)
                res.replace('Null', '', inplace=True)
                res.replace('None', '', inplace=True)
                res.replace('NONE', '', inplace=True)
                res.replace('none', '', inplace=True)
                res.replace('nan', '', inplace=True)
                # columns are being dropped from the DataFrame Result
                res = res.drop(columns=[ 'id','last_updated', 'PARTY_ID', 'BANKING_TYPE','MARGIN_TYPE', 'DP_MARGIN_CONSIDERATION', 'STOCK_STATEMENT_TYPE','BANKING_SHARE', 'COMPONENT_NAME', 'AGE', 'MARGIN', 'ID','LAST_UPDATED','inactive_reason','is_active'])
                try:
                    if 'BANKING_SHARE' in res.columns:
                        print(f"!!!!!!!!! come to banks share")
                        res['BANKING_SHARE'] = res['BANKING_SHARE'].replace(
                            to_replace=r'[^a-zA-Z0-9\s]', 
                            value='', 
                            regex=True
                        ).str.strip()
                        print(f"Updated BANKING_SHARE column: {res['BANKING_SHARE']}")
                except Exception as e:
                    logging.error(f"Error in BANKING_SHARE cleaning: {e}")
                try:
                    def remove_decimals(age):
                        if age:
                            return age.split('.')[0]
                        return age
                    res['age'] = res['age'].apply(remove_decimals)
                except:
                    logging.info(f"##error in the age conversion")
                    pass
                data_frame = res
                try:
                    data_frame['COMPOSITE_KEY'] = data_frame['PARTY_ID'].astype(str) + data_frame['COMPONENT_NAME'].astype(str) + data_frame['AGE'].astype(str)
                except:
                    data_frame['COMPOSITE_KEY'] = data_frame['party_id'].astype(str) + data_frame['component_name'].astype(str) + data_frame['age'].astype(str)
                df_unique = data_frame.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                df_unique.reset_index(drop=True, inplace=True)
                df_unique = df_unique.drop(columns=['COMPOSITE_KEY'])

                for column in df_unique.columns:
                    if column.upper() == 'COMPONENT_NAME' or column.upper() == 'BANKING_TYPE':
                        df_unique[column] = df_unique[column].str[:50]
                    else:
                        df_unique[column] = df_unique[column].str[:20]  
                        print(f"df_unique is {df_unique}")
             
                try:
                    def remove_decimals(age):
                        if age:
                            return age.split('.')[0]
                        return age
                    res['age'] = res['age'].apply(remove_decimals)
                except:
                    logging.info(f"##error in the age conversion")
                    pass
                data_frame = res
                try:
                    data_frame['COMPOSITE_KEY'] = data_frame['PARTY_ID'].astype(str) + data_frame['COMPONENT_NAME'].astype(str) + data_frame['AGE'].astype(str)
                except:
                    data_frame['COMPOSITE_KEY'] = data_frame['party_id'].astype(str) + data_frame['component_name'].astype(str) + data_frame['age'].astype(str)
                df_unique = data_frame.drop_duplicates(subset='COMPOSITE_KEY', keep='last')
                df_unique.reset_index(drop=True, inplace=True)
                df_unique = df_unique.drop(columns=['COMPOSITE_KEY'])

                for column in df_unique.columns:
                    if column.upper() == 'COMPONENT_NAME' or column.upper() == 'BANKING_TYPE':
                        df_unique[column] = df_unique[column].str[:50]
                    else:
                        df_unique[column] = df_unique[column].str[:20]  
                        print(f"df_unique is {df_unique}")
             

                
                current_ist = datetime.now(pytz.timezone(tmzone))
                currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                logging.info(f"####currentTS now is {currentTS}")
                df_unique['LAST_UPDATED'] = currentTS


                ENGINE_PATH_WIN_AUTH = os.environ['DIALECT'] + '+' + os.environ['SQL_DRIVER'] + '://' + 'hdfc_extraction' + ':' + \
                os.environ['LOCAL_DB_PASSWORD']  + '@' + os.environ['HOST_IP'] + ':' + \
                str(os.environ['DB_PORT']) + '/?service_name=' +os.environ['DATABASE_SERVICE']
                engine = create_engine(ENGINE_PATH_WIN_AUTH, pool_recycle=300, max_identifier_length=128)
                connection = engine.raw_connection()
                cursor = connection.cursor()
                try:
                    trunc_query = f'TRUNCATE TABLE {table_name}'
                    extraction_db.execute_(trunc_query)
                    logging.info(f"### TRUNCATE SUCCESSFUL FOR {table_name}")
                except Exception as e:
                    logging.info(f"## Exception occured while truncation data ..{e}")
                    pass
                data_tuples = [tuple(row) for row in df_unique.to_numpy()]
                columns = ','.join(df_unique.columns)
                placeholders = ','.join([':' + str(i+1) for i in range(len(df_unique.columns))])
                sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                print(sql)
                cursor.executemany(sql, data_tuples)
                connection.commit()
                cursor.close()
                connection.close()
                create_index(tenant_ids= [tenant_id], sources= [table_name])
                print('Created Index')
            try:
                deleted_rows = data.get('deleted_rows', [])

                for row in deleted_rows:
                    party_id = row.get('party_id', '')
                    component_name = row.get('component_name', '')

                    qry = f"""
                        DELETE FROM {table_name}
                        WHERE party_id = '{party_id}'
                        AND component_name = '{component_name}'
                    """

                    extraction_db.execute_(qry)

            except Exception as e:
                logging.error(f"Exception caught as e: {e}")
                pass
            message="Successfully modified the master data table"
            print(f"##############{message}")

            response_data = {"flag":True,"data":{"message":message}}

    except Exception as e:
        print("############### ERROR In Updating master data")
        logging.exception(e)
        message = "Something went wrong while updating the master data"
        response_data = {"flag": False, "message": message}

    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = tt()
        memory_consumed = f"{memory_consumed:.10f}"
        print(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.warning("Failed to calc end of ram and time")
        logging.exception("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass
    
    print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
    return jsonify(response_data)


def master_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'master_data_audit')
    return True


@app.route('/get_master_data', methods= ['GET', 'POST'])
def get_master_data():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass    

    data = request.json
    trace_id = generate_random_64bit_string()
    print(f'Request data: {data}')
    try:
        tenant_id = data['tenant_id']
        tenant_id = tenant_id.replace('.acelive.ai','')
    except:
        traceback.print_exc()
        message = f"tenant_id not provided"
        return jsonify({"flag": False, "message" : message})
    
    attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

    with zipkin_span(
            service_name='master_upload',
            zipkin_attrs=attr,
            span_name='get_master_data',
            transport_handler=http_transport,
            sample_rate=0.5
    ):    
        master_table_name = data.pop('master_table_name', None)
        user_name = data.pop('user', None)
        database = 'extraction'
        db_config['tenant_id'] = tenant_id
        extraction_db = DB(database, **db_config)
        flag = data.pop('flag', '')
        if flag == 'search':
            try:
                text = data['data'].pop('search_word')
                master_table_name = data['data'].pop('table_name')
                start_point = data['data']['start'] - 1
                end_point = data['data']['end']
                header_name = data['data'].get('column', None)
                offset = end_point - start_point
            except:
                traceback.print_exc()
                message = f"Input data is missing "
                return jsonify({"flag": False, "message" : message})    
            
            db_config['tenant_id'] = tenant_id
            try:
                database, table_name = master_table_name.split('.')
            except:
                traceback.print_exc()
                message = f"Master table name is in incorrect format. Check configuration."
                return jsonify({"flag": False, "message": message})
            try:
                table_db = DB(database, **db_config)
                columns_list = list(table_db.execute_(f"SHOW COLUMNS FROM `{table_name}`")['field'])
            except:
                logging.exception(f"Could not execute columns query for the table {table_name}")
            files, total = master_search(tenant_id = tenant_id, text = text, table_name = table_name, start_point = start_point, offset = offset, columns_list = columns_list, header_name=header_name)

            logging.info(f'#########Files got are: {files}')
            
            if len(files)>0:
                displayed_columns=list(files[0].keys())
            else:
                displayed_columns=[]
            dropdown_dict = {}
            if table_name=='party_master':
                if len(files)>0:
                    displayed_columns = ['ID','PARTY_ID','PARTY_NAME','RELATION_MGR_EMP_CODE','RELATION_MGR']
                    df = pd.DataFrame(files)
                    #removing TRL contanied rows
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[~df['PARTY_ID'].astype(str).str.contains('TRL')]
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='city_state' or table_name== 'RM_Master':
                if len(files)>0:
                    displayed_columns = ['EMP_CODE','EMP_NAME','BRANCH_CODE','HRMS_REGION','RM_STATE','RM_CITY','BRANCH_LOCATION','RM_REGION','WBO_REGION','HUB_CODE']
                    df = pd.DataFrame(files)
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    # try:
                    #     files = [{k: ("" if v is None else v) for k, v in record.items()} for record in files if "TRL" not in record["RM_MGR_CODE"]]
                    #     files = [{"RM_MGR_CODE":record["RM_MGR_CODE"], "RM_CITY":record["RM_CITY"], "RM_STATE":record["RM_STATE"]} for record in data]
                    # except Exception as e:
                    #     logging.exception(e)
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='component_master':
                if len(files)>0:
                    displayed_columns = ['COMPONENT_CODE','COMPONENT_NAME','STATUS','COMPONENT_CATEGORY','COMPONENT_TYPE']
                    df = pd.DataFrame(files)
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if table_name=='age_margin_working_uat' or table_name=='age_margin_working':
                if len(files)>0:
                    displayed_columns = ['IS_ACTIVE','ID','PARTY_ID','BANKING_TYPE','MARGIN_TYPE','DP_MARGIN_CONSIDERATION','STOCK_STATEMENT_TYPE','BANKING_SHARE','COMPONENT_NAME','AGE','MARGIN','INACTIVE_REASON']
                    df = pd.DataFrame(files)
                    #df = df.replace('NaN','')
                    #df = df.replace('nan','')
                    df = df.replace({np.nan: ''})
                    df = df.replace('NaN','').replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    #query = f'select DISTINCT(component_name) from age_margin_working_uat'
                    query = f"""select DISTINCT CAST(component_name as varchar2(4000)) AS COMPONENT_NAME from component_master WHERE CAST(STATUS as varchar2(4000))='ACTIVE'"""
                    component_name_df = extraction_db.execute_(query)
                    component_name_list = component_name_df['COMPONENT_NAME'].to_list()
                    dropdown_dict = {}
                    dropdown_dict['component_name']=component_name_list
                else:
                    displayed_columns = []

            if table_name=='wbo_region':
                if len(files)>0:
                    displayed_columns=['RM_STATE','WBO_REGION']
                    df = pd.DataFrame(files)
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                else:
                    displayed_columns=[]
            
            #only wanted columns displayed when user searches 

            if table_name.lower()=='hub_master':
                if len(files)>0:
                    displayed_columns = ['EMP_CODE','EMP_NAME','RM_CITY','RM_STATE','HUB_CODE','BRANCH_CODE','BRANCH_LOCATION']
                    df = pd.DataFrame(files)
                    df = df.replace('NaN','')
                    df = df.replace('nan','')
                    df = df[displayed_columns]
                    files = df.to_dict(orient='records')
                    dropdown_dict = {}
                else:
                    displayed_columns = []
            if end_point > total:
                end_point = total
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            try:
                files_=remove_duplicates(files)
            except Exception as e:
                logging.exception(f"exefdajkfndan fsfkd ..{e}")
                pass
            
            pagination = {"start": start_point, "end": len(files_), "total": total}
            
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
            
            logging.info(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
            # print(f'#########Files got are: {files}')
            return jsonify({"flag": True, "data": files_, "pagination":pagination,"displayed_columns":displayed_columns,"dropdown_columns": dropdown_dict})
        
        else:
            try:
                start_point = data['start'] - 1
                end_point = data['end']
                offset = end_point - start_point
            except:
                start_point = 0
                end_point = 20
                offset = 20
            
            group_db = DB("group_access", **db_config)
            user_groups = get_group_ids(user_name, group_db)
            logging.info(f"##### user_groups: {user_groups}")
            
            if master_table_name:
                try:
                    database, table_name = master_table_name.split('.')
                except:
                    traceback.print_exc()
                    message = f"Master table name is in incorrect format. Check configuration."
                    return jsonify({"flag": False, "message": message})
            else:
                tables_df = extraction_db.execute_(f"SELECT * FROM `master_upload_tables`")

                if not tables_df.empty:
                    tables_list = list(tables_df["table_name"].unique())
                    tables_edit=list(tables_df["editable"])
                    tables_enable = list(tables_df["enable"])
                    table_edit=[]
                    table_edit_modified = []
                    for i in range(len(tables_list)):
                        table_result = {"display_name":tables_list[i],"edit":tables_edit[i]}
                        table_result_modified = {"display_name":tables_list[i].split('.')[1],"edit":tables_edit[i]}
                        table_edit.append(table_result )
                        table_edit_modified.append(table_result_modified )
                    table_name = tables_list[0]
                    table_enable = tables_enable[0]
                    database, table_name = tables_list[0].split('.')
                else:
                    traceback.print_exc()
                    message = f"No tables in extraction database"
                    return jsonify({"flag": False, "message" : message})
            try:
                table_db = DB(database, **db_config)
                qry=f"SELECT * FROM `{table_name}` OFFSET {start_point} ROWS FETCH NEXT 20 ROWS ONLY"
                data_frame = table_db.master_execute_(qry)
                data_frame = data_frame.astype(str)
                data_frame = data_frame.replace('NaN','')
                data_frame = data_frame.replace('nan','')
                table_name_updated = database+'.'+table_name
                query = f"SELECT `enable` from master_upload_tables where table_name='{table_name_updated}'"
                table_enable = extraction_db.execute_(query)['ENABLE'][0]
                
                header_list=data_frame.columns

                columns_list = list(data_frame.columns)
                total_rows = list(table_db.execute_(f"SELECT COUNT(*) FROM `{table_name}`")['COUNT(*)'])[0]
                data_frame.replace(to_replace= "None", value= '', inplace= True)
                try:
                    data_frame = data_frame.sort('id')
                except:
                    pass
                dropdown_dict = {}
                if table_name=='party_master':
                    columns = ['id','party_id','party_name','relation_mgr_emp_code','relation_mgr']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                

                if table_name=='city_state' or table_name=='RM_Master':
                    columns = ['id','emp_code','emp_name','branch_code','hrms_region','rm_state','rm_city','branch_location','rm_region','wbo_region','hub_code']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                    # columns = ['id','rm_mgr_code','rm_city','rm_state']
                    # data_frame = data_frame[columns]
                    # columns_list = columns
                   

                if table_name=='component_master':
                    columns = ['id','component_code','component_name','status','component_category','component_type']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                #passing is_active and inactive_reason columns to ui
                if table_name=='age_margin_working_uat' or table_name=='age_margin_working':
                    columns = ['is_active','id','party_id','banking_type','margin_type','dp_margin_consideration','stock_statement_type','banking_share','component_name','age','margin','inactive_reason']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    #query = f'select DISTINCT CAST(component_name as varchar2(4000)) AS COMPONENT_NAME from component_master'
                    query = f"""select DISTINCT CAST(component_name as varchar2(4000)) AS COMPONENT_NAME from component_master WHERE CAST(STATUS as varchar2(4000))='ACTIVE'"""
                    component_name_df = extraction_db.execute_(query)
                    component_name_list = component_name_df['COMPONENT_NAME'].to_list()
                    dropdown_dict = {}
                    dropdown_dict['component_name']=component_name_list
                if table_name=='wbo_region':
                    columns = ['id','rm_state','wbo_region']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                
                #only wanted columns displayed when user normally enters 
                if table_name.lower()=='hub_master':
                    columns = ['id','emp_code', 'emp_name', 'rm_city', 'rm_state', 'hub_code', 'branch_code', 'branch_location']
                    data_frame = data_frame[columns]
                    columns_list = columns
                    dropdown_dict = {}
                data_dict = data_frame.to_dict(orient= 'records')
            except:
                traceback.print_exc()
                message = f"Could not load {table_name} from {database}"
                return jsonify({"flag": False, "message" : message})
            
            if end_point > total_rows:
                end_point = total_rows
            if start_point == 1:
                pass
            else:
                start_point += 1
            
            pagination = {"start": start_point, "end": end_point, "total": total_rows}
            logging.info(f'Data Dict is: {data_dict}')
            
            data = {
                "header": list(data_frame.columns),
                "rowData": data_dict,
                "pagination": pagination
            }
            
            button_options_data=button_options(extraction_db)
            if button_options_data['flag']==True:
                options_data=button_options_data["options_data"]
            else:
                message=f"unable to load options data"
                options_data = {}
            
            #updation details
            # try:
            #     last_updated = f"select `last_updated` from `master_upload_tables` where `table_name`= '{master_table_name}' LIMIT 1"
            #     last_updated = extraction_db.execute_(last_updated).to_dict()

            #     last_updated_by = f"select `last_updated_by` from `master_upload_tables` where `table_name`= '{master_table_name}' LIMIT 1"
            #     last_updated_by = extraction_db.execute_(last_updated_by).to_dict()

            # except:
            #     last_updated = None
            #     last_updated_by = None
            last_updated = None
            last_updated_by = None
            
            try:
                qry = f"select `last_updated`,`last_updated_by` from `master_upload_tables` where `table_name`= '{master_table_name}' LIMIT 1"
                df = extraction_db.execute_(qry)

                if df is not None and not df.empty:
                    last_updated = df.iloc[0]['last_updated']
                    last_updated_by = df.iloc[0]['last_updated_by']

            except:
                logging.exception("Error fetching last updated details")
                #last_updated = None
                #last_updated_by = None

            if master_table_name:
                to_return = {
                    'flag': True,
                    'data': {
                        'data': data,
                        'options_data':options_data,
                        'enable': str(table_enable),
                        'dropdown_columns': dropdown_dict
                        },
                    'last_updated_by':last_updated_by,
                    'last_updated':last_updated
                    }
            else:        
                to_return = {
                    'flag': True,
                    'data': {
                        'master_data': [{"display_name":"New Table","edit":0}] + table_edit,
                        'master_data_modified': [{"display_name":"New Table","edit":0}] + table_edit_modified,
                        'data': data,
                        'options_data':options_data,
                        'enable': str(table_enable),
                        'dropdown_columns': dropdown_dict
                        },
                    'last_updated_by':last_updated_by,
                    'last_updated':last_updated
                    }
            
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
            
            print(f"## Master Upload Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
            return to_return


@app.route('/automatic_script_generation', methods=['POST', 'GET'])
def automatic_script_generation():
    # get queue file counts for the user
    try:
        data = request.json

        logging.info(f'Request data for queue counts: {data}')
        tenant_id = data.get('tenant_id', None)

        try:
            memory_before = measure_memory_usage()
            start_time = time()
            logging.info(f"checkpoint memory_before - {memory_before}, start_time - {start_time}")
        except:
            logging.warning("Failed to start ram and time calc")
            pass

        attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id if tenant_id is not None else ''
        )

        with zipkin_span(
                service_name='queue_api',
                zipkin_attrs=attr,
                span_name='get_template_exceptions',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            db_config['tenant_id'] = tenant_id
            db = DB('queues', **db_config)
            #schema_name = "HDFC_QUEUES"
            table_name = "SCREEN_PROPERTIES"
            where_clause = "WHERE id IN (23,24)"  # Modify the condition as needed

            try:
                # Construct the dynamic query
                query = f"SELECT * FROM {table_name} {where_clause}"
                data=db.execute_(query).to_dict(orient='records')
                logging.info(f"data{data}")
            except:
                logging.info("error")

    except:
            logging.exception(
                f'Something went wrong while getting queues. Check trace.')
            #response_data = {'queue_counts': json.dumps(queue_counts)}
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = time()
        memory_consumed = f"{memory_consumed:.10f}"
        logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.warning("Failed to calc end of ram and time")
        logging.exception("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass
        print(f"update_queue_counts Time consumed {time_consumed}, memory_consumed {memory_consumed} ")

