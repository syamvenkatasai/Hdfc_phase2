#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 6 15:02:18 2022

@author: Gopi Teja B and Venkatesh Madasu

Note: This Folder Monitor is having specific style designed for Medusind Purpose
"""
import paramiko

# Minimal patch: only add DSSKey if it's missing (for Paramiko >= 3.0)
if not hasattr(paramiko, "DSSKey"):
    class DSSKey:
        pass
    paramiko.DSSKey = DSSKey

import pysftp  # your existing code
import os
import requests
import shutil
import uuid
import re
import pysftp
import subprocess
from reportlab.pdfgen import canvas
import os
import numpy as np
import pandas as pd 
import psutil
# import xxhash
import openpyxl
import json
import ast
import glob
import base64
import time
import fitz  # PyMuPDF
import math
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4
import random
import unicodedata
import re
import pytz
from PIL import Image
from flask import Flask, jsonify, request
from pathlib import Path
from app.db_utils import DB
from time import time as tt
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
# from pdf2image import convert_from_path
from app import app
from ace_logger import Logging
from datetime import datetime,timedelta
from datetime import datetime,timedelta
logging = Logging(name='folder_monitor')
import fitz
from cryptography.fernet import Fernet
import pwd
tmzone = 'Asia/Kolkata'
tmzone = 'Asia/Kolkata'

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}


def http_transport(encoded_span):
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

def generate_caseid_serial(tenant_id):
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    logging.info('Generating new case ID')
    query = "SELECT `id`,`case_id` FROM `process_queue` ORDER BY `process_queue`.`id` DESC LIMIT 1"

    try:
        existing_case_ids = list(db.execute(query)['case_id'])[0]
        new_case_id = str(int(existing_case_ids) + 1)
    except Exception:
        new_case_id = '100000000'

    logging.info(f'New case ID: {new_case_id}')
    return new_case_id


def generate_caseid(tenant_id, case_type, table_name, db_name):
    db_config['tenant_id'] = tenant_id
    db = DB(f'{db_name}', **db_config)
    logging.info(f"Generating new case ID")
    query = f"SELECT `{case_type}` FROM `{table_name}` where {case_type} like '%%{tenant_id[:3].upper()}%%' ORDER BY `{table_name}`.`{case_type}`"
    existing_case_ids = list(db.execute_(query)[{case_type}])

    new_case_id = tenant_id[:3].upper() + uuid.uuid4().hex.upper()[:7]
    # or condition is added bcz caseid is being created with PO starting
    while new_case_id in existing_case_ids:
        logging.debug(f'`{new_case_id}` already exists. Generating a new one.')
        new_case_id = tenant_id[:3].upper() + uuid.uuid4().hex.upper()[:7]

    logging.info(f'New case ID: {new_case_id}')
    return new_case_id


def file_hash_func(file_path):
    try:
        x = xxhash.xxh64()
        with open(file_path, 'rb') as f:
            file_data = f.read()
        x.update(file_data)

        file_hash = x.hexdigest()
    except Exception:
        file_hash = ''

    return file_hash


def convert_pdf_jpg(file_path, output_path):
    jpg_list = []
    file_full_name = file_path.name
    file_ext = file_path.suffix.lower()
    logging.debug(f"## FM Checkpoint File name: {file_full_name}")
    file_page_object = {}
    try:
        if file_ext == '.pdf':
            with fitz.open(file_path) as new_file:

                for idx, page in enumerate(new_file):
                    page_image_name = f'{file_path.stem}_{str(idx)}.jpg'
                    logging.info(f"page image name {page_image_name}")
                    page_image_path = (f'{output_path}/images/'
                                       + page_image_name)
                    logging.info(f"page image path {page_image_path}")
                    logging.debug(f'Page {str(idx)}: {page_image_path}')
                    mat = fitz.Matrix(300 / 72, 300 / 72)
                    if isinstance(page, fitz.Page):
                        try:
                            logging.info(f"in try if block")
                            
                            pix = page.get_pixmap(matrix=mat) 
                            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                            img.save(page_image_path, format="JPEG")
                        except:
                            logging.info(f"in except if block")
                            pix = page.get_pixmap() 
                            pix.pil_save(page_image_path)

                    else:
                        try:
                            logging.info(f"in try else block")
                            pix = page.get_pixmap(matrix=mat)
                            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                            img.save(page_image_path, format="JPEG")
                        except:
                            logging.info(f"in except else block")
                            pix = page.get_pixmap(matrix=mat)
                            pix.pil_save(page_image_path, 'JPEG')

                    jpg_list.append(page_image_name)
                file_page_object = {file_full_name: jpg_list}
                logging.info(f"file_page_object: {file_page_object}")
    except Exception as e:
        logging.error(f'PDF to image conversion failed with pymupdf: {e}')
        logging.info(f"convertion  pdf to jpg failed {e}")
       
    return file_page_object


def convert_pdf_jpg_old(file_path, output_path, unique_id):
    jpg_list = []
    file_full_name = file_path.name
    file_ext = file_path.suffix.lower()
    file_page_object = {}
    try:
        if file_ext == '.pdf':

            for idx, page in enumerate(convert_from_path(file_path, 300)):
                page_image_name = f'{file_path.stem}_{str(idx)}.jpg'
                page_image_path = (f'{output_path}/{unique_id}/images/'
                                   + page_image_name)
                logging.debug(f'Page {str(idx)}: {page_image_path}')
                page.save(page_image_path, 'JPEG')
                jpg_list.append(page_image_name)
            file_page_object = {file_full_name: jpg_list}
            logging.info(f"file_page_object: {file_page_object}")
    except Exception as e:
        logging.error(f'PDF to image conversion old method failed: {e}')
    return file_page_object


@app.route('/create_case_id', methods=['POST', 'GET'])
def create_case_id():
    data = request.json
    logging.debug(f"data received {data}")
    tenant_id = data.get('tenant_id', None)
    file = data.get('file', {})
    file_name = file.get('file_name')
    ace = 'ace'
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    source_of_invoice = data.get('move_from', '')
    variables = data.get("variables", {})

    db_config['tenant_id'] = tenant_id

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
        
    trace_id = generate_random_64bit_string()
        
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
        )

    with zipkin_span(
        service_name='folder_monitor_api',
        span_name='create_case_id',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):
        try:
            # Generating the case id
            unique_id = generate_caseid(tenant_id, 'case_id', 'process_queue', 'queues')

            # Creating case id into Database
            queue_db = DB('queues', **db_config)
            extraction_db = DB('extraction', **db_config)
            stats_db = DB('stats', **db_config)

            insert_case_id = ('INSERT INTO `ocr` (`case_id`,`document_id`,`highlight`) '
                              "VALUES (%s,%s,'{}')")
            params = [unique_id,unique_id]
            extraction_db.execute_(insert_case_id, params=params)

            insert_case_id_1 = ('INSERT INTO `process_file` (`case_id`,`document_id`) '
                              "VALUES (%s,%s)")
            params = [unique_id,unique_id]
            queue_db.execute(insert_case_id_1, params=params)

            logging.info(f'filename is {file_name}')
            query=f"UPDATE process_file SET file_name = '{file_name}' WHERE case_id='{unique_id}'"
            queue_db.execute(query)

            reponse_data = {"case_id": unique_id, "file_name": file_name}

            # Creating a folder with case id in htdocs
            output_path = Path(f'/app/{tenant_id}/assets/pdf/{tenant_id}')
            os.umask(0)
            logging.info(output_path)
            Path(str(output_path / unique_id)).mkdir(parents=True, exist_ok=True)
            os.chmod(str(output_path / unique_id), 0o755)
            logging.debug("folder created in lampp directory")

            try:
                shutil.move(output_path / file_name,
                            output_path / unique_id / file_name)
                os.chmod(str(output_path / unique_id / file_name), 0o755)
            
                file_path_stored = Path(str(output_path / unique_id / file_name))
                Path(str(output_path / unique_id / 'images')).mkdir(parents=True,
                                                                    exist_ok=True,mode=0o755)  # creating a image folder

                out_path = f'/app/{tenant_id}/assets/pdf/{tenant_id}/{unique_id}'
            except Exception as e:
                logging.exception (f"## Exception occured while moving files from input to output , Check Trace!")
                pass
            
            logging.debug(f'this is where the case is updated into process queue')
            insert_case_id = 'INSERT INTO `process_queue` ( `case_id`,`document_id`,`file_name`,`file_paths`,`last_updated_by`,`source_of_invoice`,`completed_processes`,`status`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
            params = [unique_id,unique_id, file_name, str(file_path_stored),"system", source_of_invoice,1,"File Uploaded"]
            queue_db.execute(insert_case_id, params=params)

            ## commenting this since the pdf to image conversion will be done in abbyy container
            # try:
            #     logging.info(f"## Going to convert pdf to images")
            #     file_object=convert_pdf_jpg(file_path_stored,out_path)
            #     queue_db.execute("update `process_queue` set `imagelist` = %s where `case_id` = %s", params=[json.dumps(file_object), unique_id])
            # except Exception as e:
            #     logging.info(e)

            logging.debug(f'this is where the case is updated into br_comparison_rules')
            insert_case_id_ = 'INSERT INTO `br_comparison_rules` (`case_id`) VALUES (%s)'
            params = [unique_id]
            extraction_db.execute(insert_case_id_, params=params)

            #adding the below lines to generate a unique number for every case
            try:
                logging.info("generating unique reference number project speicifc")
                uniquee_id = generate_reference_id()
                query = f"update ocr set STOCK_SEC_REFERENCE_ID='{uniquee_id}' where case_id ='{unique_id}'"
                extraction_db.execute_(query)
            except Exception as e:
                logging.exception(e)


            final_response_data = {"flag": True, "data": reponse_data}
    
        except Exception:
            logging.exception('Something went wrong while generating case id. Check trace.')
            final_response_data = {'flag': False, 'message': 'System error! Please contact your system administrator.'}
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

    # insert audit
    audit_data = {"tenant_id": tenant_id, "user_": 'fileupload', "case_id": unique_id,
                    "api_service": "update_queue", "service_container": "folder_monitor_api",
                    "changed_data": "New Case was generated","tables_involved": "process_queue","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(reponse_data), "trace_id": trace_id,
                    "session_id": session_id,"status":str(final_response_data['flag'])}
    try:
        insert_into_audit(unique_id, audit_data)
    except:
        logging.info(f"issue in the audit insert query ")

    return jsonify(final_response_data)

@app.route('/folder_monitor', methods=['POST', 'GET'])
def folder_monitor():
    """
    Note: If you are using this API the file ingestion should follow below keypoints
    1. folder_structure table need to be filled in io_configuration db
    2. For shared folder path: inside tenant folder of /app/input priority_folder column declared folders should be created
    3. Inside those create as many folders or structures you want the final folder where the file should be picked shoud have `input` name
    """

    data = request.json
    logging.info(f'Data is: {data}')
    # data = data['data']
    tenant_id = data.get('tenant_id', None)
    
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
        
    
    trace_id = generate_random_64bit_string()

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id if tenant_id is not None else ''
    )

    with zipkin_span(
            service_name='folder_monitor',
            zipkin_attrs=attr,
            span_name='folder_monitor',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            logging.debug(f'Connecting to tenant {tenant_id}')

            db_config['tenant_id'] = tenant_id
            db = DB('io_configuration', **db_config)

            input_config_qry = f'select * from `input_configuration`'
            input_config=db.execute(input_config_qry)
            output_config = db.get_all('output_configuration')

            # logging.debug(f'Input Config: {input_config.to_dict()}')
            # logging.debug(f'Output Config: {output_config.to_dict()}')

            # Sanity checks
            if (input_config.loc[input_config['type'] == 'Document'].empty
                    or output_config.loc[output_config['type'] == 'Document'].empty):
                message = 'Input/Output not configured in DB.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            for index, row in output_config.iterrows():
                output_path = row['access_1']
                

            for index, row in input_config.iterrows():
                input_path = row['access_1']
                
                workflow = row['workflow']

                # logging.debug(f'Input path: {input_path}')
                # logging.debug(f'Output path: {output_path}')

                if (input_path is None or not input_path
                        or output_path is None or not output_path):
                    message = 'Input/Output is empty/none in DB.'
                    logging.error(message)
                    return jsonify({'flag': False, 'message': message})

                input_path_str = "/app/input/"+input_path
                output_path = f"/app/{tenant_id}/assets/pdf/"+output_path
                input_path = Path(input_path_str)

                output_path = Path(output_path)

                logging.debug(f'Input Absolute path: {input_path}')
                logging.debug(f'Output Absolute path: {output_path}')

                reponse_data = {}

                # Only watch the folder if both are valid directory
                if input_path.is_dir():
                    logging.debug(str(input_path) + '/*')
                    # files = Path(input_path).rglob('*')
                    all_files = get_priority_files(input_path_str,tenant_id)
                    logging.info(f"All files found are: {all_files}")

                    # get first file and send it to pick as priority need to be upload first
                    if len(all_files):
                        files = [all_files[0]]
                    else:
                        files = []

                    file_names = []
                    for file_ in files:
                        logging.debug(f'move from: {file_}')
                        reponse_data['move_from'] = str(file_)
                        filename = file_.name.replace(' ', '_')
                        logging.debug(
                            f'move to: {str(output_path / filename)}')

                        # creating a copy file in error folder and
                        # will be deleted when queue assigned in camunda_api
                        #copy_to = Path(file_).parents[1] / 'error/'
                        #logging.debug(f'Creating a copy at {copy_to}')
                        
                        #shutil.move(Path(file_), Path(output_path) / filename)
                        uid = os.geteuid()
                        user_info = pwd.getpwuid(uid)
                        user_name = user_info.pw_name

                        print(f"Current user is: {user_name} (UID: {uid})")
                        # www_data_uid=33
                        # www_data_gid=33
                        # os.chown(output_path, www_data_uid, www_data_gid)
                        # os.chmod(output_path, 0o775)
                        # os.chown(Path(output_path) / filename, www_data_uid, www_data_gid)
                        shutil.copy2(Path(file_), Path(output_path) / filename)
                        # new_path='/app/input'
                        # shutil.move(Path(file_), Path(new_path))
                        Path(file_).unlink()


                        file_names.append({'file_name': filename})

                    logging.debug(f'Files: {file_names}')
                    reponse_data['files'] = file_names
                    reponse_data['workflow'] = workflow

                    final_response_data = {"flag": True, "data": reponse_data}
                else:
                    message = f'{input_path} not a directory'
                    logging.error(message)
                    final_response_data = {'flag': True, 'message': message}
        except:
            logging.exception(
                'Something went wrong watching folder. Check trace.')
            final_response_data = {'flag': False, 'message': 'System error! Please contact your system administrator.'}
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

    # insert audit
    audit_data = {"tenant_id": tenant_id, "user_": "", "case_id": "",
                    "api_service": "folder_monitor", "service_container": "folder_monitor_api",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(reponse_data), "trace_id": trace_id,
                    "session_id": "","status":json.dumps(final_response_data['flag'])}
    try:
        if len(file_names)>0:
            insert_into_audit("New File Received", audit_data)
    except:
        logging.info(f"issue in the query formation")
    return jsonify(final_response_data)


def get_folder_paths(rootdir, tmp_list=None):

    if tmp_list is None:
        tmp_list = []
    logging.info(f"#### rootdir: {os.scandir(rootdir)}")
    for it in os.scandir(rootdir):
        if it.is_dir():
            tmp_list.append(it.path)
            logging.info(f"###### tmp_list: {tmp_list}, it: {it}")
            get_folder_paths(it, tmp_list)


def filter_priority_input_folders(priority_folder, all_folder_paths):

    filtered_folders = []

    must_folder = priority_folder["priority_folder"].strip()

    end_folders = priority_folder["end_folders"].strip().split(",")

    for folder_path in all_folder_paths:

        if must_folder in folder_path.split('/'):

            end_folder_from_data_list = folder_path.strip().rsplit("/", 1)
            logging.info(
                f"######## end_folder_from_data_list: {end_folder_from_data_list}")

            if len(end_folder_from_data_list) > 1:

                end_folder_from_data = end_folder_from_data_list[1]

            elif len(end_folder_from_data_list) == 1:

                end_folder_from_data = end_folder_from_data_list[0]

            else:
                end_folder_from_data = ""

            if end_folder_from_data != "" and (end_folder_from_data in end_folders):

                filtered_folders.append(folder_path.strip())

    return filtered_folders

def insert_csv_into_db(files,tenant_id):

    try:
        logging.info(f"### CSV filepath is {files}")
        for file_path in files:
            df_csv=pd.read_csv(file_path)
            os.remove(file_path)
            db_config['tenant_id']=tenant_id
            extraction_db=DB("extraction",**db_config)
            extraction_db.insert(df_csv,'file_party_ids',if_exists='replace', index=False, method=None)
        logging.info(f"### CSV File Insertion Done")
    except Exception as e:
        logging.info(f"## Exception Occured in insert_csv_into_db .. {e}")

    return True

def get_files_from_priority_folders(filtered_folders,tenant_id):

    all_files = []

    for folder_path in filtered_folders:

        logging.info(f" ### Checking files in FOLDER:::::: {folder_path}")
        
        file_types = ['*.xls', '*.pdf','*.xlsx','*.docx','*.jpg']
        files = []
        for file_type in file_types:
            files.extend(list(Path(folder_path).rglob(file_type)))

        csv_files = list(Path(folder_path).rglob('*.csv'))
        logging.info(f"######## files: {files}")

        if len(files):
            logging.info(f"### Found files at path: {folder_path}")
            all_files += files
        if len(csv_files):
            insert_csv_into_db(csv_files,tenant_id)
            logging.info(f"### CSV File Found : {csv_files}")

    logging.info(f"##### ALL Files: {all_files}")
    return all_files


def get_priority_files(base_input_path,tenant_id):

    logging.info(f"base input path : {base_input_path}")
    io_configuration_db = DB('io_configuration', **db_config)
    query = "select * from folder_structure"
    priority_folders = io_configuration_db.execute(
        query).to_dict(orient='records')

    priority_folder_list = sorted(
        priority_folders, key=lambda d: d["priority_order"])

    all_folders_from_base_folder = []

    get_folder_paths(base_input_path, all_folders_from_base_folder)

    logging.info(f"#### priority_folder_list: {priority_folder_list}")

    for priority_folder in priority_folder_list:

        filtered_folders = filter_priority_input_folders(
            priority_folder, all_folders_from_base_folder)

        logging.info(f"#### filtered_folders:{filtered_folders}")

        priority_files = get_files_from_priority_folders(filtered_folders,tenant_id)

        logging.info(f"#### priority_files: {priority_files}")

        if len(priority_files):

            return priority_files
    return []


def generate_blob_data(file_path):
    try:
        logging.info("############ Converting the file to blob")
        file_name = file_path.rsplit("/", 1)[1]
        logging.info(f"generating blob for file {file_name}")
        file_blob = ""
        with open(file_path, 'rb') as f:
            logging.info(f"converting to blob for -----------{file_path}")
            file_blob = base64.b64encode(f.read())

        try:
            logging.debug('########### decoding with utf 8 ')
            return_blob = file_blob.decode('utf-8')
            
            return True, file_name, return_blob
        except Exception:
            message = 'Something went wrong while downloading report.'
            logging.exception(message)
            return False, "", " Error in decoding with utf 8 "

    except Exception as e:
        logging.warning("########### Error in Generating Blob Data")
        logging.exception(e)
        return False, "", " Error in Generating Blob Data"


def remove_all_except_al_num(file_full_name):
    to_return = re.sub('[,.!@#$%^&*()\-=`~\'";:<>/?]',
                       '', file_full_name.lower())
    to_return = to_return.replace(' ', '')
    return to_return


@app.route("/fetch_file_for_image", methods=['POST', 'GET'])
def fetch_file_for_image():
    data = request.json
    logging.info(f"Request Data for fetch file for image: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    file_name=data.get("file_name",None)
    db_config["tenant_id"] = tenant_id
    queue_db = DB("queues", **db_config)
    extraction_db=DB('extraction',**db_config)
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    try:
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
            service_name='folder_monitor_api',
            span_name='fetch_file_for_image',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
            try:
                variables = data.get("variables", {})
                download_type = variables["download_type"]
                additional_file_names=[]
                query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and state IS NULL"
                document_id_df = queue_db.execute_(query, params=[case_id])['document_id'].tolist()
                logging.info(f"document ids that we got is {document_id_df}")
            except:
                return{'flag':False,'message':f'{case_id} is missing in the table'}
            
            file_names=[]
            page_info={}
            main_file_names_=[]
            for document_id in document_id_df:
                #this condition will execute in the case of additional files
                # page_info_output_list = []
                query = f"select file_name from process_queue where document_id='{document_id}' and state IS NULL"
                query_data = queue_db.execute_(query)
                file_name_df=query_data['file_name'].to_list()
                try:
                    file_names=file_name_df[0]
                    file_name=file_names
                except:
                    file_names=json.loads(file_name_df[0])
                    file_name=file_names
                # try:
                #     page_info_details = list(query_data['single_doc_identifiers'])[0]
                #     page_info_details = ast.literal_eval(page_info_details)
                #     if len(page_info_details) > 0:
                #         for item in page_info_details:
                #             for page in range(item["start_page"], item["end_page"] + 1):
                #                 title = item["file_type"]
                #                 if title == '':
                #                     title = 'Not Detected'
                #                 page_info_output_list.append(
                #                     {"page": page, "title": title})
                #     page_info[main_file_names_]= page_info_output_list
                # except:
                #     pass
            # try:
            #     additional_file_names=[]
            #     if 'tab_view' in variables:  
            #         query1 = f"select `filemanager_file_name` from file_manager where case_id='{case_id}'"
            #         try:
            #             query1 = f"select `filemanager_file_name` from file_manager where case_id='{case_id}'"
            #             query_data = queue_db.execute_(query1)['filemanager_file_name']
            #             additional_file_names=json.loads(query_data[0])
            #             file_names+=additional_file_names
            #         except:
            #             query_data = queue_db.execute_(query1)['filemanager_file_name'].to_list()
            #             additional_file_names=(query_data[0])
            #             file_names.append(additional_file_names)
            #         logging.info(f"additional file_names are {additional_file_names}")
                    
                   
            # except:
            #     pass


            file_path_navigates = {}
            return_data = {}
            additional_file_data={}
            # logging.info(f"all files are: {file_names}")
            file_names=[file_name]
            for file_name in file_names:
                logging.info(f"processing for {file_name}")
                file_path = f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{file_name}'
                logging.info(f"file_path#####:  {file_path}")

                file_path_navigate = f'assets/pdf/{tenant_id}/{case_id}/{file_name}'
                file_path_navigates[file_name] = file_path_navigate

                logging.info(file_path_navigate)
                logging.info(file_path_navigates)

                if download_type == 'blob':
                    blob_status, file_name, file_blob = generate_blob_data(
                        file_path)

                    if not blob_status:
                        jsonify({"flag": False, "message": file_blob})

                    # return_data["file_name"]=file_name
                    try:
                        if additional_file_names:
                            additional_file_data[file_name] = {"blob": file_blob}
                        else:
                            return_data[file_name] = {"blob": file_blob}
                    except:
                        return_data[file_name] = {"blob": file_blob}

                elif download_type == "images":
                    
                    if file_name in additional_file_names:
                        logging.info(f"entered into with {file_name}")
                        query =f"select `filemanager_imagelist` from `file_manager` where `case_id` ='{case_id}' "
                        
                        try:
                            query_data = queue_db.execute_(query)['filemanager_imagelist'].to_list()
                            images_file_names = json.loads(query_data[0])
                            for file in images_file_names:
                                file=json.loads(file)
                                for key,value in file.items():
                                    if key == file_name:
                                        images_list=value
                        except:
                            query_data = queue_db.execute_(query)['filemanager_imagelist']
                            images_file_names = json.loads(query_data[0])
                            images_file_names =json.loads(images_file_names[0])
                            for key,value in images_file_names.items():
                                if key == file_name:
                                    images_list=value
                    else:
                        query =f"select `imagelist` from `process_queue` where `case_id` ='{case_id}'"
                        image_df=queue_db.execute_(query)
                        try:
                            images=image_df.to_dict(orient="records")
                            if images[0]['imagelist']:
                                images_=json.loads(images[0]['imagelist'])
                                try:
                                    images_list=json.loads(images_)[file_name]
                                except:
                                       images_list=images_[file_name]
                        except:
                            # have filepath
                            file_name = Path(file_path).name
                            wkspFldr = f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/images'
                            
                            logging.info(f"folder_path is {wkspFldr}")
                            
                            only_file_name = remove_all_except_al_num(file_name)
                            images_list_ = glob.glob(f'{wkspFldr}/*.jpg')
                            # logging.info(f"list of images for this file is {images_list_}")
                            images_list = [Path(p).name for p in images_list_]
                            images_list = sorted(images_list, key=lambda x: (len(x), x))
                            
                        

                logging.info(f"list of images for the file {file_name} is {images_list}")
                return_data[file_name]=images_list
                    # return it
                #checks for additional files or document set and returns the data seperately
                try:
                    if len(return_data[file_name]) == 0:
                        blob_status, file_name, file_blob = generate_blob_data(
                            file_path)

                        if not blob_status:
                            jsonify({"flag": False, "message": file_blob})

                        
                        return_data[file_name] = {"blob": file_blob}
                except Exception as e:
                    logging.info(f"image data is not empty {e}")
                    pass
            qry=f"""select party_id,due_date,date_stat,customer_name from ocr where case_id='{case_id}'"""
            df=extraction_db.execute_(qry).to_dict(orient="records")
            #case_details = df[0] if df else {}
            if df:
                row = df[0]
                case_details = {k.lower(): v for k, v in row.items()}
            else:
                case_details = {}
            logging.info(f"end of fetch file additional file data is {additional_file_data} and main file data is {return_data} ")
            return_data_ = {"flag": True, "data": return_data, "file_path_navigate": file_path_navigates, "pageInfo": page_info, "additonal_files_data" : additional_file_data,"case_details":case_details}

    except Exception as e:
        logging.info(f"something went wrong {e}")
        return_data_ = {"flag": False, "message": "Error in fetching file images"}
        
    try:
        memory_after = measure_memory_usage()
        memory_consumed = (memory_after - memory_before) / \
            (1024 * 1024 * 1024)
        end_time = tt()
        memory_consumed = f"{memory_consumed:.10f}"
        logging.info(f"checkpoint memory_after - {memory_after},memory_consumed - {memory_consumed}, end_time - {end_time}")
        time_consumed = str(round(end_time-start_time,3))
    except:
        logging.info("Failed to calc end of ram and time")
        logging.info("ram calc went wrong")
        memory_consumed = None
        time_consumed = None
        pass
    # logging.info(return_data_)

    # # Rearranging the "mail_body.pdf" to be the second item in the "data" dictionary
    # if 'mail_body.pdf' in return_data_.get('data',None):
    #     return_data_["last_index"]=['mail_body.pdf']

    # logging.info(return_data_)

    logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
    return return_data_

def generate_reference_id():
    timestamp = int(time.time() * 1000)  # Get current timestamp in milliseconds
    random_num = random.randint(1000, 9999)  # Generate a random 4-digit number
    reference_id = f"{timestamp}-{random_num}"
    return reference_id

@app.route('/convert_jpg_pdf', methods=['POST', 'GET'])
def convert_jpg_pdf():
    data = request.json
    case_id = data['case_id']
    tenant_id=data['tenant_id']
    db_config['tenant_id'] = tenant_id
    tenant_id = data.get('tenant_id', None)
    try:
        file_saving_path_case_id=Path(f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}')
        extraction_db = DB('queues', **db_config)
        query = f"select `file_name` from `process_queue` where case_id ='{case_id}'"
        query_data = extraction_db.execute_(query)['file_name'].to_list()[0]
        try:
            filename=json.loads(query_data)[0]
        except:
            filename=query_data

        if '.jpg' in filename:
            img_path=str(file_saving_path_case_id / filename)
            image = Image.open(img_path)
            pdf = canvas.Canvas(str(file_saving_path_case_id / filename.split('.')[0])+'.pdf', pagesize=image.size)
            pdf.drawInlineImage(img_path, 0, 0, width=image.size[0], height=image.size[1])
            pdf.save()
            logging.debug(f"Successfully made pdf file in the path {str(file_saving_path_case_id / filename.split('.')[0])+'.pdf'}") 
            filename=str(filename.split('.')[0]+'.pdf')
            logging.info(F"filename is {filename}")
            query = f"update `process_queue` set `file_name`='{filename}' where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)
        elif '.xlsx' in filename or '.xls' in filename:
            convert_docx_to_pdf(str(file_saving_path_case_id / filename), str(file_saving_path_case_id / filename.split('.')[0])+'.pdf')
            filename=str(filename.split('.')[0]+'.pdf')
            logging.info(F"filename is {filename}")
            query = f"update `process_queue` set `file_name`='{filename}' where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)
        elif '.docx' in filename:
            convert_xlsx_to_pdf(str(file_saving_path_case_id / filename), str(file_saving_path_case_id / filename.split('.')[0])+'.pdf')
            filename=str(filename.split('.')[0]+'.pdf')
            logging.info(F"filename is {filename}")
            query = f"update `process_queue` set `file_name`='{filename}' where case_id ='{case_id}'"
            query_data = extraction_db.execute_(query)

        os.system(f'chmod -R 755 {file_saving_path_case_id}')


    except Exception as e:
        logging.exception(e)
        logging.info(f"########## error in converting into pdf")
    return  jsonify({"flag":True,"data":{"message":"done saving to pdf"}})


def convert_docx_to_pdf(input_docx, output_pdf):
    try:
        result = subprocess.run(['unoconv', '-f', 'pdf', '-o', output_pdf, input_docx], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        output = result.stdout.decode('utf-8')  # Decode bytes to string
        print("Conversion successful!")

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8')  # Decode error bytes to string
        print(f"Conversion failed: {e}")
        print(error_output)  # Print error output for debugging


def convert_xlsx_to_pdf(input_xlsx, output_pdf):
    try:
        result = subprocess.run(['unoconv', '-f', 'pdf', '-o', output_pdf, input_xlsx], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        output = result.stdout.decode('utf-8')  # Decode bytes to string
        print("Conversion successful!")

    except subprocess.CalledProcessError as e:
        error_output = e.stderr.decode('utf-8')  # Decode error bytes to string
        print(f"Conversion failed: {e}")
        print(error_output)  # Print error output for debugging


@app.route('/upload_files', methods=['POST', 'GET'])
def upload_files():
    """
    @author: Kalyani Bendi
    @modifier: Gopi Teja B
    @built at: Am Bank Project Time
    @description: upload of single or multiple files from front end and start the flow 
    """

    data = request.json
    logging.info(f"Request Data is: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    request_payload_data = data

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    trace_id = generate_random_64bit_string() if case_id is None else case_id
    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='folder_monitor_api',
        span_name='upload_files',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        variables = data.get('variables', {})
        case_creation_details_dict = data.get('fields', {})
        fields_changed = data.get('field_changes',[])
        position = variables.get('position','')

        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)
        camunda_port = variables.get('camunda_port',8080)
        camunda_host = variables.get('camunda_host','camundaworkflow')

        deleted_list=[]

        try:
            document_set = data['fields']['casecreation_file_names']
            docset_file_names = list(document_set.keys())
            logging.info(f"==> file names are {docset_file_names}")

            # Generating the case id or fin d the case_id
            case_entry = variables.get('case_entry', None)
            if case_entry == 'New_Case':
                unique_id = generate_caseid(
                    tenant_id, 'case_id', 'process_queue', 'queues')
                logging.info(
                    f"New main case id was generated and case id is {unique_id}")

                # Creating a folder with case id in htdocs
                output_path = Path(
                    f'/app/{tenant_id}/assets/pdf/{tenant_id}')
                os.umask(0)
                logging.info(output_path)
                Path(str(output_path / unique_id)).mkdir(parents=True, exist_ok=True)
                os.chmod(str(output_path / unique_id), 0o755)
                logging.debug(
                    f"## FM checkpoint `folder created in lampp directory with {unique_id}`")
                Path(str(output_path / unique_id / 'images')
                    ).mkdir(parents=True, exist_ok=True)  # creating a image folder
            # if case is already existed for the document set
            else:
                unique_id = data.get('case_id', None)

            ocr_dict = {'case_id': unique_id, 'highlight': '{}'}
            br_comparison_rules_dict = {'case_id': unique_id}
            process_queue_dict = {'case_id': unique_id, 'last_updated_by': "System",
                                'completed_processes': 1, 'status': "File Uploaded"}
            process_file_dict = {'case_id': unique_id}

            # if files blob are found create files in specified directory and generate document_id for each file
            if docset_file_names:
                for file_name,blob_data in document_set.items():
                    file_object={}
                    if '.pdf' not in file_name and file_name != 'deletedFile':
                        logging.info(f"File name is something wrong pdf is not there received filename is {file_name}")
                        return {"flag":False, "message": f"File is not in supportive format {file_name}. Please upload pdf file."}
                    
                    elif 'blob' in blob_data:
                        input_path_ = f"/app/{tenant_id}/assets/pdf/{tenant_id}/{unique_id}/{file_name}"
                        input_path_ = Path(input_path_)
                        logging.info(f"files are stored in: {input_path_}")

                        doc_blob_data = document_set[file_name]['blob']
                        doc_blob_data = doc_blob_data.split(',', 1)[1]
                        doc_blob_data += '='*(-len(doc_blob_data) % 4)

                        with open(input_path_, 'wb') as f:
                            f.write(base64.b64decode(doc_blob_data))
                            f.close()
                        logging.debug(f'File successfully moved to {input_path_}')
                        file_object=convert_pdf_jpg(Path(input_path_),
                                            f'/app/{tenant_id}/assets/pdf/{tenant_id}/{unique_id}')
                        file_object=json.dumps(file_object)
                        logging.info(f"images are created result is {file_object}")

                        logging.debug('File successfully moved')
                        document_unique_id = generate_caseid(
                            tenant_id, 'document_id', 'process_queue', 'queues')

                        logging.info(f"document id is generated for {file_name}")
                        case_creation_details_dict['casecreation_case_status'] = 0
                        case_creation_details_dict['case_id'] = unique_id
                        case_creation_details_dict['document_id'] = document_unique_id
                        case_creation_details_dict['casecreation_file_names'] = file_name
                        case_creation_details_dict['casecreation_updated_by'] = user
                        extraction_db.insert_dict(
                            data=case_creation_details_dict, table='case_creation_details')

                        process_queue_dict['document_id'] = document_unique_id
                        process_queue_dict['file_name'] = file_name
                        process_queue_dict['file_paths'] = input_path_
                        process_queue_dict['status'] = "Files got uploaded"
                        process_queue_dict['imagelist'] = json.dumps(file_object)
                        queue_db.insert_dict(
                            data=process_queue_dict, table='process_queue')

                        process_file_dict['document_id'] = document_unique_id
                        process_file_dict['file_name'] = file_name
                        queue_db.insert_dict(
                            data=process_file_dict, table='process_file')

                        ocr_dict['document_id'] = document_unique_id
                        extraction_db.insert_dict(data=ocr_dict, table='ocr')

                        extraction_db.insert_dict(data=br_comparison_rules_dict, table='br_comparison_rules')

                        logging.info(
                            f"{document_unique_id} is generated for {file_name}")

                    #updating file status as deleted while reuploading
                    elif file_name == 'deletedFile':
                        deleted_list = document_set['deletedFile']
                        logging.info(f"deleted files are: {deleted_list}")

                        if len(deleted_list) == 1:
                            ccd_query = f"UPDATE case_creation_details SET casecreation_file_status = 'file deleted',casecreation_updated_by='{user}' WHERE casecreation_file_names = '{deleted_list[0]}' AND case_id = '{unique_id}' "
                            extraction_db.execute_(ccd_query)
                            pq_query = f"UPDATE process_queue SET state = 'file deleted' WHERE file_name = '{deleted_list[0]}' AND case_id = '{unique_id}'"
                            queue_db.execute_(pq_query)
                        elif len(deleted_list) > 1:
                            deleted_list_data = tuple(deleted_list)
                            ccd_query = f"UPDATE case_creation_details SET casecreation_file_status = 'file deleted',casecreation_updated_by='{user}' WHERE casecreation_file_names IN {deleted_list_data} AND case_id = '{unique_id}'"
                            extraction_db.execute(ccd_query)

                            pq_query= f"UPDATE process_queue SET state = 'file deleted' WHERE file_name IN {deleted_list_data} AND case_id = '{unique_id}'"
                            queue_db.execute(pq_query)

                            logging.info(f"file status  for case_id {unique_id} got updated in case_creation_details and process_queue table as deleted")
                        else:
                            logging.info(f"## FM no files in deleted_list, came to else condition - {deleted_list}")

                message = "Files Successfully Ingested"
                logging.info(message)

            else:
                message = "No files are received"
                logging.info(message)

            # below logic works if required to hit the flow directly from route call instead of monitoring / scheduler
            if "camunda_host" not in variables or "camunda_port" not in variables or "workflow_name" not in variables:
                logging.debug("camunda_host / camunda_port / workflow_name not configured in variables")
                return {'flag':'False','message':'camunda_host / camunda_port / workflow_name not configured in variables'}

            elif variables is not None and "workflow_name" in variables and position == 'First_Time' and "camunda_host" in variables and "camunda_port" in variables:
                workflow_name = variables.get("workflow_name", None)
                api_params = {"variables": {"case_id": {"value": unique_id},"session_id": {"value": session_id},"tenant_id": {"value": tenant_id},"camunda_host": {"value":camunda_host},"camunda_port": {"value":camunda_port},"user": {"value":user}}}
                request_api = f"http://{camunda_host}:{camunda_port}/rest/engine/default/process-definition/key/{workflow_name}/start"
                headers = {'Content-type': 'application/json; charset=utf-8'}
                logging.info(f"#### FM Hitting the camunda api of {request_api}, api_params = {api_params} ")
                response = requests.post(
                    request_api, json=api_params, headers=headers)
                response_dict = json.loads(response.text)
                logging.info(f"#### Response Dict {response_dict}")


            #updating the changes that are done in reupload poup into the database 
            updated_changes=variables.get("update_changes",None)
            logging.info(f"updated changes {updated_changes} and length of fields changes {len(fields_changed)}")
            if updated_changes == 'True' and len(fields_changed) > 0:
                update = {field: case_creation_details_dict[field] for field in fields_changed  if field != 'casecreation_file_names'}
                where = {
                    'case_id': unique_id
                }
                if update != {}:
                    extraction_db.update('case_creation_details', update=update, where=where)
                    logging.info(f"details got updated in case creation details")
            message="Files uploaded successfully"
            return_data = {'flag': True, 'data':{'document set': docset_file_names,'deleted files' : deleted_list,'message': message,'previous_case_id': unique_id}}

        except Exception:
            case_creation_details_dict['casecreation_case_status'] = 0
            extraction_db.insert_dict(
                data=case_creation_details_dict, table='case_creation_details')
            logging.exception("Something went wrong in upload files")
            return_data = {'flag': False, 'message': 'Something went wrong in uploading files', 'data':{}}

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
        request_payload_data.pop('fields')
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        # insert audit

        audit_data = {"tenant_id": tenant_id, "user": user, "case_id": unique_id, 
                        "api_service": "upload_files", "service_container": "folder_monitor", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(request_payload_data), 
                        "response_data": json.dumps(return_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['flag'])}
        try:
            insert_into_audit(case_id, audit_data)
        except:
            logging.info(f"issue with the insert query")
        return return_data

## While click on the tab name in the left side file  preview it will automatically go to the respected page
@app.route("/tab_navigate", methods=['POST', 'GET'])
def tab_navigate():
    try:
        data = request.json
        logging.info(f"##### tab_navigate: {data}")
        tenant_id = data.get("tenant_id", "")
        case_id = data.get("case_id", "")
        file_name=data.get("file_tab_name", "")
        tab_name = data.get("tab_name", "")
        logging.info(f" tab_name {tab_name}")
        db_config['tenant_id'] = tenant_id
        
        queue_db = DB("queues", **db_config)

        query = f'select `single_doc_identifiers` from `process_queue` where `case_id`="{case_id}" and `file_name`="{file_name}"'
        tab_names = queue_db.execute_(query)['single_doc_identifiers'][0]
        logging.info(f" tab_names is {tab_names}")
        tab_names = json.loads(tab_names)
        logging.info(f" tab_names is {tab_names}")
        logging.info(f" tab_names is {type(tab_names)}")
        
        return_data = {}                 
        for each in tab_names:
            logging.info(f"each: {each}")
            logging.info(f"each['file_type']: {each['file_type']}")
            if tab_name.lower() in each['file_type'].split('_')[0].lower():
                logging.info(f"comes to if")
                return_data = each
                break
        if return_data == {}:
            return jsonify({'flag': False, 'message': f"Could not find this '{tab_name}' file"})
                

        return jsonify({"flag": True, "data": return_data})
    
    except Exception as e:
        logging.info(f"=========> tab_navigate {e}")
        return jsonify({'flag': False, 'message': "Something went wrong"})

@app.route('/supporting_additional_files', methods=['POST', 'GET'])
def supporting_additional_files():
    """
    @author: Kalyani Bendi
    @built at: Am Bank Project Time
    @description: upload of additional files from front end and storing it in database 
    """
    data = request.json
    logging.info(f"Request Data is: {data}")
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    request_payload_data = data
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
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
        service_name='folder_monitor_api',
        span_name='supporting_additional_files',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        variables = data.get('variables', {})
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        try:
            fields = data.get('fields',{})
            comments = None
            additional_file_names = []
            if fields != {}:
                additional_files = fields.get('filemanager_file_name',None)
                additional_file_names = list(additional_files.keys())
                comments = fields.get('filemanager_comments', None)
            logging.info(f"==>additional file names are {additional_file_names}")

            query_check_case = f"select * from file_manager where case_id='{case_id}'"
            result_check_case = queue_db.execute(query_check_case).to_dict(orient='records')
            logging.info(f"checking for case existence {result_check_case}")
            
            if len(result_check_case) == 0:
                file_names_list = []
                imagelist_list = []
                insert_data = { 'case_id' : case_id,
                                'filemanager_stage' : 'additional_files'
                            }
                queue_db.insert_dict(
                        insert_data, 'file_manager')
                logging.info(f"successfully inserted case id and stage into file manager")

            else:
                file_names_list =  ast.literal_eval(result_check_case[0]['filemanager_file_name'])
                imagelist_list = ast.literal_eval(result_check_case[0]['filemanager_imagelist'])
                additional_file_names = list(set(additional_file_names) - set(file_names_list))
                logging.info(f"case already exists additonal files are {additional_file_names}")
            try:
                if additional_file_names:
                    for file_name,blob_data in additional_files.items():
                        if 'blob' in blob_data:
                            input_path_ = f"/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{file_name}"
                            input_path_ = Path(input_path_)
                            logging.info(f"files are stored in: {input_path_}")

                            doc_blob_data = additional_files[file_name]['blob']
                            doc_blob_data = doc_blob_data.split(',', 1)[1]
                            doc_blob_data += '='*(-len(doc_blob_data) % 4)

                            with open(input_path_, 'wb') as f:
                                f.write(base64.b64decode(doc_blob_data))
                                f.close()
                            logging.debug(f'File successfully moved to {input_path_}')

                            file_object=convert_pdf_jpg(Path(input_path_),
                                            f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}')
                            file_object=json.dumps(file_object)
                            logging.info(f"images are created result is {file_object}")
                            file_names_list.append(file_name)
                            imagelist_list.append(file_object)

                    update_data = { 'filemanager_imagelist' : json.dumps(imagelist_list),
                                    'filemanager_file_name' : json.dumps(file_names_list),
                                    'filemanager_comments' : comments
                                }

                    queue_db.update(table='file_manager', update = update_data,
                                    where={'case_id': case_id})
                    
                    logging.info(f"Additional files successfully stored in file_manager table")
                    
                return_data = {'flag': True, 'data':{'message': 'Successfully uploaded','additional_files': additional_file_names}}
            except:
                logging.info(f"something went wring in accessing additional files")
                return_data = {'flag': False, 'data':{'message': 'Something went wrong in accessing additional files'}}
        
        except Exception:
        
            logging.exception("Something went wrong in uploading additional files")
            return_data = {'flag': False, 'message': 'Something went wrong in uploading additional files'}
        
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
        
        logging.info(f"## TD checkpoint memory_consumed: {memory_consumed}, time_consumed: {time_consumed}")
        request_payload_data.pop('fields')
        audit_data = {"tenant_id": tenant_id, "user": user, "case_id": case_id, 
                        "api_service": "upload_files", "service_container": "folder_monitor", "changed_data": None,
                        "tables_involved": "","memory_usage_gb": str(memory_consumed), 
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(request_payload_data), 
                        "response_data": json.dumps(return_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(return_data['flag'])}
        try:
            insert_into_audit(case_id, audit_data)
        except:
            logging.info(f"issue in the insert query")
        return return_data

def extract_numeric_value(value):
    if isinstance(value, str):
        cleaned_value = re.sub(r'[^\d.]', '', value)  # Remove non-numeric characters except dot
        return pd.to_numeric(cleaned_value, errors='coerce')
    return None

def create_dataframe_from_json(json_data_list):
    columns_data = {}
    nested_keys = ["Debtors 121-150 days", "Debtors 151-180 days", "Debtors > 180 days", "TOTAL_Debitors"]  # Define your nested keys here

    try:
        for idx, json_data in enumerate(json_data_list):
      
            for key, value in json_data.items():
                if value is not None:
                    data_dict = json.loads(value)
                    for nested_key, nested_value in data_dict.items():
                        if 'DEBITORS STATEMENT' in key:
                            column_name = f"{key}_{nested_key}"
                            print(column_name,'-----------')
                        else:
                            column_name = f"{nested_key}"
                        
                        if column_name not in columns_data:
                            columns_data[column_name] = []
                        numeric_value = extract_numeric_value(nested_value)
                        columns_data[column_name].append(numeric_value)
                else:
                    # If the value is None, fill the columns with None or 0, depending on your preference
                    for nested_key in nested_keys:
                        column_name = f"{nested_key}"
                        if column_name not in columns_data:
                            columns_data[column_name] = []
                        columns_data[column_name].append(None)

        # Ensure all lists have the same length by filling with None
        max_length = max(len(data) for data in columns_data.values())
        for key, data in columns_data.items():
            data.extend([None] * (max_length - len(data)))
            
            
        
        df = pd.DataFrame(columns_data)
        df = df.dropna(how='all')
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


@app.route('/dropdown_from_db', methods=['POST', 'GET'])
def dropdown_from_db():
    data = request.json
    logging.debug(f'Data: {data}')
    tenant_id = data.get('tenant_id', None)
    variables = data.get('variables', None)
    database = str(variables['database'])
    column = str(variables['column'])
    table = str(variables['table'])
    case_id = data.get('case_id', None)

    db_config['tenant_id'] = tenant_id
    access_db = DB(database, **db_config)
    try:
        query = f"select DISTINCT({column}) as sample from `{table}` where {column} IS NOT NULL and case_id = '{case_id}'"
        df = access_db.execute_(query)
        opts = [{'display_name': opt, 'value': opt}
                for opt in sorted(list(df['sample'].unique()))]
        return {'flag': True, 'options': opts}
    except Exception:
        logging.exception('Error fetching dropdown values')
        return {'flag': True, 'message': 'Could not fetch values'}

# Decrypt the password
def decrypt_password(encrypted_password):
    key=os.environ.get('CADPRO_CRE_KEY',None)
    fernet = Fernet(key)
    decrypted_password = fernet.decrypt(encrypted_password).decode()
    return decrypted_password 

# def escape_sql(value: str) -> str:
#     return value.replace("'", "''")

def escape_sql(value: str) -> str:
    return value.replace("'", "''").replace("&", "' || CHR(38) || '")


def sanitize_filename_basic(filename: str) -> str:
    filename = unicodedata.normalize("NFKC", filename)
    # Replace known invisible characters
    for hidden in ['\x00','\x01','\x02','\x03','\x04','\x05','\x06','\x07','\x08','\x09','\x0a','\x0b','\x0c','\x0d','\x0e','\x0f','\x10','\x11','\x12','\x13','\x14','\x15','\x16','\x17','\x18','\x19','\x1a','\x1b','\x1c','\x1d','\x1e','\x1f','\x7f','\xa0','\u1680','\u2000','\u2001','\u2002','\u2003','\u2004','\u2005','\u2006','\u2007','\u2008','\u2009','\u200a','\u202f','\u205f','\u3000','\u200b','\u200c','\u200d','\u2060','\ufeff','\u200e','\u200f','\u202a','\u202b','\u202c','\u202d','\u202e','\u2066','\u2067','\u2068','\u2069','\u2018','\u2019','\u201c','\u201d','\u2013','\u2014','\u2212']:
        filename = filename.replace(hidden, '')
    cleaned = ''
    for ch in filename:
        if (32 <= ord(ch) <= 126 or ch in ' _-.()[]{}') and not unicodedata.category(ch).startswith(('C', 'Zl', 'Zp')):
            cleaned += ch
        else:
            cleaned += '_'
    while '  ' in cleaned:
        cleaned = cleaned.replace('  ', ' ')
    while '__' in cleaned:
        cleaned = cleaned.replace('__', '_')
    cleaned = cleaned.replace(' ', '_')
    cleaned = cleaned.strip(' _')
    return cleaned

 

# def convert_to_pdf(input_file: str, output_pdf: str) -> bool:
#     try:
#         ext = input_file.rsplit('.', 1)[-1].lower()
#         #df = pd.read_csv(input_file) if ext == 'csv' else pd.read_excel(input_file)
#         if ext == 'csv':
#             df = pd.read_csv(input_file)
#         # elif ext in ('xls', 'xlsx'):
#         #     try:
#         #         df = pd.read_excel(input_file, engine='xlrd' if ext == 'xls' else None)
#         #     except Exception:
#         #         df = pd.read_excel(input_file)  # Fallback
#         elif ext in ('xls', 'xlsx'):
#             # force engine based on extension
#             engine = 'xlrd' if ext == 'xls' else 'openpyxl'
#             df = pd.read_excel(input_file, engine=engine)

#         else:
#             logging.error(f"Unsupported extension: {ext}")
#             return False

#         if df.empty:
#             logging.error(f"No data found in file: {input_file}")
#             return False

#         canvas_obj = canvas.Canvas(output_pdf, pagesize=A4)
#         width, height = A4
#         text = canvas_obj.beginText(30, height - 40)
#         text.setFont("Helvetica", 8)

#         for line in df.to_string(index=False).split('\n'):
#             text.textLine(line)
#             if text.getY() < 40:
#                 canvas_obj.drawText(text)
#                 canvas_obj.showPage()
#                 text = canvas_obj.beginText(30, height - 40)
#                 text.setFont("Helvetica", 8)

#         canvas_obj.drawText(text)
#         canvas_obj.save()
#         return True
#     except Exception as err:
#         logging.error(f"Conversion failed for {input_file}: {err}")
#         return False



def convert_to_pdf(input_file: str, output_pdf: str) -> bool:
    try:
        cmd = [
            "libreoffice", "--headless", "--convert-to", "pdf",
            "--outdir", os.path.dirname(output_pdf), input_file
        ]
        subprocess.run(cmd, check=True)
        gen_pdf = os.path.join(os.path.dirname(output_pdf),
                               os.path.splitext(os.path.basename(input_file))[0] + ".pdf")
        if os.path.exists(gen_pdf):
            os.rename(gen_pdf, output_pdf)
        if os.path.exists(output_pdf) and os.path.getsize(output_pdf) > 1000:
            return True
        else:
            return False
    except Exception as e:
        logging.error(f"Error converting {input_file} to PDF: {e}")
        return False

 


def insert_bulk_file_data(records, db_conn):
    if not records:
        logging.info("No records to insert.")
        return

    insert_lines = []
    for rec in records:
        try:
            party_id, initial_file_name, final_file_name, merged_file_count, file_received_datetime, due_date = rec
            formatted_ts = file_received_datetime.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
            line = (
                f"  INTO file_data (party_id, initial_file_name, final_file_name, merged_file_count, file_received_datetime, due_date) "
                f"VALUES ('{escape_sql(party_id)}', '{escape_sql(initial_file_name)}', '{escape_sql(final_file_name)}', {merged_file_count}, "
                f"TO_TIMESTAMP('{formatted_ts}', 'DD-MON-RR HH.MI.SS.FF6 AM'), TRIM('{escape_sql(due_date)}'))"
            )
            insert_lines.append(line)
        except Exception as e:
            logging.warning(f"Skipping insert for record {rec}: {e}")
    batch_size = 500
    total_records = len(insert_lines)
    num_batches = math.ceil(total_records / batch_size)
    for i in range(num_batches):
        batch = insert_lines[i * batch_size : (i + 1) * batch_size]
        bulk_query = f"INSERT ALL\n{chr(10).join(batch)}\nSELECT * FROM dual"
        try:
            db_conn.execute_(bulk_query)
            logging.info(f"Inserted {len(batch)} records into audit_report_case")
        except Exception as e:
            logging.error(f"Failed bulk insert for batch {i+1}/{num_batches}: {e}")
            logging.exception(e)
            continue
    #if insert_lines:
    #    insert_all_sql_query = f"INSERT ALL\n{chr(10).join(insert_lines)}\nSELECT * FROM dual"
    #    db_conn.execute_(insert_all_sql_query)
    #    logging.info(f"Inserted {len(insert_lines)} records successfully into file_data.")


def normalize_filename(filename: str) -> str:
    base, ext = os.path.splitext(filename.replace(' ', '_'))
    return base + ext.lower()

@app.route('/get_files_from_sftp', methods=['POST', 'GET'])
# def get_files_from_sftp():
#     """
#     Description: This route helps to get files from one server to another server/current server
#     Author: Gopi Teja B
#     Date: 30 Apr 2024
#     """
#     try:
#         memory_before = measure_memory_usage()
#         start_time = tt()
#     except Exception:
#         logging.warning("Failed to start ram and time calc")
#         pass
    
#     trace_id = generate_random_64bit_string()
#     tenant_id = os.environ.get('TENANT_ID',None)
#     db_config['tenant_id'] = tenant_id
#     queues_db=DB('queues',**db_config)

#     attr = ZipkinAttrs(
#         trace_id=trace_id,
#         span_id=generate_random_64bit_string(),
#         parent_span_id=None,
#         flags=None,
#         is_sampled=False,
#         tenant_id=tenant_id
#     )

#     with zipkin_span(
#             service_name='folder_monitor',
#             span_name='get_files_from_sftp',
#             transport_handler=http_transport,
#             zipkin_attrs=attr,
#             port=5010,
#             sample_rate=0.5):

#         data = request.json
#         logging.info(f"### FM Request data of get_files_from_sftp: {data}")

#         try:
#             source_pdf_files=[]
#             # Get the sftp server details from env file / docker-compose.yml
#             hostname = os.environ.get('SFTP_SERVER',None)
#             # port = int(os.environ.get('SFTP_PORT',None))
#             username = os.environ.get('SFTP_USERNAME',None)
#             # key_file = f"/app/sftp_key_file/{os.environ['SFTP_KEY_FILENAME']}"
#             enc_password = os.environ.get('CADPRO_PASSWORD',None)
#             password=decrypt_password(enc_password)
#             source_file_path = os.environ.get('SFTP_SOURCE_FILEPATH',None)

#             if hostname is None:
#                 return {"flag":False, "message": "Required SFTP parameters are missing"}

#             destination_file_path = '/app/input/hdfc/normal/input'

#             # Connect to the SFTP server get the list of pdf files in source path and move to dest path
#             cnopts = pysftp.CnOpts()
#             cnopts.hostkeys = None

#             with pysftp.Connection(hostname, username=username, password=password, cnopts=cnopts) as sftp:   
#                 sftp.cwd(source_file_path)
#                 files = sftp.listdir()
#                 source_pdf_files = [file for file in files if file.lower().endswith(('.pdf','.xls','.xlsx','.csv'))]
#                 processed_files=[]
#                 logging.info(f"List of pdf files found in source path: {source_pdf_files}")
#                 processed_files=[]


#                 for filename in source_pdf_files:
#                     name,ext=os.path.splitext(filename)
#                     ext=ext.lower()
#                     new_filename=name+ext
#                     source_file_path_ = source_file_path + "/" + filename
#                     new_filename=new_filename.replace(' ','_')
#                     new_filename=sanitize_filename_basic(new_filename)
#                     destination_file_path_ = destination_file_path + "/" + new_filename
#                     logging.info(f"source file: {source_file_path_}, destination file: {destination_file_path_}")
#                     sftp.chmod(destination_file_path,0o777)
#                     sftp.get(source_file_path_, destination_file_path_)
#                     sftp.remove(filename)
#                     processed_files.append(new_filename)
#                     logging.info("Successfully received file")
#             response_data = {"flag": True,"data":{"message":"Copied file from SFTP server"}}

#             # try:
#             #     if processed_files:
#             #         # Adjust time for IST (UTC+5:30)
#             #         file_datetime = datetime.now() + timedelta(hours=5, minutes=30)

#             #         data_to_insert = [(filename, "File Uploaded", file_datetime) for filename in processed_files]
#             #         insert_query = 'INSERT INTO folder_monitor_table (file_name, current_status, created_date) VALUES (%s, %s, %s)'
                    
#             #         queues_db.executemany(insert_query, data_to_insert)  # Assuming queues_db has an executemany method
#             #         logging.info(f"Successfully logged {len(processed_files)} files to folder_monitor_table with timestamps.")
#             #     else:
#             #         logging.info("No files were processed to log to folder_monitor_table.")
#             # except Exception as e:
#             #     logging.exception("## Exception: Failed to insert processed files into folder_monitor_table.", exc_info=True)
#             #     pass

#             try:
#                 if processed_files:
#                     current_ist = datetime.now(pytz.timezone(tmzone))
#                     timestamp_str = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
#                     logging.info(f"####currentTS now is {timestamp_str}")# Optional: generate SQL for SQL Developer
#                     into_lines = "\n".join(
#                             f"  INTO folder_monitor_table (file_name, current_status, created_date) "
#                             #f"VALUES ('{filename}', 'File Uploaded', '{timestamp_str}')"
#                             f"VALUES ('{escape_sql(filename)}', 'File Uploaded', '{timestamp_str}')"
#                             for filename in processed_files
#                             )
#                     insert_all_sql = f"INSERT ALL\n{into_lines}\nSELECT * FROM dual"
#                     logging.info("Insert SQL for SQL Developer:\n%s", insert_all_sql)
#                     queues_db.execute_(insert_all_sql)
#                     logging.info(f"Successfully logged {len(processed_files)} files to folder_monitor_table.")
#                 else:
#                     logging.info("No files were processed to log to folder_monitor_table.")
#             except Exception as e:
#                 logging.exception("## Exception: Failed to insert processed files into folder_monitor_table.", exc_info=True)
#                 logging.exception(ManageException(e).resolve())
#                 pass


#         except Exception as e:
#             logging.exception(ManageException(e).resolve())
#             response_data = {"flag": False,"message":f"Copied file from SFTP server","data":{}}
#             audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
#                         "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(source_pdf_files),
#                         "tables_involved": "","request_payload": json.dumps(data),
#                         "response_data": json.dumps(response_data['message']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
#             mode = os.environ.get('MODE')
#             if mode in ('UAT','PROD','PRE-PROD','STANDBY'):
#                 insert_into_audit('', audit_data)
#             else:
#                 pass
        
#         try:
#             memory_after = measure_memory_usage()
#             memory_consumed = (memory_after - memory_before) / \
#                 (1024 * 1024 * 1024)
#             end_time = tt()
#             time_consumed = str(end_time-start_time)
#             memory_consumed = f"{memory_consumed:.10f}"
#             time_consumed = str(round(end_time-start_time, 3))
#         except:
#             logging.warning("Failed to calc end of ram and time")
#             logging.exception("ram calc went wrong")
#             memory_consumed = None
#             time_consumed = None
#             pass

#         # insert audit
#         if len(source_pdf_files) >= 1:
#             audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
#                         "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(source_pdf_files),
#                         "tables_involved": "", "memory_usage_gb": str(memory_consumed),
#                         "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
#                         "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
#             insert_into_audit('', audit_data)

#         return response_data



# def get_files_from_sftp():
#     try:
#         memory_before = measure_memory_usage()
#         start_time = tt()
#     except Exception:
#         logging.warning("Failed to start ram and time calc")
#         pass
    
#     trace_id = generate_random_64bit_string()
#     tenant_id = os.environ.get('TENANT_ID',None)
#     db_config['tenant_id'] = tenant_id
#     queues_db=DB('queues',**db_config)

#     attr = ZipkinAttrs(
#         trace_id=trace_id,
#         span_id=generate_random_64bit_string(),
#         parent_span_id=None,
#         flags=None,
#         is_sampled=False,
#         tenant_id=tenant_id
#     )

#     with zipkin_span(
#             service_name='folder_monitor',
#             span_name='get_files_from_sftp',
#             transport_handler=http_transport,
#             zipkin_attrs=attr,
#             port=5010,
#             sample_rate=0.5):

#         data = request.json
#         logging.info(f"### FM Request data of get_files_from_sftp: {data}")

#         source_pdf_files = []
#         processed_files = []
#         insert_records = []
#         failed_files = []

#         try:
#             hostname = os.environ['SFTP_SERVER']
#             username = os.environ['SFTP_USERNAME']
#             encrypted_password = os.environ['CADPRO_PASSWORD']
#             password = decrypt_password(encrypted_password)
#             source_path = os.environ['SFTP_SOURCE_FILEPATH']
#             destination_path = '/app/input/hdfc/normal/input'
#             temp_path = '/app/input'
#             file_received_time = datetime.now() + timedelta(hours=5, minutes=30)

#             cnopts = pysftp.CnOpts()
#             cnopts.hostkeys = None

#             # with pysftp.Connection(hostname, username=username, password=password, cnopts=cnopts) as sftp:
#             #     sftp.cwd(source_path)
#             #     remote_files = sftp.listdir()
#             #     valid_files = [f for f in remote_files if f.lower().endswith(('.pdf', '.xls', '.xlsx'))]


#             #     grouped_files = {}
#             #     due_date_map = {}

#             #     for filename in valid_files:
#             #         clean_filename = filename.strip("'").strip('"')
#             #         parts = clean_filename.split('_', 4)

#             #         if len(parts) >= 4:
#             #             party_id, doc_id, due_date, doc_code = parts[0], parts[1], parts[2], parts[3]
#             #             group_key = f"{party_id}_{doc_id}_{due_date}_{doc_code}"
#             #             grouped_files.setdefault(group_key, []).append(clean_filename)
#             #             due_date_map[group_key] = due_date
#             #         else:
#             #             formatted_file = os.path.splitext(clean_filename)[0] + os.path.splitext(clean_filename)[1].lower()
#             #             formatted_file = formatted_file.replace(' ', '_')
#             #             formatted_file=sanitize_filename_basic(formatted_file)
#             #             sftp.get(f"{source_path}/{clean_filename}", os.path.join(destination_path, formatted_file))
#             #             sftp.remove(clean_filename)
#             #             processed_files.append(formatted_file)
#             #             party_id = clean_filename.split('_')[0]
#             #             insert_records.append((party_id, formatted_file, formatted_file, 1, file_received_time, ""))

#             #     for group_key, files in grouped_files.items():
#             #         try:
#             #             due_date = due_date_map.get(group_key)
#             #             party_id = files[0].split('_')[0]

#             #             if len(files) == 1:
#             #                 file = files[0]
#             #                 formatted_file = os.path.splitext(file)[0] + os.path.splitext(file)[1].lower()
#             #                 formatted_file = formatted_file.replace(' ', '_')
#             #                 formatted_file=sanitize_filename_basic(formatted_file)
#             #                 sftp.get(f"{source_path}/{file}", os.path.join(destination_path, formatted_file))
#             #                 sftp.remove(file)
#             #                 processed_files.append(formatted_file)
#             #                 insert_records.append((party_id, formatted_file, formatted_file, 1, file_received_time, due_date))
#             #             else:
#             #                 pdfs_to_merge, temp_local_files = [], []
#             #                 for file in files:
#             #                     file = file.strip().strip("'").strip('"')
#             #                     ext = os.path.splitext(file)[1].lower()
#             #                     formatted_file = file.replace(' ', '_')
#             #                     formatted_file=sanitize_filename_basic(formatted_file)
#             #                     local_path = os.path.join(temp_path, formatted_file)
#             #                     sftp.get(f"{source_path}/{file}", local_path)
#             #                     sftp.remove(file)
#             #                     temp_local_files.append(local_path)

#             #                     if ext in ('.xls', '.xlsx', '.csv'):
#             #                         pdf_file = local_path.rsplit('.', 1)[0] + '.pdf'
#             #                         if convert_to_pdf(local_path, pdf_file):
#             #                             pdfs_to_merge.append(pdf_file)
#             #                         else:
#             #                             failed_files.append(file)
#             #                     elif ext == '.pdf':
#             #                         pdfs_to_merge.append(local_path)

#             #                 if not pdfs_to_merge:
#             #                     failed_files.extend(files)
#             #                     continue

#             #                 rest_part = files[0].split('_', 4)[-1]
#             #                 base_name = os.path.splitext(rest_part)[0]
#             #                 #base_name = ''.join(filter(lambda c: not c.isdigit(), base_name))
#             #                 base_name = os.path.splitext(files[0])[0].split('_')[-1]
#             #                 base_name = re.sub(r'\d+$', '', base_name)   
#             #                 merged_file_name = f"{group_key}_{base_name}_merged.pdf"
#             #                 merged_file_path = os.path.join(destination_path, merged_file_name)

#             #                 merged_pdf = fitz.open()
#             #                 for pdf_file in pdfs_to_merge:
#             #                     merged_pdf.insert_pdf(fitz.open(pdf_file))
#             #                 merged_pdf.save(merged_file_path)
#             #                 merged_pdf.close()

#             #                 for temp_file in temp_local_files:
#             #                     try:
#             #                         os.remove(temp_file)
#             #                     except Exception as cleanup_error:
#             #                         logging.warning(f"Failed to remove temp file {temp_file}: {cleanup_error}")

#             #                 processed_files.append(merged_file_name)
#             #                 for original_file in files:
#             #                     insert_records.append((party_id, original_file, merged_file_name, len(files), file_received_time, due_date))
#             #         except Exception as err:
#             #             logging.warning(f"Error processing group {group_key}: {err}")
#             #             failed_files.extend(files)

#             with pysftp.Connection(hostname, username=username, password=password, cnopts=cnopts) as sftp:
#                 sftp.cwd(source_path)
#                 remote_files = sftp.listdir()
#                 valid_files = [f for f in remote_files if f.lower().endswith(('.pdf', '.xls', '.xlsx', '.csv'))]

#                 grouped_files = {}
#                 due_date_map = {}
#                 file_received_time = datetime.now() + timedelta(hours=5, minutes=30)

#                 for filename in valid_files:
#                     try:
#                         clean_filename = filename.strip("'")
#                         parts = clean_filename.split('_', 4)
#                         if len(parts) >= 4:
#                             party_id, doc_id, due_date, doc_code = parts[0], parts[1], parts[2], parts[3]
#                             group_key = f"{party_id}_{doc_id}_{due_date}_{doc_code}"
#                             grouped_files.setdefault(group_key, []).append(filename)
#                             due_date_map[group_key] = due_date
#                         else:
#                             src = f"{source_path}/{filename}"
#                             dst_filename = normalize_filename(filename)
#                             dst_filename=sanitize_filename_basic(dst_filename)
#                             dst = os.path.join(destination_path, dst_filename)
#                             try:
#                                 sftp.get(src, dst)
#                                 sftp.remove(filename)
#                                 party_id = filename.split('_')[0]
#                                 processed_files.append(dst_filename)
#                                 insert_records.append((party_id, dst_filename, dst_filename, 1, file_received_time, ""))
#                             except Exception as e:
#                                 logging.warning(f"Skipping file {filename}: {e}")
#                                 failed_files.append(filename)
#                     except Exception as group_err:
#                         logging.error(f"Grouping failed for {filename}: {group_err}")

#                 for group_key, files in grouped_files.items():
#                     try:
#                         due_date = due_date_map.get(group_key)
#                         party_id = files[0].split('_')[0]

#                         if len(files) == 1:
#                             filename = files[0]
#                             src = f"{source_path}/{filename}"
#                             dst_filename = normalize_filename(filename)
#                             dst_filename=sanitize_filename_basic(dst_filename)
#                             dst = os.path.join(destination_path, dst_filename)
#                             try:
#                                 sftp.get(src, dst)
#                                 sftp.remove(filename)
#                                 processed_files.append(dst_filename)
#                                 insert_records.append((party_id, dst_filename, dst_filename, 1, file_received_time, due_date))
#                             except Exception as e:
#                                 logging.warning(f"Error processing single file {filename}: {e}")
#                                 failed_files.append(filename)
#                         else:
#                             temp_local_files, pdfs_to_merge = [], []
#                             for filename in files:
#                                 try:
#                                     filename = filename.strip("'")
#                                     src = f"{source_path}/{filename}"
#                                     local_path = os.path.join(temp_path, filename)
#                                     sftp.get(src, local_path)
#                                     sftp.remove(filename)

#                                     ext = os.path.splitext(filename)[1].lower().lstrip('.')
#                                     filename=filename.replace(' ','_')
#                                     if ext in ('xls', 'xlsx', 'csv'):
#                                         pdf_file = local_path.rsplit('.', 1)[0] + '.pdf'
#                                         if convert_to_pdf(local_path, pdf_file):
#                                             pdfs_to_merge.append(pdf_file)
#                                         else:
#                                             logging.warning(f"Conversion failed for {filename}")
#                                             failed_files.append(filename)
#                                     elif ext == 'pdf':
#                                         pdfs_to_merge.append(local_path)
#                                     else:
#                                         logging.warning(f"Unsupported extension in {filename}")
#                                     temp_local_files.append(local_path)
#                                 except Exception as e:
#                                     logging.warning(f"Error processing file {filename}: {e}")
#                                     failed_files.append(filename)

#                             if not pdfs_to_merge:
#                                 logging.warning(f"No PDFs to merge for group {group_key}")
#                                 continue

#                             try:
#                                 #base_name = ''.join(filter(lambda c: not c.isdigit(), os.path.splitext(files[0])[0]))
#                                 base_name = os.path.splitext(files[0])[0].split('_')[-1]
#                                 base_name = re.sub(r'\d+$', '', base_name)   
#                                 merged_name = normalize_filename(f"{group_key}_{base_name}_merged.pdf")
#                                 merged_name=sanitize_filename_basic(merged_name)
#                                 merged_path = os.path.join(destination_path, merged_name)

#                                 merged_pdf = fitz.open()
#                                 for pdf in pdfs_to_merge:
#                                     merged_pdf.insert_pdf(fitz.open(pdf))
#                                 merged_pdf.save(merged_path)
#                                 merged_pdf.close()

#                                 for temp_file in temp_local_files:
#                                     try:
#                                         os.remove(temp_file)
#                                     except Exception:
#                                         pass

#                                 processed_files.append(merged_name)
#                                 for original_file in files:
#                                     insert_records.append((party_id, normalize_filename(original_file), merged_name, len(files), file_received_time, due_date))
#                             except Exception as merge_err:
#                                 logging.error(f"Failed to merge PDFs for group {group_key}: {merge_err}")
#                                 failed_files.extend(files)
#                     except Exception as group_proc_err:
#                         logging.error(f"Error processing group {group_key}: {group_proc_err}")
#                 response_data = {"flag": True,"data":{"message":"Copied file from SFTP server"}}



#         except Exception as e:
#             logging.exception("## Exception: Something went wrong in getting sftp files",e)
#             response_data = {"flag": False,"message":f"Copied file from SFTP server","data":{}}
#             audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
#                         "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(valid_files),
#                         "tables_involved": "","request_payload": json.dumps(data),
#                         "response_data": json.dumps(response_data['message']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
#             mode = os.environ.get('MODE')
#             if mode in ('UAT','PROD','PRE-PROD','STANDBY'):
#                 insert_into_audit('', audit_data)
#             else:
#                 pass

#         insert_bulk_file_data(insert_records, queues_db)

#         try:
#             if processed_files:
#                 timestamp = datetime.now().strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
#                 into_lines = "\n".join(
#                     f"  INTO folder_monitor_table (file_name, current_status, created_date) "
#                     f"VALUES ('{escape_sql(file)}', 'File Uploaded', '{timestamp}')"
#                     for file in processed_files
#                 )
#                 monitor_query = f"INSERT ALL\n{into_lines}\nSELECT * FROM dual"
#                 queues_db.execute_(monitor_query)
#                 logging.info(f"{len(processed_files)} files logged to folder_monitor_table.")
#         except Exception as e:
#             logging.warning("Folder monitor table insert failed")

#         try:
#             memory_after = measure_memory_usage()
#             memory_consumed = (memory_after - memory_before) / \
#                 (1024 * 1024 * 1024)
#             end_time = tt()
#             time_consumed = str(end_time-start_time)
#             memory_consumed = f"{memory_consumed:.10f}"
#             time_consumed = str(round(end_time-start_time, 3))
#         except:
#             logging.warning("Failed to calc end of ram and time")
#             logging.exception("ram calc went wrong")
#             memory_consumed = None
#             time_consumed = None
#             pass

#         response_data = {
#             "flag": True,
#             "data":{"message": "Files processed successfully" if not failed_files else "Files processed with some failures"}
#         }

#         if len(valid_files) >= 1:
#             audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
#                         "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(valid_files),
#                         "tables_involved": "", "memory_usage_gb": str(memory_consumed),
#                         "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
#                         "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
#             insert_into_audit('', audit_data)

#         return response_data



def get_files_from_sftp():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time calc")
        pass

    trace_id = generate_random_64bit_string()
    tenant_id = os.environ.get('TENANT_ID',None)
    db_config['tenant_id'] = tenant_id
    queues_db=DB('queues',**db_config)

    attr = ZipkinAttrs(
        trace_id=trace_id,
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='folder_monitor',
            span_name='get_files_from_sftp',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):

        data = request.json
        logging.info(f"### FM Request data of get_files_from_sftp: {data}")

        source_pdf_files = []
        processed_files = []
        insert_records = []
        failed_files = []
        lock_file = "/tmp/get_files_from_sftp.lock"
        if os.path.exists(lock_file):
            logging.warning("Another get_files_from_sftp process is already running. Skipping this run.")
            return {
                "flag": False,
                "message": "Previous SFTP run still in progress"
            }

        # Create the lock file (acts as a signal)
        open(lock_file, 'w').close()
        logging.info("Lock file created. Starting SFTP processing...")

        try:
            hostname = os.environ['SFTP_SERVER']
            username = os.environ['SFTP_USERNAME']
            encrypted_password = os.environ['CADPRO_PASSWORD']
            password = decrypt_password(encrypted_password)
            source_path = os.environ['SFTP_SOURCE_FILEPATH']
            destination_path = '/app/input/hdfc/normal/input'
            temp_path = '/app/input'
            file_received_time = datetime.now() + timedelta(hours=5, minutes=30)

            cnopts = pysftp.CnOpts()
            cnopts.hostkeys = None
            with pysftp.Connection(hostname, username=username, password=password, cnopts=cnopts) as sftp:
                sftp.cwd(source_path)
                remote_files = sftp.listdir()
                valid_files = [f for f in remote_files if f.lower().endswith(('.pdf', '.xls', '.xlsx', '.csv'))]

                grouped_files = {}
                due_date_map = {}
                file_received_time = datetime.now() + timedelta(hours=5, minutes=30)

                for filename in valid_files:
                    try:
                        clean_filename = filename.strip("'")
                        parts = clean_filename.split('_', 4)
                        if len(parts) >= 4:
                            party_id, doc_id, due_date, raw_doc_code = parts[0], parts[1], parts[2], parts[3]
                            match = re.match(r'^\d+', raw_doc_code)
                            doc_code = match.group() if match else raw_doc_code
                            group_key = f"{party_id}_{doc_id}_{due_date}_{doc_code}"
                            grouped_files.setdefault(group_key, []).append(filename)
                            due_date_map[group_key] = due_date
                        else:
                            #src = f"{source_path}/{filename}"
                            dst_filename = normalize_filename(filename)
                            dst_filename=sanitize_filename_basic(dst_filename)

                            dst = os.path.join(destination_path, dst_filename)
                            try:
                                #sftp.get(src, dst)
                                sftp.get(filename, dst)
                                sftp.remove(filename)
                                party_id = filename.split('_')[0]
                                processed_files.append(dst_filename)
                                insert_records.append((party_id, dst_filename, dst_filename, 1, file_received_time, ""))
                            except Exception as e:
                                logging.warning(f"Skipping file {filename}: {e}")
                                failed_files.append(filename)
                    except Exception as group_err:
                        logging.error(f"Grouping failed for {filename}: {group_err}")

                for group_key, files in grouped_files.items():
                    try:
                        due_date = due_date_map.get(group_key)
                        party_id = files[0].split('_')[0]

                        if len(files) == 1:
                            #print("entered len ==1")
                            filename = files[0]
                            filename = filename.strip("'").strip()
                            #src = f"{source_path}/{filename}"
                            #src = filename
                            #print(f"src is:{src}")
                            dst_filename = normalize_filename(filename)
                            dst_filename=sanitize_filename_basic(dst_filename)

                            dst = os.path.join(destination_path, dst_filename)
                            try:
                                # print(f"Enytered TRY")
                                # #print(f"src is:{src}")
                                # print(f"dst is:{dst}")
                                # print("Current SFTP directory:", sftp.pwd)
                                # print("Files in this directory:", sftp.listdir())
                                # #print("Trying to fetch src:", repr(src))
                                #result=sftp.get(filename,dst)
                                #if(result):
                                    #print("File is getting downloaded")
                                #else:
                                    #print("Not downloaded")

                                #result2=sftp.remove(filename)
                                #if(result2):
                                    #print("File is getting downloaded")
                                #else:
                                    #print("Not downloaded")

                                sftp.get(filename, dst)
                                if os.path.exists(dst):

                                    print("downloaded")

                                else:

                                    print("Not downloaded")


                                #sftp.remove(filename)
                                sftp.remove(filename)
                                processed_files.append(dst_filename)
                                insert_records.append((party_id, dst_filename, dst_filename, 1, file_received_time, due_date))
                            except Exception as e:
                                logging.warning(f"Error processing single file {filename}: {e}")
                                failed_files.append(filename)
                        else:
                            temp_local_files, pdfs_to_merge = [], []
                            for filename in files:
                                #print(f"File name is:{filename}")
                                try:
                                    filename = filename.strip("'")
                                    #src = f"{source_path}/{filename}"
                                    local_path = os.path.join(temp_path, filename)
                                    #print(f"file_name is :{filename}")
                                    #print(f"local path is:{local_path}")
                                    #sftp.get(src, local_path)
                                    #sftp.remove(filename)
                                    download_successful = False
                                    try:
                                        #print(f"Enytered TRY")
                                        #filename = filename.strip("'")
                                        src = f"{source_path}/{filename}"
                                        sftp.get(src, local_path)
                                        download_successful = True
                                    except Exception as e:
                                        logging.warning(f"Failed to download file {filename}: {e}")
                                        failed_files.append(filename)

                                    if download_successful:
                                        try:
                                            sftp.remove(filename)
                                        except Exception as e:
                                            logging.warning(f"Failed to remove file {filename} after successful download: {e}")


                                    ext = os.path.splitext(filename)[1].lower().lstrip('.')
                                    #filename=filename.replace(' ','_')
                                    if ext in ('xls', 'xlsx', 'csv'):
                                        #print(f"entered in ext is xls xlsx")
                                        pdf_file = local_path.rsplit('.', 1)[0] + '.pdf'
                                        if convert_to_pdf(local_path, pdf_file):
                                            #print("calling convert jpg to psdf")
                                            #print(f"The pdf file is after calling convert to jpg pdf is:{pdf_file}")

                                            pdfs_to_merge.append(pdf_file)
                                        else:
                                            logging.warning(f"Conversion failed for {filename}")
                                            failed_files.append(filename)
                                    elif ext == 'pdf':
                                        pdfs_to_merge.append(local_path)
                                    else:
                                        logging.warning(f"Unsupported extension in {filename}")
                                    temp_local_files.append(local_path)
                                except Exception as e:
                                    logging.warning(f"Error processing file {filename}: {e}")
                                    failed_files.append(filename)

                            if not pdfs_to_merge:
                                logging.warning(f"No PDFs to merge for group {group_key}")
                                continue

                            try:
                                #print("enetred try to make megre file name")
                                #base_name = ''.join(filter(lambda c: not c.isdigit(), os.path.splitext(files[0])[0]))
                                #base_name = os.path.splitext(files[0])[0].split('_')[-1]
                                #base_name = re.sub(r'\d+$', '', base_name)
                                #merged_name = normalize_filename(f"{group_key}_{base_name}_merged.pdf")
                                merged_name = normalize_filename(f"{group_key}_merged.pdf")
                                #merged_name=sanitize_filename_basic(merged_name)

                                merged_path = os.path.join(destination_path, merged_name)
                                #print(f"marged path is:{merged_path}")

                                merged_pdf = fitz.open()
                                for pdf in pdfs_to_merge:
                                    merged_pdf.insert_pdf(fitz.open(pdf))
                                merged_pdf.save(merged_path)
                                #print(f"The mereged file is saved in merged path is:{merged_pdf}")
                                merged_pdf.close()

                                for temp_file in temp_local_files:
                                    #print(f"temp_file is :{temp_file}")
                                    try:
                                        os.remove(temp_file)
                                    except Exception:
                                        pass

                                processed_files.append(merged_name)
                                for original_file in files:
                                    insert_records.append((party_id, normalize_filename(original_file), merged_name, len(files), file_received_time, due_date))
                            except Exception as merge_err:
                                logging.error(f"Failed to merge PDFs for group {group_key}: {merge_err}")
                                failed_files.extend(files)
                    except Exception as group_proc_err:
                        logging.error(f"Error processing group {group_key}: {group_proc_err}")
                response_data = {"flag": True,"data":{"message":"Copied file from SFTP server"}}
        except Exception as e:
            logging.exception("## Exception: Something went wrong in getting sftp files",e)
            response_data = {"flag": False,"message":f"Copied file from SFTP server","data":{}}
            audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
                        "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(valid_files),
                        "tables_involved": "","request_payload": json.dumps(data),
                        "response_data": json.dumps(response_data['message']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
            mode = os.environ.get('MODE')
            if mode in ('UAT','PROD','PRE-PROD','STANDBY'):
                insert_into_audit('', audit_data)
            else:
                pass
        finally:
            # STEP 2: Always remove lock file at the end (even if errors occur)
            if os.path.exists(lock_file):
                os.remove(lock_file)
                logging.info("Lock file removed. SFTP process complete.")
        #print(f"beifre calling insert bul data")
        insert_bulk_file_data(insert_records, queues_db)
        #response_data = {"flag": True,"data":{"message":"Copied file from SFTP server"}}


        try:
            if processed_files:
                timestamp = datetime.now().strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
                into_lines = "\n".join(
                    f"  INTO folder_monitor_table (file_name, current_status, created_date) "
                    f"VALUES ('{escape_sql(file)}', 'File Uploaded', '{timestamp}')"
                    for file in processed_files
                )
                monitor_query = f"INSERT ALL\n{into_lines}\nSELECT * FROM dual"
                queues_db.execute_(monitor_query)
                logging.info(f"{len(processed_files)} files logged to folder_monitor_table.")
        except Exception as e:
            logging.warning("Folder monitor table insert failed")

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            time_consumed = str(end_time-start_time)
            memory_consumed = f"{memory_consumed:.10f}"
            time_consumed = str(round(end_time-start_time, 3))
        except:
            logging.warning("Failed to calc end of ram and time")
            logging.exception("ram calc went wrong")
            memory_consumed = None
            time_consumed = None
            pass

        response_data = {
            "flag": True,
            "data":{"message": "Files processed successfully" if not failed_files else "Files processed with some failures"}
        }

        if len(valid_files) >= 1:
            audit_data = {"tenant_id": tenant_id, "user_": "System", "case_id": "",
                        "api_service": "get_files_from_sftp", "service_container": "folder_monitor", "changed_data": json.dumps(valid_files),
                        "tables_involved": "", "memory_usage_gb": str(memory_consumed),
                        "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
                        "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": "", "status": str(response_data['flag'])}
            insert_into_audit('', audit_data)

        return response_data
