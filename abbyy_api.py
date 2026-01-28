import os
import json
import requests
# from time import sleep
from time import time as tt
import psutil
# from pdf2image import convert_from_path
import re
from PIL import Image
import base64
import subprocess
import time

# qr code
# from pyzbar.pyzbar import decode
# from pikepdf import Pdf
import fitz

from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
from flask_cors import CORS
from pathlib import Path

from db_utils import DB
from ace_logger import Logging
# from threading import Lock
# mutex = Lock()

try:
    from app import app
except Exception as e:
    logging.exception(f'33 exception is {e}')
    app = Flask(__name__)
    CORS(app)
try:
    import app.xml_parser_sdk as xml_parser_sdk
except Exception as e:
    logging.exception(f"39 exception is {e}")
    import xml_parser_sdk as xml_parser_sdk
from app.ocr import get_ocr
from app.ocr_extraction_map import ocr_extraction

logging = Logging(name='abbyy_api')

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}


# def file_format_identifier(file_path):
#     var = variable()
#     if file_path.lower().endswith(('.png')):
#         file_format = var.PNG
#     elif file_path.lower().endswith(('.jpg')) or file_path.lower().endswith(('.jpeg')):
#         file_format = var.JPEG
#     elif file_path.lower().endswith(('.tif')) or file_path.lower().endswith(('.tiff')):
#         file_format = var.TIFF
#     elif file_path.lower().endswith(('.pdf')) or file_path.lower().endswith(('.PDF')):
#         file_format = var.PDF
#     elif file_path.lower().endswith(('.bmp')):
#         file_format = var.BMP

#     # #print(f'file_format={file_format}')
#     return file_format


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
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True


def remove_all_except_al_num(file_full_name):
    to_return = re.sub('[,.!@#$%^&*()\-=`~\'";:<>/?]', '', file_full_name.lower())
    to_return = to_return.replace(' ', '')
    return to_return

def convert_pdf_jpg(file_path, output_path):
    jpg_list = []
    file_full_name = file_path.name
    file_ext = file_path.suffix.lower()
    logging.debug(f"## FM Checkpoint File name: {file_full_name}")
    file_page_object = {}
    try:
        if file_ext == '.pdf':
            with fitz.open(file_path) as new_file:
                # with fitz.open(file_path) as pages:
                #     new_file.insertPDF(pages, to_page=7)
                # new_file.save(file_path)

                for idx, page in enumerate(new_file):
                    page_image_name = f'{file_path.stem}_{str(idx)}.jpg'
                    logging.info(f"page image name {page_image_name}")
                    
                    page_image_path = (f'{output_path}/images/')
                    os.makedirs(page_image_path, exist_ok=True)
                    page_image_path= page_image_path + page_image_name                
                    logging.info(f"page image path {page_image_path}")
                    logging.debug(f'Page {str(idx)}: {page_image_path}')
                    mat = fitz.Matrix(300 / 72, 300 / 72)
                    # if isinstance(page, fitz.fitz.Page):
                    if isinstance(page, fitz.Page):
                        # try:
                        logging.info(f"in try if block")
                        pix = page.get_pixmap(matrix=mat)
                        img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                        img.save(page_image_path, format="JPEG")
                        # except:
                        #     logging.info(f"in except if block")
                        #     pix = page.get_pixmap()
                        #     pix.save(page_image_path,format="JPEG")

                    else:
                        try:
                            logging.info(f"in try else block")
                            pix = page.get_pixmap(matrix=mat)
                            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
                            img.save(page_image_path, format="JPEG")
                        except Exception as e:
                            logging.exception(f"in except else block {e}")
                            pix = page.get_pixmap(matrix=mat)
                            pix.pil_save(page_image_path, 'JPEG')

                    jpg_list.append(page_image_name)
                file_page_object = {file_full_name: jpg_list}
                logging.info(f"file_page_object: {file_page_object}")
    except Exception as e:
        logging.exception(f'PDF to image conversion failed with pymupdf: {e}')
        logging.exception(f"convertion  pdf to jpg failed {e}")
        # return convert_pdf_jpg_old(file_path, output_path, unique_id)
    return file_page_object

def get_last_line(file_path):
    with open(file_path, 'r') as f:
        lines = f.readlines()
        if lines:
            return lines[-1].strip()  # strip() removes trailing newline/whitespace
        else:
            return None


def get_process_id(task_id):
    # Calls Camunda REST API to get the processInstanceId for the given task.
    # The returned processInstanceId is used for tracking and controlling the workflow instance.

    # URL for the Camunda REST API
    #ip=os.environ['SERVER_IP']
    ip = 'camundaworkflow'
    url = f"http://{ip}:8080/rest/engine/default/task/{task_id}"
    print(f"URL is:{url}")

    
    # Make the GET request to the API
    response = requests.get(url.format(task_id=task_id))
    try:
        logging.info(f"Response is:{response.json()}")
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
        logging.info(f"Process Instance ID: {process_instance_id}")
    else:
        logging.info(f"Error: {response.status_code}")
        logging.info(response.text)

    return process_instance_id

@app.route('/ocr_abbyy', methods=['POST', 'GET'])
def ocr_abbyy():
    data = request.json
    logging.info(f"Request Data: {data}")

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception as e:
        logging.exception(f"Failed to start ram and time calc {e}")
        pass

    tenant_id = data.get('tenant_id', None)
    try:
        case_id= data['email']['case_id']
    except Exception as e:
        logging.exception(f"eroor {e}")
        case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    ocr_flag="abbyy"
    #ocr_flag = data.get('ocr_flag','google')

    if case_id is None:
        trace_id = generate_random_64bit_string()
    else:
        trace_id = case_id

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
        service_name='abbyy_api',
        span_name='ocr_abbyy',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5):

        try:
            file_name = data.get('file_name', None)
            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)
            #extraction_db = DB('extraction', **db_config)
            document_id_=data.get("document_id_df",False)
            logging.info(f"case id is {case_id} and {document_id_}")

            try:
                qry=f"select task_id from queue_list where case_id='{case_id}'"
                df=queue_db.execute_(qry)
                if df.empty:
                    logging.info(f"The df is currently empty")
                task_id=df.iloc[0]["task_id"]
                #logging.info(f"###Enetring for the process instamce id")
                # Fetch the Camunda process instance ID using the task_id.
                # This ID is required to uniquely identify the running workflow for this case,
                # so we can track execution, debug in Camunda Cockpit, and perform future actions
                # like variable fetch, message correlation, or process termination.
                process_instance_id=get_process_id(task_id)

                if process_instance_id:
                    # Store the process_instance_id in DB to maintain mapping between case_id and
                    # Camunda workflow instance. This helps in monitoring, auditing, and troubleshooting.
                    query=f"update queue_list set process_instance_id = '{process_instance_id}' where case_id = '{case_id}'"
                    queue_db.execute_(query)

            except Exception as e:
                logging.error(f"The exception caught in abbyy for task id is:{e}")
                pass



            mode = os.environ['MODE']
            abbyy_limit_day = int(os.environ['ABBYY_LIMIT_DAY'])

            queue_query = "select queue from queue_list where case_id= %s"
            queue_query_res = queue_db.execute_(queue_query,params=[case_id])
            dup_query = "select case_id from ocr_info where case_id = %s"
            dup_query_res = queue_db.execute_(dup_query,params=[case_id])
            queue_name = queue_query_res.iloc[0]['queue']
            logging.info(f"queue name is {queue_name}")
            #logging.info(f"OCR_INFO check for {case_id}: {dup_query_res}")
            #logging.info(f"Is dup_query_res empty? {dup_query_res.empty}")
            #logging.info(f"Length of dup_query_res: {len(dup_query_res)}")
            if queue_name == "case_creation" and len(dup_query_res) != 0:
                logging.info(f"OCR already done") 
                #logging.info(f"OCR_INFO check for {case_id}: {dup_query_res}")
                #logging.info(f"Is dup_query_res empty? {dup_query_res.empty}")
                #logging.info(f"Length of dup_query_res: {len(dup_query_res)}")
                response_data = {
                        "flag": True,
                        "data": {'message':"OCR to this case is already completed"}
                    }
                return response_data
            count = 1
            queue_query = "SELECT abbyy_run_count FROM process_queue WHERE case_id = %s"
            abbyy_run_count_df = queue_db.execute_(queue_query, params=[case_id])

            # Get abbyy_run_count value from DataFrame
            if not abbyy_run_count_df.empty:
                abbyy_run_count_val = abbyy_run_count_df.iloc[0, 0]
            else:
                abbyy_run_count_val = None
                logging.info(f"abbyy_run_count from DB: {abbyy_run_count_val}")
            if abbyy_run_count_val is None:
                query = "UPDATE process_queue SET abbyy_run_count = %s WHERE case_id = %s"
                queue_db.execute_(query, params=[count, case_id])
                #logging.info(f"First run: Updated abbyy_run_count to 1 for case_id: {case_id}")
                #logging.info(f"dup_query_res content: {dup_query_res}")
                #logging.info(f"Length of dup_query_res: {len(dup_query_res)}")
            elif abbyy_run_count_val is not None:
                total_count = int(abbyy_run_count_val) + 1
                query = "UPDATE process_queue SET abbyy_run_count = %s WHERE case_id = %s"
                queue_db.execute_(query, params=[total_count, case_id])
                logging.info(f"Subsequent run: Incremented abbyy_run_count to {total_count} for case_id: {case_id}")
                
            else:
                logging.info("OCR already done or no need to update abbyy_run_count.")
            #return response_data

            if mode == 'DEV':
                # check the count
                query = f"SELECT SUM(no_of_pages) as sum_of_pages FROM ocr_info WHERE TRUNC(created_date) = TRUNC(SYSDATE)"
                result = queue_db.execute_(query)
                sum_of_pages = result['sum_of_pages'][0] if result['sum_of_pages'][0] is not None else 0
                count_abbyy_pages = int(sum_of_pages)

                if count_abbyy_pages >= abbyy_limit_day:
                    # send the mail to admins
                    base_route="http://emailtriggerapi/"
                    email_json_data = {'template':'Abbyy Restriction','tenant_id':tenant_id}
                    final_url=base_route+'send_email_auto'
                    response = requests.post(final_url,json=email_json_data)

                    #return message
                    return {"flag":False,"message": "You had reached the daily limit. Please contact administratr!"}


            if not document_id_:
                try:
                    query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and  `state` IS NULL"
                    document_id_df = queue_db.execute_(query, params=[case_id])['document_id'].tolist()
                    logging.info(f"Successfully accessed files which are not deleted")

                except Exception as e:
                    logging.exception(f"236 error {e}")
                    return{'flag':False,'message':f'{case_id} is missing in the table'}
            else:
                document_id_df=[case_id]

            for document_id in document_id_df:
                query = "SELECT * from  process_queue where document_id = %s"
                try:
                    file_name=data['email']['attachments']['attachment'][0]
                    if '.jpg' in file_name or '.xlsx' in file_name or '.docx' in file_name or '.xls' in file_name:
                        file_name_df = queue_db.execute(query, params=[document_id])
                        file_name_df=file_name_df.to_dict(orient='records')
                        file_name=json.loads(file_name_df[0]['file_name'])[0]
                except Exception as e:
                    logging.exception(f" 250 exception {e}")
                    file_name_df = queue_db.execute(query, params=[document_id])
                    file_name_df=file_name_df.to_dict(orient='records')
                    file_name=file_name_df[0]['file_name']
                logging.info(f"files names are {file_name}")
                auto_rotation = data.get('auto_rotation', 'false')

                file_path = f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{file_name}'

                start_ocr= tt()
                if ocr_flag=="abbyy":
                ##########################################
                #RUN ABBYY
                    try:
                        ocr_word, ocr_sen, dpi, rotation, xml_abbyy, pagesCount, remaining_pages = get_ocr(file_path)
                    except Exception as e:
                        logging.exception(f"Exception occured while processing the file")
                ############################################
                elif ocr_flag=="google":
                #RUN GOOGLE
                # try:
                #     file_object=convert_pdf_jpg(Path(file_path),f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}')
                #     queue_db.execute("update `process_queue` set `imagelist` = %s where `case_id` = %s", params=[json.dumps(file_object), case_id])
                # except:
                #     return jsonify({'flag': False, 'ocr_quality': False})


                # ocr_word=[]
                # ocr_sen={}
                # image_list=file_object[list(file_object.keys())[0]]
                # for i,image_path in enumerate(image_list):
                #     image_path = f'/app/{ace}/assets/pdf/{tenant_id}/{case_id}/images/{image_path}'

                #     logging.info(image_path)
                #     ocr=ocr_extraction(image_path)
                #     if len(ocr.ocr_json_data)>0:
                #         ocr_word.append(ocr.ocr_json_data[0])
                #         ocr_sen[i+1]=ocr.text_dict[1]
                #     else:
                #         ocr_word.append([])
                #         ocr_sen[i+1]={}
                    ocr_word=[]
                    ocr_sen={}
                    try:
                        files_data={'file': open(file_path, 'rb')}
                        logging.info(f'####################### files_data is {files_data}')
                        url='http://test-training.acelive.ai:5006/file_hybrid_ocr'
                        response = requests.post(url, files=files_data,timeout=1000)
                        logging.info(f'####################### response is {response}')
                        try:
                            ocr_data=response.json()
                        except Exception as e:
                            logging.exception(f'####################### ocr_response  is {e}')
                        # logging.info(f'####################### ocr_data is {ocr_data}')
                        ocr_word=ocr_data['ocr_word']
                        ocr_sen=ocr_data['ocr_sen']
                        # file='myfile.txt'
                        # file1 = open(f"/app/{ace}/assets/pdf/{tenant_id}/{case_id}/{file}","w")
                        # file1.write(f"ocr_info={ocr_word}")
                        # file1.close()
                        file2 = f'{file_path}'
                        logging.info(f"################### file2 is # {file2}")
                        if 'blob' in ocr_data:
                            pdf = base64.b64decode(ocr_data['blob'])
                            with open(file2, 'wb') as f:
                                f.write(pdf)
                        logging.info(f"######### pdf was written")
                    except Exception as e:
                        i=0
                        logging.exception(f'####################### some error while hitting url is {e}')
                        ocr_word.append([])
                        ocr_sen[i+1]={}

            ########################################################

                if len(ocr_word)==0:
                    message="No output from OCR"
                    return jsonify({'flag': False, 'message': message})
                else:
                    message="OCR successful"

                end_ocr=tt()
                dpi = [300] * len(ocr_word)
            #################################
                if ocr_flag == "google":
                    logging.info(f"########################## flag is google ocr_info={json.dumps(ocr_word)}")

                    logging.info(f"##########################  ocr_parsed={json.dumps(ocr_sen)}")
                    queue_db.insert_dict(table='ocr_info',
                                            data={'case_id': case_id, 'document_id': document_id, 'file_name': file_name, 'ocr_data': json.dumps(ocr_word),
                                                'ocr_parsed': json.dumps(ocr_sen), 'dpi': json.dumps(dpi)})
                    # if new_file:
                    #     queue_db.insert_dict(table='ocr_info',
                    #                          data={'case_id': case_id, 'document_id': document_id, 'file_name': file_name, 'ocr_data': json.dumps(ocr_word),
                    #                                'ocr_parsed': json.dumps(ocr_sen), 'dpi': json.dumps(dpi)})
                    # else:
                    #     queue_db.execute("update `ocr_info` set `file_name` = %s, `ocr_data` = %s, `ocr_parsed` = %s, `dpi` = %s where `case_id` = %s and `document_id`= %s", params=[
                    #     file_name, json.dumps(ocr_word), json.dumps(ocr_sen), json.dumps(dpi), case_id, document_id])

            ##################################
                if ocr_flag=="abbyy":
                    ocr_word_parsed=ocr_word_main(ocr_word,1)
                    # ocr_word_parsed_pred=ocr_word_main(ocr_word,2) #'ocr_word_parsed_pred':json.dumps(ocr_word_parsed_pred),
                    query=f"""select distinct case_id from ocr_info"""
                    caseid_list = queue_db.execute(query)["case_id"].tolist()
                    if case_id in caseid_list:
                        queue_db.execute("update `ocr_info` set `file_name` = %s,`ocr_word`= %s , `ocr_data` = %s, `ocr_parsed` = %s, `dpi` = %s,`rotation` =%s, `xml_abbyy`=%s, `file_page_count`=%s, `pages_remaining`=%s where `case_id` = %s and `document_id`= %s", params=[
                                     file_name,json.dumps(ocr_word_parsed), json.dumps(ocr_word), json.dumps(ocr_sen), json.dumps(dpi), json.dumps(rotation), json.dumps(xml_abbyy), pagesCount, remaining_pages, case_id, document_id])
                    else:
                        queue_db.insert_dict(table='ocr_info',
                                        data={'case_id': case_id, 'document_id': document_id, 'file_name': file_name,'ocr_word':json.dumps(ocr_word_parsed), 'ocr_data': json.dumps(ocr_word),
                                            'ocr_parsed': json.dumps(ocr_sen), 'dpi': json.dumps(dpi), 'rotation': json.dumps(rotation), 'xml_abbyy': json.dumps(xml_abbyy), 'file_page_count': pagesCount, 'pages_remaining': remaining_pages})
                    print()
                    # if new_file:
                    #     queue_db.insert_dict(table='ocr_info',
                    #                      data={'case_id': case_id, 'document_id': document_id, 'file_name': file_name, 'ocr_data': json.dumps(ocr_word),
                    #                            'ocr_parsed': json.dumps(ocr_sen), 'dpi': json.dumps(dpi), 'rotation': json.dumps(rotation), 'xml_abbyy': json.dumps(xml_abbyy)})
                    # # else:
                    # #     queue_db.execute("update `ocr_info` set `file_name` = %s, `ocr_data` = %s, `ocr_parsed` = %s, `dpi` = %s,`rotation` =%s, `xml_abbyy`=%s where `case_id` = %s and `document_id`= %s", params=[
                    # #                  file_name, json.dumps(ocr_word), json.dumps(ocr_sen), json.dumps(dpi), json.dumps(rotation), json.dumps(xml_abbyy), case_id, document_id])
            ######################


            # Project Specific
                no_of_pages = len(dpi)
                try:
                    queue_db.execute("update `ocr_info` set `no_of_pages` = %s where `case_id` = %s and `document_id`= %s", params=[
                                    no_of_pages, case_id, document_id])
                except Exception as e:
                    logging.exception(f"382 Exception {e}")
                    pass

                # file_object=convert_pdf_jpg(Path(file_path),
                #                     f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}')

                logging.info(f"Auto rotation = {auto_rotation}")
                logging.info("Autorotating....")


                # query = "SELECT * from  process_queue where document_id = %s"
                # file_name_df = queue_db.execute(query, params=[document_id])
                # file_name_df=file_name_df.to_dict(orient='records')
                # file_name_=file_name_df[0]['file_name']
                # try:
                #     file_name_=json.loads(file_name_)
                #     logging.info(f"file_name_ value before decoding: {file_name_}")
                # except Exception as e:
                #     logging.exception(f"399 exception as {e}")
                #     pass
                logging.info(f"files names are {file_name}")
                file_object={}
                # for file_name in file_name_:
                file_path = f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/{file_name}'
                file_object.update(convert_pdf_jpg(Path(file_path),
                            f'/app/{tenant_id}/assets/pdf/{tenant_id}/{case_id}'))
                logging.info(f"images are created result is {file_object}")
                queue_db_resp = queue_db.execute("update `process_queue` set `imagelist` = %s where `case_id` = %s", params=[json.dumps(file_object), case_id])
                logging.info(f"process queue update query response {queue_db_resp}")
                response_data = {
                    "flag": True,
                    "data": {'ocr_quality': True,'ocr_time_sec':end_ocr-start_ocr,'message':message}
                }
                logging.info(f"response data got is {response_data}")
            # Audit
                # stats_db = DB('stats', **db_config)
                # query = 'Insert into `audit` (`type`, `last_modified_by`,`table_name`, `changed_data`,`reference_column`,`reference_value`) values (%s,%s,%s,%s,%s,%s)'
                # params = ['Processing', 'ocr_abbyy', 'ocr_info', json.dumps({"state": 'OCR Abbyy completed'}),'case_id and document_id',f'{case_id}_{document_id}']
                # stats_db.execute(query, params=params)


            #return jsonify({'flag': True, 'data': response_data})
        except Exception as e:
            memory_consumed = ''
            time_consumed = ''
            logging.exception(f'Abby ocr failed: {e}')
            message = 'OCR failed!!'
            response_data = {'flag': False, 'message': message,'data':{}}

        try:
            memory_after = measure_memory_usage()
            memory_consumed = (memory_after - memory_before) / \
                (1024 * 1024 * 1024)
            end_time = tt()
            time_consumed = str(end_time-start_time)
        except Exception as e:
            logging.exception("Failed to calc end of ram and time")
            logging.exception(f"ram calc went wrong {e}")
            memory_consumed = None
            time_consumed = None
            pass


        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id,
                         "api_service": "ocr_abbyy", "service_container": "abbyy_api", "changed_data": json.dumps({"ocr_flag":ocr_flag}),
                         "tables_involved": "","memory_usage_gb": str(memory_consumed),
                         "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
                         "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id,"status":str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)
       

        # Check for 'response' at the end of the code
    

        return jsonify(response_data)



def ocr_word_main(words_,api):
    ocr_word=[]
    for words in words_:
        # prnt(f"words are {words}")
        # Sort the words by their 'top' position (vertical position)
        sorted_words = sorted(words, key=lambda x: x["top"])

        # Group words on the same horizontal line
        line_groups = []
        current_line = []
        for word in sorted_words:
            #print(word)
            if not current_line:
                current_line.append(word)
            else:
                mid_word=abs(word["top"]+(word["height"]/2))
                mid_cu=abs(current_line[-1]["top"]+(current_line[-1]["height"]/2))
                diff=abs(mid_word - mid_cu)
                if diff < 2:
                    # Word is on the same line as the previous word
                    current_line.append(word)
                else:
                    # Word is on a new line
                    line_groups.append(current_line)
                    current_line = [word]

            # Add the last line to the groups
        if current_line:
            line_groups.append(current_line)
            # #print(line_groups)
        # # #print the words grouped by horizontal lines
        for line in line_groups:
            line_words = [word["word"] for word in line]
            # #print(" ".join(line_words))
        temp=preparing_ocr_parsed_data_main(line_groups,api)
        ocr_word.append(temp)
        temp=[]
        # #print(ocr_word)
    return ocr_word
    
# ---------------------------------------------------------------------------------

def preparing_ocr_parsed_data_main(datas,api):
    main=[]
    for data in datas:
        combine_list=preparing_ocr_parsed_data_1(data,api)
#       #print(f"combine list from the horizontal line words is {combine_list} \n")
        for lists in combine_list:
            combined_result = combine_dicts(lists)
            main.append(combined_result)
#     #print(f"final words in the horizontal row are {main}") 
    return main

# ---------------------------------------------------------------------------------

def find_threshold(numbers):
    counts = Counter(numbers)
    max_count = max(counts.values())
    mode = [key for key, value in counts.items() if value == max_count]
    if len(mode)>1:
        min_=min(mode)
        if min_<5:
            return min_+2
        else:
            return 7
    else:
        if mode[10]<5:
            return mode[0]+2
        else:
            return 7

# ---------------------------------------------------------------------------------

def preparing_ocr_parsed_data_1(data,api):
    data = sorted(data, key=lambda x: x["left"])
    combined_word_dicts = {}
    distances=[]
    for i,dict1 in enumerate(data):
        if (i+1)==len(data):
            break
        dict2=data[i+1]
        distance = abs(dict2["left"] - dict1["right"])
        distances.append(distance)
    # #print(f"{distances} \n")
    data = sorted(data, key=lambda x: x["left"])
    # #print(f"data is {data} \n")
#     threshold=find_threshold(distances)
    if api==1:
        threshold=9
    elif api==2:
        threshold=20
    # #print(f"threshold we got is {threshold} \n")
    combine=[]
    temp=data[0]
    temp_l=[]
    temp['x-space']=threshold
    temp_l.append(temp)
    for i in range(1,len(data)): 
        dict2=data[i]
        dict2['x-space']=threshold
        # Calculate the horizontal distance between the right edge of dict1 and the left edge of dict2
        distance = abs(dict2["left"] - temp["right"])
        # #print(dict2,temp)
        # #print(distance)
        check=char_check(temp,dict2)
        if distance <= threshold and check:
            temp_l.append(dict2)
            temp=dict2
        else:
            combine.append(temp_l)
            temp_l=[]
            temp_l.append(dict2)
            temp=dict2
    # #print(temp_l)
    if temp_l:
        combine.append(temp_l) 
    # #print(f"combine list from the horizontal line words is {combine} \n")
    return combine
# ---------------------------------------------------------------------------------

def char_check(dict1,dict2):
    word1=dict1['word']
    word2=dict2['word']
    if ':' in word1:
        colon_index = word1.index(':')
        if colon_index==len(word1)-1:
            return False
    if ':' in word2:
        colon_index = word2.index(':')
        if colon_index==0:
            return False
    return True       

# ---------------------------------------------------------------------------------

def combine_dicts(dicts):
    combined_dict = {
        "word": ' '.join([d["word"] for d in dicts]),
        "height": max([d["height"] for d in dicts]),
        "top": min([d["top"] for d in dicts]),
        "left": min([d["left"] for d in dicts]),
        "bottom": max([d["bottom"] for d in dicts]),
        "right": max([d["right"] for d in dicts]),
        "width": abs(max([d["right"] for d in dicts])-min([d["left"] for d in dicts])),
        "confidence": max([d["confidence"] for d in dicts]),
        "sen_no": dicts[0]["sen_no"],  # Assuming they all have the same sen_no
        "pg_no": dicts[0]["pg_no"],# Assuming they all have the same pg_no
        "x-space":dicts[0]["x-space"]
    }
    return combined_dict


def get_file_name(file_data, curr_dir):
    supported_files = ['.pdf', '.jpeg', '.jpg', '.png', '.tif', '.tiff']
    try:
        file_name = file_data.filename
        if len(file_data.read()) <= 0:
            logging.info('empty file')
            return ''
        file_data.seek(0)
        extension = Path(file_name).suffix.lower()
        if extension and extension in supported_files:
            file_name = 'ocr_file'+extension
            file_path = os.path.join(curr_dir, file_name)
        elif not extension:
            file_name = 'ocr_file.pdf'
        else:
            logging.info('file extension not supported',file_name)
            file_name = ''
    except Exception as e:
        file_name = 'ocr_file.pdf'
        logging.exception(f"627 Exception is {e}")
        pass

    return file_name

@app.route('/file_ocr', methods=['POST', 'GET'])
def file_ocr():
    #mutex.acquire()
    file_name = ''
    try:
        data = request.files
        #print("#############request data:",data)
        curr_dir = os.path.dirname(os.path.abspath(__file__))

        # #print(curr_dir)
        file_data = data['file']

        try:
            page_data = json.loads(data['json'].read())
        except Exception as e:
            logging.exception(f"647 exception is {e}")
            page_data = {}


        file_name = get_file_name(file_data, curr_dir)

        if not file_name:
            return jsonify({'xml_string': ''})

        file_path = os.path.join(curr_dir, file_name)
        count = 0
        # while True:
        try:
            #print('logging to save the file')
                #mutex.acquire()
            file_name = f"/app/{file_name}"
            file_data.save(file_name)
                # #print(file_name)
                # #print('tryingggg')
                # break
        except Exception as e:
            logging.exception(f"something went wrong {e}")
            return jsonify({})

        #print({type(file_data)})

        command = {'fileName': file_name}
        if page_data:
            command.update(page_data)
        command = json.dumps(command).replace(' ', '')

        #print(command)

        # inp = './Run.sh ' + file_name
        # inp = "/usr/bin/java -classpath '.:bin/.:libs/abbyy.FREngine.jar:libs/mysql-connector-java-8.0.17.jar' com.algonox.abbyy.OCRExtraction " + case_id
        # current_ld_library_path = os.environ.get('LD_LIBRARY_PATH', '')
        # os.environ['LD_LIBRARY_PATH'] = '/opt/ABBYY/FREngine12/Bin:' + current_ld_library_path
        whole_load_combined = subprocess.check_output(['/var/www/abbyy_api/app/Run.sh', command]).decode('utf-8').replace('\\r\\n', '')
        logging.info(f"whole_load_combined:::::::::{whole_load_combined}")
        if whole_load_combined == 'ERROR:\n':
            if 'pdf' in file_name:
                new_file_name = str(int(time.time()*1000)) + '.pdf'
                convert_command = 'qpdf --decrypt '+file_name+' '+new_file_name
                _ = subprocess.check_output(convert_command.split(' '))

                command = {'fileName': new_file_name}
                if page_data:
                    command.update(page_data)
                command = json.dumps(command).replace(' ', '')
                whole_load_combined = subprocess.check_output(['/var/www/abbyy_api/app/Run.sh', command]).decode('utf-8').replace('\\r\\n','')
            else:
                pass
        
        pages_remaining = -1
        if whole_load_combined:
            try:
                temp = whole_load_combined.split('$$$$$')
                whole_load = temp[0]
                pages_remaining = int(temp[1])
            except Exception as e:
                logging.exception(f"707exception as {e}")
                whole_load = ''
                pages_remaining = -1
        
        logging.info(f"whole_load:::::{whole_load}")

        # whole_load = ast.literal_eval(whole_load)
        # if '.jpg' in file_name.lower():
        #     return jsonify({'xml_string': whole_load})

        logging.debug(f"for blob key curr_dir:::{curr_dir}, file_name::: {file_name}")
        output_file = os.path.join(curr_dir, file_name)
        with open(output_file,'rb') as f:
            blob = base64.b64encode(f.read())

        if '.pdf' in output_file:
        #if True:
            return jsonify({"xml_string": whole_load, "blob": blob.decode(), "pages_remaining": pages_remaining})
        else:
            return jsonify({"xml_string": whole_load, "pages_remaining": pages_remaining})
    except Exception as e:
        logging.exception(f"something went wrong {e}")
        return jsonify({'xml_string': ''})
    # Need to add below lines after testing
    # finally:
    #     if os.path.isfile(file_name):
    #         os.remove(file_name)