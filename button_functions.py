import json
import os
from numpy import extract
import requests
import psutil
import traceback
import html
import ast
import pandas as pd
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from py_zipkin.util import generate_random_64bit_string
from time import time as tt
from db_utils import DB
from ace_logger import Logging
from datetime import datetime
from app import app
import numpy as np
import re
import random
import pytz
tmzone = 'Asia/Kolkata'
logging = Logging(name='button_functions')
import re


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



def clob_chunks_sql_function(json_str):
    if not json_str or json_str.strip() in ['null', 'None', '{}']:
        return "TO_CLOB(NULL)"

    escaped = json_str.replace("'", "''")
    chunk_size = 3000
    chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
    return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)

def ensure_initial_value(tab_json, field_name):
    tab_json.setdefault("initial_value", {})

    # already captured → do nothing
    #if field_name in tab_json["initial_value"]:
        

    if field_name not in tab_json:
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

def update_rule_text_input(case_id, extraction_db, changed_data,tab_json_cache):
    tab = changed_data.get("tab_name", "").lower()
    rule_id = changed_data.get("rule_id")
    final_field = changed_data.get("final_field")
    rule_text = changed_data.get("rule_text")

    if not (tab and rule_id and final_field and rule_text):
        return

    # ---- load OCR row ----
    qry = f"SELECT rule_text_input, {tab} FROM ocr WHERE case_id='{case_id}'"
    df = extraction_db.execute_(qry)

    rule_text_input = json.loads(df.iloc[0]["rule_text_input"]) if df.iloc[0]["rule_text_input"] else {}
    #tab_json = json.loads(df.iloc[0][tab]) if df.iloc[0][tab] else {}
    if df is None or df.empty:
        tab_json_cache[tab] = {}
    else:
        if tab not in tab_json_cache:
            raw_tab = df.iloc[0].get(tab)
            try:
                tab_json_cache[tab] = json.loads(raw_tab) if raw_tab else {}
            except Exception:
                tab_json_cache[tab] = {}

    tab_json = tab_json_cache[tab]


    # ---- CAPTURE INITIAL VALUE (INLINE TAB JSON) ----
    ensure_initial_value(tab_json, final_field)

    # ---- UPDATE RULE_TEXT_INPUT ----
    rule_text_input.setdefault(tab, {})
    rule_text_input[tab].setdefault(rule_id, {})
    rule_text_input[tab][rule_id][final_field] = {
        "previous": rule_text,
        "updated": rule_text
    }

    # ---- SAVE BACK ----
    extraction_db.execute_(f"""
        UPDATE ocr
        SET rule_text_input = {clob_chunks_sql_function(json.dumps(rule_text_input))},
            {tab} = {clob_chunks_sql_function(json.dumps(tab_json))}
        WHERE case_id = '{case_id}'
    """)
    for tab, tab_json in tab_json_cache.items():
        json_str = json.dumps(tab_json)
        clob_expr = clob_chunks_sql_function(json_str)

        qry = f"""
            UPDATE ocr
            SET {tab} = {clob_expr}
            WHERE case_id='{case_id}'
        """
        extraction_db.execute_(qry)


def replace_field_value_direct(text, field, new_value):
    key = f"{field}("
    idx = text.find(key)
    if idx == -1:
        return text

    start = idx + len(key)
    end = text.find(")", start)
    if end == -1:
        return text

    old = text[idx:end + 1]
    new = f"{field}({new_value})"
    return text.replace(old, new, 1)

def calculate_rule(rule_text, deleted_fields):
    has_real_value = False

    if not rule_text:
        return 0.0, rule_text

    total = 0.0
    updated_text = rule_text

    expr = rule_text.replace("-", "+-")
    parts = expr.split("+")

    for part in parts:
        part = part.strip()
        if not part:
            continue

        sign = -1 if part.startswith("-") else 1
        if part.startswith("-"):
            part = part[1:].strip()

        if "(" not in part or ")" not in part:
            continue

        open_idx = part.rfind("(")
        close_idx = part.rfind(")")
        field = part[:open_idx].strip()
        val_str = part[open_idx + 1:close_idx].strip()

        # if field in deleted_fields and val_str == "":
        #     val = 0
        # else:
        #     try:
        #         val = float(val_str.replace(",", ""))
        #     except:
        #         val = 0
        if val_str not in ("", None):
            try:
                val = float(val_str.replace(",", ""))
                has_real_value = True   #  REAL VALUE FOUND
            except:
                val = 0
        else:
            val = 0


        total += sign * val

        old_fragment = part
        #new_fragment = f"{field}()" if field in deleted_fields else f"{field}({val_str})"
        if field in deleted_fields and val_str == "":
            new_fragment = f"{field}()"
        else:
            new_fragment = f"{field}({val_str})"
        updated_text = updated_text.replace(old_fragment, new_fragment, 1)
    if not has_real_value:
        return "", updated_text
    return round(total, 2), updated_text


def calculate_final_value(rule_text):
    if not rule_text:
        return None

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
                open_idx = current.rfind("(")
                close_idx = current.rfind(")")
                if open_idx != -1 and close_idx > open_idx:
                    try:
                        val = float(current[open_idx + 1:close_idx].strip())
                        sign = -1 if current.strip().startswith("-") else 1
                        total += sign * val
                    except:
                        pass
            current = ch
        else:
            current += ch

    # last term
    if current.strip():
        open_idx = current.rfind("(")
        close_idx = current.rfind(")")
        if open_idx != -1 and close_idx > open_idx:
            try:
                val = float(current[open_idx + 1:close_idx].strip())
                sign = -1 if current.strip().startswith("-") else 1
                total += sign * val
            except:
                pass

    return round(total, 2)




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


@zipkin_span(service_name='folder_monitor', span_name='produce_with_zipkin')
def produce_with_zipkin(first_route, data):
    logging.info('Producing with zipkin...')

    zipkin_headers = create_http_headers_for_new_span()
    data['zipkin_headers'] = zipkin_headers
    logging.debug(f'Zipkin data: {data}')

    produce(first_route, data)


def get_task_id_by_index(tab_id, sub_queue_list, sub_queue_task_id_list):

    logging.info(f"####### tab_id: {tab_id}")
    logging.info(f"###### sub_queue_list: {sub_queue_list}")
    logging.info(f"###### sub_queue_task_id_list: {sub_queue_task_id_list}")

    queue_list = json.loads(sub_queue_list)
    task_id = ""

    if type(queue_list) == list:
        tab_index = queue_list.index(tab_id)
        task_id = json.loads(sub_queue_task_id_list)[tab_index]

    return task_id


def send_email_alert(tenant_id, template):
    host = os.environ.get('HOST_IP', "")
    data = {"tenant_id": tenant_id, "template": template}
    logging.debug(f"Data sending to email trigger is {data}")
    try:
        headers = {'Content-type': 'application/json; charset=utf-8'}
        logging.info("Entering to mail############")
        email_response = requests.post(
            f'http://{host}:5002/send_email', json=data, headers=headers)
        logging.info(f"####### EMAIL API Response: {email_response.text}")
    except Exception as e:
        traceback.logging.debug_exc()
        logging.info(f"Mail Failed {e}")
    return True


def get_task_id(processInstanceId):
 
    # URL for the Camunda REST API
    ip='camundaworkflow'
    url = f"http://{ip}:8080/rest/engine/default/task"
    # Parameters for the API request
    params = {
        "processInstanceId": processInstanceId
    }
    # Make the GET request to the API
    response = requests.get(url, params=params)
    task_id=''
    logging.info(f"#######response in get_task_id is {response}")
    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        task_data = response.json()
        logging.info(f"#######task_data in get_task_id is {task_data}")

        if task_data:
            task_id = task_data[0].get("id")
            logging.debug(f"Task ID: {task_id}")

        else:
            logging.debug("No tasks found for the given process instance ID.")

    else:
        logging.debug(f"Error: {response.status_code}")
        logging.debug(response.text)
 
    return task_id
 

@app.route('/execute_button_function', methods=['POST', 'GET'])
def execute_button_function():
    data = request.json
    logging.info(f"Request Data of Template Detection: {data}")

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except Exception:
        logging.warning("Failed to start ram and time calc")
        pass

    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    document_id = data.get('document_id', '')
    move_to = data.get("group", "default")
    session_presence = data.get('session_presence', True)

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
            service_name='button_functions',
            span_name='execute_button_function',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        
        user__ = user
        variables = data.get("variables", {})
        fields = data.get("fields", {})
        original_case_id = data.get('case_id', None)
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)

        # Session Validation for button functions
        if session_id and user:
            session_db = DB('group_access', **db_config)
            query_session_id = "SELECT * FROM `live_sessions` WHERE status = 'active' AND user_ = %s AND session_id = %s"
            output_session_id = session_db.execute_(query_session_id, params=[user, session_id]) 
            logging.info(f"output_session_id is {output_session_id}")

            if isinstance(output_session_id, pd.DataFrame) and not output_session_id.empty:
                logging.info(f"Session is active: {output_session_id}")
            else:
                return jsonify({'flag': False, 'message': 'User or Session not Valid'})
        elif session_presence is False:
            logging.info(f"Session presence is False, skipping session validation")
            session_id = None
        else:
            return jsonify({'flag': False, 'message': 'User or Session ID Required'})


        if tenant_id == 'ambanketrade':
            camunda_host = data.get('camunda_host', 'camundaworkflowtrade')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)

        if tenant_id == 'ambankdisbursement':
            camunda_host = data.get('camunda_host', 'camundaworkflowdisb')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)

        if tenant_id == 'kmb' or tenant_id == 'hdfc':
            camunda_host = data.get('camunda_host', 'camundaworkflow')
            camunda_port = data.get('camunda_port', '8080')
            camunda_port = int(camunda_port)
    


        if 'retrain' in data.keys():
            host = 'trainingapi'
            port = 80
            route = 'train'
            logging.debug(f'Hitting URL: http://{host}:{port}/{route}')
            headers = {
                'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
            response = requests.post(
                f'http://{host}:{port}/{route}', json=data, headers=headers)
            response = response.json()
            if response['flag']:
                return jsonify({'flag': True, 'data': {'message': 'Training completed!'}})
            else:
                msg = response['message']
                logging.exception('error in loading the training')
                return jsonify({'flag': False, 'message': msg})
        
        #If user clicks on Re-Extraction or Move to Maker then the what ever fields are edited those becomes empty
        if move_to == 'Re-Extraction' or move_to=='Move to Maker':
            logging.debug(f"enterd re-extraction")
            try:
                update_query = """ UPDATE ocr SET edited_fields = '', SELECTED_UNITS='' WHERE case_id = :case_id"""
                extraction_db.execute_(update_query, params=[case_id])
                logging.info(f"Re-extraction button clicked for case_id {case_id}. 'edited_fields' data cleared.")
            except Exception as e:
                logging.exception(f"Error occurred during Re-Extraction: {e}")
                
        if move_to == 'Approve':
            try:
                host = 'foldermonitor'
                port = 443
                route = 'dp_formula_calculation'
                logging.debug(f'Hitting URL: https://{host}:{port}/{route}')
                logging.debug(f'Sending Data: {data}')
                headers = {'Content-type': 'application/json; charset=utf-8',
                        'Accept': 'text/json'}
                response = requests.post(
                    f'https://{host}:{port}/{route}', json=data, headers=headers,verify=False)

                logging.info(f"in dp formula execution")
            except:
                message = '###issue in dp formula'
                logging.exception(message)


        try:
            queue_id = data.get('queue_id', None)
            
            tab_id = data.get('tab_id', None)


            update = {
                'last_updated_by': user__,
            }
            where = {
                'case_id': original_case_id
            }

            if original_case_id:
                queue_db.update('process_queue', update=update, where=where)

            queue_name = ''

            if queue_id:
                queue_name = list(queue_db.execute_(
                    f'select unique_name from queue_definition where id = {queue_id}')['unique_name'])[0]

            

            task_id = ""
            if tab_id is None:
                query_queue_list = f"select task_id from queue_list where case_id = '{case_id}' and queue='{queue_name}'"
                task_list = queue_db.execute_(query_queue_list).task_id
                if len(task_list) > 0:
                    task_id = list(queue_db.execute_(
                        query_queue_list).task_id)[0]
                else:
                    logging.info(
                        f"############ task id is empty for queue: {queue_name} and case id: {case_id}")

            else:

                

                if "variables" in data:
                    if "email_alert" in data["variables"]:
                        logging.debug(
                            "email alert is present in variables of button")

                        template = data["variables"]["email_alert"]

                        logging.debug("sending to send email alert mail")
                        flag = send_email_alert(tenant_id, template)

                    if "tab_id" in data["variables"]:
                        logging.info(f"Found tab id")
                        tab_id = data["variables"]["tab_id"]

                logging.info(f"##################333 Tab id found is {tab_id}")
                sub_queue_query = f"select sub_queue_list,sub_queue_task_id_list from process_queue where case_id='{case_id}'"
                sub_queue_df = queue_db.execute_(sub_queue_query)
                sub_queue_list = sub_queue_df["sub_queue_list"][0]
                sub_queue_task_id_list = sub_queue_df["sub_queue_task_id_list"][0]

                task_id = get_task_id_by_index(
                    tab_id, sub_queue_list, sub_queue_task_id_list)
           
            # ------------------ STEP 0: Read UI Input ------------------
            extra_params = data.get("extra_params", {})
            component_dict = extra_params.get("component_dict", {})

            # ------------------ STEP 0: Collect selected components per category ------------------
            selected_by_category = {}

            for category, cfg in component_dict.items():
                selected = cfg.get("selected_component_fields", [])
                selected = [c.strip() for c in selected if c and c.strip()]
                if selected:
                    selected_by_category[category.lower()] = selected

            # nothing selected → just skip this logic
            if not selected_by_category:
                pass


            # ------------------ STEP 1: Fetch party_id & existing mandatory_fields ------------------
            qry = """
                SELECT party_id, mandatory_fields
                FROM ocr
                WHERE case_id = %s
            """
            ocr_df = extraction_db.execute_(qry, params=[case_id])

            if ocr_df is None or ocr_df.empty:
                pass

            party_id = ocr_df.iloc[0]["party_id"]
            raw_mandatory = ocr_df.iloc[0]["mandatory_fields"]

            mandatory_fields = {}
            if raw_mandatory not in (None, "", "null", "None"):
                try:
                    mandatory_fields = json.loads(raw_mandatory)
                except Exception:
                    mandatory_fields = {}


            # ------------------ STEP 2: Flatten components for DB queries ------------------
            all_selected_components = set()
            for comps in selected_by_category.values():
                all_selected_components.update(comps)

            if all_selected_components:

                component_list_sql = ",".join(
                    "'" + c.replace("'", "''") + "'" for c in all_selected_components
                )

                # ------------------ STEP 3: Fetch AGE & MARGIN ------------------
                age_margin_qry = f"""
                    SELECT 
                        CAST(cm.component_name AS VARCHAR2(4000)) AS component_name,
                        am.age,
                        am.margin
                    FROM AGE_MARGIN_WORKING_UAT am
                    JOIN COMPONENT_MASTER cm
                        ON CAST(am.component_name AS VARCHAR2(4000)) = CAST(cm.component_name AS VARCHAR2(4000))
                    WHERE am.party_id = '{party_id}'
                    AND am.is_active = 1
                    AND CAST(cm.component_name AS VARCHAR2(4000)) IN ({component_list_sql})
                """

                age_margin_df = extraction_db.execute_(age_margin_qry)

                if age_margin_df is None or age_margin_df.empty:
                    pass

                age_margin_map = {}
                for _, row in age_margin_df.iterrows():
                    age_margin_map[row["COMPONENT_NAME"]] = {
                        "age": str(row["AGE"]) if row["AGE"] is not None else "",
                        "margin": str(row["MARGIN"]) if row["MARGIN"] is not None else ""
                    }

                # ------------------ STEP 4: MERGE (CATEGORY-WISE, NO DUPLICATES) ------------------
                for category, components in selected_by_category.items():

                    mandatory_fields.setdefault(category, [])

                    existing_map = {}
                    for item in mandatory_fields[category]:
                        for k, v in item.items():
                            existing_map[k] = v

                    for comp in components:
                        if comp in existing_map:
                            continue
                        # if comp not in age_margin_map:
                        #     continue

                        # existing_map[comp] = age_margin_map[comp]
                        existing_map[comp] = age_margin_map.get(comp, {"age": "", "margin": ""})

                    mandatory_fields[category] = [{k: v} for k, v in existing_map.items()]


                # ------------------ STEP 5: Save back to OCR ------------------
                
                mandatory_json = json.dumps(mandatory_fields)
                clob_expr = clob_chunks_sql_function(mandatory_json)

                update_qry = f"""
                    UPDATE ocr
                    SET mandatory_fields = {clob_expr}
                    WHERE case_id = '{case_id}'
                """

                extraction_db.execute_(update_qry)

            if move_to == 'Cancel':
                query_pq = 'UPDATE `process_queue` SET `case_lock`=0 WHERE `case_id`=%s'
                queue_db.execute(query_pq, params=[case_id])
                return jsonify({'flag': True})
            """if move_to == 'Submit Unhold Comments':
                
                return jsonify({'flag': True})
            if move_to == 'Submit Hold Comments':
                
                return jsonify({'flag': True})
            
            if move_to == 'Re-Apply':
                
                return jsonify({'flag': True})"""
            

            if "variables" in data:
                
                if "case_status" in data["variables"]:
                    update['status'] = data["variables"]["case_status"]

                if "screen_name" in data["variables"]:
                    move_to = move_to + ' ' + data["variables"]["screen_name"]


            api_params = {"variables": {"button": {"value": move_to},
                                         "ui_data": {"value": data}}, "case_id": case_id}
            
            # api_params = {"variables":{"button":{"value":"move_to"}},"case_id":"case_id"}

            logging.info(f'API Params: {api_params}')

            logging.info(
                f'Hitting URL: http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete')
            headers = {'Content-type': 'application/json; charset=utf-8'}

            request_post_ = requests.post(
                f'http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete', json=api_params, headers=headers , timeout=1500)

            request_post = request_post_.json
            logging.info(f"##########request_post is {request_post} ")

            
            if request_post_.status_code == 500 or request_post_.status_code == 404:   
                query_queue_list = f"select * from queue_list where case_id = '{case_id}'"
                queue_list_data = queue_db.execute_(query_queue_list)
 
                process_instance_id=queue_list_data['process_instance_id'].to_list()[0]
 
                #logging.debug(f"Request failed with status code {response.status_code}")
                #logging.debug(f"Error response content: {response.text}")
 
                task_id=get_task_id(process_instance_id)
 
                update2 = f"update queue_list set task_id ='{task_id}' where case_id = '{case_id}'"
                queue_db.execute_(update2)
           
                logging.debug(f'Hitting URL: http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete')
                request_post_2=requests.post(
                    f'http://{camunda_host}:{camunda_port}/rest/engine/default/task/{task_id}/complete',
                    json=api_params,
                    headers=headers
                )
                logging.debug(f"request_post_2 is:{request_post_2}")
                if request_post_2.status_code == 500 or request_post_2.status_code == 404:
                        message = json.loads(request_post_2.content)
                        logging.info(f"Camunda sent 500 the message is: {message}")
                        message = message['message'].split(':')[0]
                        message = f"{message}: Case is in use by multiple users"
                        logging.info(f"Final message: {message}")
                        if message == case_id:
                            message = "Something went wrong while saving the data! Please try again."
                        response_data = {'flag': False, 'message': message, 'data': {
                            "button_name": move_to}}

                        query = f"select pre_task_id from queue_list where case_id = '{case_id}'"
                        pre_task_id = queue_db.execute_(query)['pre_task_id'].to_list()[0]
                        logging.debug(f"pre_task_id is {pre_task_id}")

                        logging.debug(f'hitting with pre task id')

                        update2 = f"UPDATE queue_list SET task_id = '{pre_task_id}' WHERE case_id = '{case_id}'"
                        
                        queue_db.execute_(update2)

                        logging.debug(
                            f'Hitting URL: http://{camunda_host}:{camunda_port}/rest/engine/default/task/{pre_task_id}/complete')

                        requests.post(f'http://{camunda_host}:{camunda_port}/rest/engine/default/task/{pre_task_id}/complete', json=api_params, headers=headers)
                    
                        

       
 
            
            
            
            response_data = {'flag': True, 'data': {"status": "Button executed successfully", "button_name": move_to}}
            
            queue_db.update('process_queue', update=update,
                            where={'case_id': case_id})
            
        except KeyError as e:
            logging.exception(
                'Something went wrong executing button functions. Check Trace.')
            response_data = {'flag': False, 'message': 'something went wrong', 'data': {
                "button_name": move_to}}

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
        

        
        # insert audit
        audit_data = {"tenant_id": tenant_id, "user_": user, "case_id": case_id,
                      "api_service": "execute_button_function", "service_container": "button_functions", "changed_data": None,
                      "tables_involved": "", "memory_usage_gb": str(memory_consumed),
                      "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
                      "response_data": json.dumps(response_data['data']), "trace_id": trace_id, "session_id": session_id, "status": str(response_data['flag'])}
        insert_into_audit(case_id, audit_data)

        return jsonify(response_data)





def normalize_rule_text(rule_text: str) -> str:
    """
    For each segment (split by '+'), find the last parentheses group.
    If that last group's content contains NO letters, replace that last group with ().
    Leave all earlier parentheses groups unchanged.
    """
    if not rule_text:
        return rule_text

    # Keep plus signs as separate tokens so we rejoin preserving spacing
    #parts = re.split(r'(\+)', rule_text)
    parts = re.split(r'([+-])', rule_text)

    out_parts = []
    paren_re = re.compile(r'\([^)]*\)')

    for part in parts:
        # If this part is a plus symbol, keep it (spacing handled below)
        if part in ('+', '-'):
            out_parts.append(part)
            continue

        # Work on the segment: find all paren matches
        matches = list(paren_re.finditer(part))
        if not matches:
            out_parts.append(part)
            continue

        last = matches[-1]
        content = last.group(0)[1:-1]   # inside the parentheses

        # If the last paren contains any alphabetic char -> leave it
        if re.search(r'[A-Za-z]', content):
            out_parts.append(part)
            continue

        # Replace only the last paren span with "()"
        start, end = last.span()
        new_part = part[:start] + "()" + part[end:]
        out_parts.append(new_part)

    # Reconstruct string: join and normalize spacing around '+'.
    result = "".join(out_parts)

    # Normalize spacing: ensure single space around '+' like " A + B "
    result = re.sub(r'\s*\+\s*', ' + ', result)
    result = re.sub(r'\s*\-\s*', ' - ', result)
    # Collapse multiple spaces
    result = re.sub(r'\s{2,}', ' ', result).strip()
    return result






@app.route("/save_rules", methods=['POST', 'GET'])
def save_rules():
    """
    This endpoint /save_rules handles both POST and GET requests to save business rules into the rule_base_phase_2 table in the BUSINESS_RULES database.
    It fetches the related party_id based on a provided case_id from the OCR table in the extraction database and enriches the input data with audit fields like CREATED_BY and LAST_MODIFIED_BY. 
    The rule data, including a list-like field component_field, is inserted into the database after being properly formatted.
    """
    data = request.json
    logging.info(f'Request data for save_rules: {data}')

    case_id = data.get('case_id', '')
    tenant_id = data.get('tenant_id')
    created_by = data.get('user')
    changed_data = data.get('changed_data', {})
    tab_json_cache={}

    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    business_rules_db = DB('BUSINESS_RULES', **db_config)


    # --------------------------------------------------
    # MAIN FLOW
    # --------------------------------------------------

    try:
        if not case_id:
            return {"flag": False, "message": "Case_id is not provided"}

        party_df = extraction_db.execute_(f"""
            SELECT PARTY_ID FROM OCR WHERE CASE_ID='{case_id}'
        """)
        party_id = party_df['PARTY_ID'].iloc[0] if not party_df.empty else None

        changed_data['PARTY_ID'] = party_id
        changed_data['CREATED_BY'] = created_by
        changed_data['LAST_MODIFIED_BY'] = created_by
        changed_data["component_field"] = json.dumps(changed_data.get("component_field", []))
        changed_data.pop("field_names", None)
        raw_rule_text = changed_data.get("rule_text", "")

        #  UPDATE OCR INLINE DATA FIRST
        update_rule_text_input(case_id, extraction_db, changed_data,tab_json_cache)

        # ---- AUDIT ----
        changed_fields_json = json.dumps({
            "rule_name": changed_data.get("rule_name"),
            "tab_name": changed_data.get("tab_name")
        })

        new_values_json = json.dumps({"new_value": raw_rule_text})

        currentTS = datetime.now(pytz.timezone(tmzone)).strftime(
            '%d-%b-%y %I.%M.%S.%f %p'
        ).upper()

        extraction_db.execute_(f"""
            INSERT INTO audit_report_case
            (CASE_ID, PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES,
             MAKER_ID, MAKER_DATETIME, CATEGORY)
            VALUES (
                '{case_id}',
                '{party_id}',
                '{changed_fields_json.replace("'", "''")}',
                ' ',
                '{new_values_json.replace("'", "''")}',
                '{created_by}',
                '{currentTS}',
                'Creating a new business_rule'
            )
        """)

        # ---- SAVE RULE ----
        changed_data["original_rule_text"] = raw_rule_text
        changed_data["rule_text"] = normalize_rule_text(raw_rule_text)
        changed_data["created_case_id"] = case_id

        business_rules_db.insert_dict(changed_data, 'rule_base_phase_2')

        return {"flag": True, "message": "Successfully Saved the Rule"}

    except Exception as e:
        logging.exception("save_rules failed")
        return {"flag": False, "message": "Exception while saving the Rule"}


@app.route("/rules_dropdown_values", methods=['POST', 'GET'])
def rules_dropdown_values():
    """
    Retrieves dropdown field values and existing rule data for rule creation or modification.

    Functionality:
    - When 'flag' is 'create':
        - Returns dropdown field values grouped by tab from the REQUIRED_FIELDS_TABLE.
    - When 'flag' is 'modify':
        - Retrieves the PARTY_ID using the given case_id from the OCR table.
        - Returns both the dropdown field values and the existing rules for that PARTY_ID
          from the rule_base_phase_2 table, grouped by rule_name.
    """
    # Get JSON request body
    data = request.json
    logging.info(f'Request data for rules_dropdown_values: {data}')

    # Extract required parameters from the request
    tenant_id = data.get('tenant_id', None)
    case_id = data.get('case_id', None)
    flag = data.get('flag', None)
    rules_deleted=data.get('rules_deleted',{})
    # Set the tenant context for DB connection
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    business_rules_db = DB('business_rules', **db_config)
    queues_db = DB('queues', **db_config)
    user_=data.get('user',None)
   

    try:
        # Query to fetch tab-wise required fields
        tab_fields_query = """Select TABLE_DIV, FIELDS FROM REQUIRED_FIELDS_TABLE WHERE SEG = 'non_stand'"""
        query_df = extraction_db.execute_(tab_fields_query)
        table_div_list = query_df['TABLE_DIV'].tolist()
        columns_str = ', '.join(table_div_list)

        tabs_data_query = f"""SELECT {columns_str} FROM ocr WHERE case_id = '{case_id}'"""
        tabs_df = extraction_db.execute_(tabs_data_query)
        final_result = {}

        #get party_id from ocr
        party_id_query = f"""select PARTY_ID from ocr where case_id = '{case_id}'"""
        party_id_df = extraction_db.execute_(party_id_query)
        party_id = party_id_df['PARTY_ID'].tolist()[0] if not party_id_df.empty else None


        # Assuming tabs_df has only 1 row (for the case_id)
        tab_data_row = tabs_df.iloc[0]
        tab_data_new = {k.upper(): v for k, v in tab_data_row.items()}
        for _, row in query_df.iterrows():
            tab_name = row['TABLE_DIV']
            fields_lob = row['FIELDS']
            try:
                fields_str = str(fields_lob.read()) if hasattr(fields_lob, 'read') else str(fields_lob)
                nested_list = ast.literal_eval(fields_str)
                required_fields = [item for sublist in nested_list for item in sublist]
    
                tab_key = tab_name.lower()
                TAB_CATEGORY_MAP = {"STOCKS":"Stock","STOCK":"Stock","INVENTORY":"Inventory","DEBTORS":"Debtors","RECEIVABLES":"Receivable","RECEIVABLE":"Receivable","CREDITORS":"Creditors","ADVANCES":"Advances"}
                tab_map = tab_name.upper()          # for mapping
                #print(f"tab_map is....:{tab_map}")

                component_category = TAB_CATEGORY_MAP.get(tab_map)
                #print(f"component_category is...........:{component_category}")
                if not component_category:
                    #print(f"[WARN] No component category mapping for tab: {tab_name}")
                    component_category = None
                component_query = f"""
                        SELECT DISTINCT CAST(cm.component_name AS VARCHAR2(4000)) AS component_name
                        FROM AGE_MARGIN_WORKING_UAT am
                        JOIN COMPONENT_MASTER cm on CAST(am.component_name AS VARCHAR2(4000))=CAST(cm.component_name AS VARCHAR2(4000))
                        WHERE CAST(cm.status AS VARCHAR2(4000)) = 'ACTIVE'
                        AND CAST(cm.component_name AS VARCHAR2(4000)) NOT LIKE '%dummy%' AND CAST(cm.component_category AS VARCHAR2(4000))='{component_category}'
                    """
                #print(f"component_query is...:{component_query}")
                component_df = extraction_db.execute_(component_query)

                component_master_fields = (
                    component_df["COMPONENT_NAME"]
                    .dropna()
                    .str.strip()
                    .tolist()
                    if component_df is not None and not component_df.empty
                    else []
                )
                #print(f"component_master_fields is..:{component_master_fields}")

                component_master_fields = set(component_master_fields)
                component_master_fields = [
                    f.strip()
                    for f in component_master_fields
                ]

                # component_fields = component_dict.get(tab_key, [])
                # duplicated_fields = duplicated_dict.get(tab_key, [])
                # dynamic_fields = dynamic_dict.get(tab_key,[])
                #flat_fields = required_fields + component_fields + duplicated_fields + dynamic_fields

                tab_name_new = tab_name.upper()
                #print(f"tab_name_new is........:{tab_name_new}")
                tab_data_raw = tab_data_new.get(tab_name_new, {})
                #print(f"tab_data_raw is.....:{tab_data_raw}")
                #tab_fields = set(k for k in tab_data_raw.keys())
                #print(f"tab_fields is......:{tab_fields}")
                #print(f"component_master_fields is............:{component_master_fields}")
                #remaining_tab_fields = list(tab_fields - component_master_fields)  
                #flat_fields=component_master_fields+remaining_tab_fields
                if hasattr(tab_data_raw, "read"):
                    tab_data_raw = tab_data_raw.read()

                if isinstance(tab_data_raw, str):
                    try:
                        tab_data_raw = ast.literal_eval(tab_data_raw)
                    except Exception:
                        tab_data_raw = json.loads(tab_data_raw)

                if not isinstance(tab_data_raw, dict):
                    tab_data_raw = {}

                # STEP 2: TAB FIELDS (CASE_ID ONLY)
                tab_fields = [
                    k.strip()
                    for k in tab_data_raw.keys()
                    if k.lower() != "tab_view"  
                ]

                # print(f"TAB NAME      : {tab_name}")
                # print(f"TAB FIELDS    : {tab_fields}")
                # print(f"COMPONENTS    : {component_master_fields}")

                # STEP 3: REMOVE COMPONENT FIELDS (LIST → LIST)
                remaining_tab_fields = [
                    f for f in tab_fields
                    if f not in component_master_fields
                ]
                #print(f"remaining_tab_fields are......:{remaining_tab_fields}")
                # STEP 4: BUILD flat_fields (SAME DATA TYPE)
                flat_fields = (
                    component_master_fields
                    + remaining_tab_fields
                )
                #print(f"flat_fields are..........:{flat_fields}")

                # remove duplicates, preserve order
                flat_fields = list(dict.fromkeys(flat_fields))
                if hasattr(tab_data_raw, "read"):
                    try:
                        tab_data_raw = tab_data_raw.read()
                    except Exception:
                        tab_data_raw = "{}"  # fallback if read fails
                if isinstance(tab_data_raw, str):
                    try:
                        tab_data_raw = ast.literal_eval(tab_data_raw)
                    except Exception:
                        #tab_data_raw = {}
                        tab_data_raw = json.loads(tab_data_raw)
                
                extracted_fields = {}
                for field in flat_fields:
                    field_data = tab_data_raw.get(field, {})
                    if isinstance(field_data, dict):
                        a_v_data = field_data.get("a.v", {})
                        # Just get the first value inside a.v, whatever the key is
                        if isinstance(a_v_data, dict) and a_v_data:
                            converted_value = list(a_v_data.values())[0]
                        else:
                            converted_value = "0"
                    elif isinstance(field_data, (str, int, float)):
                        converted_value = field_data
                    else:
                        converted_value = 0
                    extracted_fields[field] = str(converted_value) if converted_value not in [None, "", "null"] else "0"
                final_result[tab_name] = extracted_fields

            except Exception as e:
                logging.debug(f"Error processing tab {tab_name}: {e}")
                final_result[tab_name] = {}

        stocks = final_result.get("Stocks", {})

        # Remove unwanted similar keys if present
        stocks.pop("Stores  & Spares", None)
        stocks.pop("Consumable & Spares", None)

        # Update dictionary back
        final_result["Stocks"] = stocks

        # If flag is "create", only return dropdown values
        if flag.lower() == 'create':
            response = {
                "flag": True,
                "data":final_result
            }
        

        elif flag.lower() == 'modify':
            if case_id:

                # ---------------- GET PARTY_ID ----------------
                party_id_query = f"""
                    SELECT PARTY_ID FROM OCR WHERE CASE_ID = '{case_id}'
                """
                result_df = extraction_db.execute_(party_id_query)
                party_id = result_df['PARTY_ID'].tolist()[0] if not result_df.empty else None
                dropdown_values = final_result

                if not party_id:
                    return {
                        "flag": False,
                        "message": "Party ID not found"
                    }

                # ---------------- FETCH RULES ----------------
                rules_query = f"""
                    SELECT * FROM rule_base_phase_2
                    WHERE party_id = '{party_id}'
                """
                rules_df = business_rules_db.execute_(rules_query)

                rules_dict = rules_df.to_dict(orient='records')
                rule_data = {rule['RULE_NAME']: rule for rule in rules_dict}

                # ---------------- LOAD RULE_TEXT_INPUT ----------------
                qry = f"""
                    SELECT rule_text_input
                    FROM ocr
                    WHERE case_id = '{case_id}'
                """
                df = extraction_db.execute_(qry)

                if df is not None and not df.empty and df.iloc[0]["rule_text_input"]:
                    rule_text_input = json.loads(df.iloc[0]["rule_text_input"])
                else:
                    rule_text_input = {}


                
                # ---------------- OVERRIDE RULE_TEXT + FINAL_VALUE ----------------
                for rule_name, rule_item in rule_data.items():

                    rule_id = rule_item.get("RULE_ID")
                    tab_name = (rule_item.get("TAB_NAME") or "").lower()
                    final_field = rule_item.get("FINAL_FIELD")

                    # default from DB
                    rule_text = rule_item.get("RULE_TEXT")

                    #  override from OCR rule_text_input
                    if (
                        tab_name in rule_text_input
                        and rule_id in rule_text_input[tab_name]
                        and final_field in rule_text_input[tab_name][rule_id]
                    ):
                        stored_rule = rule_text_input[tab_name][rule_id][final_field]

                        if isinstance(stored_rule, dict):
                            rule_text = stored_rule.get("previous", "")
                        else:
                            rule_text = stored_rule
                        #rule_text = rule_text_input[tab_name][rule_id][final_field]

                    rule_item["rule_text"] = rule_text
                    rule_item["final_value"] = calculate_final_value(rule_text)

                response = {
                    "flag": True,
                    "data": rule_data,
                    "dropdown_values":dropdown_values
                }



        if rules_deleted:

            # -------------------------
            # Fetch PARTY_ID
            # -------------------------
            party_id_query = f"""
                SELECT PARTY_ID
                FROM OCR
                WHERE CASE_ID = '{case_id}'
            """
            result_df = extraction_db.execute_(party_id_query)
            party_id = result_df['PARTY_ID'].iloc[0] if not result_df.empty else None

            # -------------------------
            # Load rule_text_input ONCE
            # -------------------------
            rule_q = f"""
                SELECT rule_text_input
                FROM OCR
                WHERE CASE_ID = '{case_id}'
            """
            rule_df = extraction_db.execute_(rule_q)

            if rule_df is not None and not rule_df.empty and rule_df.iloc[0]["rule_text_input"]:
                rule_text_input = json.loads(rule_df.iloc[0]["rule_text_input"])
            else:
                rule_text_input = {}

            current_ist = datetime.now(pytz.timezone(tmzone))
            currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()

            # -------------------------
            # Loop deleted rules
            # -------------------------
            for tab_name, rule_map in rules_deleted.items():

                tab = tab_name.lower()

                for rule_id, final_field in rule_map.items():

                    # -------------------------
                    # OLD VALUE FROM rule_text_input
                    # -------------------------
                    old_value = None
                    if (
                        tab in rule_text_input
                        and rule_id in rule_text_input[tab]
                        and final_field in rule_text_input[tab][rule_id]
                    ):
                        old_value = rule_text_input[tab][rule_id][final_field]

                    changed_fields = {
                        "rule_id": rule_id,
                        "rule_name": final_field,
                        "tab_name": tab_name
                    }

                    sql = f"""
                        INSERT INTO audit_report_case
                        (
                            CASE_ID,
                            PARTY_ID,
                            CHANGED_FIELDS,
                            OLD_VALUES,
                            NEW_VALUES,
                            MAKER_ID,
                            MAKER_DATETIME,
                            CATEGORY
                        )
                        VALUES
                        (
                            '{case_id}',
                            '{party_id}',
                            '{json.dumps(changed_fields).replace("'", "''")}',
                            '{json.dumps(old_value).replace("'", "''") if old_value else ''}',
                            NULL,
                            '{user_}',
                            '{currentTS}',
                            'Deleting a business_rule'
                        )
                    """
                    extraction_db.execute_(sql)
                    if tab in rule_text_input and rule_id in rule_text_input[tab]:
                        rule_text_input[tab].pop(rule_id, None)

                        # remove empty tab
                        if not rule_text_input[tab]:
                            rule_text_input.pop(tab, None)

            # -------------------------
            # SAVE UPDATED rule_text_input
            # -------------------------
            rule_json = json.dumps(rule_text_input)
            rule_clob = clob_chunks_sql_function(rule_json)

            update_sql = f"""
                UPDATE OCR
                SET rule_text_input = {rule_clob}
                WHERE CASE_ID = '{case_id}'
            """
            extraction_db.execute_(update_sql)
            # ---------------- FETCH field_accuracy ONCE ----------------
            qr2 = f"""
                SELECT fields_extracted, fields_edited, not_extracted_fields,
                    extraction, manual
                FROM field_accuracy
                WHERE case_id='{case_id}'
            """
            df2 = queues_db.execute(qr2).to_dict(orient='records')

            if not df2:
                return

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
            total = 0


            # ---------------- APPLY DELETIONS ----------------
            for tab_name, rule_map in rules_deleted.items():
                for rule_id, final_field in rule_map.items():

                    # Remove from edited
                    if final_field in fields_edited:
                        fields_edited.pop(final_field)
                        manual = max(manual - 1, 0)

                    # Remove from not extracted
                    if final_field in not_extracted_fields:
                        not_extracted_fields.pop(final_field)
                        not_extracted_fields[final_field]=""

                    # Remove from extracted
                    if final_field in fields_extracted:
                        fields_extracted.pop(final_field)
                        extraction = max(extraction - 1, 0)


            # ---------------- RECALCULATE PERCENTAGES ONCE ----------------
            total = extraction + manual

            extraction_percentage = round(
                (extraction / total) * 100 if total > 0 else 0, 2
            )

            manual_percentage = round(
                (manual / total) * 100 if total > 0 else 0, 2
            )

            # ---------------- UPDATE field_accuracy ----------------
            json_str = json.dumps(fields_extracted)
            clob_expr = clob_chunks_sql_function(json_str)

            json_str_1 = json.dumps(fields_edited)
            clob_expr_1 = clob_chunks_sql_function(json_str_1)

            json_str_2 = json.dumps(not_extracted_fields)
            clob_expr_2 = clob_chunks_sql_function(json_str_2)

            update_qry = f"""
                UPDATE field_accuracy
                SET fields_extracted = {clob_expr},
                    fields_edited = {clob_expr_1},
                    not_extracted_fields = {clob_expr_2},
                    extraction = {extraction},
                    extraction_percentage = {extraction_percentage},
                    manual_percentage = {manual_percentage},
                    total = {total}
                WHERE case_id='{case_id}'
            """
            queues_db.execute(update_qry)


        
    except Exception as e:
        logging.info(f'exception at {e}')
        return {
            "flag": False,
            "message": str(e)
        }
    
    try:
        if rules_deleted:
            tab_json_cache={}

            
            # ---------------- GET PARTY_ID ----------------
            party_q = f"""
                SELECT PARTY_ID
                FROM OCR
                WHERE CASE_ID = '{case_id}'
            """
            pid_df = extraction_db.execute_(party_q)
            if pid_df is None or pid_df.empty:
                raise Exception("Invalid case_id — PARTY_ID not found")

            party_id = pid_df['PARTY_ID'].iloc[0]

            # ---------------- LOAD RULE_TEXT_INPUT ----------------
            rule_q = f"""
                SELECT rule_text_input
                FROM OCR
                WHERE CASE_ID = '{case_id}'
            """
            rule_df = extraction_db.execute_(rule_q)
            rule_text_input = (
                json.loads(rule_df.iloc[0]["rule_text_input"])
                if rule_df is not None and not rule_df.empty and rule_df.iloc[0]["rule_text_input"]
                else {}
            )

            # ---------------- PROCESS DELETIONS ----------------
            for tab_name, rule_map in rules_deleted.items():

                tab = tab_name.lower()
                deleted_fields = set()

                for rule_id, final_field in rule_map.items():
                    deleted_fields.add(final_field)

                    business_rules_db.execute_(f"""
                        DELETE FROM RULE_BASE_PHASE_2
                        WHERE PARTY_ID = '{party_id}'
                        AND RULE_ID = '{rule_id}'
                    """)

                    if tab in rule_text_input and rule_id in rule_text_input[tab]:
                        rule_text_input[tab].pop(rule_id)
                        if not rule_text_input[tab]:
                            rule_text_input.pop(tab)

                    ocr_df = extraction_db.execute_(f"""
                        SELECT {tab}
                        FROM OCR
                        WHERE CASE_ID = '{case_id}'
                    """)

                    if ocr_df is None or ocr_df.empty:
                        continue

                    #tab_json = json.loads(ocr_df.iloc[0][tab])

                    if ocr_df is None or ocr_df.empty:
                        tab_json_cache[tab] = {}
                    else:
                        if tab_name not in tab_json_cache:
                            raw_tab = ocr_df.iloc[0].get(tab)

                            try:
                                tab_json_cache[tab] = json.loads(raw_tab) if raw_tab else {}
                            except Exception:
                                tab_json_cache[tab] = {}

                    tab_json = tab_json_cache[tab]


                    if final_field in tab_json:
                        tab_json[final_field] = ""

                    if "tab_view" in tab_json and "rowData" in tab_json["tab_view"]:
                        for row in tab_json["tab_view"]["rowData"]:
                            if row.get("fieldName") == final_field:
                                row["value"] = ""
                                row["formula"] = ""

                

                # -------- DEPENDENT RULE RECALCULATION --------
                if tab in rule_text_input:
                    for rid, rules in rule_text_input[tab].items():
                        for dep_field, rule_obj in rules.items():
                            rule_text = (
                                rule_obj.get("previous")
                                if isinstance(rule_obj, dict)
                                else rule_obj
                            )
                            #print(f"the rule text first is...............:{rule_text}")

                            # -------- RESTORE DELETED FIELD FROM tab_json.initial_value --------
                            init_map = tab_json.get("initial_value", {})

                            for deleted_field in deleted_fields:

                                init_entry = init_map.get(deleted_field, {})
                                #if prev_val in ("", None, "null", "None"):
                                #    continue

                                #  VERY IMPORTANT: restore only if rule contains the field
                                if f"{deleted_field}(" not in rule_text:
                                    #print(f"continueeeeeeeeeeeeeeeeeeeeedddddddddddddddddd")
                                    continue
                                prev_val = init_entry.get("previous")


                                #  restore ONLY if previous exists
                                if prev_val not in ("", None, "null", "None"):

                                    # restore rule texti
                                    # print(f"rule_text is...............:{rule_text}")
                                    # print(f"deleted_field is going to replace is......:{deleted_field}")
                                    # print(f"prev_val is.................:{prev_val}")
                                    rule_text = replace_field_value_direct(
                                        rule_text,
                                        deleted_field,
                                        prev_val
                                    )
                                    #print(f"rule_text is after repalce ...............:{rule_text}")

                                    #  restore OCR tab field
                                    tab_json[deleted_field] = str(prev_val)

                                    #  restore tab_view
                                    if "tab_view" in tab_json and "rowData" in tab_json["tab_view"]:
                                        for row in tab_json["tab_view"]["rowData"]:
                                            if row.get("fieldName") == deleted_field:
                                                row["value"] = str(prev_val)
                                                break

                                    #  flip initial_value (DO NOT touch previous)
                                    tab_json["initial_value"][deleted_field] = {
                                        "previous": "",
                                        "updated": ""
                                    }
                            #print(f"calcuate text goong rule_text is..................:{rule_text}")
                            new_total, new_rule_text = calculate_rule(rule_text, deleted_fields)

                            #  INITIAL VALUE RESTORE (RULE TEXT + OCR TAB)
                            
                            if isinstance(rule_obj, dict):
                                rule_text_input[tab][rid][dep_field]["updated"] = new_rule_text
                                rule_text_input[tab][rid][dep_field]["previous"] = new_rule_text

                            if dep_field in tab_json:
                                tab_json[dep_field] = str(new_total)

                


            extraction_db.execute_(f"""
                UPDATE OCR
                SET rule_text_input = {clob_chunks_sql_function(json.dumps(rule_text_input))}
                WHERE CASE_ID = '{case_id}'
            """)
            for tab, tab_json in tab_json_cache.items():
                json_str = json.dumps(tab_json)
                clob_expr = clob_chunks_sql_function(json_str)

                qry = f"""
                    UPDATE ocr
                    SET {tab} = {clob_expr}
                    WHERE case_id='{case_id}'
                """
                extraction_db.execute_(qry)

            logging.info("Completed rules_deleted cleanup successfully")

    except Exception:
        logging.exception("Error while deleting rules")

    return response




@app.route("/update_rules", methods=['POST', 'GET'])   
def update_rules():
    """
    Updates an existing business rule in the rule_base_phase_2 table.

    Functionality:
    - Accepts updated rule data from the client, including a rule_id to identify the rule.
    - Serializes the 'component_field' (usually a list or dict) into a JSON string for storage.
    - Performs an update operation on the rule_base_phase_2 table using the provided rule_id.
    """
    # Extract JSON request data
    data = request.json
    logging.info(f'Request data for update_rules: {data}')

    # Extract required parameters from the request
    tenant_id = data.get('tenant_id', None)
    changed_data = data.get('changed_data', {})
    rule_id = changed_data.get('rule_id', None)
    user=data.get('user',None)
    case_id = data.get('case_id', '')
    tab_json_cache={}

    # Set the tenant context for DB connection
    db_config['tenant_id'] = tenant_id
    business_rules_db = DB('business_rules', **db_config)
    extraction_db=DB('extraction',**db_config)

    try:
        # Fetch party_id associated with the given case_id
        if case_id:
            party_id_query = f""" SELECT PARTY_ID FROM OCR WHERE CASE_ID = '{case_id}'"""
            result_df = extraction_db.execute_(party_id_query)

            # Extract party_id from query result, if any
            party_id = result_df['PARTY_ID'].tolist()[0] if not result_df.empty else None
    except Exception as e:
        logging.info(f'exception-{e}')
        pass
    

    
    old_value_query = f"""SELECT rule_text FROM rule_base_phase_2 where rule_id='{rule_id}'"""
    result = business_rules_db.execute_(old_value_query)
    old_values = result['rule_text'].iloc[0]  if not result.empty else {}
    
    changed_fields = {
        'rule_name': changed_data.get('rule_name', ''),
        'tab_name': changed_data.get('tab_name', '')
    }
    changed_data.pop("field_names", None)

    #new_values = {
    #    'new_value': changed_data.get('rule_text', '')
    #}
    raw_rule_text = changed_data.get('rule_text', '')

    new_values = {
        'new_value': raw_rule_text
    }
    changed_data.pop("field_names", None)
    current_ist = datetime.now(pytz.timezone(tmzone))
    currentTS = current_ist.strftime('%d-%b-%y %I.%M.%S.%f %p').upper()
    logging.info(f"####currentTS now is {currentTS}")


    # ---------------- UPDATE OCR RULE_TEXT_INPUT ----------------

    try:
        tab_name    = changed_data.get("tab_name", "").lower()
        final_field = changed_data.get("final_field")
        rule_text   = raw_rule_text   # keep RAW text here (not normalized)
        
        if tab_name and rule_id and final_field and rule_text:


            qry = f"SELECT {tab_name}, rule_text_input FROM ocr WHERE case_id = '{case_id}'"
            df = extraction_db.execute_(qry)
            #tab_json = json.loads(df.iloc[0][tab_name]) if df.iloc[0][tab_name] else {}
            if df is None or df.empty:
                tab_json_cache[tab_name] = {}
            else:
                if tab_name not in tab_json_cache:
                    raw_tab = df.iloc[0].get(tab_name)

                    try:
                        tab_json_cache[tab_name] = json.loads(raw_tab) if raw_tab else {}
                    except Exception:
                        tab_json_cache[tab_name] = {}

            tab_json = tab_json_cache[tab_name]

            #  CAPTURE INITIAL VALUE INSIDE TAB (ONCE)
            #ensure_initial_value(tab_json, final_field)


            if df is not None and not df.empty and df.iloc[0]["rule_text_input"]:
                rule_text_input = json.loads(df.iloc[0]["rule_text_input"])
            else:
                rule_text_input = {}

            # ensure structure
            rule_text_input.setdefault(tab_name, {})
            rule_text_input[tab_name].setdefault(rule_id, {})

            # update ONLY this rule
            #rule_text_input[tab_name][rule_id][final_field] = rule_text
            rule_text_input[tab_name][rule_id][final_field] = {
                "previous": rule_text,
                "updated": rule_text
            }
            rule_json = json.dumps(rule_text_input)
            rule_clob = clob_chunks_sql_function(rule_json)

            qry = f"""
                UPDATE ocr
                SET rule_text_input = {rule_clob},
                {tab_name} = {clob_chunks_sql_function(json.dumps(tab_json))}
                WHERE case_id = '{case_id}'
            """
            extraction_db.execute_(qry)
            # init_json = json.dumps(initial_value_json)
            # init_clob = clob_chunks_sql_function(init_json)

            # qry = f"""
            #     UPDATE ocr
            #     SET initial_value = {init_clob}
            #     WHERE case_id = '{case_id}'
            # """
            # extraction_db.execute_(qry)
            for tab, tab_json in tab_json_cache.items():
                json_str = json.dumps(tab_json)
                clob_expr = clob_chunks_sql_function(json_str)

                qry = f"""
                    UPDATE ocr
                    SET {tab} = {clob_expr}
                    WHERE case_id='{case_id}'
                """
                extraction_db.execute_(qry)


    except Exception as e:
        logging.exception("Failed to update rule_text_input in OCR")


    # Convert all dict/values to JSON strings
    changed_fields_json = json.dumps(changed_fields, ensure_ascii=False)
    new_values_json = json.dumps(new_values, ensure_ascii=False)
    old_values_json = json.dumps(old_values, ensure_ascii=False)
    #  keep user-edited original text
    changed_data["original_rule_text"] = raw_rule_text

    #  keep template normalized
    changed_data["rule_text"] = normalize_rule_text(raw_rule_text)
    # Final SQL
    sql = f"""
    INSERT INTO audit_report_case 
    (CASE_ID, PARTY_ID, CHANGED_FIELDS, OLD_VALUES, NEW_VALUES, MAKER_ID, MAKER_DATETIME, CATEGORY) 
    VALUES (
        '{case_id}',
        '{party_id}',
        '{changed_fields_json.replace("'", "''")}',
        '{old_values_json.replace("'", "''")}',
        '{new_values_json.replace("'", "''")}',
        '{user}',
        '{currentTS}',
        'Modifying the business_rule'
    )
    """
    extraction_db.execute_(sql)
    
    try:
        # Convert component_field (if present) to a JSON string
        changed_data["component_field"] = json.dumps(changed_data.get("component_field", []))

        # Perform the update in the database for the given rule_id
        business_rules_db.update('rule_base_phase_2', update= changed_data, where={'rule_id': rule_id})
        
        # Return success response
        response = {
            "flag": True,
            "message": "Successfully Updated the Rule"
        }

    except Exception as e:
        logging.info(f'exception at {e}')
        response = {
            "flag": False,
            "message": "Exception while Updating the Rule"
        }
    return response
