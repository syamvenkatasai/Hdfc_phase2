import argparse
import ast
import base64
import json
import requests
import traceback
import warnings
import os
import pandas as pd
import shutil
import math
import psutil

import numpy as np

from collections import OrderedDict
from dateutil import parser
from app.elasticsearch_utils import elasticsearch_search

import re
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from datetime import datetime, timedelta
import datetime as dt
from db_utils import DB
from flask import Flask, request, jsonify
from flask_cors import CORS
from pandas import Series, Timedelta, to_timedelta
from time import time
from itertools import chain, repeat, islice, combinations
from ace_logger import Logging
from collections import defaultdict

from py_zipkin.util import generate_random_64bit_string
    
from app.get_fields_info import get_fields_info
from app.get_fields_info_utils import sort_ocr
from app.file_tree_maker import make_tree
from app.master_data_filter import Trie
from app import app
from app import cache 
logging = Logging(name='queue_api')

time_diff_hours = int(os.environ.get('SERVER_HOUR_DIFF','0'))
time_diff_minutes = int(os.environ.get('SERVER_MINUTES_DIFF','0'))

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def http_transport(encoded_span):
    body =encoded_span
    requests.post(
        'http://servicebridge:80/zipkin',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )
@app.route('/queue_api_health_check', methods=['POST', 'GET'])
def queue_api_health_check():
    print("In healthcheck")
    logging.info("The Hit reached Health Check")
    return jsonify({'flag': True})

def get_template_exceptions(db, data, tenant_id=None, queue_uid=None, queue_id='', user_case_filters=[], search_text = None):
    logging.info('Getting template exceptions')
    # logging.info(f'Data: {data}')
    start_point = data['start']
    end_point = data['end']
    offset = end_point - start_point

    db_config['tenant_id'] = tenant_id

    template_db = DB('template_db', **db_config)

    columns_data = get_columns(queue_uid, tenant_id, True) 
    columns = columns_data['columns']
    column_mapping = columns_data['column_mapping']
    column_order = list(column_mapping.keys())

    all_st = time()
    
    prefix_column = []

    for column in columns:
        prefix_column.append('pq.'+column.upper())

    elastic_input = {}
    if search_text:
        elastic_input['text'] = search_text
    elastic_input['columns'] = prefix_column
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    elastic_input['filter'] = [{'field': 'QUEUE_LIST.queue', 'value': queue_uid}] + user_case_filters
    elastic_input['source'] = 'process_queue'
    elastic_input['tenant_id'] = tenant_id

    
    try:

        
        files, total_files = elasticsearch_search(elastic_input)

        for document in files:
            document['pq.CREATED_DATE'] = parser.parse(document['pq.CREATED_DATE'])
            
            document['pq.CREATED_DATE'] = (document['pq.CREATED_DATE'] + timedelta(hours=time_diff_hours, minutes=time_diff_minutes)).strftime(r'%B %d, %Y %I:%M %p')
            
            for idx, column in enumerate(columns):
                if prefix_column[idx] in document:
                    document[column] = document.pop(prefix_column[idx])


        trained_templates = []

        if end_point > total_files:
            end_point = total_files

        pagination = {"start": start_point + 1, "end": end_point, "total": total_files}

        return {'flag': True, 'data': {'columns': columns, 'column_mapping': column_mapping,'files': files, 'template_dropdown': trained_templates, 'pagination': pagination, 'column_order': column_order}}
    except Exception as e:
        message = f'Error occured while getting template exception details. {e}'
        logging.error(message)
        return {'flag': False, 'message': message}





@cache.memoize(86400)
def get_dropdown(queue_id, tenant_id=None):

    try:

        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)

        

        query = f"""SELECT * FROM field_definition WHERE INSTR (queue_field_mapping, {queue_id}, 1, 1) > 0 and type != 'Table' ORDER BY field_order """
        field_ids = list(queue_db.execute_(query).id)

        dropdown_definition = queue_db.get_all('dropdown_definition')
        field_dropdown = dropdown_definition.loc[dropdown_definition['field_id'].isin(
            field_ids)]  # Filter using only field IDs from the file
        
        unique_field_ids = list(field_dropdown.field_id.unique())
        field_definition = queue_db.get_all('field_definition')
        # Get field names using the unique field IDs
        dropdown_fields_df = field_definition.ix[unique_field_ids]
        dropdown_fields_names = list(dropdown_fields_df.unique_name)

        dropdown = {}
        for index, f_id in enumerate(unique_field_ids):
            dropdown_options_df = field_dropdown.loc[field_dropdown['field_id'] == f_id]
            dropdown_options = list(dropdown_options_df.dropdown_option)
            dropdown[dropdown_fields_names[index]] = dropdown_options

        fields_df = field_definition.ix[field_ids]

        query = "select unique_name from field_definition where type LIKE '%%picker%%'"
        date_columns = list(queue_db.execute_(query).unique_name)

        # cascade_object
        cascade_object = '{}'

        

        return dropdown, fields_df, date_columns, cascade_object
    except Exception as e:
        logging.info(f"## Exception Occured in GET DROPDON .. {e}")
        return {},[], [], "{}"

@cache.memoize(86400)
def get_frontend_executable_rules(tenant_id):
    """Return the rules which are to be executed in frontend"""
    if not tenant_id:
        return []

    db_config['tenant_id'] = tenant_id # pass the tenant_id
    business_rules_db = DB('business_rules', **db_config)
    try:
        rule_strings_query = "SELECT rule_string from sequence_rule_data where executions='frontend'"
    except:
        pass

    try:
        return [json.loads(ele) for ele in list(business_rules_db.execute_default_index(rule_strings_query)['rule_string'])]
    except Exception as e:
        logging.error(f"This particular tenant {tenant_id} might not be configured correct.Please check executions column is present in sequence_rule_data table")
        logging.error(f"There might error in json parsing of the rules.Check whether all the rule strings are in good json format")
        logging.error(str(e))
        return []

@cache.memoize(86400)
def get_blob(case_id, tenant_id):
    db_config['tenant_id'] = tenant_id

    db = DB('queues', **db_config)

    query = "SELECT id, TO_BASE64(merged_blob) as merged_blob FROM merged_blob WHERE case_id=%s"
    blob_data = 'data:application/pdf;base64,' + list(db.execute(query, params=[case_id]).merged_blob)[0]

    return blob_data

@app.route("/get_blob_data", methods=['POST', 'GET'])
def get_blob_data():
    data = request.json
    case_id = data['case_id']
    tenant_id = data.pop('tenant_id', None)
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
            span_name='get_blob_data',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        blob_data = get_blob(case_id, tenant_id)

        return jsonify({"flag": True, "data": blob_data})


@app.route("/clear_cache", methods=['POST', 'GET'])
def clear_cache():
    try:
        shutil.rmtree('/tmp')
    except Exception as e:
        traceback.print_exc()
        pass
    return jsonify({"flag": True, "message": "Successfully cleared cache"})

@cache.memoize(86400)
def get_button_attributes(queue_id, queue_definition, tenant_id):
    db_config['tenant_id'] = tenant_id

    db = DB('queues', **db_config)

    query = "SELECT * FROM workflow_definition WHERE queue_id=%s"
    queue_workflow = db.execute(query, params=[queue_id])

    button_ids = list(queue_workflow['button_id'])

    if not button_ids:
        return []

    button_ids_str = ', '.join(str(x) for x in button_ids)
    query = f'SELECT `button_definition`.*, `button_functions`.`route`, `button_functions`.`parameters` FROM '\
        f'`button_function_mapping`, `button_definition`, `button_functions` WHERE '\
        f'`button_function_mapping`.`button_id`=`button_definition`.`id` AND '\
        f'`button_function_mapping`.`function_id`=`button_functions`.`id` AND '\
        f'`button_function_mapping`.`button_id` in ({button_ids_str}) order by button_definition.button_order'
    buttons_df = db.execute_(query)
    button_attributes = buttons_df.to_dict(orient='records')

    # logging.info(f'Button Attributes: {button_attributes}')

    final_dict = {}
    final_button_list = []
    for ele in button_attributes:
        route = ele.pop('route')
        parameters = ele.pop('parameters')

        if ele['text'] in final_dict:
            final_dict[ele['text']]['functions'].append({
                'route': route,
                'parameters': parameters.split(',')
            })
        else:
            ele['functions'] = [{
                'route': route,
                'parameters': parameters.split(',')
            }]
            final_dict[ele['text']] = ele

    for key, value in final_dict.items():
        final_button_list.append(value)

    button_attributes = final_button_list

    for button in button_attributes:
        workflow_button = queue_workflow.loc[queue_workflow['button_id'] == button['id']]
        button_rule_group = list(workflow_button['rule_group'])[0]
        button_email_template = list(workflow_button['email_template'])[0]
        button_draft_template = list(workflow_button['draft_template'])[0]
        button_move_to = list(workflow_button['move_to'])[0]

        if button_rule_group is not None:
            button['stage'] = button_rule_group.split(',')
        if button_move_to is not None:
            button['move_to'] = list(queue_definition.loc[[button_move_to]]['unique_name'])[0]
        if button_email_template is not None:
            button['email_template'] = button_email_template
        if button_draft_template is not None:
            button['draft_template'] = button_draft_template
    # db.engine.dispose()
    return button_attributes

@cache.memoize(86400)
def queue_name_type(queue_id, tenant_id, filters = {}):
    db_config['tenant_id'] = tenant_id

    db = DB('queues', **db_config)

    qid_st = time()
    logging.info(f"queue_is########{queue_id}")
    queue_definition = db.get_all('QUEUE_DEFINITION').set_index('queue_id')
    queue_df = queue_definition.loc[str(queue_id)]

    queue_name = queue_df['name']
    queue_uid = queue_df['unique_name']
    queue_type = queue_df['type']
    try:
        is_snapshot = queue_df['is_snapshot']
    except:
        is_snapshot = 0

    try:
        all_filters = json.loads(queue_df['static_filters'])
        contains_filter = all_filters.get('contains_filter', [])
        must_not_filter = all_filters.get('must_not_filter', [])
    except:
        contains_filter = []
        must_not_filter = []
    

    return queue_uid, queue_name, queue_type, queue_definition, is_snapshot, contains_filter, must_not_filter

@cache.memoize(86400)
def get_columns(queue_name, tenant_id, template_exceptions=None):
    logging.info('Getting columns (cache)')
    
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    
    columns_definition = db.get_all('column_definition')
    columns_df = columns_definition[columns_definition['queue'] == queue_name]

    dd = columns_df.to_dict(orient='list')
    to_map = []

    for _, value in dd.items():
        to_map.append(value)
    logging.info(f"###############3 to map: {to_map}")
    column_mapping = {}
    for i in range(len(to_map[0])):
        column_mapping[to_map[3][i]] = to_map[2][i] 

    columns = list(columns_df.loc[columns_df['source'] == 'process_queue']['column_name'])
    if template_exceptions:
        return_data = {
            'columns': columns,
            'column_mapping': column_mapping
        }
        
        return return_data

    extraction_columns_df = columns_df.loc[columns_df['source'] != 'process_queue']
    

    util_columns = ['total_processes', 'completed_processes', 'case_lock', 'failure_status', 'file_paths', 'freeze']
    
    columns += util_columns
    

    extraction_columns_list = list(extraction_columns_df['source'] + '.' + extraction_columns_df['column_name'])
    date_columns = list(columns_definition[columns_definition['date'] == 1]['column_name'])

    return_data = {
        'columns': columns,
        'column_mapping': column_mapping,
        'util_columns': util_columns,
        'extraction_columns_df': extraction_columns_df,
        'extraction_columns_list': extraction_columns_list,
        'date_columns': date_columns
    }
    
    return return_data

@cache.memoize(86400)
def get_table_columns(table_name, db_name, tenant_id):
    logging.info(f'Fetching columns for table {table_name} from DB {db_name} (cached)')
    db_config['tenant_id'] = tenant_id
    db = DB(db_name, **db_config)

    query = f"SELECT column_name FROM user_tab_columns WHERE table_name = '{table_name.upper()}'"
    result = db.execute_(query)
    return [col.lower() for col in result['COLUMN_NAME'].tolist()]


@cache.memoize(86400)
def get_tab_definition(tenant_id):
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)
    tab_definition = db.get_all('tab_definition')

    return tab_definition

@cache.memoize(86400)
def get_queue_definition(tenant_id, queue_name):
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)
    queue_definition = db.get_all('queue_definition')

    queue_info = queue_definition.loc[queue_definition['unique_name'] == queue_name]
    queue_id = queue_definition.index[queue_definition['unique_name'] == queue_name].tolist()[0]

    return queue_definition, queue_info, queue_id

@cache.memoize(86400)
def get_fields_tab_queue(queue_id, tenant_id, case_creation=0):
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)
    bizrules_db = DB('business_rules', **db_config)
    if case_creation:
        query = f"SELECT * FROM case_creation_fields"
    else:
        
        query = f'SELECT * FROM field_definition WHERE INSTR (queue_field_mapping, {queue_id}, 1, 1) > 0 ORDER BY field_order'
        fields_df = pd.DataFrame(db.execute(query))
        
    
    query = 'select * from tab_definition order by tab_order'
    tab_definition = db.execute(query)
    datasrc_time = time()
    excel_display_data = {}
    tab_type_mapping = {}

    logging.info('Formatting field info...')
    for index, row in fields_df.iterrows():
        # logging.info(f' => {row}')
        if not case_creation:
            tab_dict = json.loads(row['tab_id']).copy()           
            for k,v in tab_dict.items():
                if queue_id in v:
                    tab_id = int(k)
                    break
        else:
            tab_id = 1

        tab_name = tab_definition.loc[tab_id]['text']
        tab_source = tab_definition.loc[tab_id]['source']
        tab_type = tab_definition.loc[tab_id]['type']
        fields_df.loc[index, 'tab_id'] = tab_name

        tab_type_mapping[tab_name] = tab_type

        if tab_type == 'excel':
            source_table_name = tab_source + '_source'

            # Get excel source data and convert it to dictionary
            excel_source_data = extraction_db.get_all(source_table_name)

            if tab_name not in excel_display_data:
                excel_display_data[tab_name] = {
                    'column': list(excel_source_data),
                    'data': excel_source_data.to_dict(orient='records')[:100]
                }
    # logging.info(f'Time taken for formatting field {time()-datasrc_time}')
    field_attributes = fields_df.to_dict(orient='records')
    if not case_creation:
        for field in field_attributes:
            try:
                value = list(ast.literal_eval(field['editable']))
            except:
                if field['editable']:
                    value = [int(field['editable'])]
                else:
                    value = None
            if not value:
                field['editable'] = 0
            elif int(queue_id) in value:
                field['editable'] = 1
            else:
                field['editable'] = 0
            try:
                value = list(ast.literal_eval(field['checkbox']))
            except:
                try:
                    value = [int(field['checkbox'])]
                except:
                    value = None
            if not value:
                field['checkbox'] = 0 
            elif int(queue_id) in value:
                field['checkbox'] = 1
            else:
                field['checkbox'] = 0
    else:
        for field in field_attributes:
            field['editable'] = 1

    tabs = list(fields_df.tab_id.unique())
    tabs_def_list = list(tab_definition['text'])
    tabs_reordered = []

    for tab in tabs_def_list:
        if tab in tabs:
            tabs_reordered.append(tab)

    query = "SELECT id, display_name, unique_name, tab_id, pattern FROM field_definition WHERE `type` = 'table'"
    table_fields = db.execute(query)

    
    return field_attributes, tabs_reordered, excel_display_data, tab_type_mapping, table_fields

def recon_get_columns(table_unique_id, tenant_id):

    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    query = f"SELECT `column_definition`.*, `recon_column_mapping`.`column_order` FROM `column_definition`, `recon_column_mapping` where `column_definition`.`id` = `recon_column_mapping`.`column_id` and `recon_column_mapping`.`table_unique_id` = %s ORDER BY `recon_column_mapping`.`column_order` ASC"
    columns_df = db.execute_(query, params=[table_unique_id])
    extraction_columns_df = columns_df.loc[columns_df['source'] != 'process_queue']
    columns = list(columns_df['column_name'])

    extraction_columns_list = list(extraction_columns_df['column_name'])

    return_data = {
        'columns': columns,
        'extraction_columns_df': extraction_columns_df.to_dict(orient= 'records'),
        'extraction_columns_list': extraction_columns_list,
        'columns_df': columns_df
    }

    return return_data

def get_recon_data(queue_id, queue_name, tenant_id):
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)
    
    query = f"SELECT * FROM `recon_table_mapping` where `queue_id` = '{queue_id}'"
    recon_table_mapping_df = db.execute(query)
    
    table_unique_ids_mapped = list(recon_table_mapping_df['table_unique_id'])
    table_unique_ids_mapped = ["'" +x+ "'" for x in table_unique_ids_mapped]    

    query_1 = f"SELECT * FROM `recon_definition` where `table_unique_id` in (SELECT `table_unique_id` FROM `recon_table_mapping` where `queue_id` = '{queue_id}')"
    recon_definition_df = db.execute(query_1)
    
    if len(recon_definition_df) > 2:
        return {'flag' : 'False', 'msg' : 'Too many queues mapped in DB.'}
    to_return = {}
    keys_ = {} 
    for _, row in recon_definition_df.iterrows():
        table_column_mapping = recon_get_columns(row['table_unique_id'], 'karvy')
        
        if row['dependency']:
            keys_['primary_table'] = row['table_unique_id']
        else:
            keys_['secondary_table'] = row['table_unique_id']
        
        if len(recon_definition_df) == 1:
            keys_['primary_table'] = row['table_unique_id']
            
        dd = table_column_mapping['columns_df'].to_dict(orient='list')
        to_map = []

        for _, value in dd.items():
            to_map.append(value)

        column_mapping = {}
        for i in range(len(to_map[0])):
            column_mapping[to_map[2][i]] = to_map[1][i] 
        
        to_return[row['table_unique_id']] = { 
                'route' : row['route'],
                'parameters' : row['parameters'],
                'show_table' : row['show_table'],
                'unique_key' : row['unique_key'],
                'match_id_field': row['match_id_field'],
                'match_table' : row['match_table'],
                'dependency' : row['dependency'] if row['dependency'] else '',
                'columns' : table_column_mapping['columns'],
                'columns_df' : table_column_mapping['columns_df'].to_dict(orient= 'records'),
                'column_mapping': column_mapping,
                'column_order': list(column_mapping.keys()),
                'queue_table_name' : row['queue_table_name'] if row['queue_table_name'] else '',
                'check_box' : row['check_box']
                }
    to_return  = {**to_return, **keys_}
    
    return to_return




def get_users_mesh(tenant_id, classify_users, group_db):
    try:
        app_def_df = group_db.execute_("SELECT * FROM `app_definition`")
        app_access_df = group_db.execute_("SELECT * FROM `app_access`")
        users_mesh = {}
        # logging.info(f" ### CLASSIGY USERS ARE {classify_users}")
        # logging.info(f" #### app_access_df['group_id'] is {app_access_df['group_id']}")
        for k,v in classify_users.items():
            for user, group_list in v.items():
                user_app_ids = list(app_access_df[app_access_df['group_id'].isin(group_list)]['app_id'])
                # logging.info(f"### user_app idds {user_app_ids}")
                user_app_defs = app_def_df.loc[app_def_df['id'].isin(user_app_ids)]
                # user_app_defs = app_def_df.loc[app_def_df['id']]

                users_mesh[user] = user_app_defs.to_dict(orient= 'records')
        return users_mesh
    except:
        traceback.print_exc()
        message = f"Something went wrong while fetching app tables"
        logging.exception(message)
        logging.error("Setting mesh to empty dictionary")
        return {}

def get_group_ids(user, db):
    # logging.info(f'Getting group IDs for user `{user}`')

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
    group_def_df = db.get_all('group_definition')

    if group_def_df.empty:
        logging.info(f'Groups not defined in `group_definition`')
        return

    group_def = group_def_df.to_dict(orient='index')
    group_ids = []
    for index, group in group_def.items():
        # logging.info(f'Index: {index}')
        # logging.info(f'Group: {group}')

        try:
            group_dict = json.loads(group['group_definition'])
        except:
            logging.error('Could not load group definition dict.')
            break

        # Check if all key-value from group definition is there in the user group
        if group_dict.items() <= user_group_dict.items():
            group_ids.append(index)

    # logging.info(f'Group IDs: {group_ids}')
    return group_ids

@cache.memoize(86400)
def get_user_case_filters(user, queue_uid, tenant_id=None, filters = {}):
    try:
        db_config['tenant_id'] = tenant_id
        db = DB('group_access', **db_config)
        
        user_queues, _ = get_queues_cache(tenant_id, filters)

        get_user_queues = user_queues[user]
        
        sequence = 1    
        for queue in get_user_queues:
            if queue['path'] == queue_uid:
                sequence = queue['sequence']
        # Get user ID from active directory
        active_directory = db.get_all('active_directory', condition={'username': user})
        user_id = list(active_directory.index.values)[0]
                
        query = f"select * from user_organisation_mapping where user_id = '{user_id}' and sequence_id = {sequence} and type != 'user'"
        filters = db.execute(query).to_dict(orient='records')
        
        organisation_attributes = db.execute('select * from organisation_attributes').reset_index()
    
        all_filters = []
        for filt in filters:
            attribute = filt['organisation_attribute']
            field = filt['value']
            query_result = organisation_attributes[organisation_attributes['id'] == attribute]
            attribute_value = list(query_result.attribute)[0]
            attribute_source = list(query_result.source)[0] if list(query_result.source)[0] != 'user_tag' else 'ocr'
            filter_dict = {"field": attribute_source+'.'+attribute_value, "value": field}
            all_filters.append(filter_dict)

        user_filters = pd.DataFrame(all_filters).set_index('field').groupby(['field'])['value'].apply(lambda x: ','.join(x.astype(str)).split(',')).reset_index().to_dict('records')         
    except:
        user_filters = []
    
    return user_filters

@app.route('/get_file_count', methods = ['GET', 'POST'])
def get_file_count():
    data = request.json
    user = data['user']
    queue_id = data['queue_id']
    case_ids = data['case_ids']
    tenant_id = data['tenant_id']
    queue_name = data['queue_name']

    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    columns_data = get_columns(queue_name, tenant_id)
    columns = columns_data['columns']

    elastic_input = {}

    prefix_column = []

    columns += ['created_date', 'freeze', 'status', 'file_paths']

    columns = list(set(columns))
    for column in columns:
        prefix_column.append('pq.' + column.upper())

    try:
        user_case_filters = get_user_case_filters(user, queue_name, tenant_id)
    except:
        logging.exception('Error ocurred while getting user case filter.')
        user_case_filters = []

    # logging.info(f"user_case_filters: {user_case_filters}")

    elastic_input['columns'] = prefix_column
    elastic_input['tenant_id'] = tenant_id

    if case_ids:
        case_filter = {'field': 'pq.CASE_ID', 'value': case_ids}
        if user in ['11111111', '22222222', '33333333', '44444444']:
            query = "SELECT case_id FROM process_queue WHERE case_id NOT in (SELECT parent_case_id FROM case_relation)"
            case_ids_children = list(db.execute_(query).case_id)
            case_ids = list(set(case_ids) & set(case_ids_children))
            case_filter = {'field': 'pq.CASE_ID', 'value': case_ids}
            elastic_input['filter'] = [case_filter] + user_case_filters
        else:
            elastic_input['filter'] = [case_filter] + user_case_filters + [{'field': 'QUEUE_LIST.queue', 'value': queue_name}]
    elastic_input['source'] = 'process_queue'
    elastic_input['sort'] = [{'pq.CREATED_DATE': 'desc' }]

    logging.info(f'Elasticsearch Input: {elastic_input}')
    _, total_files = elasticsearch_search(elastic_input)

    return jsonify({"file_count": total_files})

@app.route('/get_recon_secondary_table', methods = ['GET', 'POST'])
def get_recon_secondary_table():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')
    db_config['tenant_id'] = tenant_id

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_recon_secondary_table',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        primary_unique_key_value = data['primary_unique_key_value']
        primary_table_unique_key = data['primary_table_unique_key']
        primary_queue_table_name = data['primary_queue_table_name']
        columns_df = data['columns_df']
        columns = data['columns']
        extraction_db = DB('extraction', **db_config)
        extraction_columns_df = pd.DataFrame(columns_df)

        if not extraction_columns_df.empty:
            select_columns_list = []
            for _, row in extraction_columns_df.iterrows():
                col_name = row['column_name']                   
                table = row['source']
            
                if table:
                    select_columns_list.append(f'`{table}`.`{col_name}`')
            
            tables_list = [source for source in list(extraction_columns_df['source'].unique()) if source]
            tables_list_ = []
            if primary_queue_table_name not in tables_list:
                tables_list_ = tables_list + [primary_queue_table_name]
            else:
                tables_list_ = tables_list
            where_conditions_list = []
            for combo in combinations(tables_list_, 2):
                where_conditions_list.append(f'`{combo[0]}`.`{primary_table_unique_key}` = `{combo[1]}`.`{primary_table_unique_key}`')

            where_conditions_list += [f"`{tables_list[0]}`.`{primary_table_unique_key}` IN ('{primary_unique_key_value}')"]

            select_part = ', '.join(select_columns_list)
            from_part = ', '.join([f'`{table}`' for table in tables_list_])
            where_part = ' AND '.join(where_conditions_list)
            
            query = f'SELECT {select_part} FROM {from_part} WHERE {where_part}'
            query_result_df = extraction_db.execute_(query)
            try:
                query_result_list = query_result_df.to_dict('records')
            except:
                pass
            
            rows_arr = []
            for _, row in query_result_df.iterrows():
                rows_dict = {}
                for col in columns:
                    rows_dict = {**rows_dict, **{col : row[col]}}
                rows_arr.append(rows_dict)
            return jsonify({'columns' : columns,'rows' : rows_arr})
        else:
            return jsonify("No data to display")

@app.route('/get_recon_table_data', methods = ['GET', 'POST'])
def get_recon_table_data():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')
    db_config['tenant_id'] = tenant_id

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_recon_secondary_table',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        unique_key = data['unique_key']
        queue_table_name = data['queue_table_name']
        columns_df = data['columns_df']
        columns = data['columns']
        queue_id = data['queue_id']
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)
        query = f"SELECT `unique_name` FROM `queue_definition` where `id` = '{queue_id}'"
        queue_unique_name = queue_db.execute_(query)
        queue_unique_name  = list(queue_unique_name['unique_name'])[0]
        invoice_files_df = extraction_db.execute_(f"SELECT * from `{queue_table_name}` where `queue`= '{queue_unique_name}'")
        case_ids = list(invoice_files_df[unique_key].unique())
        extraction_columns_df = pd.DataFrame(columns_df)
        if case_ids:    
            placeholders = ','.join(['%s'] * len(case_ids))
            if not extraction_columns_df.empty:
                select_columns_list = []
                for _, row in extraction_columns_df.iterrows():
                    col_name = row['column_name']                   
                    table = row['source']
                
                    if table:
                        select_columns_list.append(f'`{table}`.`{col_name}`')
                
                tables_list = [source for source in list(extraction_columns_df['source'].unique()) if source]
                tables_list_ = []
                if queue_table_name not in tables_list:
                    tables_list_ = tables_list + [queue_table_name]
                else:
                    tables_list_ = tables_list
                where_conditions_list = []
                for combo in combinations(tables_list_, 2):
                    where_conditions_list.append(f'`{combo[0]}`.`{unique_key}` = `{combo[1]}`.`{unique_key}`')

                where_conditions_list += [f'`{queue_table_name}`.`{unique_key}` IN ({placeholders})']
            
                select_part = ', '.join(select_columns_list)
                from_part = ', '.join([f'`{table}`' for table in tables_list_])
                where_part = ' AND '.join(where_conditions_list)


                query = f'SELECT {select_part} FROM {from_part} WHERE {where_part}'
                query_result_df = extraction_db.execute_(query, params=case_ids)
                query_result_list = query_result_df.to_dict('records')
                
                rows_arr = []
                for _, row in query_result_df.iterrows():
                    rows_dict = {}
                    for col in columns:
                        rows_dict = {**rows_dict, **{col : row[col]}}
                    rows_arr.append(rows_dict)
                return jsonify({'columns' : columns,'rows' : rows_arr})
        else:
            return jsonify("No data to display")

def append_filters_in_query(filter, where_statements = [], order_statements = []):
    column_name = filter["column_name"]
    
    type = filter["data_type"]

    if type == "number":
        if filter.get("less_than"):
            where_statements.append("`"+column_name+"` < "+str(filter.get("less_than")))

        if filter.get("greater_than"):
            where_statements.append("`"+column_name+"` < "+str(filter.get("greater_than")))

        if filter.get("equalTo"):
            where_statements.append("`"+column_name+"` < "+str(filter.get("equalTo")))
    
    if type == "string":
        if filter.get("search_text"):
            where_statements.append("LOWER(CONCAT(`file_name`, '', `case_id`, '', `document_type`, '', `queue`, '', `task_id`, '', `template_name`, '', `status`, '', `reference_number`, '', `file_paths` )) LIKE "+"LOWER('%"+filter.get("search_text")+"%')")
    
    if type == "date":
        if filter.get("before_date"):
            if filter.get("after_date"):
                where_statements.append("(`"+column_name+"` BETWEEN "+"'"+filter.get("before_date")+"' AND "+"'"+filter.get("after_date")+"')")
            else:
                where_statements.append("(`"+column_name+"` >= "+"'"+filter.get("before_date")+"'")
    
        elif filter.get("after_date"):
            where_statements.append("(`"+column_name+"` <= "+"'"+filter.get("after_date")+"'")


    if filter.get("sort_order"):
        if filter.get("sort_order")!="":
            order_statements.append("`"+column_name+"` "+str(filter.get("sort_order")))
    
    
        

    return where_statements, order_statements

@app.route('/cover_letter_gen', methods=['POST', 'GET'])
def cover_letter_gen():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        db_config['tenant_id'] = tenant_id
        db = DB('queues', **db_config)

        query = f"update process_queue set queue = 'cover_letter' where case_id='{data['case_id']}'"
        db.execute_(query)

        return jsonify({"flag": True, "message":"Successfully updated"})

    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})

@app.route('/distr_det_doc_gen', methods=['POST', 'GET'])
def distr_det_doc_gen():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        db_config['tenant_id'] = tenant_id
        db = DB('queues', **db_config)

        query = f"update process_queue set queue = 'distribution_details_document' where case_id='{data['case_id']}'"
        db.execute_(query)

        return jsonify({"flag": True, "message":"Successfully updated"})

    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})

@app.route('/po_gen', methods=['POST', 'GET'])
def po_gen():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        db_config['tenant_id'] = tenant_id
        db = DB('queues', **db_config)

        query = f"update process_queue set queue = 'po_generation_task' where case_id='{data['case_id']}'"
        db.execute_(query)

        return jsonify({"flag": True, "message":"Successfully updated"})

    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})

@app.route('/transaction_id_gen', methods=['POST', 'GET'])
def transaction_id_gen():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        db_config['tenant_id'] = tenant_id
        db = DB('queues', **db_config)

        query = f"update process_queue set queue = 'transaction_id' where case_id='{data['case_id']}'"
        db.execute_(query)

        return jsonify({"flag": True, "message":"Successfully updated"})

    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})

@app.route('/approved_quota_gen', methods=['POST', 'GET'])
def approved_quota_gen():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        db_config['tenant_id'] = tenant_id
        db = DB('queues', **db_config)

        query = f"update process_queue set queue = 'approved_quota' where case_id='{data['case_id']}'"
        db.execute_(query)

        return jsonify({"flag": True, "message":"Successfully updated"})

    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})



@app.route('/get_queue_with_filters', methods=['POST', 'GET'])
def get_queue_with_filters():
    try:
        data = request.json
        logging.info(f'Request data: {data}')
        tenant_id = data.get('tenant_id', '')
        # db_config['tenant_id'] = tenant_id

        query = 'SELECT '
        cols_to_select = [*data]
        
        # print(cols_to_select)
        
        query+='`'+'`,`'.join(cols_to_select)+'`'
        logging.info(f"########################## COLUMNS TO SELECT FOR FILTER: {query}")
        query+=' FROM `process_queue` '

        where_statements = []
        order_statements = []
        for i in cols_to_select:
            where_statements, order_statements = append_filters_in_query(query, data, i)
        query+='WHERE '+' AND '.join(where_statements)+' ORDER BY '+', '.join(order_statements)
        logging.info(query)

        db = DB('queues', **db_config)
        result =  db.execute(query)

        return jsonify({"result",result})
    except Exception as e:
        return jsonify({"error":f"Unhandled error {e}"})

def return_with_filter(files, search_filter):
    type = search_filter["data_type"]
    column_name =  search_filter["column_name"]

    #########################33 FILTER TYPES: STRING, NUMBER, DATE AND BOOL
    
    if type == "number":
        #################33 NUMBER FILTERS
        less_than, greater_than, equalTo = [None, None, None]

        less_than = search_filter.get("less_than", None)
        greater_than = search_filter.get("greater_than", None)
        equalTo = search_filter.get("equalTo", None)

        # try:
        for file in files[:]:
            if less_than is not None and less_than != "":
                less_than = int(less_than)
                if file[column_name]>=less_than:
                    files.remove(file)  
            if greater_than is not None and greater_than != "":
                greater_than = int(greater_than)
                if file[column_name]<=greater_than:
                    files.remove(file)
            if equalTo is not None and equalTo != "":
                equalTo = int(equalTo)
                if file[column_name]!=equalTo:
                    files.remove(file)
        
           
        # except Exception as e:
        #     return {"flag":"false", "message": f"less_than or greater_than or equalTo is not a number for {files['column_name']}"}

        
    
    if type=="string":
        ########################## STRING FITERS

        search_text = search_filter.get("search_text",None)
        try:
            if search_text is not None and search_text != "":
                search_text = str(search_text)
            else:
                return None
        except Exception as e:
            return {"flag":"false", "message": f"search_text is not a string"}
        
        search_files = []
        string_columns = [col for col in [*(files[0])] if isinstance(files[0][col], str)]

        # logging.info(f"####################3 sTRING OLUMNS ARE {string_columns}")

        for file in files:
            for column in string_columns:
                
                if file[column]:
                    logging.info("###########33 IT COMES HERE")
                    if re.search(search_text, file[column]):
                        logging.info(f"$$$$$$$$$$$$$$$$$$4 THIS COLUMN MATCHED {file[column]}")
                        search_files.append(file)

        files = search_files
    
    if type=="date":
        ##################### date filters
        less_than, greater_than, equalTo = [None, None, None]

        less_than = search_filter.get("less_than", None)
        greater_than = search_filter.get("greater_than", None)
        equalTo = search_filter.get("equalTo", None)

        column_name = search_filter.get("column_name")

        for file in files:
            if less_than is not None and less_than != "":
                less_than = str(less_than)
            if greater_than is not None and greater_than != "":
                greater_than = str(greater_than)
            if equalTo is not None and equalTo != "":
                equalTo = str(equalTo)
    



    
    return files

def data_filter_function2(data,search_filters):
    columns = [*data[0]]
    for search_filter in search_filters:
        data = return_with_filter(data, search_filter)
    
    return data

#############################      APPEND MORE CLOUMN TYPES WHEN ADDING MORE DATABASE TYPES ###############################
#############################               CURRENTLY CONFIGURED FOR MYSQL, MSSQL, ORACLE           ###############################
db_column_types= {
    "mysql":{
        "int": "number",
        "tinyint": "number",
        "smallint": "number",
        "mediumint": "number",
        "bigint": "number",
        "double": "number",
        "float": "number",
        
        "date": "date",
        "datetime": "date",
        "timestamp" : "date",
        "time": "date",

        "char": "string",
        "varchar": "string",
        "blob": "string",
        "text": "string",
        "tinytext": "string",
        "mediumtext": "string",
        "longtext": "string",
        "tinyblob": "string",
        "mediumblob": "string",
        "longblob": "string",
        "enum": "string"
    },
    "oracle":{
        "int": "number",
        "integer": "number",
        "decimal": "number",
        "tinyint": "number",
        "smallint": "number",
        "mediumint": "number",
        "bigint": "number",
        "double": "number",
        "float": "number",
        "number": "number",

        
        "date": "date",
        "datetime": "date",
        "timestamp" : "date",
        "time": "date",

        "char": "string",
        "varchar": "string",
        "varchar2": "string",
        "blob": "string",
        "clob": "string",
        "nvarchar": "string",
        "nvarchar2": "string",
        "nblob": "string",
        "nclob": "string",
        "text": "string",
        "tinytext": "string",
        "mediumtext": "string",
        "longtext": "string",
        "tinyblob": "string",
        "mediumblob": "string",
        "longblob": "string",
        "enum": "string"
    },
    "mssql":{
        "bigint": "number",
        "bit": "number",
        "decimal": "number",
        "int": "number",
        "money": "number",
        "numeric": "number",
        "smallint": "number",
        "smallmoney": "number",
        "tinyint": "number",
        "float": "number",
        
        "date": "date",
        "datetime2": "date",
        "datetime": "date",
        "datetimeoffset": "date",
        "smalldatetime": "date",
        "time": "date",
        
        "char": "string",
        "text": "string",
        "varchar": "string",
        "nchar": "string",
        "nvarchar": "string",
        "ntext": "string",
        

    }
}
def get_column_data_types(columns, tenant_id):

    # logging.info(f"^^^^^^^^^^^^^^^ COLUMNS ARE {columns}")

    db_config['tenant_id'] = tenant_id
    queue_db  = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)

    query = "SELECT * FROM `process_queue` LIMIT 1"
    pq_columns = list(queue_db.execute_(query).columns.values)

    query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'process_queue'"
    pq_column_types = list(queue_db.execute_(query)['data_type'])

    query = "SELECT * FROM `ocr` LIMIT 1"
    ocr_columns = list(extraction_db.execute_(query).columns.values)

    query = "SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'ocr'"
    ocr_column_types = list(extraction_db.execute_(query)['data_type'])

    pq = dict(zip(pq_columns, pq_column_types))
    ocr = dict(zip(ocr_columns, ocr_column_types))

    logging.info(f"#########################33 PQ VALUE IS {pq} OCR IS {ocr}")

    all_db_types = {**pq, **ocr}

    logging.info(f"$$$$$$$$$$$$$$$$$$$$$$$$$44 ALL DB TYPES IS {all_db_types}")

    col_data_types = {}

    db_type=os.environ.get('DB_TYPE','mysql').lower()

    for column in columns:
        col_data_types[column] = db_column_types[db_type].get(all_db_types.get(column, ""), "unknown")
    
    return col_data_types


def checking_duplicate(files, tenant_id):
    """
    Function: checking_duplicate

    Description:
        Identifies potential duplicate cases in the OCR system 
        based on `party_id`, `document_id`, and `due_date`.
        Only considers cases in 'maker_queue' or 'rejected_queue'.
        Efficiently fetches matched data and queue names in batch.

    Parameters:
        files (list of dict): List of input file records (from queue) that may include pq.CASE_ID.
        tenant_id (str): Identifier to configure tenant-specific DB connections.

    Returns:
        list of dict: Updated list of files with possible `matched_queue` (list of queue names) for each document.
    """
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)

    try:
        all_case_ids = [doc.get("pq.CASE_ID", "") for doc in files if doc.get("pq.CASE_ID", "")]
        if not all_case_ids:
            return files

        formatted_case_ids = ', '.join(f"'{cid}'" for cid in all_case_ids)

        # Step 1: Filter case_ids that are in 'maker_queue' or 'rejected_queue'
        queue_filter_query = f"""
            SELECT case_id 
            FROM queue_list 
            WHERE queue IN ('maker_queue', 'rejected_queue') 
              AND case_id IN ({formatted_case_ids})
        """
        valid_case_result = queue_db.execute(queue_filter_query)
        case_ids = set(valid_case_result["case_id"].tolist()) if not valid_case_result.empty else set()
        if not case_ids:
            logging.info("[INFO] No case_ids in maker_queue or rejected_queue.")
            return files

    except Exception as e:
        logging.error(f"[ERROR] Failed to filter case_ids from queue_list: {e}")
        return files

    try:
        formatted_case_ids = ', '.join(f"'{cid}'" for cid in case_ids)
        query_ocr_main = f"""
            SELECT case_id, party_id, document_id, due_date 
            FROM ocr 
            WHERE case_id IN ({formatted_case_ids})
        """
        ocr_main_result = extraction_db.execute(query_ocr_main)
    except Exception as e:
        logging.error(f"[WARNING] Failed to fetch OCR data: {e}")
        return files

    if ocr_main_result.empty:
        return files

    case_info_map = {
        row["case_id"]: {
            "party_id": row["party_id"],
            "document_id": row["document_id"],
            "due_date": row["due_date"]
        }
        for _, row in ocr_main_result.iterrows()
    }

    # Step 2: Prepare unique key set
    unique_keys = list(set(
        (info["party_id"], info["document_id"], info["due_date"])
        for info in case_info_map.values()
    ))

    # Step 3: Batch fetch matching duplicates from ocr
    matched_cases = []
    try:
        valid_keys = [
        (str(p).replace("'", "''"), str(d).replace("'", "''"), str(dd).strip())
        for (p, d, dd) in unique_keys
        if p and d and dd and str(dd).lower() != "none"
        ]

        if valid_keys:
            values_clause = ", ".join(
                f"('{p}', '{d}', '{dd}')" for p, d, dd in valid_keys
            )

            match_query = f"""
                SELECT case_id, party_id, document_id, due_date
                FROM ocr
                WHERE (party_id, document_id, due_date) IN (
                    {values_clause}
                )
            """
            match_result = extraction_db.execute(match_query)
            matched_cases = match_result.to_dict(orient="records") if not match_result.empty else []

    except Exception as e:
        print(f"[WARNING] Failed to batch fetch duplicates: {e}")

    # Step 4: Prepare duplicates_map keyed by (party_id, document_id, due_date)
    duplicates_map = {}
    for row in matched_cases:
        key = (row["party_id"], row["document_id"], row["due_date"])
        duplicates_map.setdefault(key, []).append(row["case_id"])

    # Step 5: Fetch all queues for matched case_ids
    try:
        all_match_ids = set(
            match_case_id
            for match_ids in duplicates_map.values()
            for match_case_id in match_ids
        )
        formatted_match_ids = ', '.join(f"'{cid}'" for cid in all_match_ids)

        queue_query = f"""
            SELECT case_id, queue 
            FROM queue_list 
            WHERE case_id IN ({formatted_match_ids})
        """
        queue_result = queue_db.execute(queue_query)
        queue_map = {
            row["case_id"]: row["queue"]
            for _, row in queue_result.iterrows()
        } if not queue_result.empty else {}

    except Exception as e:
        logging.exception(f"[WARNING] Failed fetching queues for matched cases: {e}")
        queue_map = {}

    # Step 6: Build reverse map case_id -> queues (as sets)
    case_id_to_queue = {cid: {queue_map[cid]} if cid in queue_map else set() for cid in all_match_ids}

    # Step 7: Attach matched_queue (list of queue names) to each file with O(n) complexity
    for document in files:
        case_id = document.get("pq.CASE_ID", "")
        document["matched_queue"] = []

        if not case_id or case_id not in case_info_map:
            continue

        info = case_info_map[case_id]
        dup_key = (info["party_id"], info["document_id"], info["due_date"])
        match_case_ids = duplicates_map.get(dup_key, [])

        # Collect all queues for duplicates except own case_id queue
        matched_queues_set = set()
        own_queue = queue_map.get(case_id)

        for mid in match_case_ids:
            if mid != case_id:
                matched_queues_set.update(case_id_to_queue.get(mid, set()))

        # Remove own queue if present to avoid self-matching
        if own_queue and own_queue in matched_queues_set:
            matched_queues_set.remove(own_queue)

        document["matched_queue"] = list(matched_queues_set)

    return files



def extractCase(files, tenant_id, extraction_db):
    try:
        # Collect all case IDs
        all_case_ids = [f.get("pq.CASE_ID") for f in files if f.get("pq.CASE_ID")]
        if not all_case_ids:
            return files

        formatted_ids = "'" + "', '".join(all_case_ids) + "'"

        # Fetch comments
        query = f"""
            SELECT CASE_ID, WAITING_QUEUE_REJECTED_COMMENTS 
            FROM ocr 
            WHERE CASE_ID IN ({formatted_ids})
        """
        df = extraction_db.execute(query)

        if df.empty:
            return files

        # Faster: Convert to dict directly
        raw_map = df.set_index("CASE_ID")["WAITING_QUEUE_REJECTED_COMMENTS"].to_dict()

        case_comment_map = {}

        # Parse JSON-only once per row
        for case_id, raw_text in raw_map.items():
            if not raw_text or str(raw_text).strip().lower() == "none":
                case_comment_map[case_id] = ""
                continue

            raw_text = str(raw_text).strip()

            try:
                if raw_text.startswith("{") or raw_text.startswith("["):
                    data = json.loads(raw_text)

                    # extract comment
                    if isinstance(data, list) and data:
                        case_comment_map[case_id] = data[0].get("comment", "")
                    elif isinstance(data, dict):
                        case_comment_map[case_id] = data.get("comment", "")
                    else:
                        case_comment_map[case_id] = ""
                else:
                    # plain text
                    case_comment_map[case_id] = raw_text

            except Exception:
                # fallback to raw text
                case_comment_map[case_id] = raw_text

        # Attach comments to files (single loop)
        for f in files:
            cid = f.get("pq.CASE_ID")
            if cid in case_comment_map:
                f["waiting_queue_rejected_comments"] = case_comment_map[cid]

        return files

    except Exception as e:
        logging.info(f"[ERROR] extractCase failed: {e}")
        return files




def getting_rm_name(files, tenant_id): 
    try:
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)

        #all_case_ids = [doc.get("pq.CASE_ID", "") for doc in files if doc.get("pq.CASE_ID", "")]
        #print(f"All case_id's:{all_case_ids}")
        #if not all_case_ids:
        #    print("[INFO] No CASE_IDs found in input files.")
        #    return [], []

        #formatted_case_ids = ', '.join(f"'{cid}'" for cid in all_case_ids)
        all_case_ids = [doc.get("pq.CASE_ID", "") for doc in files if doc.get("pq.CASE_ID", "")]
        if not all_case_ids:
            return files
        #print(f"All case_id's:{all_case_ids}")

        formatted_case_ids = ', '.join(f"'{cid}'" for cid in all_case_ids)
        qry = f"""
            SELECT  relation_mgrname 
            FROM ocr 
            WHERE case_id IN ({formatted_case_ids})
        """
        try:
            valid_mgr_result=extraction_db.execute(qry)
        except Exception as db_err:
            print(f"[ERROR] DB query failed: {db_err}")
            return []

        rm_names = []
       
        #print("\n Result from `qry` (e.g., RM-related data):")
        for row in valid_mgr_result:
            print(row)

        if not valid_mgr_result.empty:
            rm_names = valid_mgr_result['relation_mgrname'].dropna().astype(str).tolist()
        else:
            logging.info("[INFO] No matching case_ids found in process_queue.")

        return rm_names

    except Exception as e:
        logging.error(f"[ERROR] Unexpected failure in getting_rm_branch: {e}")
        return []

def getting_branch_code(files, tenant_id): 
    try:
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction', **db_config)

        #all_case_ids = [doc.get("pq.CASE_ID", "") for doc in files if doc.get("pq.CASE_ID", "")]
        #print(f"All case_id's:{all_case_ids}")
        #if not all_case_ids:
        #    print("[INFO] No CASE_IDs found in input files.")
        #    return [], []

        #formatted_case_ids = ', '.join(f"'{cid}'" for cid in all_case_ids)
        all_case_ids = [doc.get("pq.CASE_ID", "") for doc in files if doc.get("pq.CASE_ID", "")]
        if not all_case_ids:
            return files
        #print(f"All case_id's:{all_case_ids}")

        formatted_case_ids = ', '.join(f"'{cid}'" for cid in all_case_ids)
        queue_filter_query = f"""
            SELECT  branch_code 
            FROM process_queue 
            WHERE case_id IN ({formatted_case_ids})
        """
        try:
            valid_case_result = queue_db.execute(queue_filter_query)
        except Exception as db_err:
            logging.error(f"[ERROR] DB query failed: {db_err}")
            return []

        branch_codes = []
        #print("\n Result from `queue_filter_query`:")
        for row in valid_case_result:
            print(row)


        if not valid_case_result.empty:
            #rm_names = valid_case_result['rm_name'].dropna().astype(str).tolist()
            branch_codes = valid_case_result['branch_code'].dropna().astype(str).tolist()
        else:
            logging.info("[INFO] No matching case_ids found in process_queue.")
        return branch_codes

    except Exception as e:
        logging.error(f"[ERROR] Unexpected failure in getting_rm_branch: {e}")
        return []



def get_snapshot(db, data, queue_uid, tenant_id, search_text = "",operator = "",date_filter=""):
    start_point = data['start']
    end_point = data['end']
    offset = data['offset']
    db_config['tenant_id'] = tenant_id
    group_access_db = DB('group_access', **db_config)
    # offset = end_point - start_point

    # columns_data = get_columns(queue_uid, tenant_id)
    columns_data = get_columns('snapshot', tenant_id)
    columns = columns_data['columns']
    column_mapping = columns_data['column_mapping']
    extraction_columns_list = columns_data['extraction_columns_list']
    # Apply prefix conversion for Elasticsearch column names
    prefix_map = {
        "queue_list": "ql",
        "ocr": "o",
        "process_queue": "pq",
    }

    updated_extraction_columns = []

    for col in extraction_columns_list:
        if "." in col:
            table, column = col.split(".", 1)

            if table in prefix_map:
                new_col = f"{prefix_map[table]}.{column.upper()}"
            else:
                new_col = col  # unchanged
        else:
            new_col = col

        updated_extraction_columns.append(new_col)

    # Replace original
    extraction_columns_list = updated_extraction_columns

    column_order = list(column_mapping.keys())

    prefix_column = []

    for column in columns:
        prefix_column.append('pq.' + column.upper())
    #print(f"prefix_column is..:{prefix_column}")
    #print(f"extraction_columns_list is...:{extraction_columns_list}")

    prefix_column = prefix_column + extraction_columns_list
    extraction_columns = [col.split('.')[-1] for col in extraction_columns_list]
    columns = columns + extraction_columns

    

    elastic_input = {}
    elastic_input['columns'] = prefix_column
    elastic_input['start_point'] = start_point
    elastic_input['size'] = offset
    elastic_input['filter'] = []
    elastic_input['source'] = 'process_queue'
    elastic_input['tenant_id'] = tenant_id
    if search_text:
        elastic_input['text'] = search_text
    queues = [
        "maker_queue",
        "accepted_queue",
        "waiting_queue",
        "rejected_queue",
        "case_creation"
    ]
    elastic_input['filter'] = [
        {
            "field": "ql.QUEUE.keyword",
            "value": queues
        }
    ]   

    elastic_input['track_total_hits'] = True
    elastic_input['sort'] = [
        {
            "pq.CREATED_DATE": "desc"
        }
    ]
    initial_start_point=start_point
    try:
        if start_point > 10000:
            # last page logic
            start_point = 0
            offset = 20
            force_last_page = True
        else:
            force_last_page = False

    except:
        start_point = 0
        offset = 20
        force_last_page = False
    #if force_last_page:
    #    # Override sorting for last page
    #    elastic_input['sort'] = [
    #        {"pq.CREATED_DATE": "asc"}
    #    ]
    if force_last_page:
        elastic_input['start_point'] = 0
        elastic_input['size'] = 20
        elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]
    else:
        elastic_input['sort'] = [{"pq.CREATED_DATE": "desc"}]
    #date_filter = data.get('search_filters', {}).get('Date Filter', {})
    # logging.info(f"date_filter of the date_filter {date_filter}")
    begin_date = date_filter.get('begin', '')
    end_date = date_filter.get('end', '')
    # logging.info(f"end_date of the filter {end_date}")
    try:
        if begin_date and end_date:
            start_dates = datetime.strptime(begin_date, '%Y-%m-%d')
            end_dates = datetime.strptime(end_date, '%Y-%m-%d')
                
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
                
                    #query__ = f"SELECT pq.case_id FROM process_queue pq INNER JOIN queue_list ql ON pq.CASE_ID = ql.CASE_ID WHERE pq.CREATED_DATE >= TO_DATE('{start_date_formatted} 00:00', 'YYYY-MM-DD HH24:MI') AND pq.CREATED_DATE <= TO_DATE('{end_date_formatted} 23:59', 'YYYY-MM-DD HH24:MI') AND ql.QUEUE = 'maker_queue' ORDER BY pq.LAST_UPDATED DESC FETCH FIRST 20 ROWS ONLY"
                    #process_queue_cases = queues_db.execute_(query__)

                    # Convert DataFrame to list of case IDs
                    #case_ids = process_queue_cases['case_id'].tolist()
                    # logging.info(f"case_ids{case_ids}")
                    #if not case_ids:
                    #    case_ids.append("NO_DATA")
                    
            start_dates = start_dates.strftime("%Y-%m-%dT00:00:00")
            end_dates = end_dates.strftime("%Y-%m-%dT23:59:59")
                    
            output = [{'range': {"field":'pq.CREATED_DATE',"gte": start_dates,
                               "lte": end_dates}}]
                    # logging.info(f'5.output filters list is: {output}')
                    # logging.info(f"elastic_input at date filtering{elastic_input}")
            elastic_input['filter'] = elastic_input['filter'] + \
                output
            logging.info(f"elastic_input after date filter{elastic_input}")
        else:
            elastic_input['filter'] = elastic_input['filter']
            logging.info(f"elastic_input after date filter{elastic_input}")
    except Exception as e:
        logging.info(f"Exception is:{e}")
        pass



    logging.info(f"######elastic_input is {elastic_input}")

    try:
        files, total_files = elasticsearch_search(elastic_input)
        # -----------------------------------------
        # ASC FALLBACK LOGIC (ADD THIS BLOCK HERE)
        # -----------------------------------------
        start_point=initial_start_point
        is_empty = (len(files) == 0)
        requested_last_row = start_point + offset
        is_last_page = (requested_last_row >= total_files)

        force_last_page = False
        # Only fallback if ES returned empty AND this is the true last page
        if  start_point >= 10000 :
            force_last_page = True

        if force_last_page:
            # Compute ASC offset so that pagination works backwards
            #asc_start_point = total_files - end_point
            asc_start_point = total_files - (start_point + offset)
            #print(f"asc_start_point is........:{asc_start_point}")
            if asc_start_point < 0:
                asc_start_point = 0

            elastic_input['start_point'] = asc_start_point
            elastic_input['size'] = offset
            elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]
            remaining = total_files - start_point
            if remaining < offset:
                elastic_input['size'] = remaining
            else:
                elastic_input['size'] = offset
            #print(f"second time elastci input is.:{elastic_input}")
            files, total_files = elasticsearch_search(elastic_input)

            # Reverse ASC results to preserve DESC UI
            files.reverse()

        for document in files:
            #document = {k.lower(): v for k, v in document.items()}
            if 'pq.created_date' in document:
                document['pq.created_date'] = parser.parse(document['pq.created_date'])
                # document['process_queue.created_date'] = (document['process_queue.created_date']).strftime(r'%B %d, %Y %I:%M %p')
                document['pq.created_date'] = (document['pq.created_date'] + timedelta(hours=time_diff_hours, minutes=time_diff_minutes)).strftime(r'%B %d, %Y %I:%M %p')           
            for idx, column in enumerate(columns):
                if prefix_column[idx] in document:
                    document[column] = document.pop(prefix_column[idx])

        # queue_files = process_queue_df
        # files = queue_files[columns].to_dict(orient='records')
        # for document in files:
        #     document['created_date'] = (document['created_date']).strftime(r'%B %d, %Y %I:%M %p')

        if end_point > total_files:
            end_point = total_files

        if initial_start_point > 10000:
            pagination = {"start": initial_start_point + 1,
                      "end": end_point, "total": total_files}
        else:

            pagination = {"start": start_point + 1,
                      "end": end_point, "total": total_files}
        for i, document in enumerate(files):
            files[i] = {k.lower(): v for k, v in document.items()}

        #return {'flag': True, 'data': {'columns': columns, 'column_mapping': column_mapping, 'files': files, 'pagination': pagination, 'column_order': column_order}}
        return {'flag': True, 'data': {'column_mapping': column_mapping, 'files': files, 'pagination': pagination, 'column_order': column_order}}
    except Exception as e:
        message = f'Error occured while getting snapshot details. {e}'
        print(message)
        return {'flag': False, 'message': message}










@app.route('/get_queue', methods=['POST', 'GET'])
@app.route('/get_queue/<queue_id>', methods=['POST', 'GET'])

def get_queue(queue_id=None):
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')
    queue_id=data.get('queue_id',None)
    user=data.get('user',None)
   


    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_queue',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:            
            logging.info(f'Queue ID: {queue_id}')
            
            operator = data.get('user', None)
            
            zipkin_context.update_binary_annotations({'Tenant':tenant_id})

            try:
                start_point = data['start'] - 1
                end_point = data['end']
                if end_point is None:
                    end_point = start_point+20
                offset = end_point - start_point
            except:
                start_point = 0
                end_point = 20
                offset = 20

            search_text = data.get('search_text', None)

            # logging.info(f'Start point: {start_point}')
            # logging.info(f'End point: {end_point}')
            # logging.info(f'Offset: {offset}')

            if queue_id is None:
                message = f'Queue ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            try:
                queue_id = int(queue_id)
            except ValueError:
                message = f'Invalid queue. Expected queue ID to be integer. Got {queue_id}.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            #PURPOSE : store and update selected filters of user .it allows users previous filter selections when they login.
            # in the below code if user selects a filter it will store in the group_acces database active directory table and all_filters column.
            # From frontend, the backend receives selected filters like region, hub code. These filters are stored in the active_directory.all_filters column in the group_access database.
            # If the user already has filters saved, it updates them.
            # If not, it still tries to update with the new filters.

            db_config['tenant_id'] = tenant_id
            db = DB('queues', **db_config)
            group_access_db = DB('group_access', **db_config)
            username = data.get('user', '')
            filters = data.get('selected_filters', {})
            if not username:
                logging.warning("No username provided. Skipping DB check and update.")
            else:
                logging.warning(f"No record found for user {username} in active_directory.")
            #     try:
            #         group_access_db = DB('group_access', **db_config)
            #         #Convert filters dict to JSON string
            #         filters_json = json.dumps(filters)
            #         #Check if user exists and has a row in active_directory
            #         check_query = """
            #             SELECT all_filters
            #             FROM active_directory
            #             WHERE username = :username
            #         """
            #         result_df = group_access_db.execute_(check_query, {'username': username})
            #         if not result_df.empty:
            #             update_query = """
            #                 UPDATE active_directory
            #                 SET all_filters = :filters
            #                 WHERE username = :username
            #             """
            #             group_access_db.execute_(update_query, {'filters': filters_json, 'username': username})
            #             logging.info(f"Filters updated for user: {username}")
                        
            #         else:
            #             # Optional: Insert logic if user doesn't exist
            #             logging.warning(f"No record found for user {username} in active_directory.")       
            #     except Exception as e:
            #         logging.error(f"Error handling filters for user {username}: {str(e)}")  
            # #group_access_db = DB('group_access', **db_config)
            if operator is not None:
                update_operator_q = "UPDATE `process_queue` SET `case_lock`=0 WHERE `operator`=%s and `case_lock`=1"
                db.execute(update_operator_q, params=[operator])
                update_operator_q = "UPDATE `process_queue` SET `operator`=%s WHERE `operator`=%s"
                db.execute(update_operator_q,params=[None,operator])
                
            
            queue_uid=""
            try:
                queue_uid, queue_name, queue_type, queue_definition, is_snapshot, contains_filter, must_not_filter = queue_name_type(queue_id, tenant_id)
            except:
                message = 'Some error in queue definition configuration'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            # Check if queue has children -- No child queues in hdfc
            children_dropdown = []
            # query = "select id,name from queue_definition where unique_name=%s"
            # result = db.execute_(query,params=[queue_uid])
            # count = len(result.values.tolist())
        

            # if count > 0:
            #     queue_id = list(result.id)[0]
            #     children_dropdown = result.to_dict(orient='records')

            #     try:
            #         queue_uid, queue_name, queue_type, queue_definition, is_snapshot, contains_filter, must_not_filter = queue_name_type(queue_id, tenant_id)
            #     except:
            #         message = 'Some error in queue definition configuration'
            #         logging.exception(message)
            #         return jsonify({'flag': False, 'message': message})

            try:
                user_case_filters = get_user_case_filters(operator, queue_uid, tenant_id)
                # logging.info(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ USER CASE FILTERS {user_case_filters} \n @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
            except:
                logging.exception('Error ocurred while getting user case filter.')
                user_case_filters = []

            # logging.info(f"user_case_filters: {user_case_filters}")
            # logging.info(f"################ QUEUE TYPE IS {queue_type}")


            ################# Storing roted api response ############
            queue_repsonse_data={}
            if queue_type == 'train':
                logging.info(f' > Redirecting to `get_template_exception` route.')

                response = get_template_exceptions(db, {'start': start_point, 'end': end_point}, tenant_id, queue_uid, queue_id, user_case_filters, search_text)

                logging.info(f'Response: {response}')
                return jsonify(response)
            elif queue_type == 'reports':
                logging.info(f' > Redirecting to `get_reports_queue` route.')

                host = 'reportsapi'
                port = 443
                route = 'get_reports_queue'
                response = requests.post(
                    f'https://{host}:{port}/{route}', json=data,verify=False)
                response_data = response.json()
                # logging.info(f'response_data:{response_data}')

                active_directory_user_query = f"SELECT ROLE FROM `active_directory` where username = '{user}'"
                active_directory_user_df = group_access_db.execute_(active_directory_user_query)
                user_role = active_directory_user_df.iloc[0]['ROLE']
                role_rights_query=f"select new_rights_assigned_status from role_rights where display_role_rights='View All Queues' and role_name='{user_role}'"
                rights_data=group_access_db.execute_(role_rights_query)['new_rights_assigned_status'].iloc[0]
                show_buttons=True
                if rights_data.lower()=='yes':
                    show_buttons=False
                response_data['data'].update({'show_buttons':show_buttons})
                logging.info(f'show_buttons:{response_data}')
                queue_repsonse_data = response_data
                return jsonify(response_data)
            
            elif queue_type == 'snapshot':
                date_filter = data.get('search_filters', {}).get('Date Filter', {})
                logging.info(f' > Redirecting to `get_snapshot` route.')

                response_data = get_snapshot(db, {'start': start_point, 'end': end_point, 'offset':offset}, queue_id, tenant_id, search_text,operator,date_filter)

                return jsonify(response_data)
            
            elif queue_type == 'recon':
                logging.info(f' > Redirecting to `/get_recon` route.')
                response_data = get_recon_data(queue_id, queue_name, tenant_id)
                button_time = time()
                logging.info(f'Getting button details for `{queue_name}`...')
                button_attributes = get_button_attributes(queue_id, queue_definition, tenant_id)
                logging.info(f'Time taken for button functions {time()-button_time}')
                response_data['buttons'] = button_attributes
                
                return jsonify({'data':response_data, 'flag' : True})

            all_st = time()



            
            
            try:
                columns_data = get_columns(queue_uid, tenant_id)
                columns = columns_data['columns']
                util_columns = columns_data['util_columns']
                extraction_columns_df = columns_data['extraction_columns_df']
                extraction_columns_list = columns_data['extraction_columns_list']
                column_mapping = columns_data['column_mapping']
                # logging.info(f'PRINTING COLUMNS DATA{columns_data}')
                
                
            except:
                message = 'Some column ID not found in column definition table.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            elastic_input = {}

            prefix_column = []

            columns += ['created_date', 'freeze', 'status']

            if 'case_id' not in columns:
                columns.append('case_id')

            columns = list(set(columns))
            for column in columns:
                
                prefix_column.append('pq.' + column.upper())
            # logging.info(f"----{prefix_column}")
                

            case_ids = data.get('case_ids', [])

            if search_text:
                elastic_input['text'] = search_text
                
            elastic_input['start_point'] = start_point
            elastic_input['size'] = offset
            elastic_input['tenant_id'] = tenant_id
            elastic_input['filter'] = []

            if case_ids:
                case_filter = {'field': 'pq.CASE_ID', 'value': case_ids}
                elastic_input['filter'] = [case_filter] + user_case_filters
            elif is_snapshot == 2:
                queue_ids = list(db.execute_("select unique_name from queue_definition").unique_name)
                elastic_input['filter'] =  [{'field': 'ql.QUEUE', 'value': queue_ids}]
                elastic_input.pop('size')
            elif is_snapshot == 1:
                

                query = "SELECT case_id FROM process_queue WHERE id in (select MIN(id) from process_queue group by file_name)"
                
                case_ids = list(db.execute(query).case_id)

                elastic_input['filter'] = [{'field': 'ql.CASE_ID', 'value': case_ids}] + user_case_filters 
            else:
                if queue_uid == 'reassign':
                    asd = 'lea_maker'
                else:
                    asd = queue_uid
                elastic_input['filter'] = [{'field': 'ql.QUEUE', 'value': asd}] + user_case_filters

            elastic_input['filter'] = elastic_input['filter'] + contains_filter
            # elastic_input['filter'] = []
            elastic_input['must_not'] = must_not_filter
            logging.info(f'Adding search filters from UI...')
            extraction_db = DB('extraction', **db_config)
            #db = DB('queues', **db_config)
            query_ocr = f"SELECT * FROM user_tab_columns WHERE table_name = 'OCR'"
            
            ocr_columnss = extraction_db.execute_(query_ocr)
            # logging.info(f"####ocr_columnss   {ocr_columnss}")
            ocr_columns = ocr_columnss['COLUMN_NAME'].tolist()
            ocr_columns = [column.lower() for column in ocr_columns]
            logging.info(f"####ocr_columns after changing variable   {ocr_columns}")
            ocr_columns = get_table_columns('OCR', 'extraction', tenant_id)
            #Route : get_queue 
            #Purpose : To fetch filtered data from the PROCESS_QUEUE table using Elasticsearch based on user-selected filters like region and hubcode.
            # Backend sends available filters to Frontend
            #List of Regions: SELECT DISTINCT region_name FROM regions
            #List of Hub Codes: SELECT DISTINCT HUB_CODE FROM RM_MASTER
            #sending data to frontend using filter_options.Frontend uses this to populate dropdown filters for regions and hubcodes.
            #in this route we are fetching data by filtering using elasticsearch from process queue.
            #using elasticsearch filter convert into elastic search query and fetches the cases that matches the selected filter.
            #Elasticsearch uses the filters to retrieve matching cases from the PROCESS_QUEUE.

            try:
                # Query to get column data from the database in the table 'PROCESS_QUEUE'
                query_ = f"SELECT * FROM user_tab_columns WHERE table_name = 'PROCESS_QUEUE'"
                process_queue_columns = db.execute_(query_)
                process_queue_columns = process_queue_columns['COLUMN_NAME'].tolist()
                process_queue_columns = [column.lower() for column in process_queue_columns]
                process_queue_columns = get_table_columns('PROCESS_QUEUE', 'queues', tenant_id)
                # Extracting filters from the request data
                search_filters = data.get('search_filters', {})
                selected_hub_code=data.get('selected_hub_code','') #Specific hub code, if selected
                selected_filters = data.get('selected_filters', {}) #getting regions and hubcodes from frontend
                selected_rm_name=data.get('selected_rm_name','') #Specific , rm_name if selected
                selected_branch_code=data.get('selected_branch_code','') #Specific branch_code, if selected
                logging.info(f"exception search_filters{search_filters}") 
                logging.info(f"selected_filters: {selected_filters}") 
                search_filters_list = []

                region_map = {
                    'West i': ['West i', 'West I'],
                    'West ii': ['West ii', 'West II'],
                    'South i': ['South i', 'South I'],
                    'South ii': ['South ii', 'South II'],
                    'North': ['North'],
                    'East': ['East'],
                    'Mumbai': ['Mumbai'],
                    'NA': ['NA', '', None]
                }

                # ----------------- REGION -----------------
                region_values = selected_filters.get('regions', [])
                if region_values:
                    expanded = []
                    for r in region_values:
                        for variants in region_map.values():
                            if r in variants:
                                expanded.extend(variants)
                                break
                        else:
                            expanded.append(r)

                    expanded = list(set(expanded))
                    if expanded:
                        search_filters_list.append({'field': 'pq.REGION', 'value': expanded})

                # ----------------- RM NAME (single logic) -----------------
                rm_values = selected_filters.get('rm_name', [])
                if selected_rm_name:
                    rm_values.append(selected_rm_name)

                if rm_values:
                    rm_values = list(set(rm_values))
                    search_filters_list.append({'field': 'o.RELATION_MGRNAME', 'value': rm_values})

                # ----------------- BRANCH CODE (single logic) -----------------
                branch_values = selected_filters.get('branch_code', [])
                if selected_branch_code:
                    branch_values.append(selected_branch_code)

                if branch_values:
                    branch_values = list(set(branch_values))
                    search_filters_list.append({'field': 'pq.BRANCH_CODE', 'value': branch_values})

                # ----------------- HUB CODE (single logic) -----------------
                hub_values = selected_filters.get('hubcodes', [])
                if selected_hub_code:
                    hub_values.append(selected_hub_code)

                if hub_values:
                    hub_values = list(set(hub_values))
                    search_filters_list.append({'field': 'pq.HUB_CODE', 'value': hub_values})

                
                #iterates through each search filetr selected from frontend
                for search_col, search_vals in search_filters.items():
                    try:
                        parsed_vals = json.loads(search_vals) #parse the filter value,should be in jsonformate.
                        if search_col.lower() in ocr_columns: 
                            search_filters_list.append({
                                'field': f'o.{search_col.upper()}',
                                'value': parsed_vals
                            })
                        else:
                            search_filters_list.append({
                                'field': f'pq.{search_col.upper()}',
                                'value': parsed_vals
                        })
                    #logs warning if json parsing fails
                    except Exception as e:
                        logging.warning(f"Invalid JSON in OCR filter `{search_col}`: {e}")
                    logging.info(f'search_filters_list:{search_filters_list}')
                #convert into elastic search query and fetches the cases that matches the selected filter
                elastic_input['filter'] = elastic_input.get('filter', []) + search_filters_list
                logging.info(f"Elastic Input after all filters: {elastic_input}")
                
            except Exception as e:
                logging.info(f"exception in filtering the region{e}")
            elastic_input['source'] = 'process_queue'
            # logging.info('Adding the date filter in the list view')
            date_filter = data.get('search_filters', {}).get('Date Filter', {})
            # logging.info(f"date_filter of the date_filter {date_filter}")
            begin_date = date_filter.get('begin', '')
            end_date = date_filter.get('end', '')
            # logging.info(f"end_date of the filter {end_date}")
            try:
                if begin_date and end_date:
                    start_dates = datetime.strptime(begin_date, '%Y-%m-%d')
                    end_dates = datetime.strptime(end_date, '%Y-%m-%d')
                
                    start_date_formatted = start_dates.strftime('%Y-%b-%d')
                    end_date_formatted = end_dates.strftime('%Y-%b-%d')
                
                    query__ = f"SELECT pq.case_id FROM process_queue pq INNER JOIN queue_list ql ON pq.CASE_ID = ql.CASE_ID WHERE pq.CREATED_DATE >= TO_DATE('{start_date_formatted} 00:00', 'YYYY-MM-DD HH24:MI') AND pq.CREATED_DATE <= TO_DATE('{end_date_formatted} 23:59', 'YYYY-MM-DD HH24:MI') AND ql.QUEUE = 'maker_queue' ORDER BY pq.LAST_UPDATED DESC FETCH FIRST 20 ROWS ONLY"
                    process_queue_cases = db.execute_(query__)

                    # Convert DataFrame to list of case IDs
                    case_ids = process_queue_cases['case_id'].tolist()
                    # logging.info(f"case_ids{case_ids}")
                    if not case_ids:
                        case_ids.append("NO_DATA")
                    
                    start_dates = start_dates.strftime("%Y-%m-%dT00:00:00")
                    end_dates = end_dates.strftime("%Y-%m-%dT23:59:59")
                    
                    output = [{'range': {"field":'pq.CREATED_DATE',"gte": start_dates,
                                    "lte": end_dates}}]
                    # logging.info(f'5.output filters list is: {output}')
                    # logging.info(f"elastic_input at date filtering{elastic_input}")
                    elastic_input['filter'] = elastic_input['filter'] + \
                        output
                    logging.info(f"elastic_input after date filter{elastic_input}")
                else:
                    elastic_input['filter'] = elastic_input['filter']
                    logging.info(f"elastic_input after date filter{elastic_input}")

            except Exception as e:
                logging.info(f"error in the date filter")


            queue_df = queue_definition.loc[str(queue_id)]
            # logging.info(f"####queue_df   {queue_df}")
            try:
                sort_on = json.loads(queue_df['sort_by'])
                logging.info(f'sort_on from queue definition: {sort_on}')
                sort_columns = [list(col.keys())[0] for col in sort_on if list(col.keys())[0] not in prefix_column]
                logging.info(f'sort_columns to be added: {sort_columns}, prefix_column: {prefix_column}')
                prefix_column.extend(sort_columns)
                logging.info(f'Updated prefix_column: {prefix_column}')
            except:
                sort_on = [{'pq.CREATED_DATE': 'desc'}]
                logging.exception(f'sort by is not defined or not properly defined')
            elastic_input['sort'] = sort_on
            logging.info(f'Final sort input: {elastic_input["sort"]}')

            for co in extraction_columns_list:
                prefix_column.append('o.' + co.split('.')[1].upper())

            elastic_input['columns'] = prefix_column

            #logging.info(f'Elasticsearch Input: {elastic_input}')
            initial_start_point=start_point
            original_sort = sort_on
            try:
                if start_point > 10000:
                    # last page logic
                    start_point = 0
                    offset = 20
                    force_last_page = True
                else:
                    force_last_page = False

            except:
                start_point = 0
                offset = 20
                force_last_page = False
            if force_last_page:
                elastic_input['start_point'] = 0
                elastic_input['size'] = 20
                elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]
            else:
                elastic_input['sort'] = original_sort

            logging.info(f"final elastic input 1 is........:{elastic_input}")
            files, total_files = elasticsearch_search(elastic_input)
            #files, total_files = elasticsearch_search(elastic_input)
            logging.info(f"entered in this 1st loppppppppppppppppp")
            """try:
                # Determine if fallback should be used
                if initial_start_point >= 10000:
                    force_last_page = True
                else:
                    force_last_page = False

            except:
                force_last_page = False

            # If ASC fallback is needed
            if force_last_page:
                # Compute ASC pagination offset
                logging.info(f"total files are..:{total_files}")
                logging.info(f"intial_start_point is:{initial_start_point}")
                asc_start_point = total_files - (initial_start_point + offset)
                logging.info(f"asc_start_point is........:{asc_start_point}")

                if asc_start_point < 0:
                    asc_start_point = 0

                elastic_input['start_point'] = asc_start_point
                elastic_input['size'] = offset
                elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]

            else:
                # Normal descending pagination
                elastic_input['start_point'] = initial_start_point
                elastic_input['size'] = offset
                elastic_input['sort'] = [{"pq.CREATED_DATE": "desc"}]

            logging.info(f"final elastic input is........:{elastic_input}")

            # Run ES query
            files, total_files = elasticsearch_search(elastic_input)"""


            # -------------------------------------------------------------
            # Special case: ES max_result_window == 10000 workaround
            # -------------------------------------------------------------
            if total_files == 10000:
                elastic_input['track_total_hits'] = True
                logging.info("entered in 2nd loop")

                try:
                    if initial_start_point >= 10000:
                        force_last_page = True
                    else:
                        force_last_page = False

                except:
                    force_last_page = False

                if force_last_page:
                    asc_start_point = total_files - (initial_start_point + offset)
                    if asc_start_point < 0:
                        asc_start_point = 0

                    elastic_input['start_point'] = asc_start_point
                    elastic_input['size'] = offset
                    elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]

                else:
                    elastic_input['start_point'] = initial_start_point
                    elastic_input['size'] = offset
                    elastic_input['sort'] = original_sort

                logging.info(f"final elastic input2 is........:{elastic_input}")
                files, total_files = elasticsearch_search(elastic_input)
                try:
                    # Determine if fallback should be used
                    if initial_start_point >= 10000:
                        force_last_page = True
                    else:
                        force_last_page = False

                except:
                    force_last_page = False

                # If ASC fallback is needed
                if force_last_page:
                    # Compute ASC pagination offset
                    logging.info(f"total files are..:{total_files}")
                    logging.info(f"intial_start_point is:{initial_start_point}")
                    asc_start_point = total_files - (initial_start_point + offset)
                    logging.info(f"asc_start_point is........:{asc_start_point}")

                    if asc_start_point < 0:
                        asc_start_point = 0

                    elastic_input['start_point'] = asc_start_point
                    elastic_input['size'] = offset
                    elastic_input['sort'] = [{"pq.LAST_UPDATED": "asc"}]

                else:
                    # Normal descending pagination
                    elastic_input['start_point'] = initial_start_point
                    elastic_input['size'] = offset
                    elastic_input['sort'] = original_sort
                remaining = total_files - initial_start_point
                #logging.info(f"remaining is...:{remaining}")
                if remaining < offset:
                    elastic_input['size'] = remaining
                else:
                    elastic_input['size'] = offset
                
                logging.info(f"final elastic input isi after 3rd callll........:{elastic_input}")
                files, total_files = elasticsearch_search(elastic_input)
                #files.reverse()
                if force_last_page:
                    logging.info("Reversing files because ASC fallback was used...")
                    files = list(files)
                    files.reverse()

            #elastic_input['track_total_hits'] = True
            #initial_start_point=start_point
            """try:
                if start_point > 10000:
                    # last page logic
                    start_point = 0
                    offset = 20
                    force_last_page = True
                else:
                    force_last_page = False

            except:
                start_point = 0
                offset = 20
                force_last_page = False
            if force_last_page:
                elastic_input['start_point'] = 0
                elastic_input['size'] = 20
                elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]
            else:
                elastic_input['sort'] = [{"pq.CREATED_DATE": "desc"}]

            logging.info(f"final elastic input is........:{elastic_input}")     
            files, total_files = elasticsearch_search(elastic_input)
            if total_files==10000:
                logging.info(f"entered in this 2nd  loppppppppppppppppp")
                elastic_input['track_total_hits'] = True
                #initial_start_point=start_point
                try:
                    if initial_start_point > 10000:
                        # last page logic
                        start_point = 0
                        offset = 20
                        force_last_page = True
                    else:
                        force_last_page = False

                except:
                    start_point = 0
                    offset = 20
                    force_last_page = False
                if force_last_page:
                    elastic_input['start_point'] = 0
                    elastic_input['size'] = 20
                    elastic_input['sort'] = [{"pq.CREATED_DATE": "asc"}]
                else:
                    elastic_input['sort'] = [{"pq.CREATED_DATE": "desc"}]
                logging.info(f"final elastic input2 is........:{elastic_input}")
                files, total_files = elasticsearch_search(elastic_input)"""
            case_ids = set()

            files = [file for file in files if file]
            files=checking_duplicate(files,tenant_id)
            files=extractCase(files,tenant_id,extraction_db)
            #relation_manager_names= getting_rm_name(files, tenant_id)
            #branch_numbers=getting_branch_code(files, tenant_id)
            #logging.info("RM Names:", relation_manager_names)
            #logging.info("Branch Codes:", branch_numbers)
            #rm_names = list(set(relation_manager_names))
            #branch_codes = list(set(branch_numbers))

            #logging.info(f'Elasticsearch files = {files}')
            for document in files:                
                document['pq.CREATED_DATE'] = parser.parse(document['pq.CREATED_DATE'])
                document['pq.CREATED_DATE'] = (document['pq.CREATED_DATE']  + timedelta(hours=time_diff_hours, minutes=time_diff_minutes)).strftime(r'%B %d, %Y %I:%M %p')
                case_ids.add(document['pq.CASE_ID'])

                for idx, column in enumerate(columns):
                    try:
                        document[column] = document.pop(prefix_column[idx])
                    except Exception as e:
                        logging.info(f" Exception Occured ... {e}")
                        pass

            

            if queue_type != 'formqueue':
                # if case_ids:
                    # Get more case IDs from case relation table
                    # logging.info(f'Getting case relation data to get extraction data for shallow case')
                    # case_ids_string = ', '.join([f"'{case_id}'" for case_id in case_ids])
                    # query = f"select * from case_relation where queue_case_id in ({case_ids_string})"
                    # case_relation_df = db.execute(query)
                    
                    # case_relation_dict = defaultdict(list)
                    # case_ids = list(case_ids)
                    # for case_id in case_ids:
                    #     case_relation = case_relation_df.loc[case_relation_df['queue_case_id'] == case_id]
                    #     if case_relation.empty:
                    #         logging.info(f'No relation for `{case_id}`.')
                    #         continue

                    #     data_case_id = list(case_relation['data_case_id'])[0]

                    #     case_relation_dict[case_id].append(data_case_id)

                    #     if case_id == data_case_id:
                    #         logging.info(f'`{case_id}` is deep forked. Skipping')
                    #         continue

                    #     if data_case_id not in case_ids:
                    #         logging.info(f'Found related case. Adding `{data_case_id}`.')
                    #         case_ids.append(data_case_id)
                            
                    # select_columns_list = []
                    # for _, row in extraction_columns_df.iterrows():
                    #     col_name = row['column_name']                   
                    #     table = row['source']

                    #     if table=='ocr':
                    #         select_columns_list.append(f'o.{col_name}')
                    #     else:
                    #         select_columns_list.append(f'{table}.{col_name}')

                    # select_columns_list += ['o.id', 'o.case_id']

                    # select_part = ', '.join(select_columns_list)

                    # # logging.info(f'Select part: {select_part}')

                    # columns = select_columns_list
                    # case_ids = list(case_ids)

                    # elastic_input = {}
                    # elastic_input['columns'] = columns
                    # elastic_input['filter'] = [{'field': 'o.case_id', 'value': case_ids},{'field': 'QUEUE_LIST.queue', 'value': queue_uid}]
                    # elastic_input['source'] = 'process_queue'
                    # elastic_input['tenant_id'] = tenant_id
                    # elastic_input["size"] = len(case_ids)*4
                    # # logging.info(f"elastic_input at case_ids coming after search{elastic_input}")

                    # query_result_list, _ = elasticsearch_search(elastic_input)

                    # # logging.info(f'Files: {files}')
                    # for document in files:
                    #     document.pop('file_paths', None)
                    #     # Default percentage_done -- uncomment based on client
                    #     percentage_done = '0'

                    #     try:
                    #         percentage_done = str(int((document['completed_processes']/document['total_processes'])*100))
                    #     except:
                    #         percentage_done = '0'
                    #     if int(percentage_done) > 100:
                    #         percentage_done = '100'
                        
                    #     # logging.info('Creating status key')
                        
                    #     try:
                    #         for row in query_result_list:
                    #             row_case_id = row['o.case_id']
                    #             doc_case = document['case_id']

                    #             for col, val in row.items():
                    #                 case_relation = case_relation_dict[doc_case]
                    #                 if len(case_relation) > 0:
                    #                     data_case_id = case_relation[0]
                    #                 else:
                    #                     data_case_id = None
                                    
                    #                 column_wo_prefix = col.split('.')[1]
                    #                 if row_case_id == doc_case:
                    #                     document[column_wo_prefix] = val
                    #                     continue
                    #                 elif row_case_id == data_case_id:
                    #                     if column_wo_prefix != 'case_id':
                    #                         document[column_wo_prefix] = val
                    #     except:
                    #         logging.exception('something wrong in form queue')
                
                # query = "select column_name from column_definition where date_ = 1"
                # columns_to_change = list(db.execute_(query).column_name)
                
                columns_to_change=['created_date','last_updated']
                # logging.info(f'Converting date format for fields: {columns_to_change}')

                for document in files:
                    for column in columns_to_change:
                        try:
                            document[column] = parser.parse(document[column])
                            if column == 'created_date' or column == 'last_updated':
                                document[column] = (document[column]).strftime(r'%B %d, %Y %I:%M %p')
                            
                            else:
                                document[column] = (document[column]).strftime(r'%B %d, %Y')
                        except ValueError:
                            document[column] = ''
                        except:
                            
                            pass

                # for document in files:
                #     for column in ['Shipment_date_txn']:
                #         try:
                #             document[column] = parser.parse(document[column])
                #             if column == 'Shipment_date_txn':
                #                 document[column] = (document[column]).strftime(r'%Y-%m-%d')
                #         except ValueError:
                #             document[column] = ''
                #         except:
                #             pass

                columns = [col for col in columns if col not in util_columns]
                columns += extraction_columns_list
                # logging.info(f'New columns: {columns}')
            else:
                column_mapping = {}
            
            column_data_types = {}
            # if len(files)>0:
            #     columns_in_files = files[0].keys()
            #     column_data_types = get_column_data_types(columns_in_files, tenant_id)

            # * BUTTONS
            button_time = time()

            #workflow definition is so commenting this code
            button_attributes=[]
            #button_attributes = get_button_attributes(queue_id, queue_definition, tenant_id)
             

            # logging.info(f"button_attribures got is {button_attributes}")
            
        
            # * FIELDS
            # logging.info(f'Getting fields details for `{queue_name}`...')

            #Below are not required for UI so , Commenting them

            # if queue_type == 'casemgmt':
            #     field_attributes, tabs, excel_display_data, tab_type_mapping, _ = get_fields_tab_queue(
            #         queue_id, tenant_id, 1)
            # else:
            #     field_attributes, tabs, excel_display_data, tab_type_mapping, _ = get_fields_tab_queue(
            #         queue_id, tenant_id)
            
            if end_point > total_files:
                end_point = total_files
            total_files = '1000+' if total_files == 1000 else total_files
            if initial_start_point > 10000:
                pagination = {"start": initial_start_point + 1,
                        "end": end_point, "total": total_files}
            else:

                pagination = {"start": start_point + 1,
                        "end": end_point, "total": total_files}
            #pagination = {"start": start_point + 1, "end": end_point, "total": total_files}
            dropdown={}
            cascade_object={}
            # dropdown, _, _, cascade_object = get_dropdown(queue_id, tenant_id)
            
            #front_end_biz = get_frontend_executable_rules(tenant_id)
            #Front biz rules are not read form sequence rule data in ACE-Camunda projects so removed it 
            front_end_biz=[]


            ####### Checking if rerouted api's have some data before send it to UI####

            file_response=queue_repsonse_data.get('files',files)
            # logging.info(f'Getting files data for status and last_updated {files}')

            pagination_response=queue_repsonse_data.get('pagination',pagination)
            reports_reponse=queue_repsonse_data.get('reports',[])

            layout_view_q = f"select layout_view from queue_definition where id = {queue_id}"
            # logging.info(f"@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ HERE IN LINE NO 1577")
            layout_view = list(db.execute_(layout_view_q)["layout_view"])
            if len(layout_view)>0:
                layout_view = layout_view[0]
            else:
                layout_view = ""

            ne_res=[]
            for fi in file_response:
                temp={}
                for key,val in fi.items():
                    try:
                        k=key.split('.')[1].lower()
                    except:
                        k=key
                    temp[k]=val
                ne_res.append(temp)

            # -------------------- HUB CODES --------------------
            try:
                hubcodes_list = extraction_db.execute_("SELECT DISTINCT HUB_CODE FROM RM_MASTER")['HUB_CODE'].tolist()
            except Exception as e:
                logging.info(f'Error fetching hubcodes: {e}')
                hubcodes_list = []

            # -------------------- REGIONS --------------------
            try:
                regions_list = extraction_db.execute_("SELECT DISTINCT region_name FROM regions")['region_name'].tolist()
            except Exception as e:
                logging.info(f'Error fetching regions: {e}')
                regions_list = []

            # -------------------- HUB CODE FOR OPERATOR --------------------
            hub_code_ = ""
            try:
                emp_df = group_access_db.execute_(f"SELECT employee_name FROM active_directory WHERE username='{operator}'")

                if not emp_df.empty:
                    employee_name = emp_df.iloc[0]['EMPLOYEE_NAME']

                    hub_df = extraction_db.execute_(f"SELECT hub_code FROM RM_MASTER WHERE emp_name='{employee_name}'")
                    if not hub_df.empty:
                        hub_code_ = hub_df.iloc[0]['HUB_CODE']
            except Exception as e:
                logging.info(f'Error fetching hub_code for operator: {e}')

            # -------------------- RM NAMES --------------------
            try:
                rm_name_list = extraction_db.execute_("SELECT DISTINCT relation_mgrname FROM ocr")['RELATION_MGRNAME'].tolist()
            except Exception as e:
                logging.info(f'Error fetching RM names: {e}')
                rm_name_list = []

            # -------------------- BRANCH CODES --------------------
            try:
                branch_code_list = db.execute_("SELECT DISTINCT branch_code FROM process_queue")['BRANCH_CODE'].tolist()
            except Exception as e:
                logging.info(f'Error fetching branch codes: {e}')
                branch_code_list = []

            # -------------------- CLIMS FAILED CASES --------------------
            try:
                clims_query = f"""
                    SELECT DISTINCT q.case_id 
                    FROM queue_list q 
                    JOIN hdfc_extraction.ocr o ON q.case_id=o.case_id 
                    WHERE o.no_of_retries >= 3 
                    AND q.queue = 'waiting_queue' 
                    AND o.clims_status = 'NSTP' 
                    AND q.case_id IN (
                            SELECT case_id FROM process_queue 
                            WHERE TO_CHAR(last_updated_by) = '{operator}'
                    )
                """

                clims_df = db.execute_(clims_query)
                unique_case_ids = list(clims_df['CASE_ID'].unique()) if not clims_df.empty else []
            except Exception as e:
                logging.info(f'Error fetching CLIMS failed cases: {e}')
                unique_case_ids = []

            #store in filteroptions to be sent to frontend
            filter_options = {
                "regions": regions_list,
                "hubcodes": hubcodes_list,
                "rm_name" : rm_name_list,
                "branch_code":branch_code_list
                }
            #when no data there for particular hub code then need to display msg 
            is_hub_code_selected=data.get('is_hub_code_selected','')
            no_data_msg=""
            #if selected_filters=={} and  search_filters == {} and selected_hub_code!='' and ne_res==[] and is_hub_code_selected==True:
            #    no_data_msg="Emp code could not be found in HUB code master"
            if selected_hub_code=='' and  is_hub_code_selected==True:
                no_data_msg="Emp code could not be found in HUB code master"
                ne_res=[]
            data = {
                'files': ne_res,
                'buttons': button_attributes,
                'hub_code_message':no_data_msg,
                # 'field': field_attributes,
                # 'tabs': tabs,
                # 'excel_source_data': excel_display_data,
                # 'tab_type_mapping': tab_type_mapping,
                'pagination': pagination,
                'column_mapping': column_mapping,
                'column_order': list(column_mapping.keys()),
                'children_dropdown': children_dropdown,
                'pdf_type': 'folder' if tenant_id else 'blob',
                'biz_rules': front_end_biz,
                'dropdown_values': dropdown,
                'cascade_object' : cascade_object,
                'column_data_types': column_data_types,
                'reports':reports_reponse,
                'layout_view': layout_view,
                'filter_options' : filter_options,
                'hub_code':hub_code_,
                'clims_api_failed_files': unique_case_ids
                }
            logging.info(f'Queue data for `{queue_name}`: {data}')
            # logging.info(f'queue_id:{queue_id}')
            # db.engine.dispose() 
            response = {'flag': True, 'data': data}
            return jsonify(response)
        except Exception as e:
            logging.exception(f'{e}, Something went wrong while getting queues. Check trace.')
            response = {'flag': False, 'message':'System error! Please contact your system administrator.'}
            return jsonify(response)



@app.route('/clims_group_reject', methods=['POST', 'GET'])
def clims_group_reject():
    data = request.json
    logging.info(f'Request data: {data}')
    try:
        case_id_list = data.get('case_ids', None)
        tenant_id = data.get('tenant_id', None)
        operator = data.get('user', None)
        session_id = data.get('session_id', None)
        host = 'button_functions'
        route = 'execute_button_function'
        port = 443
        headers = {
        'Content-type': 'application/json; charset=utf-8',
                            'Accept': 'application/json'
        }
        hit_request_api=f'https://{host}:{port}/{route}'
        print(f'Hitting URL: https://{host}:{port}/{route} for execute button functions request formation for moving to reject on nstp')
        move_to = 'Move to Reject'
        for case_id in case_id_list:
            api_params = {"group":move_to, "case_id": case_id, "tenant_id": tenant_id,"user": operator, "queue_id":9, "session_id": session_id}
            reject_response = requests.post(hit_request_api,json=api_params,headers=headers,stream= True,verify=False)
            if reject_response.status_code == 200:
                request_post = reject_response.json()
                logging.info(f"##########request_post is {request_post} ")
            else:
                logging.info('Got non 200 code from button_functions')
        return jsonify({'flag': True, 'message': f'Cases moved to Rejected queue successfully.'})
    except Exception as e:
        logging.exception(f'Error in clims_group_reject: {e}')
        return jsonify({'flag': False, 'message': 'Something went wrong while moving the cases to Rejected queue.'})
                    
@app.route('/make_case_lock', methods=['POST', 'GET'])
def make_case_lock():
    data = request.json
    logging.info(f'Request data: {data}')
    try:
        case_id = data.pop('case_id', None)
        tenant_id = data.pop('tenant_id', None)
        operator = data.pop('user', None)
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        extraction_db = DB('extraction',**db_config)
        clims_query = f"""SELECT q.case_id
FROM queue_list q
JOIN hdfc_extraction.ocr o ON q.case_id = o.case_id
WHERE q.queue = 'waiting_queue'
  AND o.party_id = (
    SELECT o2.party_id
    FROM hdfc_extraction.ocr o2
    JOIN queue_list q2 ON o2.case_id = q2.case_id
    WHERE o2.case_id = '{case_id}' AND q2.queue = 'maker_queue'
  )"""
        case_list = queue_db.execute_(clims_query)
        if not case_list.empty:
            logging.info(f"party id already in waiting queue")
            return {'flag':False, 'message':f'Party_id is already in waiting_queue, cannot open the case !!'}
        else:
            query = 'SELECT process_queue.*, queue_list.queue FROM `process_queue` inner join queue_list on queue_list.case_id = process_queue.case_id and process_queue.`case_id`=%s'
            case_files = queue_db.execute_(query, params=[case_id])
            # logging.info(f"case_files is {case_files}")

        # Check the case lock status and allow the process based on it
        case_lock_status = case_files['case_lock'][0]
        case_user = case_files['operator'][0]
        if int(case_lock_status)  == 1:
            logging.info(f"Case locked for this case {case_id}")
            return {'flag':False, 'message':f'Sorry, the case is currently in use by another user {case_user}.'}
        else:
            logging.info(f'Locking case `{case_id}` by operator `{operator}`')
            update = {
                'operator': operator, 'case_lock': 1
            }
            where = {
                'case_id': case_id
            }
            queue_db.update('process_queue', update=update, where=where)
            logging.info(f"case lock got updated to 1 for {case_id}")
            return {'flag':True, 'data':{'message':'Locked!'}}
    except:
        return {'flag':False, 'message':'something went wrong'}
    

def get_forecast_dropdown():
    try:
        db_config['tenant_id'] = 'drldea'
        extraction_db = DB('extraction', **db_config)
        query_product_details = "select * from `product_details`"
        product_with_strength = "select * from `product_strength`"
        product_details_df = extraction_db.execute(query_product_details).to_dict(orient='records')
        product_strength_df = extraction_db.execute(product_with_strength).to_dict(orient='records')
        chemicals = []
        dropdown_dict = {}
        dropdown = {}
        for detail in product_details_df:
            if detail['chemical'] not in chemicals:
                chemicals.append(detail['chemical'])
        for chemical in chemicals:   
            chemical_strength=[]     
            for each in product_details_df:
                if each['chemical']==chemical:
                    for each_ in product_strength_df:
                        if each_['product']==each['product']:
                            product_ = each_['product_with_strength']
                            chemical_strength.append(product_)
                        
            dropdown_dict[chemical]=chemical_strength
            
        dropdown['forecast_table'] = dropdown_dict
        return dropdown
    except:
        traceback.print_exc()

@app.route('/get_queue_components', methods=['POST', 'GET'])
@app.route('/get_queue_components/<queue_id>', methods=['POST', 'GET'])
def get_queue_components(queue_id=None):
    data = request.json
    logging.info(f'Request data: {data}')
    user=data.get('user',None)
    tenant_id = data.get('tenant_id', '')
    queue_id = data.get('queue_id','')
    queue_id = str(queue_id)
    db_config['tenant_id'] = tenant_id
    screen_id = data.get('screen_id', None)
    try:
        if not screen_id:
            return jsonify({'flag': False, 'message': "Screen Name not provided"})

        queue_db = DB('queues', **db_config)
        group_access_db = DB('group_access', **db_config)

        queue_name = list(queue_db.execute_(f"select unique_name from queue_definition where id = {queue_id}").unique_name)[0]

        query = f"select screen_layout,property_mesh from screen_properties where queue = '{queue_name}' and screen_id = '{screen_id}'"
        screen_data = queue_db.execute(query).to_dict(orient='records')[0]

        active_directory_user_query = f"SELECT ROLE FROM `active_directory` where username = '{user}'"
        active_directory_user_df = group_access_db.execute_(active_directory_user_query)
        user_role = active_directory_user_df.iloc[0]['ROLE']
        role_rights_query=f"select new_rights_assigned_status from role_rights where display_role_rights='View All Queues' and role_name='{user_role}'"
        rights_data=group_access_db.execute_(role_rights_query)['new_rights_assigned_status'].iloc[0]
        show_buttons=True
        if rights_data.lower()=='yes':
            show_buttons=False
    
        return jsonify({'flag': True,'mesh_layout': json.loads(screen_data['screen_layout']),'show_buttons':show_buttons, 'components': json.loads(screen_data['property_mesh'])})

    except Exception as e:
        logging.info(f"############### Error in get queue Component")
        logging.exception(e)
        return jsonify({'flag': False, 'message': "Error in getting screen_id"})


@app.route('/get_folder', methods=['POST', 'GET'])
def get_folder():
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')
    queue_case_id = data['case_id']
    db_config['tenant_id'] = tenant_id

    db = DB('queues', **db_config)

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_files',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        logging.info(f'Making file tree!')

        query = f"select * from case_relation where queue_case_id  = '{queue_case_id}'"
        case_relation_df = db.execute(query)

        queue_case_relation = case_relation_df.loc[case_relation_df['queue_case_id'] == queue_case_id]

        if queue_case_relation.empty:
            logging.info(f'No relation found. Using `{queue_case_id}` to fetch file tree.')
            data_case_id = queue_case_id
        else:
            data_case_id = list(queue_case_relation['data_case_id'])[0]
            logging.info(f'Forked case. Using `{data_case_id}` instead of `{queue_case_id}`.')

        file_manager_db = DB('file_manager', **db_config)
        query = f"select * from file_manager where case_id = '{data_case_id}'"
        try:
            file_manager_table = file_manager_db.execute(query).reset_index(level=0)
            file_manager_table["file_name"] = file_manager_table["file_name"].map(str) + '  <' + file_manager_table["time"].astype(str) + '>'

            folder_manager_table = file_manager_db.get_all('folder_manager').reset_index(level=0)
        except:
            file_manager_table = file_manager_db.execute(query).reset_index(level_=0)
            file_manager_table["file_name"] = file_manager_table["file_name"].map(str) + '  <' + file_manager_table["time"].astype(str) + '>'

            folder_manager_table = file_manager_db.get_all('folder_manager').reset_index(level_=0)
        case_file_manager = file_manager_table.loc[file_manager_table['case_id'] == data_case_id]
        logging.info(f'Folder Manager Table: {folder_manager_table.to_dict()}')
        logging.info(f'Case file manager: {case_file_manager.to_dict()}')
        

        folder_ids = list(case_file_manager['folder_id'])
        logging.info(f'List of folder IDs: {folder_ids}')

        file_tree = make_tree(folder_manager_table, case_file_manager)
        logging.info(f'File tree created! Yay!')
        logging.info(f'Tree: {file_tree}')

        response = {'flag': True, 'tree': file_tree}
        return jsonify(response)

def get_filters_function2(filters):
    query = 'SELECT * '
    where_statements = []
    order_statements = []
    for filter in filters:
        where_statements, order_statements =  append_filters_in_query(filter, where_statements, order_statements)
    query+=' FROM `process_queue` '
    if order_statements == []:
        query+='WHERE '+' AND '.join(where_statements)
    else:
        query+='WHERE '+' AND '.join(where_statements)+' ORDER BY '+', '.join(order_statements)
    logging.info(f"&&&&&&&&&&&&&&&&&&&&&&&& THE FINAL QUERY LOOKS LIKE THIS: {query}")

    return query

@app.route('/get_dropdowns', methods=['POST', 'GET'])
def get_dropdowns():
    data = request.json
    logging.info(f'Request data: {data}')
    operator = data.pop('user', None)
    case_id = data.pop('case_id', None)
    tenant_id = data.pop('tenant_id', None)
    column_name = data.get('column_name','')
    db_config['tenant_id'] = tenant_id

    queue_db = DB('queues', **db_config)
    
    query = f"select chemical,product_name from process_queue where case_id='{case_id}'"
    chem_product_df=queue_db.execute_(query)
    chemical_name = list(chem_product_df['chemical'])[0]
    product_name = list(chem_product_df['product_name'])[0]
    logging.info(f"chemical_names: {chemical_name}")
    logging.info(f"product names: {product_name}")
    extraction_db = DB('extraction', **db_config)
    if product_name != '' or product_name == None:
        if column_name == 'Name of the drug with Strength' or column_name == 'Name of the Product' or column_name == 'Product Name':
            query_prod_stre = f"select product_with_strength from product_strength where product='{product_name}'"
            prod_strengths = list(extraction_db.execute_(query_prod_stre)['product_with_strength'])
            options = []
            for i in prod_strengths:
                #print(i)
                options_e ={"display_name": i,"value": i}
                options.append(options_e)

       ####for getting product name 
       

    returning_data = {"flag": True,"options": options,"column_name": column_name}
    return returning_data

@app.route('/get_files', methods=['POST', 'GET'])
@app.route('/get_files/<queue_id>', methods=['POST', 'GET'])
def get_files(queue_id=None):
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_files',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:           
            logging.info(f'Queue ID: {queue_id}')
            
            operator = data.get('user', None)
            
            zipkin_context.update_binary_annotations({'Tenant':tenant_id})

            try:
                start_point = data['start'] - 1
                end_point = data['end']
                offset = end_point - start_point
            except:
                start_point = 0
                end_point = 20
                offset = 20

            logging.info(f'Start point: {start_point}')
            logging.info(f'End point: {end_point}')
            logging.info(f'Offset: {offset}')

            if queue_id is None:
                message = f'Queue ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            try:
                queue_id = int(queue_id)
            except ValueError:
                message = f'Invalid queue. Expected queue ID to be integer. Got {queue_id}.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            db_config['tenant_id'] = tenant_id

            db = DB('queues', **db_config)

            search_text = data.get('search_text', None)
            search_filters = data.get('search_filters', [])
            
            if operator is not None:
                update_operator_q = "UPDATE `process_queue` SET `operator`=%s WHERE `operator`=%s"
                db.execute(update_operator_q,params=[None,operator])

            # Check if queue has children
            try:
                queue_uid, queue_name, queue_type, queue_definition, is_snapshot, contains_filter, must_not_filter = queue_name_type(queue_id, tenant_id, search_filters)
            except:
                message = 'Some error in queue definition configuration'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            try:
                user_case_filters = get_user_case_filters(operator, queue_uid, tenant_id)
                logging.info(f"USER CASE FILTERS ARE {user_case_filters}")
            except:
                logging.exception('Error ocurred while getting user case filter.')
                user_case_filters = []

            logging.info(f"user_case_filters: {user_case_filters}")

            if queue_type == 'train':
                logging.info(f' > Redirecting to `get_template_exception` route.')

                response = get_template_exceptions(db, {'start': start_point, 'end': end_point}, tenant_id, queue_uid, queue_id, user_case_filters, search_text)

                logging.info(f'Response: {response}')
                return jsonify(response)
            elif queue_type == 'reports':
                logging.info(f' > Redirecting to `get_reports_queue` route.')

                host = 'reportsapi'
                port = 80
                route = 'get_reports_queue'
                response = requests.post(
                    f'http://{host}:{port}/{route}', json=data)
                response_data = response.json()

                return jsonify(response_data)

            elif queue_type == 'snapshot':
                date_filter = data.get('search_filters', {}).get('Date Filter', {})
                logging.info(f' > Redirecting to `get_snapshot` route.')

                response_data = get_snapshot(db, {'start': start_point, 'end': end_point, 'offset':offset}, queue_id, tenant_id, search_text,operator,date_filter)

                return jsonify(response_data)
            
            elif queue_type == 'recon':
                logging.info(f' > Redirecting to `/get_recon` route.')
                response_data = get_recon_data(queue_id, queue_name, tenant_id)
                button_time = time()
                logging.info(f'Getting button details for `{queue_name}`...')
                button_attributes = get_button_attributes(queue_id, queue_definition, tenant_id)
                logging.info(f'Time taken for button functions {time()-button_time}')
                response_data['buttons'] = button_attributes
                
                return jsonify({'data':response_data, 'flag' : True})
            
            try:
                columns_data = get_columns(queue_id, tenant_id)
                columns = columns_data['columns']
                util_columns = columns_data['util_columns']
                extraction_columns_df = columns_data['extraction_columns_df']
                extraction_columns_list = columns_data['extraction_columns_list']
            except:
                message = 'Some column ID not found in column definition table.'
                logging.exception(message)
                return jsonify({'flag': False, 'message': message})

            elastic_input = {}

            prefix_column = []

            columns += ['created_date', 'freeze', 'status']

            if 'case_id' not in columns:
                columns.append('case_id')

            columns = list(set(columns))
            for column in columns:
                prefix_column.append('pq.' + column.upper())


            case_ids = data.get('case_ids', [])

            if search_text:
                elastic_input['text'] = search_text

            elastic_input['start_point'] = start_point
            elastic_input['size'] = offset
            elastic_input['tenant_id'] = tenant_id
            elastic_input['filter'] = []

            from_unlock = data.get('from_unlock', None)

            if from_unlock and case_ids:
                case_filter = {'field': 'pq.case_id', 'value': case_ids}
                queue_filter = {'field': 'QUEUE_LIST.queue', 'value': queue_uid}
                elastic_input['filter'] = [case_filter] + user_case_filters + [queue_filter]
            elif case_ids:
                case_filter = {'field': 'pq.CASE_ID', 'value': case_ids}
                if operator in ['11111111', '22222222', '33333333']:
                    elastic_input['filter'] = [case_filter] + user_case_filters
                else:
                    #### HSBC
                    if queue_uid == 'reassign':
                        asd = ['lea_maker', 'checker']
                    else:
                        asd = queue_uid
                    elastic_input['filter'] = [case_filter] + user_case_filters + [{'field': 'QUEUE_LIST.queue', 'value': asd}]
            elif is_snapshot == 2:
                queue_ids = list(db.execute_("select unique_name from queue_definition").unique_name)
                elastic_input['filter'] =  [{'field': 'queue_list.queue', 'value': queue_ids}]
                elastic_input.pop('size')
            elif is_snapshot == 1:
                query = "SELECT case_id FROM process_queue WHERE id in (select MIN(id) from process_queue group by file_name)"
                
                case_ids = list(db.execute(query).case_id)

                elastic_input['filter'] = [{'field': 'pq.CASE_ID', 'value': case_ids}] + user_case_filters 
            else:
                # HSBC harcoding... 
                if queue_uid == 'reassign':
                    asd = 'lea_maker'
                else:
                    asd = queue_uid
                

            elastic_input['source'] = 'process_queue'
            queue_df = queue_definition.loc[queue_id]

           
            try:
                sort_on = json.loads(queue_df['sort_by'])
                sort_columns = [list(col.keys())[0] for col in sort_on if list(col.keys())[0] not in prefix_column]
                prefix_column.extend(sort_columns)
            except:
                sort_on = [{'pq.CREATED_DATE': 'desc'}]
                logging.exception(f'sort by is not defined or not properly defined')
            elastic_input['sort'] = sort_on

            elastic_input['filter'] = elastic_input['filter'] + contains_filter
            elastic_input['must_not'] = must_not_filter
            elastic_input['columns'] = prefix_column

            # elastic_input['search_filter'] = search_filters
            # ####################### SEARCH FILTERS QUERIES APPENDED HERE 27.10.2020 ##################################
            for filter in search_filters:
                f = {}
                f['field'] = "pq."+filter["column_name"].upper()
                if filter["data_type"] == "number":
                    if filter.get("less_than", "") != "":
                        f['range'] = True
                        f['lte'] = filter["less_than"]
                    if filter.get("greater_than", "") != "":
                        f['range'] = True
                        f['gte'] = filter["greater_than"]
                    if filter.get("equalTo", "") != "":
                        f['range'] = True
                        f['gte'] = filter["equalTo"]
                        f['lte'] = filter["equalTo"]
                elif filter["data_type"] == "string":
                    if filter.get("search_text", "") != "":
                        elastic_input['text'] = filter["search_text"]
                        # continue
                elif filter["data_type"] == "date":
                    if filter.get("less_than", "") != "":
                        f['range'] = True
                        f['lte'] = filter["less_than"]
                    if filter.get("greater_than", "") != "":
                        f['range'] = True
                        f['gte'] = filter["greater_than"]
                    if filter.get("equalTo", "") != "":
                        f['range'] = True
                        f['gte'] = filter["equalTo"]
                        f['lte'] = filter["equalTo"]
                
                if filter.get("sort_order", "") != "":
                    if elastic_input.get("sort", False) is False:
                        elastic_input["sort"] = []
                    elastic_input["sort"].append({f['field']:filter["sort_order"]})
                
                elastic_input['filter'].append(f)
                logging.info("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&77 ADDDING SOME ELASTIC INPUTS HERE")
                


            # logging.info(f'Elasticsearch Input: {elastic_input}')
            files, total_files = elasticsearch_search(elastic_input)
            case_ids = set()

            files = [file for file in files if file]

            for document in files:                
                document['pq.CREATED_DATE'] = parser.parse(document['pq.CREATED_DATE'])
                document['pq.CREATED_DATE'] = (document['pq.CREATED_DATE']  + timedelta(hours=time_diff_hours, minutes=time_diff_minutes)).strftime(r'%B %d, %Y %I:%M %p')
                case_ids.add(document['pq.CASE_ID'])

                for idx, column in enumerate(columns):
                    document[column] = document.pop(prefix_column[idx])

            if queue_type != 'formqueue':
                if case_ids:
                    # Get more case IDs from case relation table
                    logging.info(f'Getting case relation data to get extraction data for shallow case')
                    case_ids_string = ', '.join([f"'{case_id}'" for case_id in case_ids])
                    query = f"select * from case_relation where queue_case_id in ({case_ids_string})"
                    case_relation_df = db.execute(query)
                    
                    case_relation_dict = defaultdict(list)
                    case_ids = list(case_ids)
                    for case_id in case_ids:
                        case_relation = case_relation_df.loc[case_relation_df['queue_case_id'] == case_id]
                        if case_relation.empty:
                            logging.info(f'No relation for `{case_id}`.')
                            continue

                        data_case_id = list(case_relation['data_case_id'])[0]

                        case_relation_dict[case_id].append(data_case_id)

                        if case_id == data_case_id:
                            logging.info(f'`{case_id}` is deep forked. Skipping')
                            continue

                        if data_case_id not in case_ids:
                            logging.info(f'Found related case. Adding `{data_case_id}`.')
                            case_ids.append(data_case_id)

                    select_columns_list = []
                    for _, row in extraction_columns_df.iterrows():
                        col_name = row['column_name']                   
                        table = row['source']

                        if table:
                            select_columns_list.append(f'{table}.{col_name}')

                    select_columns_list += ['o.id', 'o.case_id']

                    select_part = ', '.join(select_columns_list)

                    logging.info(f'Select part: {select_part}')

                    columns = select_columns_list
                    case_ids = list(case_ids)

                    elastic_input = {}
                    elastic_input['columns'] = columns
                    elastic_input['filter'] = [{'field': 'o.case_id', 'value': case_ids}]
                    elastic_input['source'] = 'process_queue'
                    elastic_input['tenant_id'] = tenant_id
                    elastic_input["size"] = len(case_ids)

                    query_result_list, _ = elasticsearch_search(elastic_input)
                    # files = data_filter_function2(files,search_filters)

                    logging.info(f'Files: {files}')
                    for document in files:
                        document.pop('file_paths', None)
                        try:
                            percentage_done = str(int((document['completed_processes']/document['total_processes'])*100))
                        except:
                            percentage_done = '0'
                        if int(percentage_done) > 100:
                            percentage_done = '100'
                        
                        logging.info('Creating status key')
                        try:
                            if document['status']:
                                document['status'] = {
                                    'percent_done': percentage_done,
                                    'current_status':document['status'],
                                    'case_lock':document['case_lock'],
                                    'failure_status':document['failure_status']
                                }
                            else:
                                document['status'] = None
                        except:
                            logging.exception('Failed to create the status key.')
                            pass

                        try:
                            for row in query_result_list:
                                row_case_id = row['o.case_id']
                                doc_case = document['case_id']

                                for col, val in row.items():
                                    case_relation = case_relation_dict[doc_case]
                                    if len(case_relation) > 0:
                                        data_case_id = case_relation[0]
                                    else:
                                        data_case_id = None
                                    
                                    column_wo_prefix = col.split('.')[1]
                                    if row_case_id == doc_case:
                                        document[column_wo_prefix] = val
                                        continue
                                    elif row_case_id == data_case_id:
                                        if column_wo_prefix != 'case_id':
                                            document[column_wo_prefix] = val
                        except:
                            logging.exception('something wrong in form queue')

                columns_to_change = columns_data.get('date_columns', [])

                # logging.info(f'Converting date format for fields: {columns_to_change}')

                for document in files:
                    for column in columns_to_change:
                        try:
                            document[column] = parser.parse(document[column])
                            if column == 'created_date':
                                document[column] = (document[column]).strftime(r'%B %d, %Y %I:%M %p')
                            else:
                                document[column] = (document[column]).strftime(r'%B %d, %Y')
                        except ValueError:
                            document[column] = ''
                        except:
                            pass

                columns = [col for col in columns if col not in util_columns]
                columns += extraction_columns_list
            else:
                pass

            if end_point > total_files:
                end_point = total_files

            pagination = {"start": start_point + 1, "end": end_point, "total": total_files}

            data = {
                'files': files,
                'pagination': pagination,
            }

            # data['files'] = data_filter_function2(data['files'],search_filters)

            response = {'flag': True, 'data': data}
            return jsonify(response)
        except Exception as e:
            logging.exception(f'{e}, Something went wrong while getting queues. Check trace.')
            response = {'flag': False, 'message':'System error! Please contact your system administrator.'}
            return jsonify(response)

@app.route('/get_display_fields/<case_id>', methods=['POST', 'GET'])
def get_display_fields(case_id=None):
    # ! MAKE THIS ROUTE AFTER THE PREVIOUS ROUTE IS STABLE
    try:
        data = request.json
        
        logging.info(f'Request data: {data}')
        queue_id = data.pop('queue_id', None)
        tenant_id = data.pop('tenant_id', '')

        attr = ZipkinAttrs(
            trace_id=generate_random_64bit_string(),
            span_id=generate_random_64bit_string(),
            parent_span_id=None,
            flags=None,
            is_sampled=False,
            tenant_id=tenant_id
        )

        with zipkin_span(
                service_name='queue_api',
                zipkin_attrs=attr,
                span_name='get_display_fields',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            if queue_id is None:
                message = f'Queue ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            if case_id is None:
                message = f'Case ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            db_config['tenant_id'] = tenant_id

            db = DB('queues', **db_config)
            # db = DB('queues')

            # Get queue name using queue ID
            queue_definition = db.get_all('queue_definition')

            # * BUTTONS
            logging.info('Getting button data...')

            logging.info(f'Fetching workflow for queue `{queue_id}`')
            # Get workflow definition for the selected queue ID
            workflow_definition = db.get_all('workflow_definition')
            queue_workflow = workflow_definition.loc[workflow_definition['queue_id'] == queue_id]

            logging.info(f'Fetching button IDs for queue `{queue_id}`')
            # Get button IDs for the queue ID
            button_ids = list(queue_workflow['button_id'])

            logging.info(f'Fetching button attributes for queue `{queue_id}`')
            # Get buttons' attributes from button definition
            button_definition = db.get_all('button_definition')
            buttons_df = button_definition.ix[button_ids]
            button_attributes = buttons_df.to_dict(orient='records')

            logging.info(f'Converting button IDs to button name in workflow')
            # Add which queue to move to in button attributes
            raw_move_to_ids = list(queue_workflow['move_to'])
            move_to_ids = [id if id is not None else -1 for id in raw_move_to_ids]
            move_to_df = queue_definition.ix[move_to_ids]
            move_to = list(move_to_df['unique_name'])
            for index, button in enumerate(button_attributes):
                if move_to[index] != -1:
                    button['move_to'] = move_to[index]

            logging.info(f'Fetching button functions and mappings for queue `{queue_id}`')
            # Get button functions
            button_functions_df = db.get_all('button_functions')
            button_function_mapping = db.get_all('button_function_mapping')
            button_id_function_mapping = button_function_mapping.loc[button_function_mapping['button_id'].isin(button_ids)]
            # TODO: Convert this loop into a function. Using it later again for tab_id
            for index, row in button_id_function_mapping.iterrows():
                button_id = row['button_id']
                button_name = button_definition.loc[button_id]['text']
                button_id_function_mapping.loc[index, 'button_id'] = button_name

            for button in button_attributes:
                button_name = button['text']
                button_function_id_df = button_id_function_mapping.loc[button_id_function_mapping['button_id'] == button_name]
                button_function_id = list(button_function_id_df['function_id'])
                button['functions'] = []
                # Add all functions
                for function_id in button_function_id:
                    function_id_df = button_functions_df.loc[function_id]
                    function = function_id_df.to_dict()
                    function['parameters'] = function['parameters'].split(',') # Send list of parameters instead of string
                    button['functions'].append(function)

            # * FIELDS
            logging.info(f'Getting field data...')

            logging.info(f'Fetching queue field maping for queue `{queue_id}`')
            # Get field IDs for the queue field mapping
            # try:
            #     query = f"SELECT id FROM field_definition WHERE FIND_IN_SET({queue_id},queue_field_mapping) > 0 ORDER BY field_order"
            #     field_ids = list(db.execute_(query).id)
            # except:
            query = f"""SELECT * FROM field_definition WHERE INSTR (queue_field_mapping, {queue_id}, 1, 1) > 0 and type != 'Table' ORDER BY field_order """
            field_ids = list(db.execute_(query).id)

            logging.info(f'Fetching field defintion for queue `{queue_id}`')
            # Get field definition corresponding the field IDs
            field_definition = db.get_all('field_definition')
            fields_df = field_definition.ix[field_ids]
            fields_df['unique_name'] = Series('', index=fields_df.index)

            logging.info(f'Fetching tab defintion for queue `{queue_id}`')
            # Get tab definition
            tab_definition = get_tab_definition(tenant_id)

            # Replace tab_id in fields with the actual tab names
            # Also create unique name for the buttons by combining display name
            # and tab name
            logging.info(f'Renaming tab ID to tab name')
            for index, row in fields_df.iterrows():
                logging.info(f' => {row}')
                tab_dict = json.loads(row['tab_id']).copy()      
                for k,v in tab_dict.items():
                    if queue_id in v:
                        tab_id = int(k)
                        break
                tab_name = tab_definition.loc[tab_id]['text']
                fields_df.loc[index, 'tab_id'] = tab_name

                formate_display_name = row['display_name'].lower().replace(' ', '_')
                unique_name = f'{formate_display_name}_{tab_name.lower()}'.replace(' ', '_')
                fields_df.loc[index, 'unique_name'] = unique_name

            field_attributes = fields_df.to_dict(orient='records')
            for field in field_attributes:
                try:
                    value = list(ast.literal_eval(field['editable']))
                except:
                    value = [int(field['editable'])]
                if int(queue_id) in value:
                    field['editable'] = 1
                else:
                    field['editable'] = 0
            tabs = list(fields_df.tab_id.unique())

            response_data = {
                'buttons': button_attributes,
                'field': field_attributes,
                'tabs': tabs
            }

            response = {'flag': True, 'data': response_data}
            logging.info(f'Response: {response}')
            return jsonify(response)
    except Exception as e:
        logging.exception('Something went wrong while getting display fields. Check trace.')        
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})

@cache.memoize(86400)
def get_ocr(case_id, tenant_id):
    try:
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        query = 'SELECT ocr_data FROM `ocr_info` WHERE `case_id`=%s'
        ocr_data = queue_db.execute(query, params=[case_id])
        ocr_data = list(ocr_data['ocr_data'])[0]
    except:
        ocr_data = '[[]]'
        logging.exception('Error in extracting ocr from db')

    return ocr_data

def get_ocr_uncached(case_id, tenant_id):
    try:
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        query = 'SELECT ocr_data FROM `ocr_info` WHERE `case_id`=%s'
        ocr_data = queue_db.execute(query, params=[case_id])
        ocr_data = list(ocr_data['ocr_data'])[0]
    except:
        ocr_data = '[[]]'
        logging.exception('Error in extracting ocr from db')

    return ocr_data

@cache.memoize(86400)
def check_if_forked(case_id, tenant_id):
    logging.info(f'Checking if case `{case_id}` is a shallow or deep copy')
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    query = 'SELECT * FROM `case_relation` WHERE `queue_case_id`=%s'
    case_relation = queue_db.execute(query, params=[case_id])
    data_case_id = ''
    if case_relation.empty:
        logging.info(f'`{case_id}` not forked. Using same case data.')
    else:
        data_case_id = list(case_relation['data_case_id'])[0]
        logging.info(f'Data linked to `{case_id}` is `{data_case_id}`.')
        case_id = data_case_id

    if not data_case_id:
        return check_if_forked_uncached(case_id, tenant_id)

    return data_case_id

def check_if_forked_uncached(case_id, tenant_id):
    logging.info(f'Checking if case `{case_id}` is a shallow or deep copy')
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    query = 'SELECT * FROM `case_relation` WHERE `queue_case_id`=%s'
    case_relation = queue_db.execute(query, params=[case_id])
    data_case_id = ''
    if case_relation.empty:
        logging.info(f'`{case_id}` not forked. Using same case data.')
    else:
        data_case_id = list(case_relation['data_case_id'])[0]
        logging.info(f'Data linked to `{case_id}` is `{data_case_id}`.')
        case_id = data_case_id

    return data_case_id

def DFSUtil(graph, curr, visited, path):
    
    visited[curr] = True
    
    walks = []
    if len(graph[curr]) == 0:
        return [path]
    for i in graph[curr]:
        walk = list(path)
        walk.append(i)
        paths = DFSUtil(graph, i, visited, walk)
        # print(paths)
        if paths:
            walks.extend(paths)
    return walks

def DFS(graph, curr):
    # Mark all the vertices as not visited
    visited = {}
    for k, v in graph.items():
        visited[k] = False
    # Call the recursive helper function
    # to print DFS traversal
    path = []
    path.append(curr)
    walks = DFSUtil(graph, curr, visited, path)
    return walks

def get_nodes(case_id, tenant_id, graph):
    """
    Author : Akshat Goyal

    Args:
        case_id:
        tenant_id:

    Returns:

    """
    if not graph:
        return []
    db_config['tenant_id'] = tenant_id
    db = DB('stats', **db_config)
    table_name = 'audit'

    query = f'select * from {table_name} where table_name="process_queue" and reference_value="{case_id}" and reference_column="case_id" order by id'
    df = db.execute(query)

    return_list = []
    nodes = list(df['changed_data'])
    response = []
    node_not_visited = set(graph.keys())
    for node in nodes:
        node = json.loads(node)
        if 'queue' in node:
            if node['queue']:
                return_list.append(node['queue'])
            if not node['queue']:
                continue
            if node['queue'] in node_not_visited:
                node_not_visited.remove(node['queue'])
            temp = {'label': node['queue'], 'type': 'passed'}
            response.append(temp)

    if response:
        response[-1]['type'] = 'current'

    node_not_visited = list(node_not_visited)
    for node in node_not_visited:
        temp = {'label': node, 'type': 'yettopass'}
        response.append(temp)

    return return_list #response



def get_graph(tenant_id):
    """
    Author : Akshat Goyal

    Args:
        tenant_id:

    Returns:

    """
    db_config['tenant_id'] = tenant_id
    db = DB('queues', **db_config)

    table_name = 'file_workflow'

    query = f'select * from {table_name}'

    link_df = db.execute(query)

    links = link_df[['source', 'target']].to_dict('records')

    graph = OrderedDict()
    for node in links:
        source = node['source']
        target = json.loads(node['target'])
        graph[source] = target

    return graph

def get_links(graph):
    """

    Args:
        graph:

    Returns:

    """
    if not graph:
        return []
    links = []
    for key, value in graph.items():
        for node in value:
            link = {}
            link['source'] = key
            link['target'] = node
            links.append(link)
    return links

def get_changed_data(case_id, tenant_id):
    try:
        db_config['tenant_id'] = tenant_id
        db = DB('stats', **db_config)
        table_name = 'AUDIT_'

        query = f"select * from `AUDIT_` where api_service='update_queue' and case_id = '{case_id}' order by id"
        df = db.execute(query)
        df['updated_date'] = pd.to_datetime(
            df['updated_date'] + timedelta(hours=time_diff_hours, minutes=time_diff_minutes))

        # sort the dataframe by 'updated_date'
        df = df.sort_values('updated_date')
        result = []
        current_case = ""
        current_queue = ""
        queue_start_time = None

        for _, row in df.iterrows():
            case = row['case_id']
            queue = json.loads(row['response_data'])
            queue = queue.get('updated_queue', '')
            last_modified_by = row['user']
            last_modified_time = row['updated_date']

            if case != current_case or queue != current_queue:
                if queue_start_time is not None:
                    age = (last_modified_time -
                           queue_start_time).total_seconds()
                    result.append({
                        'age': age,
                        'last_modified_by': last_modified_by.title().replace('_', ' '),
                        'last_modified_time': last_modified_time.strftime("%B %d, %Y %I:%M %p"),
                        'queue': queue.title()
                    })
                if queue_start_time is None:
                    age = (last_modified_time -
                           last_modified_time).total_seconds()
                    result.append({
                        'age': age,
                        'last_modified_by': last_modified_by.title().replace('_', ' '),
                        'last_modified_time': last_modified_time.strftime("%B %d, %Y %I:%M %p"),
                        'queue': queue.title()
                    })
                current_case = case
                current_queue = queue
                queue_start_time = last_modified_time
        result[0]['queue']='File Upload'

    except Exception as e:
        logging.exception(f"Something went wrong in get_audit {e}")

    return result  # response



@app.route('/get_fields_ocr', methods=['POST', 'GET'])
def get_fields_ocr():
    data = request.json

    logging.info(f'Request data: {data}')

    if "case_id" not in data:
        return jsonify({'flag': False, 'message': "not sending case id"})
    case_id = data.pop('case_id', None)
    tenant_id = data.pop('tenant_id', None)
    db_config["tenant_id"] = tenant_id

    queue_db = DB("queues", **db_config)
    # data_case_id = check_if_forked(case_id, tenant_id)

    # if data_case_id:
    #     case_id = data_case_id
    ocr_data_return = {}
    query = "select file_name from process_queue where case_id= %s and state IS NULL"
    query_data = queue_db.execute_(query, params=[case_id])
    query_file = list(query_data['file_name'])
    for file in query_file:
        ocr_data= get_ocr(case_id,tenant_id)

        if ocr_data == '[[]]':
            ocr_data = get_ocr_uncached(case_id,tenant_id)

        ocr_data_return[file] = ocr_data
    return jsonify({"flag": True, "ocr_data": ocr_data_return, "dpi_page": '300'})



@app.route('/check_first_time', methods=['POST', 'GET'])
def check_first_time():
    data = request.json
    tenant_id = data['tenant_id']
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    queue = data.get('queue', '')
    year = dt.date.today().year
    try:
        query = f"select count(*) as count from QUEUE_LIST where queue = '{queue}' and created_date like '{year}%%'"
        mail_sent = queue_db.execute_(query)["count"][0]
        if mail_sent > 0:
            mail_sent = True
        else:
            mail_sent = False

        return jsonify({"flag": True, "mail_sent" : mail_sent})
    except Exception as e:
        return jsonify({"flag": False, "message" : f"Error occured : {e}"})

@app.route('/check_if_document_uploaded', methods=['POST', 'GET'])
def check_if_document_uploaded():
    data = request.json
    tenant_id = data['tenant_id']
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    queue = data.get('queue', '')
    year = dt.date.today().year
    try:
        query = f"select count(*) as count from file_manager where queue = '{queue}' and year = {year}"
        uploaded = queue_db.execute_(query)["count"][0]
        if uploaded > 0:
            uploaded = True
        else:
            uploaded = False

        return jsonify({"flag": True, "uploaded" : uploaded})
    except Exception as e:
        return jsonify({"flag": False, "message" : f"Error occured : {e}"})


def get_case_ocr_status(tenant_id,case_id):

    try:
        db_config["tenant_id"]=tenant_id

        extraction_db=DB("extraction",**db_config)

        status_query=f"select status from ocr where case_id='{case_id}'"

        status_list = extraction_db.execute_(status_query)["status"].tolist()

        if len(status_list)>0:
            return True,status_list[0]

        else:
            return False,""


    except Exception as e:
        logging.info(f"########### Error in getting case ocr status")
        logging.exception(e)
        message=f"Error in getting case ocr status"
        return False,message

@app.route('/get_file_paths', methods=['POST', 'GET'])
def get_file_paths():
    data = request.json

    logging.info(f'Request data: {data}')
    case_id = data.pop('case_id', None)
    tenant_id = data.pop('tenant_id', None)
    queue_id = data.pop('queue_id', None)
    queue_id = int(queue_id)
    try:
        tab_id = data['tab_id']
    except:
        tab_id = ''

    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)


    try:
        
        queue_name = ''
        file_paths_ = []
        if queue_id == 18:
            queue_name = list(queue_db.execute_(f'select unique_name from queue_definition where id = {queue_id}')['unique_name'])[0]
            query = f"select `chemical` from process_queue where case_id = '{case_id}'"
            chemical_name = list(queue_db.execute_(query)['chemical'])[0]
            logging.info(f"chemical name is {chemical_name}")
            if tab_id == 'Cover Letter':
                chemical_name = chemical_name + '.pdf'
                file_paths_.append(chemical_name)
            
        elif queue_id == 20:
            file_name = 'po_gen_'+case_id+'.pdf'
            file_paths_.append(file_name)
        elif queue_id == 47:
            queue_name = list(queue_db.execute_(f'select unique_name from queue_definition where id = {queue_id}')['unique_name'])[0]
            query = f"select `chemical` from process_queue where case_id = '{case_id}'"
            chemical_name = list(queue_db.execute_(query)['chemical'])[0]
            logging.info(f"chemical name is {chemical_name}")
            if tab_id == 'Cover Letter':
                chemical_name = 'additional_' + chemical_name + '.pdf'
                file_paths_.append(chemical_name)

        logging.info(f"file paths are :{file_paths_}")

    except:
        traceback.print_exc()
        file_paths_ = []

    return jsonify({"flag": True, "data": file_paths_})



@app.route('/get_fields', methods=['POST', 'GET'])
@app.route('/get_fields/<case_id>', methods=['POST', 'GET'])
def get_fields(case_id=None):
    try:
        data = request.json

        logging.info(f'Request data: {data}')
        operator = data.pop('user', None)
        try:
            case_id = data['CASE_ID']
        except:
            case_id = data['case_id']    
        tenant_id = data.pop('tenant_id', None)
        original_case_id = case_id

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
                span_name='get_fields',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            if operator is None:
                message = f'Operator name not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            if case_id is None:
                message = f'Case ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})


            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)
            biz_db = DB('business_rules', **db_config)
            extraction_db = DB('extraction', **db_config)

            original_case_id = case_id
            renamed_higlight = {}
            renamed_fields_all={}
            case_creation_details={}
            file_manager={}

            

            logging.info(f'Getting all data from process queue for case `{case_id}`')
            query = 'SELECT process_queue.*, QUEUE_LIST.queue as q FROM `process_queue` inner join QUEUE_LIST on QUEUE_LIST.case_id = process_queue.case_id and process_queue.`case_id`=%s'
            case_files = queue_db.execute_(query, params=[case_id])
            queue_name = list(case_files['q'])[0]
            


            if case_files.empty:
                message = f'No case ID `{case_id}` found in process queue.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})
            else:
                file_name = list(case_files.file_name)[0]
                file_name = ''
                if list(case_files.q)[0] == 'Failed':
                    message = 'Just display the image'
                    return jsonify({'flag': True, 'message': message, 'corrupted': True, 'file_name':file_name})

            try:
                file_paths = json.loads(list(case_files.file_paths)[0])
            except:
                file_paths = []

            case_operator = list(case_files.operator)[0]
            pdf_type = list(case_files.document_type)[0]

            
            logging.info(f"$$$$$$ QUEUE NAME IS  skjjdk {queue_name}")
            _, queue_info, queue_id = get_queue_definition(tenant_id, queue_name)

            logging.info(f'Getting queue field mapping info for case `{case_id}`')

            date_columns=[]
            
            

            logging.info(f'Getting Extraction data')
            
            tx_master_db = DB('tx_master', **db_config)
            query_master = f"select * from master_upload_tables"
            master_upload_tables_df = tx_master_db.execute(query_master)
            renamed_fields = {}
            query = f"SELECT `party_id` from  `ocr` where `case_id` = '{case_id}'"
            party_id = extraction_db.execute_(query)['party_id'].to_list()[0]
            for each in master_upload_tables_df.index:
                total_table_name = master_upload_tables_df['table_name'][each]
                data_base = total_table_name.split('.')[0]
                table = total_table_name.split('.')[1]
                purpose = master_upload_tables_df['purpose'][each]
                # logging.info(f"table: {table}, db: {data_base}")
                
                db_conn = DB(data_base, **db_config)
                
                try:
                    query = f"select file_name,document_id from process_queue where case_id='{case_id}' and state IS NULL"
                    query_data = queue_db.execute_(query)
                    query_file=list(query_data['file_name'])
                    query_id=list(query_data['document_id'])
                    if table == 'ocr' or purpose != None:
                        if table == 'ocr':
                            for file,document_id in zip(query_file,query_id):
                                renamed_fields = {}
                                query = f"SELECT * FROM `{table}` WHERE case_id= '{case_id}' limit 1"
                                # temp_dict = db_conn.execute_(query)
                                try:
                                    temp_dict = db_conn.execute_(query).to_dict('records')
                                except:
                                    temp_dict = []
                                if len(temp_dict)>0:
                                    temp_dict = temp_dict[0]
                                else:
                                    temp_dict = {}
                                renamed_fields.update(temp_dict)
                                try:
                                    renamed_higlight[file] = json.loads(renamed_fields['highlight'])
                                except:
                                    renamed_higlight = {}
                                for k in date_columns:
                                    try:
                                        if 'date' in k or 'Date' in k:
                                            renamed_fields[k] = (renamed_fields[k]).strftime(
                                                r'%Y-%M-%D %I:%M')
                                        else:
                                            renamed_fields[k] = (
                                                renamed_fields[k]).strftime(r'%Y-%M-%D')
                                    except ValueError:
                                        renamed_fields[k] = ''
                                    except:
                                        logging.exception(f'Could not parse {k} value. `{k}`.')
                                        pass

                                # logging.info(f"CUSTOMER_NAME got is {CUSTOMER_NAME}")

                                temaplate_db=DB('template_db',**db_config)
                                unsubcribed_fields=[]
                                try:
                                    if party_id:
                                        logging.info(f"the partyid::::{party_id}")
                                        # CUSTOMER_NAME = CUSTOMER_NAME.replace("LIMITED", "").strip()
                                        query = f"SELECT `UNSUBCRIBED_FIELDS` from  `trained_info` where `party_id` = '{party_id}'"
                                        unsubcribed_fields_db = temaplate_db.execute_(query)['UNSUBCRIBED_FIELDS'].to_list()
                                        if unsubcribed_fields_db:
                                            unsubcribed_fields_db=json.loads(unsubcribed_fields_db[0])
                                            unsubcribed_fields=unsubcribed_fields_db
                                except:
                                    pass
                            
                                renamed_fields['unsubscribed_fields'] = unsubcribed_fields

                                # logging.info(f"file is {file} and renamed_fields is {renamed_fields}")
                                # logging.info("###########renamed_fields['REAPPLY_DONE]",renamed_fields.keys())
                                
                                if 'REAPPLY_DONE' in renamed_fields.keys():
                                    reapply_done = renamed_fields['REAPPLY_DONE']
                                else:
                                    # logging.info("reapply_Done collumn not found in ocr table")
                                    reapply_done = 'False'
                                
                                

                                if reapply_done == 'True':
                                    try:
                                        edited_fields_value = renamed_fields.get('edited_fields',{})
                                        edited_fields = json.loads(edited_fields_value)
                                    except:
                                        edited_fields = {}
                                    # logging.info(f"Edited fields: {edited_fields}")

                                    query = f"select biz_rules_update from ocr where case_id='{case_id}'"
                                    query_data = extraction_db.execute_(query)
                                    # logging.info(f"biz rules update is : {query_data}")
                                    # logging.info(type(query_data))
                                    df=pd.DataFrame(query_data)
                                    # logging.info(f"df is : {df}")
                                    data_list = []
                                    if not df.empty and 'biz_rules_update' in df.columns:
                                          data = df['biz_rules_update'][0]
                                        #   logging.info(f"data is {data}")
                                          if isinstance(data, str) and data:  # Check if data is a non-empty string
                                                # logging.info(f"data is {data}")
                                                data_list = data.replace("\\n", "\n").split("\n")
                                                
                                          else:
                                                logging.info("No valid data found in 'biz_rules_update'.")
                                    else:
                                          logging.info("No data found or column 'biz_rules_update' does not exist.")
                                    master_data_fields = ["customer_name", "stock_doc_month", "stock_doc_year","customer_category","bank_share","due_date"]
                                    master_data_fields= set(master_data_fields + data_list)
                                    master_data_fields = sorted(master_data_fields)
                                    

                                    edited_fields = [item for item in edited_fields if item not in master_data_fields]
                                    

                                    renamed_fields['edited_fields'] = json.dumps(edited_fields)
                                    

                                    renamed_fields_all[file] = renamed_fields

                                    edited_fields = list(set(json.loads(renamed_fields_all[file]['edited_fields'])))
                                    renamed_fields_all[file]['EDITED_FIELDS'] = json.dumps(edited_fields)
                                    # logging.info(f"Edited fields in renamed_fields_all: {renamed_fields_all[file]['EDITED_FIELDS']}")
                                else:
                                    renamed_fields_all[file] = renamed_fields

                    
           
                    else:
                        query = f"SELECT * FROM `{table}` WHERE case_id = '{case_id}' LIMIT 1"
                        # temp_dict = db_conn.execute_(query)
                        try:
                            temp_dict = db_conn.execute_(query).to_dict('records')
                        except:
                            temp_dict = []
                        if len(temp_dict)>0:
                            temp_dict = temp_dict[0]
                        else:
                            temp_dict = {}
                        renamed_fields.update(temp_dict)
                        for fields in renamed_fields_all.values():
                            fields.update(renamed_fields)
                except Exception as e:
                    logging.info(f"### Exception occured ..{e}")       
            
        
        failure_msgs_data =''


        try:
            # 1. Get party_id (reuse earlier OCR row if available)
            logging.info(f"party_id is {party_id}")

            # 2. Init mappings
            category_mapping = {
                'stock': 'stocks',
                'debtors': 'debtors',
                'term lone': 'term',
                'advances': 'advances',
                'creditors': 'creditors',
                'working capital': 'working_capital_ocr'
            }
            category_mapping = {k.lower(): v for k, v in category_mapping.items()}

            component_dict = {
                v: {"component_fields": [], "dynamic_fields": [], "duplicated_fields": []}
                for v in category_mapping.values()
            }

            # 3. Fetch saved dynamic fields
            try:
                df = extraction_db.execute_("SELECT * FROM dynamic_fields WHERE party_id=%s", params=[party_id])
                if not df.empty:
                    row = df.iloc[0].to_dict()
                    selected_fields = {
                        'component_fields': ast.literal_eval(row.get('component_fields', '{}')),
                        'dynamic_fields': ast.literal_eval(row.get('dynamic_fields', '{}')),
                        'duplicated_fields': ast.literal_eval(row.get('duplicated_fields', '{}')),
                    }
                else:
                    selected_fields = {'component_fields': {}, 'dynamic_fields': {}, 'duplicated_fields': {}}
            except:
                selected_fields = {'component_fields': {}, 'dynamic_fields': {}, 'duplicated_fields': {}}

            # 4. Insert saved values
            for cat, data in component_dict.items():
                data['dynamic_fields'] = selected_fields['dynamic_fields'].get(cat, [])
                data['duplicated_fields'] = selected_fields['duplicated_fields'].get(cat, [])

            # 5. Load all component master items
            cm_df = extraction_db.execute_("SELECT component_name, component_category FROM component_master")

            for _, row in cm_df.iterrows():
                name = row['component_name']
                category = row['component_category']

                if not category or str(category).lower() == "null":
                    continue
                if not name or "dummy" in name.lower():
                    continue

                norm = category.lower().strip()
                mapped = category_mapping.get(norm, norm)

                if mapped not in component_dict:
                    component_dict[mapped] = {"component_fields": [], "dynamic_fields": [], "duplicated_fields": []}

                if name not in component_dict[mapped]["component_fields"]:
                    component_dict[mapped]["component_fields"].append(name)

            logging.info(f"component_dict is {component_dict}")

        except Exception as e:
            logging.info(f"### Exception occurred while getting component name {e}")
            component_dict = {}

            
        custom_rules = {}

        try:
            # ---------------- LOAD rule_text_input ----------------
            rule_q = f"""
                SELECT rule_text_input
                FROM OCR
                WHERE CASE_ID = '{case_id}'
            """
            rule_df = extraction_db.execute_(rule_q)

            if rule_df is None or rule_df.empty or not rule_df.iloc[0]["rule_text_input"]:
                custom_rules = {}
            else:
                rule_text_input = json.loads(rule_df.iloc[0]["rule_text_input"])
                custom_rules = {}

                # ---------------- BUILD custom_rules ----------------
                for tab, rules_by_id in rule_text_input.items():

                    if tab not in custom_rules:
                        custom_rules[tab] = {}

                    for rule_id, rule_block in rules_by_id.items():
                        for final_field, rule_text in rule_block.items():
                            #custom_rules[tab][final_field] = rule_text
                            if isinstance(rule_text, dict):
                                custom_rules[tab][final_field] = rule_text.get("previous", "")
                            else:
                                custom_rules[tab][final_field] = rule_text

        except Exception as e:
            logging.exception("Error generating custom_rules from rule_text_input")


        mandatory_fields = {}

        try:
            qry = """
                SELECT mandatory_fields
                FROM ocr
                WHERE case_id = %s
            """
            df = extraction_db.execute_(qry, params=[case_id])

            if df is not None and not df.empty:
                db_val = df.iloc[0].get("mandatory_fields")

                if db_val not in [None, "", "null", "NULL"]:
                    try:
                        mandatory_fields = json.loads(db_val)
                    except Exception:
                        logging.exception("Failed to parse mandatory_fields JSON")
                        mandatory_fields = {}
                else:
                    mandatory_fields = {}
            else:
                mandatory_fields = {}

        except Exception as e:
            logging.exception("Error fetching mandatory_fields from OCR")
            mandatory_fields = {}





        extra_field_mappings = {}

        try:
            qry = f"""
                SELECT extra_variables
                FROM ocr
                WHERE case_id='{case_id}'
            """
            res = extraction_db.execute_(qry).to_dict(orient='records')

            db_val = res[0].get("extra_variables") if res else None
            logging.info(f"the extr afileds are are::::{db_val}")
            if db_val not in [None, "", "null", "NULL"]:
                try:
                    extra_field_mappings = json.loads(db_val)
                except:
                    extra_field_mappings = {}

        except Exception as e:
            logging.info(f"Error reading extra variables: {e}")
            extra_field_mappings = {}
        logging.info("the query here for qunique names:::::")
        # Insert mapping inside data hierarchy
        for file in renamed_fields_all:
            renamed_fields_all[file]['extra_field_mappings'] = extra_field_mappings
        
        # Fetch table_div column
        ocr_columns_df = extraction_db.execute_(
            "SELECT table_div FROM required_fields_table"
        )

        # Convert to a clean lowercase unique set
        table_divs = set(
            ocr_columns_df['TABLE_DIV'].str.lower().str.strip().tolist()
        )

        logging.info("The valid tables are:", table_divs)

        initial_hirarchy = {
                div: {"new_add_fields": []}
                for div in table_divs
            }

        ui_hierarchy = {}


        logging.info("UI new_fields is empty → fetching from DB")
        qry = f"""
            SELECT new_add_fields 
            FROM trained_info 
            WHERE party_id = '{party_id}'
        """
        db_res = temaplate_db.execute_(qry).to_dict(orient="records")
        logging.info(f"the res is::::::{db_res}")
    
        try:
            new_add_fields_res = json.loads(db_res[0]["new_add_fields"])

            # Check if ANY field contains values
            has_new = any(
                len(section.get("new_add_fields", [])) > 0
                for section in new_add_fields_res.values()
            )

            if has_new:
                ui_hierarchy = new_add_fields_res
            else:
                ui_hierarchy = initial_hirarchy

        except Exception as e:
            ui_hierarchy = initial_hirarchy
            logging.info(f"####Error while converting json -> {e}")
        def clob_chunks_sql_function(json_str):
            if not json_str or json_str.strip() in ['null', 'None', '{}', '[]']:
                return "TO_CLOB(NULL)"

            escaped = json_str.replace("'", "''")
            chunk_size = 3000
            chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
        
        # Add into renamed_fields_all for each file
        for file in renamed_fields_all:
            renamed_fields_all[file]['new_fields'] = ui_hierarchy


        try:
            #print(f"enetredddddddd here,,,,,,,,,,,")
            qr2 = f"""
                    SELECT fields_extracted, fields_edited, not_extracted_fields,
                        extraction, manual, extraction_percentage
                    FROM field_accuracy
                    WHERE case_id='{case_id}'
                """
            df2 = queue_db.execute(qr2).to_dict(orient='records')
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
            except Exception as e:
                logging.error(f"The exception caugfht is............:{e}")
                pass
            for file, file_data in renamed_fields_all.items():

                unsubscribed_fields = file_data.get("unsubscribed_fields", [])
                #print(f"unsubscribed_fields are...................:{unsubscribed_fields}")
                if not unsubscribed_fields:
                    continue

                # Loop tabs present in custom_rules
                for tab, rules in custom_rules.items():

                    # Load existing tab JSON from OCR data
                    tab_json = file_data.get(tab)
                    if not tab_json:
                        continue

                    # tab_json may be string
                    if isinstance(tab_json, str):
                        try:
                            tab_json = json.loads(tab_json)
                        except:
                            continue

                    tab_view = tab_json.get("tab_view", {})

                    updated = False
                    #print(f"original_tab json are...................:{tab_json}")
                    for final_field in list(rules.keys()):
                        if final_field in tab_json:
                            if final_field in unsubscribed_fields:
                                if final_field in fields_edited:
                                    fields_edited.pop(final_field)
                                    manual = max(manual - 1, 0)
                                if final_field  in fields_extracted:
                                    fields_extracted.pop(final_field)
                                    # FIRST TIME extraction → increase count
                                    extraction = max(extraction - 1, 0)
                                if final_field  in not_extracted_fields or final_field not in not_extracted_fields:
                                    #not_extracted_fields.pop(final_field)
                                    not_extracted_fields[final_field]=""

                                # ---- EXTRACTION COUNT HANDLING ----
                                
                                # Make values empty
                                tab_json[final_field] = ""
                                #tab_view[final_field][value] = ""
                                if "rowData" in tab_view:
                                    for row in tab_view["rowData"]:
                                        if row.get("fieldName") == final_field:
                                            row["value"] = ""
                                            break
                                updated = True

                    if updated:
                        def clob_chunks_sql_function(json_str):
                            if not json_str or json_str.strip() in ['null', 'None', '{}', '[]']:
                                return "TO_CLOB(NULL)"

                            escaped = json_str.replace("'", "''")
                            chunk_size = 3000
                            chunks = [escaped[i:i + chunk_size] for i in range(0, len(escaped), chunk_size)]
                            return ' || '.join(f"TO_CLOB('{chunk}')" for chunk in chunks)
                        tab_json["tab_view"] = tab_view
                        mandatory_json = json.dumps(tab_json)
                        clob_expr = clob_chunks_sql_function(mandatory_json)
                        #print(f"mandatory_json is.....................:{mandatory_json}")
                        #print(f"clob_expr is.........................:{clob_expr}")
                        # 🔹 Update OCR table
                        qry = f"""
                            UPDATE ocr
                            SET {tab} = {clob_expr}
                            WHERE case_id = '{case_id}'
                        """
                        extraction_db.execute_(
                            qry
                        )
                        extraction_percentage = round(
                            (extraction / (extraction + manual)) * 100
                            if (extraction + manual) > 0 else 0, 2
                        )
                        manual_percentage = round(
                            (manual / (extraction + manual)) * 100
                            if (extraction + manual) > 0 else 0, 2
                        )
                        no_of_fields= extraction + manual

                        json_str = json.dumps(fields_extracted)
                        clob_expr = clob_chunks_sql_function(json_str)
                        json_str_1 = json.dumps(fields_edited)
                        clob_expr_1 = clob_chunks_sql_function(json_str_1)
                        json_str_2 = json.dumps(not_extracted_fields)
                        clob_expr_2 = clob_chunks_sql_function(json_str_2)
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
                            queue_db.execute(update_qry)
                        except Exception as e:
                            logging.exception(f"The Error got is:{e}")
                            pass
                        # Keep response in sync
                        file_data[tab] = mandatory_json

        except Exception:
            logging.exception("Error handling unsubscribed fields")


        # ---------------- REMOVE custom_rules for edited fields ----------------
        # try:
        #     for file, file_data in renamed_fields_all.items():
        #         edited_fields = file_data.get("edited_fields")

        #         if not edited_fields:
        #             continue

        #         # edited_fields may be JSON string
        #         if isinstance(edited_fields, str):
        #             edited_fields = json.loads(edited_fields)

        #         edited_fields_set = set(edited_fields)

        #         for tab in list(custom_rules.keys()):
        #             for final_field in list(custom_rules[tab].keys()):
        #                 if final_field in edited_fields_set:
        #                     custom_rules[tab].pop(final_field)

        #             # remove empty tabs
        #             if not custom_rules[tab]:
        #                 custom_rules.pop(tab)

        # except Exception:
        #     logging.exception("Error while removing edited_fields from custom_rules")




        # Save the component_dict into a global dictionary under the current file
        renamed_fields_all[file]['component_dict'] = component_dict
        response_data = {
            'flag': True,
            'data': renamed_fields_all,
            'highlight': renamed_higlight,
            'file_name': file_name,
            'time_spent': 0,
            'timer': list(queue_info.timer)[0],
            'template_name': list(case_files.template_name)[0],
            #'parent_case': data_case_id,
            'pdf_type': pdf_type,
            'failures' : failure_msgs_data,
            'original_case_id': original_case_id,
            'file_paths': file_paths,
            'queue': queue_name,
            'custom_rules':custom_rules,
            'mandatory_fields':mandatory_fields
        }

        logging.info(f'Locking case `{case_id}` by operator `{operator}`')
        update = {
            'operator': operator
        }
        where = {
            'case_id': case_id
        }
        queue_db.update('process_queue', update=update, where=where)

        return jsonify(response_data)
    except:
        logging.exception('Something went wrong getting fields data. Check trace.')
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})




@app.route('/dummy_api', methods=['POST', 'GET'])
def dummy_api(case_id=None):
    try:
        data = request.json

        logging.info(f'Request data: {data}')
        
        tenant_id = data.get('tenant_id', None)
        db_config['tenant_id'] = tenant_id

        query = f"SELECT * FROM APP_LAYOUT"
        queue_db = DB('queues', **db_config)


        case_files = queue_db.execute(query)
        return jsonify({'flag': True, "message":f'{case_files}'})

    except:
        logging.exception('Something went wrong refreshin fields. Check trace.')
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})



@app.route('/refresh_fields', methods=['POST', 'GET'])
def refresh_fields(case_id=None):
    try:
        data = request.json

        logging.info(f'Request data: {data}')
        case_id = data.pop('case_id')
        tenant_id = data.get('tenant_id', None)

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
                span_name='refresh_fields',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            if case_id is None:
                message = f'Case ID not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)

            extraction_db = DB('extraction', **db_config)

            tab_definition = queue_db.get_all('tab_definition')

            logging.info(f'Getting case info from process queue for case `{case_id}`...')
            query = 'SELECT * FROM `process_queue` WHERE `case_id`=%s'
            case_files = queue_db.execute(query, params=[case_id])
            case_files.queue = queue_db.execute('select * from QUEUE_LIST where case_id=%s', params = [case_id])["queue"]


            if case_files.empty:
                message = f'No case ID `{case_id}` found in process queue.'
                logging.warning(message)
                return jsonify({'flag': False, 'message': message})

            logging.info('Fetching queue info')
            queue_name = list(case_files['queue'])[0]
            queue_definition = queue_db.get_all('queue_definition')
            queue_id = queue_definition.index[queue_definition['unique_name'] == queue_name].tolist()[0]
            
            query = f'SELECT * FROM field_definition WHERE INSTR (queue_field_mapping, {queue_id}, 1, 1) > 0 ORDER BY field_order'
            field_ids = list(queue_db.execute_(query).id)
            field_definition = queue_db.get_all('field_definition')
            fields_df = field_definition.ix[field_ids]  

            logging.info(f'Renaming fields for case `{case_id}`')
            renamed_fields = {}
            for index, row in fields_df.iterrows():
                tab_dict = json.loads(row['tab_id']).copy()              
                for k,v in tab_dict.items():
                    if queue_id in v:
                        tab_id = int(k)
                        break
                tab_name = tab_definition.loc[tab_id]['text']
                table_name = tab_definition.loc[tab_id]['source']
                fields_df.loc[index, 'tab_id'] = tab_name

                display_name = row['display_name']
                unique_name = row['unique_name']

                query = f'SELECT * FROM `{table_name}` WHERE `case_id`=%s'
                case_tab_files = extraction_db.execute(query, params=[case_id])
                if case_tab_files.empty:
                    message = f' - No such case ID `{case_id}` in `{table_name}`.'
                    logging.error(message)
                    continue
                case_files_filtered = case_tab_files.loc[:, 'created_date':] 
                fields_df = case_files_filtered.drop(columns='created_date') 
                table_fields_ = fields_df.to_dict(orient='records')[0] 

                if display_name in table_fields_:
                    renamed_fields[unique_name] = table_fields_[display_name]

            response_data = {
                'flag': True,
                'updated_fields_dict': renamed_fields,
                'message': "Successfully applied all validations"
            }

            logging.info(f'Response: {response_data}')
            return jsonify(response_data)
    except:
        logging.exception('Something went wrong refreshin fields. Check trace.')
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})

def get_addon_table(table_pattern, addon_table):       
    if len(addon_table) < len(table_pattern): 
        addon_headers = []
        for i in addon_table:
            addon_headers.append(i['header'])
        for i in table_pattern:
            if i not in addon_headers:
                addon_table.append({'header': i, 'rowData': []})                

    return json.dumps(addon_table)

@app.route('/unlock_case', methods=['POST', 'GET'])
def unlock_case():
    try:
        data = request.json

        logging.info(f'Request data: {data}')
        operator = data.pop('username', None)
        tenant_id = data.pop('tenant_id', None)
        queue_id = data.pop('queue_id', None)
        case_id = data.pop('case_id', None)

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
                span_name='unlock_case',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            if operator is None:
                message = f'Username not provided.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            db_config['tenant_id'] = tenant_id
            queue_db = DB('queues', **db_config)

            update = {
                'operator': None,
                'last_updated_by': operator
            }
            where = {
                'operator': operator
            }
            queue_db.update('process_queue', update=update, where=where)

            
            route = f'get_files/{queue_id}'
            logging.info(f'Hitting URL: http://{os_queueapi_host}/{route}')
            json_data = {"tenant_id": tenant_id, "case_ids": [case_id], "user": operator, "start": 1, "end": 20, "from_unlock": 1}
            headers = {'Content-type': 'application/json; charset=utf-8', 'Accept': 'text/json'}
            response = requests.post(f'http://{os_queueapi_host}/{route}', json=json_data, headers=headers)
            response_json = response.json()
            try:
                file_data = response_json['data']['files'][0]
            except:
                traceback.print_exc()
                file_data = {}

            logging.info('Unlocked file(s).')
            return jsonify({'flag': True, 'message': 'Unlocked file.', 'file': file_data})
    except:
        logging.exception('Something went wrong unlocking case. Check trace.')
        return jsonify({'flag': False, 'message':'System error! Please contact your system administrator.'})

@app.route('/get_ocr_data', methods=['POST', 'GET'])
def get_ocr_data():
    try:
        data = request.json

        logging.info(f'Request data: {data}')
        case_id = data['case_id']
        tenant_id = data.pop('tenant_id', None)

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
                span_name='get_ocr_data',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            try:
                retrain = data['retrain']
            except:
                retrain = ''

            db_config['tenant_id'] = tenant_id
            db = DB('queues', **db_config)
    
            trained_db = DB('template_db', **db_config)
            extraction_db = DB('extraction', **db_config)
            table_db = DB('table_db', **db_config)

            logging.info('Getting mandatory fields')
            try:
                queue_id = list(db.execute("select * from queue_definition where unique_name = 'template_exceptions'").index.values)[0]
                query = f'SELECT * FROM field_definition WHERE FIND_IN_SET({queue_id},queue_field_mapping) > 0 ORDER BY field_order'
                ocr_fields_df = db.execute(query)
                mandatory_fields = list(ocr_fields_df['display_name'])
                # logging.info(f'OCR Fields DF: {ocr_fields_df}')
                
            except Exception as e:
                logging.warning(f'Error getting mandatory fields: {e}')
                mandatory_fields = []

            query = "Select * from process_queue where case_id = %s"

            case_files = db.execute(query,params=[case_id])
            if case_files.empty:
                message = f'No such case ID {case_id}.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            pdf_type = list(case_files.document_type)[0]

            query = 'SELECT * FROM `ocr_info` WHERE `case_id`=%s'
            params = [case_id]
            ocr_info = db.execute(query, params=params)
            ocr_data = list(ocr_info.ocr_data)[0].replace('\\','\\\\')
            ocr_data = json.loads(ocr_data)
            ocr_data = [sort_ocr(data) for data in ocr_data]

            template_list = list(trained_db.get_all('trained_info').template_name)

            fields_list = list(ocr_fields_df['display_name'])
            logging.info(f'Fields List: {fields_list}')

            if retrain.lower() == 'yes':
                template_name = list(case_files['template_name'])[0]
                trained_info = trained_db.get_all('trained_info')
                trained_info = trained_info.loc[trained_info['template_name'] == template_name]
                field_data = json.loads(list(trained_info.field_data)[0])
                
                table_train_info = table_db.get_all('table_info')
                table_train_info = table_train_info.loc[table_train_info['template_name'] == template_name]
                try:
                    table_info = json.loads(list(table_train_info.table_data)[0])
                except:
                    table_info = {}
                extraction_ocr = extraction_db.get_all('ocr')
                extraction_ocr = extraction_ocr.loc[extraction_ocr['case_id'] == case_id]
                highlight = json.loads(list(extraction_ocr.highlight)[0])

                fields_info = get_fields_info(ocr_data,highlight,field_data)

                return jsonify({'flag': True,
                    'data': ocr_data,
                    'info': {
                        'fields': fields_info,
                        'table': table_info
                    },
                    'template_name': template_name,
                    'template_list': sorted(template_list),
                    'mandatory_fields': mandatory_fields,
                    'fields': fields_list,
                    'type': pdf_type})

            return jsonify({'flag': True, 'data': ocr_data, 'template_list': sorted(template_list), 'mandatory_fields': mandatory_fields,'fields': fields_list, 'type': 'blob'})
    except Exception as e:
        logging.exception('Something went wrong when getting ocr data. Check trace.')
        return jsonify({'flag':False, 'message':'System error! Please contact your system administrator.'})

def create_children(queue, queue_definition_record,list_):
    queue_name = queue['name']
    queue_uid = queue['unique_name']
    queue_children = list(queue_definition_record.loc[queue_definition_record['parent'] == queue_uid].name)
    logging.info(f'Queue Name: {queue_name}')
    logging.info(f'Queue UID: {queue_uid}')
    logging.info(f'Queue Children: {queue_children}')
    if queue_children:
        queue['children'] = []
        temp_dict = queue_definition_record.loc[queue_definition_record['parent'] == queue_uid].to_dict(orient='records')
        for index, definition in enumerate(temp_dict):
            if definition['id'] not in list_:
                continue
            children = {}
            children['name'] = definition['name']
            tokens = definition['name'].split()
            children['path'] = definition['unique_name'].replace(' ', '')
            children['pathId'] = definition['id']
            children['type'] = definition['type'] 
            children['queue_order'] = definition['queue_order']
            children['unique_name'] = definition['unique_name']
            children['default_screen'] = definition['default_screen']
            children['icon'] = definition['icon']
            children['active_icon'] = definition['active_icon']
            queue['children'].append(children)
    
    return queue

@cache.memoize(86400)
def get_search_filters(tenant_id=None):
    db_config['tenant_id'] = tenant_id
    group_db = DB('group_access', **db_config)
    filters = group_db.execute_("SELECT CONCAT(CONCAT(`organisation_attributes`.`source`,'.'),`organisation_attributes`.`attribute`) as field, `user_organisation_mapping`.`value`  FROM `user_organisation_mapping`, `organisation_attributes`  where `user_organisation_mapping`.`type` != 'user' and `user_organisation_mapping`.`organisation_attribute` = `organisation_attributes`.`id`")
    filters = filters.to_dict(orient='records')
    filters = [dict(y) for y in set(tuple(x.items()) for x in filters)]

    # group_db.engine.dispose()
    # queue_db.engine.dispose()

    return filters

@cache.memoize(86400)
def get_queues_cache(tenant_id=None, user_name= None,filters = {}):
    logging.info('INSIDE GET QUEUES CACHE   ')
    db_config['tenant_id'] = tenant_id

    group_db = DB('group_access', **db_config)
    queue_db = DB('queues', **db_config)

    # query = "SELECT id, username from active_directory"
    # user_list = dict(zip(group_db.execute_(query).id.tolist(),group_db.execute_(query).username.tolist()))

    # logging.info(f"USER LIST {user_list}")

    # query = "SELECT * from user_organisation_mapping where type = 'user'"
    # user_details = group_db.execute_(query).to_dict(orient='records')

    # logging.info(f"USER DETIALS {user_details}")

    # query = "SELECT * from organisation_attributes"
    # attributes_df = group_db.execute_(query)
    # attributes = group_db.execute_(query).to_dict('list')

    # query = "SELECT * from organisation_hierarchy"
    # hierarchy = group_db.execute_(query).set_index('h_group').to_dict()['h_order']
    
    # query = "SELECT id, group_definition from group_definition"
    # group_definition = group_db.execute_(query).set_index('id').group_definition.to_dict()

    # logging.info(f"############# Group Definition: {group_definition}")

    screen_children = {}
    try:
        query = "select queue, screen_id, screen_name, children from screen_properties"
        screen_properties = queue_db.execute_(query)
        screens = screen_properties.to_dict(orient='records')

        for record in screens:
            queue = record['queue']
            record.pop('queue')
            if queue in screen_children:
                screen_children[queue].append(record)
            else:
                screen_children[queue] = [record]
    except:
        pass

    active_directory_query = f"select username,role from active_directory"
    group_definition_query = f"SELECT id, group_definition from group_definition"
    active_directory_data = group_db.execute_(active_directory_query).to_dict(orient='records')
    group_definition_data = group_db.execute_(group_definition_query).to_dict(orient='records')


    # Step 1: Parse group definitions into a dictionary
    role_to_group = {}
    for group in group_definition_data:
        group_id = group['id']
        group_roles = json.loads(group['group_definition']).get('role', [])
        role_name = group_roles[0]
        role_to_group[role_name] = group_id
    # print(role_to_group)


    # Step 2: Assign group IDs to active_directory users
    for user in active_directory_data:
        role = user['role']
        group_ids = role_to_group.get(role, '')
        # print(group_ids)
        user['group_ids'] = group_ids

    # Step 3: Build the output dictionary
    classify_users = {"1": {}}

    for user in active_directory_data:
        username = user['username'].lower()
        classify_users["1"].setdefault(username, []).append(user['group_ids'])
    
    logging.info(f"#######classify_users is {classify_users}")
    # user_sequence = {}
    # for user_detail in user_details:
    #     try:
    #         user_sequence[user_detail['sequence_id']].append(user_detail)
    #     except:
    #         user_sequence[user_detail['sequence_id']] = [user_detail]
    
    # logging.info(f"3######## USER SEQUENCE {user_sequence}")
            
    # attribute_dropdown = group_db.get_all('attribute_dropdown_definition')   
    
    
    # attribute_mapping = dict(zip(attributes_df['id'], attributes_df['attribute']))
    # attribute_dropdown['attribute_id'] = attribute_dropdown['attribute_id'].map(attribute_mapping)
         
    # user_info = defaultdict(dict)
    # for k, v in user_sequence.items():
    #     for user_detail in v:
    #         name = user_list[user_detail['user_id']]
    #         index = user_detail['organisation_attribute']
    #         logging.info(f"attributes:{attributes['att_id']}")
    #         logging.info(f"index:{index}")
    #         ### Using att_id as reference as per new Ace builder COnfiguration
    #         index_of_att_id = attributes['att_id'].index(index)
    #         attribute_name = attributes['attribute'][index_of_att_id]
    #         attribute_value = user_detail['value']             
    #         try:
    #             if attribute_name in user_info[k][name]:
    #                 user_info[k][name][attribute_name] = ','.join(set(user_info[k][name][attribute_name].split(',') + [attribute_value]))
    #             else:
    #                 user_info[k][name][attribute_name] = attribute_value
    #         except:
    #             user_info[k][name] = {attribute_name: attribute_value}
                               
    #         for key, val in hierarchy.items():
    #             if attribute_name in val.split(','):
    #                 attribute_loop = val.split(attribute_name+',')[1].split(',') if len(val.split(attribute_name+',')) > 1 else val
    #                 for child_attribute in attribute_loop:
    #                     condition = (attribute_dropdown['parent_attribute_value'] == attribute_value) & (attribute_dropdown['attribute_id'] == child_attribute)
    #                     query_result = attribute_dropdown[condition]
    #                     if not query_result.empty:
    #                         child_attribute_value = list(query_result.value.unique())
    #                         user_info[k][name][child_attribute] =  ','.join(child_attribute_value)
    
    # logging.info(f"3######## USER INFO {user_info}")
                    
    
    # group_dict = defaultdict(dict)
    # for key_, val_ in user_info.items():
    #     for k,v in val_.items():
            
    #         group_list = []
    #         for key, val in v.items():
    #             subset = []
    #             val = val.split(',')
    #             for i in val:
    #                 for group, attribute in group_definition.items(): 
    #                     logging.info(f'Group: {group} attribute: {attribute}')
    #                     attribute = json.loads(attribute)
    #                     for x,y in attribute.items():
    #                         logging.info(f'key: {key}, x: {x}, i: {i}, y: {y}')
    #                         if type(y) == 'str':
    #                             if key.lower() == x.lower() and i.lower() == y.lower():
    #                                 subset.append(group)
    #                         else:
    #                             for y_ in y:
    #                                 if key.lower() == x.lower() and i.lower() == y_.lower():
    #                                     subset.append(group)
    #             if subset != []:
    #                 group_list.append(subset)
    #         group_dict[key_][k] = group_list
    
    # logging.info(f"3######## GROUO DICT {group_dict}")
    
            
    # classify_users = defaultdict(dict)
    # for key, val in group_dict.items():
            
    #     for user, value in val.items():
    #         if value and len(value) > 1:
    #             classify_users[key][user] = list(set.intersection(*map(set,value)))
    #         else:
    #             if len(value) > 0:
    #                 classify_users[key][user] = value[0]
    #             else:
    #                 pass
                
    users_mesh = get_users_mesh(tenant_id, classify_users, group_db)

    # logging.info(f"######################## CLASSIFY USERS {classify_users}")

    query = "SELECT * from queue_access"
    queue_group_id = group_db.execute_(query)

    
    
    user_queues = defaultdict(dict)
    for key, val in classify_users.items():
        for user, group_id in val.items(): 
            # logging.info(f"############   GROUP ID IS {group_id}")
            user_queues[key][user] = list(set(queue_group_id.loc[queue_group_id['group_id'].isin(group_id)].queue_id))        
    active_directory_user_query = f"SELECT ROLE FROM `active_directory` where username = '{user_name}'"
    active_directory_user_df = group_db.execute_(active_directory_user_query)
    user_role = active_directory_user_df.iloc[0]['ROLE']

    query=f"select display_role_rights,new_rights_assigned_status from role_rights where display_role_rights in ('UAM Reports','Operation Reports','View All Queues', 'Modify All Queues') and role_name='{user_role}'"
    rights_data=group_db.execute_(query).to_dict(orient= 'records')
    rights_status = {record["display_role_rights"]: record["new_rights_assigned_status"].lower() for record in rights_data}

    if any(rights_status.get(role) == "yes" for role in ['UAM Reports','Operation Reports']):
        user_queues['1'][user_name].append(2)
    if any(rights_status.get(role) == "yes" for role in ['View All Queues', 'Modify All Queues']):
        user_queues['1'][user_name].extend([1,9,8,10,11])
        

    # logging.info(f"############### USER QUEUES: {user_queues}")
    full_query = 'SELECT * FROM queue_definition ORDER BY queue_order ASC'
    
    queue_definition_full = queue_db.execute_(full_query)
   
    child_query = 'SELECT * FROM queue_definition where level_=2 ORDER BY queue_order ASC'
    child_queues = list(queue_db.execute_(child_query).unique_name)
    queue_definition = queue_db.execute('SELECT * FROM queue_definition ORDER BY queue_order ASC')
    
    
    return_queues = {} 
    for key_, val_ in user_queues.items():
        for user, value in val_.items(): 
            queues = []
            if value:
                
                
                queue_definition_dict = queue_definition.reset_index()

                queue_definition_dict = queue_definition.sort_values(by=['queue_order'])

                queue_definition_dict = queue_definition.to_dict(orient='records')
                
                for index, definition in enumerate(queue_definition_dict):
                    if definition['unique_name'] in child_queues:
                        continue
                    queue = {}
                    queue['name'] = definition['name']
                    queue['path'] = definition['unique_name']
                    queue['pathId'] = definition['id']
                    queue['type'] = definition['type']
                    queue['unique_name'] = definition['unique_name']
                    queue['default_screen'] = definition['default_screen']
                    queue['icon'] = definition['icon']
                    queue['queue_order'] = definition['queue_order']
                    queue['active_icon'] = definition['active_icon']
                    queue['sequence'] = key_
                    queue['file_count'] = definition['file_count']
                    queue['layout_view'] = definition['layout_view']
                    queue['icon_name'] = definition['icon_name']
                    queue = create_children(queue, queue_definition_full,value)
                    queues.append(queue)
                    
                    
            if user in return_queues:
                return_queues[user] += queues  
            else:
                return_queues[user] = queues 

    # logging.info(f"############ RETURN QUEUES: {return_queues}")

    for user,queues in return_queues.items():
        temp_list = []
        final_list = []
        for queue in queues:
            if queue['name'] not in temp_list:
                final_list.append(queue) 
            temp_list.append(queue['name'])
        return_queues[user] = final_list

   
    return return_queues, users_mesh, screen_children



@app.route('/get_queues', methods=['POST', 'GET'])
def get_queues():
    try:
        data = request.json
        tenant_id = data.get('tenant_id', '')
    except:
        tenant_id = None
    data = request.json
    logging.info(f'Request data: {data}')
    tenant_id = data.get('tenant_id', '')
    db_config['tenant_id'] = tenant_id
    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='queue_api',
            zipkin_attrs=attr,
            span_name='get_queues',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            data = request.json

            logging.info(f'Request data: {data}')
            if data is None or not data:
                message = f'Data recieved is none/empty.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})

            username = data.pop('username', None)
            tenant_id = data.pop('tenant_id', None)

            logging.info('Getting queues')

            
            logging.info(f'User name - {username}')
            user_queues, user_mesh, screen_children = get_queues_cache(tenant_id,username)

            if not username:
                return jsonify({'flag': False, 'message': 'logout'})
            
            # logging.info(f"USER QUEUES ARE {user_queues}")
            if username not in user_queues:
                message = f'No queues available for role `{username}`.'
                logging.error(message)
                return jsonify({'flag': False, 'message': message})
            
            if user_mesh:     
                if not user_mesh[username]:
                    message = f'No apps available for role `{username}`.'
                    logging.error(message)
            else:
                message = f'Something went wrong while fetching apps for the tenant_id `{tenant_id}`.'
                logging.error(message)
            logging.info('Successfully got queues.')
           

            # search filters are being sent empty
            # search_filters = get_search_filters(tenant_id) 
            search_filters=[] 
            # u_queues = []
            u_queues = [{k:v if v == v else "" for k,v in u.items()} for u in user_queues[username]]
            u_queues = [u for u in u_queues if u['unique_name']!=""]

            group_db = DB('group_access', **db_config)
            query = f"select role from active_directory where username='{username}'"
            role = group_db.execute_(query)['role'][0]

            # print(f'u_queues is: {u_queues}')
            query = f"""
                SELECT display_role_rights, new_rights_assigned_status 
                FROM role_rights 
                WHERE display_role_rights IN (
                    'View All Queues', 'Modify All Queues', 'Add User', 'Modify User', 
                    'Approve UAM Maker Activity', 'Reject UAM Maker Activity', 
                    'UAM Reports', 'Operation Reports'
                ) 
                AND role_name = '{role}'
            """
            rights_data = group_db.execute_(query).to_dict(orient='records')

            # Convert rights data into a dictionary for quick lookup
            rights_status = {record["display_role_rights"]: record["new_rights_assigned_status"].lower() for record in rights_data}
            if (rights_status.get("UAM Reports") == "yes" or rights_status.get("Operation Reports") == "yes") and rights_status.get("View All Queues") == "no" and rights_status.get("Modify All Queues") == "no":
                #if role == 'UAM Checker' or role == 'UAM Maker' or role == 'UAM Reviewer':
                l = []
                u = []
                report_queue = u_queues[-1]
                l.append(report_queue)
                u_queues = l
                search_filters = []
                screen_children = []
                try:
                    u.append(user_mesh[username][-1]) 
                    user_mesh[username] = u
                except:
                    user_mesh[username] = []

            data = {
                'queues': u_queues,
                'mesh': user_mesh[username],
                'search_filters': search_filters,
                'screen_children': screen_children
                }
            query=f"select role_rights,new_rights_assigned_status from role_rights where display_role_rights in ('Approve UAM Maker Activity','Reject UAM Maker Activity','Add User','Modify User','Add Roles','Modify Roles') and role_name='{role}'"
            uam_activity=group_db.execute_(query).to_dict(orient= 'records')
            # logging.info(f'uam_activity :{uam_activity}')
            if any(record['new_rights_assigned_status'] == 'Yes' for record in uam_activity) and role!='UAM Reviewer':
                query=f"select * from app_definition where unique_name='user_management'"
                values=group_db.execute_(query)
                data['mesh'].append({
                    'app_order':int(values['app_order'].iloc[0]),
                    'display_name':values['display_name'].iloc[0],
                    'enabled':int(values['enabled'].iloc[0]),
                    'external':int(values['external'].iloc[0]),
                    'icon':values['icon'].iloc[0],
                    'id':int(values['id'].iloc[0]),
                    'parameters':values['parameters'].iloc[0],
                    'redirect_url':values['redirect_url'].iloc[0],
                    'unique_name':values['unique_name'].iloc[0],
                })
            
            # logging.info(f"data is {data['mesh']}")


            return jsonify({'flag': True, 'data': data})
        except Exception:
            logging.exception('Something went wrong getting queues. Check trace.')
            return jsonify({'flag':False, 'message':'System error! Please contact your system administrator.'})


def fix_JSON(json_message=None):
    logging.info('Fixing JSON')
    logging.info(f'JOSN Message: {json_message}')
    result = None
    try:
        result = json.loads(json_message)
    except Exception as e:
        idx_to_replace = int(str(e).split(' ')[-1].replace(')',''))
        json_message = list(json_message)
        json_message[idx_to_replace] = ' '
        new_message = ''.join(json_message)
    
        return fix_JSON(json_message=new_message)

    logging.info(f'Response: {result}')
    return result


@app.route('/move_to_verify', methods=['POST', 'GET'])
def move_to_verify():
    try:
        data = request.json
        
        logging.info(f'Request data: {data}')
        case_id = data['case_id']
        queue = data['queue']
        tenant_id = data.pop('tenant_id', None)
        unique_queue = data.get('queue_name',None)

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
            extraction_db = DB('extraction', **db_config)
            stats_db = DB('stats', **db_config)

            query = "SELECT id, created_date FROM process_queue WHERE case_id = %s"
            created_date = str(list(db.execute(query, params = [case_id]).created_date)[0])
            batch_id = created_date[:4] + created_date[5:6].replace('0','') + created_date[6:10].replace('-','') + '0'

            if queue == 'failed':
                template_name = 'Failed Template'
            else:
                template_name = 'Dummy Template'


            if unique_queue:
                new_queue = unique_queue
                get_queue_name_query = "select id, name, unique_name from queue_definition where unique_name = %s"
                result = db.execute(get_queue_name_query, params=[new_queue])
                queue_name = list(result.name)[0]
            else:           
                get_queue_name_query = "select id, name, unique_name from queue_definition where id in (SELECT `workflow_definition`.`move_to` FROM `workflow_definition` WHERE `workflow_definition`.`queue_id` in (SELECT id from queue_definition where name = %s))"
                result = db.execute(get_queue_name_query, params=[queue])
                new_queue = list(result.unique_name)[0]
                queue_name = list(result.name)[0]
            update_fields = {'queue': new_queue, 'template_name': template_name}

            logging.info(f'Updating queue to `{new_queue}` for case `{case_id}`')
            
            db.update('QUEUE_LIST', update=update_fields, where={'case_id': case_id})

            audit_data = {
                    "type": "update", "last_modified_by": "Move to Verify", "table_name": "process_queue", "reference_column": "case_id",
                    "reference_value": case_id, "changed_data": json.dumps(update_fields)
                }
            stats_db.insert_dict(audit_data, 'audit')

            logging.info(f'Inserting to OCR')
            query = "INSERT into ocr (`case_id`, `highlight`) VALUES (%s,%s)"
            extraction_db.execute(query, params=[case_id, '{}'])

            response = {'flag': True, 'status_type': 'success', 'message': f"Successfully sent to {queue_name}"}
            logging.info(f'Response: {response}')
            return jsonify(response)
    except:
        logging.exception(f'Something went wrong while getting queues. Check trace.')
        return jsonify({'flag':False, 'status_type': 'failed', 'message':'System error! Please contact your system administrator.'})

@app.route('/get_search_result', methods=['POST', 'GET'])
def get_search_result():
    """
    if filter is non range
        filter = [
            {
                "field": field_name
                "value": value
            }
        ]

    else
        filter = [
            {
                "range": True
                "field": field_name
                "value": value
                "gte": greater_than_equal_to
                "lte": less'_than_equal_to
            }
        ]

    :return:
    """
    try:
        ui_data = request.json

        tenant_id = ui_data.get('Tenant_id', None)
        
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
                span_name='get_search_result',
                transport_handler=http_transport,
                sample_rate=0.5
        ) as zipkin_context:
            text = ui_data.get('text', None)
            filters = ui_data.get('filters', None)
            start_point = ui_data.get('start', 1) - 1
            end_point = ui_data.get('end', 10)

            elastic_input = {}
            elastic_input['text'] = text
            elastic_input['filter'] = filters
            
            elastic_input['start_point'] = start_point
            elastic_input['size'] = end_point-start_point
            elastic_input['source'] = 'process_queue'
            elastic_input['sort'] = [{'pq.CREATED_DATE': 'desc'}]
            elastic_input['tenant_id'] = tenant_id

            return_data = elasticsearch_search(elastic_input)

            return jsonify({'flag':True, 'data': return_data})
    except:
        message = 'something wrong wiht get_search_result'
        logging.exception(message)
        return jsonify({'flag':False, 'message':message})


@app.route('/replace_file', methods=['POST', 'GET'])
def replace_file():
    data = request.json
        
    logging.info(f'Request data: {data}')
    case_id = data['case_id']
    tenant_id = data.pop('tenant_id', '')
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
            span_name='replace_file',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        file_name = data['file_name']
        file_blob = data['file_blob']['blob']

        file_blob = file_blob.replace('data:application/vnd.openxmlformats-officedocument.wordprocessingml.document;base64,', '')

        file_path = f'/app/input/{tenant_id}/{case_id}/{file_name}'

        os.system(f'chattr -i /app/input/{tenant_id}')
        os.system(f"chmod -R 777 /app/input/{tenant_id}")

        with open(file_path, 'wb') as f:
            f.write(base64.b64decode(file_blob))

        return jsonify({'flag':True, 'message': 'Successfully replaced file'})


@app.route('/get_audit', methods=['POST', 'GET'])
def get_audit():
    data = request.json
    case_id = data['case_id']
    tenant_id = data.pop('tenant_id', '')
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
            span_name='replace_file',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            

            nodes = get_changed_data(case_id, tenant_id)

            response = {'flag': True, 'nodes':nodes}
            return jsonify(response)
        except:
            logging.exception('error in get audit log')
            response = {'flag': False}
            return jsonify(response)
        
def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

@app.route('/update_queue_counts', methods=['POST', 'GET'])
def update_queue_counts():
    # get queue file counts for the user
    try:
        data = request.json
        
        logging.info(f'Request data for queue counts: {data}')
        operator = data['user']
        tenant_id = data.get('tenant_id', None)
        queue_uid = data.get('queue_uid')

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

            queue_counts = { 
            'property':{},
            'values':{}
            }
            queue_counts_type = os.environ['QUEUE_COUNTS'].lower()
            
            if queue_counts_type=='unread':
                queue_counts['property'] = {'display' :True , 'count_type': 'unread'}
            elif queue_counts_type=='all':
                queue_counts['property'] = {'display' :True , 'count_type': 'all'}
            elif queue_counts_type=='no':
                queue_counts['property'] = {'display' :False , 'count_type': 'no'}
                return jsonify({'flag':True,'queue_counts':queue_counts})
            else:
                queue_counts['property'] = {'display' :False , 'count_type': 'unknown'}
                return jsonify({'flag':True,'queue_counts':queue_counts})

            elastic_input = {}
            elastic_input['source'] = 'process_queue'
            elastic_input['tenant_id'] = tenant_id

            user_case_filters = get_user_case_filters(operator, queue_uid, tenant_id)

            # get all queue unique ids for the user
            user_quname_list = get_user_qunames(tenant_id,operator)

            #get file counts for each queue the user belongs to
            for quname in user_quname_list:
                elastic_input['filter'] = []
                if queue_counts_type=='unread':
                    # add read flag filter if queue count type is unread
                    elastic_input['filter'] += [{'field': 'pq.READ_FLAG', 'value': 'false' }]

                elastic_input['filter'] += [{'field': 'ql.QUEUE', 'value': quname}] 
                elastic_input['filter'] += user_case_filters 
                # _ , total_files = elasticsearch_search(elastic_input)
                # if total_files == 10000:
                elastic_input['track_total_hits'] = True
                files_, total_files = elasticsearch_search(elastic_input)
                # print(f"total files are {total_files} and the queue_count is {queue_counts} and the files are {files_}")
                queue_counts['values'][quname] = total_files
            response_=[]  
            response_.append(queue_counts)
            response_data = {"queue_counts": json.dumps(queue_counts)}
    except:
            logging.exception(
                f'Something went wrong while getting queues. Check trace.')
            response_data = {'queue_counts': json.dumps(queue_counts)}
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
        # print(f"update_queue_counts Time consumed {time_consumed}, memory_consumed {memory_consumed} ")

    return {"flag":True,"data":response_data} 
        
def get_user_qunames(tenant_id,operator):
    user_quname_list = []

    queues_cache,_,_ = get_queues_cache(tenant_id,operator)
    # logging.info(f"{queues_cache} : q cache")
    if operator in queues_cache:
        for queue in queues_cache[operator]:
            if 'children' not in queue:
                user_quname_list.append(queue['unique_name'])
            else:
                for child_queue in queue['children']:
                    user_quname_list.append(child_queue['unique_name'])
    
    return user_quname_list

@app.route('/show_decision_tree', methods=['POST', 'GET'])
def show_decision_tree():
    data = request.json
    # print(data)
    try:
        case_id = data['case_id']
        # tenant_id = data['tenant_id']
        tenant_id = 'wns.acelive.ai'
        # print(f'This is tenant_id {tenant_id} and this is {case_id}')
    except:
        traceback.print_exc()
        logging.error(f"Did not receive proper inputs. Check request header")
    db_config['tenant_id'] = tenant_id

    db = DB('business_rules', **db_config)
    
    
    try:
        sequence_rule_data_df = db.execute("""SELECT * from `sequence_rule_data` WHERE `group` = 'assign' AND `display_next_if_success` IS NOT NULL and `display_next_if_failure` IS NOT NULL ORDER BY decession_rule_order """)
        # chained_rules = [[e['rule_id'], e['rule_string'], e['next_if_sucess'], e['next_if_failure'], e['stage']] for e in df.to_dict(orient='records') ]
        chained_rules = sequence_rule_data_df.to_dict(orient = 'records')
        # print(chained_rules)
    except:
        # print("Error fetching details from sequence_rule_data.")
        traceback.print_exc()   
        chained_rules = []
    try:
        rule_data_df = db.execute(f"SELECT * FROM `rule_data` WHERE `case_id` = '{case_id}'")
        # print(f'this is the rule data dataframe{rule_data_df}')
        if not rule_data_df.empty:
            trace_array = json.loads(list(rule_data_df['trace_data'])[0])
        else:
            message = f"No trace data available for the case id {case_id}"
            logging.error(f"No trace data available for the case id {case_id}")
            return jsonify({"flag": False, "message": message})
    except:
        print("Error in Fetching trace_array from decision_tree_trace.")
        traceback.print_exc()
        trace_array = []
    try:
        trace_dict = json.loads(list(rule_data_df['rule_params'])[0])
    except:
        print("Error in Fetching trace_dict from decision_tree_trace.")
        traceback.print_exc()
        trace_dict = {}
    # print("trace_dict", trace_dict)
    # Uncomment trace_dict line in the output dictionary once trace_dict generation
    # is enabled in chained_rules.py 
    if chained_rules:
        output = {
                    'flag' : 'True',
                    'data' : chained_rules,
                    'trace': trace_array,
                    'testData' : trace_dict,
                    'show_decision_tree' : True
                    }
        return jsonify(output)
    else:
        return jsonify({'flag' : 'False', 'message' : 'No chained rules in DB'})


@app.route('/random_true', methods=['POST', 'GET'])
def random_true():
    return {'flag': True}

@app.route('/get_session_status', methods=['POST', 'GET'])
def get_session_status():
    data = request.json

    logging.info(f'Request data: {data}')

    user=data.get('user', '')
    tenant_id = data.get('tenant_id', '')
    db_config["tenant_id"] = tenant_id

    group_access_db = DB("group_access", **db_config)
    session_id=''
    if user:
        query=f"select `status`,`session_id` from live_sessions where user_='{user}'"
        query_data = group_access_db.execute_(query)
        status=query_data['status'].to_list()[0]
        session_id=query_data['session_id'].to_list()[0]
        if status=='closed':
            return jsonify({"flag": False, "session_id": session_id,"status":status,"message":"Session is expired"})
        else:
            return jsonify({"flag": True, "session_id": session_id,"status":status,"message":"Session is alive"})
    else:
        return jsonify({"flag": False, "session_id": session_id,"status":'closed',"message":"Session is expired"})

@app.route('/get_session', methods=['POST', 'GET']) 
def get_session():
    data = request.json

    logging.info(f'Request data: {data}')

    #read session id from cookie
    try:
        cookies=request.cookies
        session_id = cookies.get('session_id','')
    except:
        session_id=data.get('session_id', '')
    tenant_id = data.get('tenant_id', '')
    db_config["tenant_id"] = tenant_id
    group_access_db = DB("group_access", **db_config)

    username = data.get('username','None')
    # logging.info(f"####username is {username}")
    
    user_status_adm_query = f"SELECT STATUS FROM active_directory_modifications WHERE username = '{username}' and STATUS NOT IN ('approved','rejected')"
    user_status_adm_query = group_access_db.execute_(user_status_adm_query)
    # logging.info(f"####user_status_adm_query is {user_status_adm_query}")
    if user_status_adm_query.empty:
        
        user_status_ad_query = f"SELECT STATUS FROM active_directory WHERE username = '{username}'"
        user_status_ad_query = group_access_db.execute_(user_status_ad_query)
        user_status_ad = list(user_status_ad_query['STATUS'])
        # logging.info(f"####user_status_adm is {user_status_ad}")
        user_status = user_status_ad[0]
    else:
        user_status_adm = list(user_status_adm_query['STATUS'])
        user_status = user_status_adm[0]
    # logging.info(f"user_status is {user_status}")

    
    if session_id:
        query=f"select `user_` from live_sessions where session_id='{session_id}'"
        query_data = group_access_db.execute_(query)
        user=query_data['user_'].to_list()
        if user and user_status not in ('delete','disable'):
            return jsonify({"flag": True, "message": "Session is alive"})
        else:
            return jsonify({"flag": False, "message": "Session has expired"})
    else:
        return jsonify({"flag": False,"message":"Session has expired or Empty Cookie"})
"""
author: Amara Sai Krishna Kumar
changes:- added 'tries' data structure code for search functionality of master_data from ui and sending the result suggestions.
"""
def get_variable_value(data, key):
    return next((variable["value"] for variable in data["variables"] if variable["key"] == key), None)
@app.route("/get_search_data", methods=['POST', 'GET'])
def get_search_data():
    try:
        data = request.json
        logging.info(f'Request data for get_search_data: {data}')
        tenant_id = data.get('tenant_id', None)
        
        search_val = data.get('search_val')
        party_name = get_variable_value(data, "party_name")
        table_name = get_variable_value(data, "table_name")
        
        db_config['tenant_id'] = tenant_id
        extraction_db = DB('extraction', **db_config)
        query = f"select {party_name} from {table_name}"
        df = extraction_db.execute_(query)
        # Assuming 'client_name' is the dynamic column name you want to extract
        column_to_extract = party_name # Replace 'client_name' with the actual variable or string containing the column name
        client_name_list = df[column_to_extract].tolist()

        trie = Trie()

        for word in client_name_list:
            trie.insert(word.upper())

        # Search for subsets in the column_data
        #search_prefix = search_val
        results = trie.search_prefix(search_val.upper())
        
        response_data = {
                    'flag': True,
                    'search_results': results
                }

        logging.info(f'Response: {response_data}')
        return jsonify(response_data)
    except Exception as e:
        logging.exception(f'Something went wrong exporting data : {e}')
        return {'flag': False, 'message': 'Unable to export data.'}





# @app.route("/get_dynamic_fields", methods=['POST', 'GET'])
# def get_dynamic_fields():
#     """
#     Fetches dynamic component names for a dropdown, filtered by tab name and excluding dummy values.
#     The result is a list of unique, sorted component names formatted for dropdown options.

#     Args:
#         data (dict): Input dictionary containing tenant_id, case_id, and variables (like tab_name)

#     Returns:
#         dict: Response with 'flag' (True/False) and 'options' (list of dropdown items)
#     """
#     data = request.json
#     # Extract necessary data from input
#     tenant_id = data.get('tenant_id')                      # Get tenant ID
#     case_id = data.get('case_id')                          # Get case ID
#     variables = data.get('variables', {})                  # Get variables dictionary
#     tab_name = variables.get('tab_name', '')               # Get tab name for filtering

#     # Configure DB with tenant ID and connect to 'extraction' database
#     db_config['tenant_id'] = tenant_id
#     extraction_db = DB('extraction', **db_config)

#     try:
#         # Step 1: Get party_id associated with given case_id
        
#         query = 'SELECT party_id FROM ocr WHERE case_id=%s'
#         party_id_list = extraction_db.execute_(query, params=[case_id])

#         # Debugging print: Check the structure of the returned data
#         print(f"Returned data (party_id_list): {party_id_list}")

#         # Check if the result is not empty and has the expected column
#         if party_id_list is not None and 'party_id' in party_id_list.columns:
#             # Extract party_id from the result
#             party_id = party_id_list['party_id'].iloc[0] if not party_id_list.empty else None
#         else:
#             party_id = None

#         # Debugging print: Check what the final party_id value is
#         print(f"party_id is {party_id}")

#         # Step 2: If party_id is found, fetch all components for the given tab
#         if party_id:
#             query = """SELECT cm.component_name
#            FROM component_master cm
#            JOIN age_margin_working_uat amw
#            ON TO_CHAR(cm.component_name) = TO_CHAR(amw.component_name)
#            WHERE amw.party_id = %s
#            AND TO_CHAR(cm.component_category) = %s"""

#             component_master = extraction_db.execute_(query, params=[party_id, tab_name])

#             # Debugging print: Check the structure of the returned data
#             print(f"Returned data (component_master): {component_master}")

#             # Ensure that the data is in the expected format (likely a DataFrame or Series)
#             if component_master is not None and 'component_name' in component_master.columns:
#                 # Extract component names into a list
#                 component_names = component_master['component_name'].tolist()  # tolist() instead of to_list()
#             else:
#                 component_names = None

#             # Debugging print: Check the component names
#             print(f"component_master is {component_names}")

#             # Step 3: Filter out any component with 'dummy' in name, ensure uniqueness and sort
#             component_names = sorted(set(
#                 name for name in component_names if 'dummy' not in name.lower()
#             ))
#             print(f"Filtered & sorted component_names: {component_names}")

#             # Step 4: Prepare the dropdown options
#             options = [{'display_name': name, 'value': name} for name in component_names]

#             # Prepare final response
#             response = {'flag': True, 'options': options}

#         else:
#             # If no valid component names, return empty options list
#             response = {'flag': True, 'options': []}

#     except Exception as e:
#         # Log any unexpected errors and return safe default
#         logging.info(f"### Exception occurred while getting component name: {e}")
#         response = {'flag': True, 'options': []}

#     return response
