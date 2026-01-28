#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 18 11:37:38 2019

@author: Amith
"""
import binascii
from operator import truediv
import os
import json
import requests
import traceback
import ast
import pandas as pd
import psutil
import re
import pandas as pd
import numpy as np
import datetime
import calendar
import random
import time

#from db_utils import DB
from flask import Flask, request, jsonify
from flask_cors import CORS
from app.stats_db import DB
from app.stats_db_06_06_2025 import Stats_db
from datetime import datetime, timedelta
from statistics import mean
from ace_logger import Logging

from py_zipkin.util import generate_random_64bit_string
from collections import defaultdict
from app import app
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, create_http_headers_for_new_span
from time import time as tt

logging = Logging(name='stats')

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


def make_chunks(chart_data, split):
    for i in range(0, len(chart_data), split):
        yield chart_data[i:i + split]


def sum_points(chunks):
    average_points = []
    for chunk in chunks:
        average_points.append(sum(chunk))
    return average_points


def create_data(all_dates, db_data):
    chart_data = []
    for i, day in enumerate(all_dates):
        found = None
        for ele in db_data:
            if day.date() == ele['date']:
                chart_data.append(ele['no_of_files'])
                found = True
        if not found:
            chart_data.append(0)
    return chart_data


def insert_into_audit(data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True


def get_user_groups(tenant_id):
    logging.info('INSIDE GET User groups   ')
    db_config['tenant_id'] = tenant_id

    group_db = DB('group_access', **db_config)
    queue_db = DB('queues', **db_config)

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
    print(role_to_group)


    # Step 2: Assign group IDs to active_directory users
    for user in active_directory_data:
        role = user['role']
        group_ids = role_to_group.get(role, '')
        print(group_ids)
        user['group_ids'] = group_ids

    # Step 3: Build the output dictionary
    classify_users = {"1": {}}

    for user in active_directory_data:
        username = user['username'].lower()
        classify_users["1"].setdefault(username, []).append(user['group_ids'])

    # query = "SELECT id, username from active_directory"
    # user_list = dict(zip(group_db.execute_(query).id.tolist(),
    #                  group_db.execute_(query).username.tolist()))

    # logging.info(f"USER LIST {user_list}")

    # query = "SELECT * from user_organisation_mapping where type = 'user'"
    # user_details = group_db.execute_(query).to_dict(orient='records')

    # logging.info(f"USER DETIALS {user_details}")

    # query = "SELECT * from organisation_attributes"
    # attributes_df = group_db.execute_(query)
    # attributes = group_db.execute_(query).to_dict('list')
    # logging.info(f"attributesattributes{attributes}")

    # query = "SELECT * from organisation_hierarchy"
    # hierarchy = group_db.execute_(query).set_index(
    #     'h_group').to_dict()['h_order']

    # query = "SELECT id,group_definition from group_definition"
    # group_definition = group_db.execute(query).group_definition.to_dict()

    # logging.info(f"############# Group Definition: {group_definition}")

    # screen_children = {}
    # try:
    #     query = "select queue, screen_id, screen_name, children from screen_properties"
    #     screen_properties = queue_db.execute_(query)
    #     screens = screen_properties.to_dict(orient='records')

    #     for record in screens:
    #         queue = record['queue']
    #         record.pop('queue')
    #         if queue in screen_children:
    #             screen_children[queue].append(record)
    #         else:
    #             screen_children[queue] = [record]
    # except:
    #     pass

    # user_sequence = {}
    # for user_detail in user_details:
    #     try:
    #         user_sequence[user_detail['sequence_id']].append(user_detail)
    #     except:
    #         user_sequence[user_detail['sequence_id']] = [user_detail]

    # logging.info(f"3######## USER SEQUENCE {user_sequence}")

    # # attribute_dropdown = group_db.get_all('attribute_dropdown_definition')
    # qry = f'select * from attribute_dropdown_definition'
    # # attribute_dropdown = group_db.get_all('attribute_dropdown_definition')
    # attribute_dropdown = group_db.execute_(qry)
    # logging.info(f"### ATTRIBUTE DROPDOWN IS {attribute_dropdown} AND TYPE IS {type(attribute_dropdown)} \n columns are  {attribute_dropdown.columns} ")

    # # attribute_dropdown['attribute_id'] = attribute_dropdown['attribute_id'].apply(
    # #     lambda x: attributes_df['attribute'])
    # attribute_mapping = dict(zip(attributes_df['id'], attributes_df['attribute']))
    # attribute_dropdown['attribute_id'] = attribute_dropdown['attribute_id'].map(attribute_mapping)


    # user_info = defaultdict(dict)
    # for k, v in user_sequence.items():
    #     for user_detail in v:
    #         logging.info(f"user_detailuser_detail{user_detail}")
    #         name = user_list[user_detail['user_id']]
    #         index = user_detail['organisation_attribute']
    #         logging.info(f"index{index}")
    #         # Using att_id as reference as per new Ace builder COnfiguration
    #         index_of_att_id = attributes['att_id'].index(index)
    #         logging.info(f"index_of_att_idindex_of_att_id{index_of_att_id}")
    #         attribute_name = attributes['attribute'][index_of_att_id]
    #         #logging.info(f"attribute_name{attribute_name}")
    #         attribute_value = user_detail['value']
    #         try:
    #             if attribute_name in user_info[k][name]:
    #                 user_info[k][name][attribute_name] = ','.join(
    #                     set(user_info[k][name][attribute_name].split(',') + [attribute_value]))
    #             else:
    #                 user_info[k][name][attribute_name] = attribute_value
    #         except:
    #             user_info[k][name] = {attribute_name: attribute_value}

    #         for key, val in hierarchy.items():
    #             if attribute_name in val.split(','):
    #                 attribute_loop = val.split(
    #                     attribute_name+',')[1].split(',') if len(val.split(attribute_name+',')) > 1 else val
    #                 for child_attribute in attribute_loop:
    #                     condition = (attribute_dropdown['parent_attribute_value'] == attribute_value) & (
    #                         attribute_dropdown['attribute_id'] == child_attribute)
    #                     query_result = attribute_dropdown[condition]
    #                     if not query_result.empty:
    #                         child_attribute_value = list(
    #                             query_result.value.unique())
    #                         user_info[k][name][child_attribute] = ','.join(
    #                             child_attribute_value)

    # logging.info(f"3######## USER INFO {user_info}")

    
    # group_dict = defaultdict(dict)
    # for key_, val_ in user_info.items():
    #     for k, v in val_.items():
            
    #         group_list = []
    #         for key, val in v.items():
    #             subset = []
    #             val = val.split(',')
    #             for i in val:
    #                 for group, attribute in group_definition.items():
    #                     attribute = json.loads(attribute)
    #                     for x, y in attribute.items():
    #                         logging.debug(
    #                             f'key: {key}, x: {x}, i: {i}, y: {y}')
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
    #             classify_users[key][user] = list(
    #                 set.intersection(*map(set, value)))
    #         else:
    #             if len(value) > 0:
    #                 classify_users[key][user] = value[0]
    #             else:
    #                 pass
    logging.info(f"3######## classify_users {classify_users}")

    return classify_users



@app.route('/get_segment_filter', methods=['POST', 'GET'])
def get_segment_filter():

    data = request.json
    try:
        tenant_id = data.pop('tenant_id', None)
        logging.info(f'tenant id {tenant_id}')
        try:
            user_name = data['user']
        except:
            message = "username is missing"
            return jsonify({"flag": False, "message": message})
        return_data={}
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        filters = [{"displayName": "Regions", "type": "dropdown", "options": ["North", "East", "West i", "West ii","South i","South ii","Mumbai","NA"]}]
        role_query = f"select role from active_directory where username='{user_name}'"
        df_case_id = group_access_db.execute_(role_query).to_dict(orient="records")[0]
        role=df_case_id['role'].lower()
        role = role.replace(" ", "")
        
        return_data_ = {"flag": True,"Filters" : filters}
        
        return return_data_
    except Exception as e:
        return_data= {"flag": False, "message": "something went wrong"}
        error_str = str(e)
        logging.info(f'error_str:{str(e)}') 
        try:
            # Convert string to actual dictionary
            error_dict = ast.literal_eval(error_str)
            
            # Append all key-value pairs from error_dict
            return_data.update(error_dict)
            logging.info(f'return_data1:{return_data}')
        except Exception:
            # If error_str is not a proper dict string, add it as a single message
            return_data['error'] = error_str
            logging.info(f'return_data2:{return_data}')
        return return_data


@app.route("/get_stats_cards", methods=['POST', 'GET'])
def get_stats_cards():
    data = request.json
    tenant_id = data.pop('tenant_id', None)
    logging.info(f'tenant id {tenant_id}')

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
        service_name='stats',
        span_name='get_stats_cards',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        # port=5010,
            sample_rate=0.5):

        try:
            from_date = data['fromDate']
            to_date = data['toDate']
        except Exception as e:
            logging.error("Unexpected request data", e)
            return jsonify({"flag": False, "message": "Please check the filters data in the request"})

        try:
            user_name = data['user']
            filters=data.get('filters',{})
        except:
            message = "username is missing"
            return jsonify({"flag": False, "message": message})

        stats_db_obj = Stats_db(tenant_id=f'{tenant_id}')
        db_config['tenant_id'] = tenant_id
        group_access_db = DB('group_access', **db_config)
        try:

            all_filters = data.get('all_filters', {})
            regionss = all_filters.get('Regions', "")
            StartDate = all_filters.get('StartDate', "")
            EndDate = all_filters.get('EndDate', "")

            

            active_stats_dict = stats_db_obj.get_stats_master()
            active_stats_df = pd.DataFrame(active_stats_dict)

            stat_access_query = 'SELECT * FROM `stats_access`'
            stat_access_df = group_access_db.execute_(stat_access_query)

            user_groups = get_user_groups(tenant_id)

            consolidated_group = defaultdict(dict)
            consolidated_group = user_groups.get("1",{})
            # for k, v in user_groups.items():
            #     for user, groups_ in v.items():
            #         consolidated_group[user] = consolidated_group[user] + \
            #             groups_ if consolidated_group[user] else groups_
            if filters and filters['Segments']:
                seg=filters['Segments'].lower()
                db = DB('group_access', **db_config)
                query = f"select `id`,`group_definition` from `group_definition`"
                query_data = db.execute_(query).to_dict(orient="records")
                if query_data:
                    for group_ in query_data:
                        group=(json.loads(group_['group_definition']))
                        
                            
                        group_list=[group_['id']]
                    
            
            else:
                logging.info(f"###consolidated_group#{consolidated_group}")
                group_list = consolidated_group[user_name]
            logging.info(
                f"############# user: {user_name}  and GROUP List: {group_list}")
                
            logging.info(f"stats activeddfd{stat_access_df}")
            group_lists = [34]
            stats_id_list = list(stat_access_df[stat_access_df['group_id'].isin(group_lists)].stats_id)
            

            logging.info(
                f"########### Stats list for user: {user_name} are List: {stats_id_list}")
            active_stats_dict = active_stats_df[active_stats_df['id'].isin(
                stats_id_list)].to_dict(orient='records')

            for card in active_stats_dict:
                if card.get("header") == "Case Summary":
                    card["queue"] = "['maker_queue','waiting_queue','accepted_queue','rejected_queue']"
                    card["legend"] = "['Processed','Pushed to Clims','Completed','Rejected']"
            return_data = {"flag": True, "data": active_stats_dict}
            
           
        except:
            return_data = {"flag": False,
                           "message": "Error in fetching stats from stats db"}
            error_str = str(e)
            logging.info(f'error_str:{str(e)}') 
            try:
                # Convert string to actual dictionary
                error_dict = ast.literal_eval(error_str)
                
                # Append all key-value pairs from error_dict
                return_data.update(error_dict)
                logging.info(f'return_data1:{return_data}')
            except Exception:
                # If error_str is not a proper dict string, add it as a single message
                return_data['error'] = error_str
                logging.info(f'return_data2:{return_data}')

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
        logging.info(
            f"## Stats Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        
    # insert audit
    audit_data = {"tenant_id": tenant_id, "user_": "", "case_id": "",
                    "api_service": "folder_monitor", "service_container": "folder_monitor_api",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(return_data), "trace_id": '',
                    "session_id": "","status":json.dumps(return_data['flag'])}
    # try:
    #     insert_into_audit(audit_data)
    # except:
    #     logging.info(f"issue in the query formation")
    return return_data



def stacked_bar_to_apex(stacked_bar_data, stacked=False):
    # Convert stacked_bar to apex stacked bar
    apex_data = {
         "chart": {
             "type": "bar",
             "height": 350,
             "colors": ["#eb1f2f"]  },
        "xaxis": {
             "categories": stacked_bar_data['axiscolumns'] },
        "plotOptions": {
        "bar": {
          "horizontal": False,
          "barWidth": "20%"
        }
      },
      "dataLabels": {
        "enabled": "false"
      },
    }


    series = []

    if stacked:
        names = list(stacked_bar_data['stackedcolumns'].keys())
        values = list(stacked_bar_data['stackedcolumns'].values())
        empty = [0] * len(stacked_bar_data['stackedcolumns'])

        logging.debug(f'Empty set: {empty}')
        logging.debug(f'Descriptions: {names}')
        logging.debug(f'Values: {values}')

        for idx in range(len(stacked_bar_data['stackedcolumns'])):
            data = empty.copy()
            l=[]
            data[idx] = values[idx]
            logging.debug(f'Data: {data}')
            series.append({'name': names[idx], 'data': data})
    else:
        l=[]
        logging.info(f"______in else____")
        for name, series_point in stacked_bar_data['stackedcolumns'].items():
            l.append(series_point)
        series.append({'name':'cases','data':l})

    apex_data['series'] = series

    return apex_data



def stacked_bar_to_apex_false(stacked_bar_data):
    """Convert stacked_bar to apex bar"""
    logging.info(f'******************{stacked_bar_data}')
    apex_data = {}
    #default_data = {"chart": {"type": "bar", "stacked": True}, "xaxis": {"type": "category", "axisBorder": {"show": True}, "categories": []},"yaxis": {"type": "category", "axisBorder": {"show": True}, "categories": []}}
    default_data = {"chart": {"type": "bar", "height": 350, "stacked": False, "toolbar": {"show": False,
                                                                                          "export": {
                                                                                              "csv": {
                                                                                                  "filename": stacked_bar_data['filename'],
                                                                                              },
                                                                                              "svg": {
                                                                                                  "filename": stacked_bar_data['filename'],
                                                                                              },
                                                                                              "png": {
                                                                                                  "filename": stacked_bar_data['filename'],
                                                                                              }
                                                                                          }}, "zoom": {"enabled": True}}, "plotOptions": {"bar": {"horizontal": False, "distributed": False}}, "xaxis": {"type": "category", "categories": []}, "yaxis": {"type": "category", "categories": []},  "legend": {"position": "right", "offsetY": 40}, "fill": {"opacity": 1}}
    apex_data.update(default_data)
    apex_data['xaxis']['categories'] = stacked_bar_data['axiscolumns']
    series = []
    length_of_series_data = len(stacked_bar_data['stackedcolumns'].keys())
    for name, series_point in (stacked_bar_data['stackedcolumns'].items()):
        logging.info(name, series_point)
        data = {}
        data['name'] = name
        data['data'] = series_point
        series.append(data)
    apex_data['series'] = series
    try:
        apex_data['desc'] = stacked_bar_data['desc']
    except:
        pass

    return apex_data


def stacked_bar_to_apex_data(stacked_bar_data, stacked=False):
    """Convert stacked_bar to apex stacked bar"""
    logging.info(f'******************{stacked_bar_data}')
    apex_data = {
        "chart": {
            "type": "bar",
            "height": 350,
            "stacked": True,
            'colors': ['#03fc6f'],
            "toolbar": {"show": False,
                        "export": {
                            "csv": {
                                "filename": stacked_bar_data['filename'],
                            },
                            "svg": {
                                "filename": stacked_bar_data['filename'],
                            },
                            "png": {
                                "filename": stacked_bar_data['filename'],
                            }
                        }},
            "zoom": {"enabled": True}
        },
        "xaxis": {
            "type": "category",
            "categories": stacked_bar_data['axiscolumns']
        },
        "yaxis": {"type": "category", "categories": []},

        "fill": {"opacity": 1}
    }

    series = []

    if stacked:
        names = list(stacked_bar_data['stackedcolumns'].keys())
        values = list(stacked_bar_data['stackedcolumns'].values())
        empty = [0] * len(stacked_bar_data['stackedcolumns'])

        logging.debug(f'Empty set: {empty}')
        logging.debug(f'Descriptions: {names}')
        logging.debug(f'Values: {values}')

        for idx in range(len(stacked_bar_data['stackedcolumns'])):
            data = empty.copy()
            data[idx] = values[idx]
            logging.debug(f'Data: {data}')
            series.append({'name': names[idx], 'data': data})
    else:
        for name, series_point in stacked_bar_data['stackedcolumns'].items():
            series.append({'name': name, 'data': series_point})

    apex_data['series'] = series

    return apex_data


def change_dt_fmt(text, in_fmt=r'%Y-%m-%d', out_fmt='%d %b %Y'):
    return datetime.strptime(text, in_fmt).strftime(out_fmt)



@app.route('/no_of_files', methods=['POST', 'GET'])
def no_of_files():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    logging.info(f"data is {data}")
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
            service_name='stats',
            span_name='no_of_files',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        try:
            start_date = data.get('fromDate', '')
            end_date = data.get('toDate', '') 
            database = data.get('database', '')
            table = data.get('table', '')
            queue = data.get('queue', '')
            header = data.get('header', '')
            username=data.get('user','')
            # Extract dates from data dictionary
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')

            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            filters=data.get('filters',{})
            card_name = data.get('card_name', '')
            end_datetime = datetime.strptime(end_date, r"%Y-%m-%d").date()
            end_datetime_=datetime.strftime(end_datetime, r"%Y-%m-%d")
            if end_date == start_date:
                end_date = (datetime
                            .strftime(end_datetime + timedelta(days=1), r'%Y-%m-%d'))
            database = ast.literal_eval(database)
            database = database[0]

            table = ast.literal_eval(table)
            table = table[0]

            region_map = {'West i': ['West i', 'West I'],'West ii': ['West ii', 'West II'],'South i': ['South i', 'South I'],'South ii': ['South ii', 'South II'],'North': ['North'],'East': ['East'],'Mumbai': ['Mumbai'],'NA': ['NA']}

            group_access_db = DB('group_access', **db_config)
            # try:
            #     filters_query=f"select all_filters from active_directory where username='{username}'"
            #     all_filters=group_access_db.execute_(filters_query)['all_filters']
            #     all_filters__=json.loads(all_filters)
            #     logging.info(f'all_filters__:{all_filters__}')
            #     region=data['filters']['Regions']
            # except:
            #     region=''


            region = ''
            try:
                try:
                    #print(f"emtereddd tryyy")
                    # Safe SQL (basic quote escape)
                    filters_query = f"SELECT all_filte FROM active_directory WHERE username = '{username}'"
                    
                    result_df = group_access_db.execute_(filters_query)

                    logging.info(f'result_df:{result_df}')
                except Exception as e:
                    logging.info("entered the second try except")
                    error_str = str(e)
                    logging.info(f'error_str:{str(e)}')   
                    result_df=None
                    

                #if not result_df.empty:
                #if result_df is not None and not result_df.empty:
                if result_df is not None and hasattr(result_df, "empty") and not result_df.empty:
                    #logging.info("Regionssssss")
                    all_filters_str = result_df.iloc[0]['all_filters']
                    all_filters = json.loads(all_filters_str)
                    logging.info(f'all_filters: {all_filters}')

                    # Safely access nested keys
                    region = all_filters.get('Regions', '')
                else:
                    #print("entered elseeee")
                    region = data['filters']['Regions']
                    logging.info(f"region isss:{region}")

            except Exception as e:
                logging.warning(f"Could not retrieve region: {str(e)}")
                region = ''
            
        
            #if len(region)==1:
            #    if ('West') in region:
            #        reg_column=('West i', 'West ii','West I', 'West II')
            #    elif ('South') in region:
            #        reg_column=('South i','South ii','South I','South II')
            #    else:
            #        reg_column="('"+region+"')"
            #else:
            #   reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai','NA')
                
            #if region == 'NA':
            #    #region_command= 'is Null'
            #    region_command= "IN ('NA')"
            #else:
            #    region_command=f'in {reg_column}'

            if isinstance(region, list) and len(region) > 1:  # If multiple regions are selected
                reg_column = tuple(region)  # Convert list to tuple
            elif region in region_map:  # If the region exists in the map
                reg_column = tuple(region_map[region])
            else:  # Default case when no region is provided
                reg_column = ('North', 'East', 'West i', 'West I', 'West ii', 'West II',
                       'South i', 'South I', 'South ii', 'South II', 'Mumbai', 'NA')

            if region == 'NA':
                region_command = "IN ('NA')"
            elif isinstance(reg_column, tuple):
                region_command = f"IN ({', '.join([f"'{r}'" for r in reg_column])})"
            else:
                region_command = f"= '{reg_column}'"




            logging.info(
                f"in no_of_files route database: {database} table: {table}")
            db_config["tenant_id"] = tenant_id
            database = database.split('_')[1]
            database = DB(f'{database}', **db_config)
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
            except ValueError as e:
                logging.error(f"Error: {e}")
                # Handle the error (e.g., provide default values or terminate the process)
            params = [start_date, end_date]
            logging.info(f'Queue is: {queue}')

            all_regions = tuple({r for group in region_map.values() for r in group})  # flatten into single tuple

            #print("Incoming Region:", region, "| Type:", type(region))

            # Normalize region to list
            if not region:
                region_list = list(all_regions)
            elif isinstance(region, str):
                region_list = [region]
            elif isinstance(region, list):
                region_list = region
            else:
                region_list = list(all_regions)

            # Expand region_list using region_map
            expanded_regions = []
            for r in region_list:
                expanded_regions.extend(region_map.get(r, [r]))

            # Remove duplicates & clean whitespace
            expanded_regions = list({r.strip() for r in expanded_regions})

            logging.info("Expanded region list:", expanded_regions)

            # -------- BUILD FILTER CONDITIONS --------
            if expanded_regions == ['NA']:  # Only NA selected
                region_filter = "(pq.REGION = 'NA' OR pq.REGION IS NULL)"

            elif 'NA' in expanded_regions:  # Multi-select including NA
                values = [f"'{r}'" for r in expanded_regions if r != 'NA']
                region_filter = f"((pq.REGION IN ({', '.join(values)}) OR pq.REGION = 'NA' OR pq.REGION IS NULL))"

            elif len(expanded_regions) == 1:  # Single region, not NA
                region_filter = f"pq.REGION = '{expanded_regions[0]}'"

            else:  # Multi-select without NA
                values = [f"'{r}'" for r in expanded_regions]
                region_filter = f"pq.REGION IN ({', '.join(values)})"

            logging.info("Final region_filter:", region_filter)
 




            if queue == None or queue == 'NULL':
                queue = ''
            if queue != '':
                queue = ast.literal_eval(queue)
                if len(queue) == 1:
                    queue = queue[0]
                    if header == "Cases Recieved":
                        queues_list = ('maker_queue','rejected_queue')
                        query = f"""SELECT COUNT(*) AS COUNT FROM QUEUE_LIST ql JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id WHERE ql.queue in {queues_list} AND {region_filter}
                        AND pq.CREATED_DATE BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')"""
                        logging.info(f"Cases Received query is {query}")
                    elif header == "Cases Processed":
                        queues_list = ('maker_queue','waiting_queue','completed_queue','accepted_queue','rejected_queue')
                        query = f"""SELECT COUNT(*) AS COUNT
                        FROM QUEUE_LIST ql
                        JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                        WHERE ql.queue in {queues_list}
                        AND {region_filter}
                        AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI')
                            AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')
                            AND pq.LAST_UPDATED > (pq.created_date + INTERVAL '5' HOUR + INTERVAL '35' MINUTE)"""
                    else:
                        query = f"""SELECT COUNT(*) AS COUNT
                        FROM QUEUE_LIST ql
                        JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                        WHERE ql.queue = '{queue}'
                        AND {region_filter}
                        AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI')
                            AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')"""


                    
                   
                    
                else:
                    queue = tuple(queue)
                    query = f"SELECT COUNT(*) as COUNT FROM `{table}` where queue in {queue} and last_updated LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') AND REGION {region_command}"
                logging.info(f'Query is: {queue}')
                selected_df = database.execute_(query)
            else:
                query = f"SELECT COUNT(CASE_ID) AS COUNT  FROM OCR WHERE CREATED_DATE BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') AND REGION {region_command}"
                selected_df = database.execute_(query)
            logging.info(f"************** {selected_df}")
            if selected_df.empty: 
                message = 'No data for the selected date range'
                logging.info(message)
                return {'flag': True, 'no_data_flag': True, 'no_data_message': message}
            count = 0 if selected_df.empty else selected_df.iloc[0]['COUNT']
            value = [int(count)]
            logging.debug(f'Count: {value}')
            response_data = {
                "value": value
            }
            st, en = change_dt_fmt(start_date), change_dt_fmt(end_datetime_)
            if st == en:
                response_data['name'] = f'{card_name}'
            else:
                response_data['name'] = f'{card_name} ({st} - {en})'

            return_data = {
                "flag": True, "message": "Successfully got count", "data": response_data}

        except Exception as e:
            error_str = str(e)
            logging.info(f'error_str:{str(e)}') 
            return_data = {'flag': False, 'message': 'Something went wrong'}
            try:
                # Convert string to actual dictionary
                error_dict = ast.literal_eval(error_str)
                
                # Append all key-value pairs from error_dict
                return_data.update(error_dict)
                logging.info(f'return_data1:{return_data}')
            except Exception:
                # If error_str is not a proper dict string, add it as a single message
                return_data['error'] = error_str
                logging.info(f'return_data2:{return_data}')
            
            logging.exception(f"failed {e}")
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
        logging.info(
            f"## Stats no_of_files Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return return_data


@app.route('/bot_status_chart_card', methods=['POST', 'GET'])
def bot_status_chart_card():
    """
     @built at: Am Bank disbursement Project Time
    @description: Based on bot status it will give count, the bot status may look like rejected/inprogress/completed 
    """

    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    try:
        data = request.json
        tenant_id = data.get('tenant_id', '')
    except Exception as e:
        logging.info(f"Exception is {e}")

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='stats',
            span_name='bot_status_chart_card',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        try:
            logging.info(f"request data is {data}")
            start_date = data['fromDate']
            end_date = data['toDate']
            database = data.get('database', '')
            table = data.get('table', '')
            column = data.get('column', '')
            legend_data = data.get('legend', '')
            card_name = data.get('header', '')

            database = ast.literal_eval(database)
            database = database[0]

            column = ast.literal_eval(column)
            column = column[0]

            table = ast.literal_eval(table)
            table = table[0]

            legend_data = ast.literal_eval(legend_data)
            legend_data = legend_data

            try:
                region=data['filters']['Regions']
            except:
                region=''

            if len(region)>1:
                reg_column="('"+region+"')"
            else:
                reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai')

            try:
                database = database.split('_')[1]
            except:
                database = database

            logging.info(
                f"#####database- {database} table- {table} column- {column} legend data- {legend_data}")
            dynamic_db = DB(f'{database}', **db_config)

            stacked_cols = []
            if start_date == end_date:
                logging.info(
                    f"start and end dates are same so end date incrementing for 1 day")
                logging.info(
                    f"end_date is {end_date} and type is {type(end_date)}")
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
                end_datetime_=end_date.strftime("%Y-%m-%d")
                end_date = end_date + timedelta(days=1)
                end_date = end_date.strftime("%Y-%m-%d")
                logging.info(
                    f"end date after increment is {end_date} and type is {type(end_date)}")
            else:
                end_datetime_=end_date


            for i in range(len(legend_data)):
                logging.info(
                    f"############legend data-{i} and  exceuting for {legend_data[i]}")
                query = f"SELECT COUNT(*) AS count FROM `{table}` WHERE {column} = '{legend_data[i]}' AND last_updated BETWEEN '{start_date}' and '{end_date}'"

                count = dynamic_db.execute_(query)
                stacked_cols.append(count.to_dict(
                    orient='records')[0]['count'])

            logging.info(f"stacked columns ---------------{stacked_cols}")

            chart_options = {
                'legend': {
                    'show': True,
                    'customLegendItems': legend_data
                }
            }
            response_data = {
                    "labels": legend_data,
                    "series": stacked_cols,
                    'flag': True,
                    "chart": {
                            "type": "donut",
                            "width": 450,
                            "colors": ["#2196F3","#FFEB3B","#673AB7","#FF5722"]
                            },
                            "legend": {
                                "position": 'bottom',
                                "show": True
                            }
            }


            st, en = change_dt_fmt(start_date), change_dt_fmt(end_datetime_)
            if st == en:
                response_data['name'] = f'{card_name}'
            else:
                response_data['name'] = f'{card_name} ({st} - {en})'
            logging.info(f"BOT status count retuned successfully")
        except Exception as e:
            response_data = {
                'flag': False,
                'message': 'Something went wrong'
            }
            logging.info(f"Something went wrong in bot status chart card {e}")
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
        logging.info(
            f"## Stats Bot_status_chart_card Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")

        return response_data


@app.route('/chart_card_bar', methods=['POST', 'GET'])
def chart_card_bar():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    try:
        data = request.json
        tenant_id = data.get('tenant_id', '')
    except Exception as e:
        logging.info(f"Exception is {e}")

    attr = ZipkinAttrs(
        trace_id=generate_random_64bit_string(),
        span_id=generate_random_64bit_string(),
        parent_span_id=None,
        flags=None,
        is_sampled=False,
        tenant_id=tenant_id
    )

    with zipkin_span(
            service_name='stats',
            span_name='chart_card_bar',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        try:
            logging.info(f"request data is {data}")
            start_date = data['fromDate']
            end_date = data['toDate']
            database = data.get('database', '')
            table = data.get('table', '')
            queue = data.get('queue', '')
            legend_data = data.get('legend', '')
            card_name = data.get('header', '')
            column_name = data.get('column', '')
            # Extract dates from data dictionary
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')
            # try:
            #     region=data['filters']['Regions']
            # except:
            #     region=''
            group_access_db = DB('group_access', **db_config)
            username=data.get('user','')
            
            # region = ''
            region_map = {'West i': ['West i', 'West I'],'West ii': ['West ii', 'West II'],'South i': ['South i', 'South I'],'South ii': ['South ii', 'South II'],'North': ['North'],'East': ['East'],'Mumbai': ['Mumbai'],'NA': ['NA']}
            # try:
            #     # Safe SQL (basic quote escape)
            #     filters_query = f"SELECT all_filters FROM active_directory WHERE username = '{username}'"
                
            #     result_df = group_access_db.execute_(filters_query)

            #     if not result_df.empty:
            #         all_filters_str = result_df.iloc[0]['all_filters']
            #         all_filters = json.loads(all_filters_str)
            #         logging.info(f'all_filters: {all_filters}')

            #         # Safely access nested keys
            #         region = all_filters.get('Regions', '')
            #     else:
            #         region = data['filters']['Regions']

            # except Exception as e:
            #     logging.warning(f"Could not retrieve region: {str(e)}")
            #     region = ''


            region = ''
            try:
                try:
                    #print(f"emtereddd tryyy")
                    # Safe SQL (basic quote escape)
                    filters_query = f"SELECT all_filte FROM active_directory WHERE username = '{username}'"
                    
                    result_df = group_access_db.execute_(filters_query)

                    logging.info(f'result_df:{result_df}')
                except Exception as e:
                    logging.exception("entered the second try except")
                    error_str = str(e)
                    logging.info(f'error_str:{str(e)}')   
                    result_df=None
                    

                #if not result_df.empty:
                #if result_df is not None and not result_df.empty:
                if result_df is not None and hasattr(result_df, "empty") and not result_df.empty:
                    #print("Regionssssss")
                    all_filters_str = result_df.iloc[0]['all_filters']
                    all_filters = json.loads(all_filters_str)
                    logging.info(f'all_filters: {all_filters}')

                    # Safely access nested keys
                    region = all_filters.get('Regions', '')
                else:
                    #print("entered elseeee")
                    region = data['filters']['Regions']
                    logging.info(f"region si:{region}")

            except Exception as e:
                logging.warning(f"Could not retrieve region: {str(e)}")
                region = ''

            logging.info(f"Region got is: {region}")

            #if len(region)>1:
            #    reg_column="('"+region+"')"
            #else:
            #    reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai')

            #if region == 'NA':
            #    region_command= 'is Null'
            #else:
            #    region_command=f'in {reg_column}'
            if isinstance(region, list) and len(region) > 1:  # If multiple regions are selected
                reg_column = tuple(region)  # Convert list to tuple
            elif region in region_map:  # If the region exists in the map
                reg_column = tuple(region_map[region])
            else:  # Default case when no region is provided
                reg_column = ('North', 'East', 'West i', 'West I', 'West ii', 'West II',
                       'South i', 'South I', 'South ii', 'South II', 'Mumbai', 'NA')

            if region == 'NA':
                #region_command = "IN ('NA')"
                region_command = "IN ('NA') OR pq.REGION IS NULL"
            elif isinstance(reg_column, tuple):
                region_command = f"IN ({', '.join([f"'{r}'" for r in reg_column])})"
            else:
                region_command = f"= '{reg_column}'"

            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            end_datetime = datetime.strptime(end_date, r"%Y-%m-%d").date()
            end_datetime_=datetime.strftime(end_datetime, r"%Y-%m-%d")
            if end_date == start_date:
                end_date = (datetime
                            .strftime(end_datetime + timedelta(days=1), r'%Y-%m-%d'))
            db_config["tenant_id"] = tenant_id
            database = ast.literal_eval(database)
            database = database[0]

            table = ast.literal_eval(table)
            table = table[0]

            queue = ast.literal_eval(queue)
            #print(f"queue is {queue}")

            column_name = ast.literal_eval(column_name)
            column_name = column_name[0]
            

            logging.info(f"column name {column_name}")

            try:
                database = database.split('_')[1]
            except:
                database = database
            logging.info(f"database is {database}")
            database = DB(f'{database}', **db_config)
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(hours=23, minutes=59, seconds=59)
            except ValueError as e:
                logging.error(f"Error: {e}")

            params = (start_date, end_date)
            try:
                logging.info(f'### in try')
                queue = ast.literal_eval(queue)
                logging.info(
                    f"type of queue is {type(queue)} and length is {len(queue)}")
            except:
                logging.info(f'### in exception')
                queue = queue
                logging.info(
                    f"type of queue is {type(queue)} and length is {len(queue)}")

            try:
                legend_data = ast.literal_eval(legend_data)
            except:
                legend_data = legend_data
            logging.info(
                f"database: {database} queue: {queue} legend data: {legend_data}")
            stacked_cols = {}
            PROCESSED_QUEUES = ("maker_queue","waiting_queue","accepted_queue","rejected_queue","completed_queue")
            all_regions = tuple({r for group in region_map.values() for r in group})  # flatten into single tuple

            logging.info("Incoming Region:", region, "| Type:", type(region))

            # Normalize region to list
            if not region:
                region_list = list(all_regions)
            elif isinstance(region, str):
                region_list = [region]
            elif isinstance(region, list):
                region_list = region
            else:
                region_list = list(all_regions)

            # Expand region_list using region_map
            expanded_regions = []
            for r in region_list:
                expanded_regions.extend(region_map.get(r, [r]))

            # Remove duplicates & clean whitespace
            expanded_regions = list({r.strip() for r in expanded_regions})

            logging.info("Expanded region list:", expanded_regions)

            # -------- BUILD FILTER CONDITIONS --------
            if expanded_regions == ['NA']:  # Only NA selected
                region_filter = "(pq.REGION = 'NA' OR pq.REGION IS NULL)"

            elif 'NA' in expanded_regions:  # Multi-select including NA
                values = [f"'{r}'" for r in expanded_regions if r != 'NA']
                region_filter = f"((pq.REGION IN ({', '.join(values)}) OR pq.REGION = 'NA' OR pq.REGION IS NULL))"

            elif len(expanded_regions) == 1:  # Single region, not NA
                region_filter = f"pq.REGION = '{expanded_regions[0]}'"

            else:  # Multi-select without NA
                values = [f"'{r}'" for r in expanded_regions]
                region_filter = f"pq.REGION IN ({', '.join(values)})"

            logging.info("Final region_filter:", region_filter)

            for i in range(len(queue)):
                if queue[i] == 'maker_queue':
                    query = f"""SELECT COUNT(*) AS COUNT
                        FROM QUEUE_LIST ql
                        JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                        WHERE ql.queue ='maker_queue'
                        AND {region_filter}
                        AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI')
                            AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') AND pq.LAST_UPDATED > (pq.created_date + INTERVAL '5' HOUR + INTERVAL '35' MINUTE) """
                elif queue[i] == 'waiting_queue' or queue[i]=='clims':
                    query = f"""SELECT COUNT(*) AS COUNT
                        FROM QUEUE_LIST ql
                        JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                        WHERE ql.queue in ('waiting_queue')
                        AND {region_filter}
                        AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI')
                            AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') AND pq.LAST_UPDATED > (pq.created_date + INTERVAL '5' HOUR + INTERVAL '35' MINUTE)"""
                elif queue[i] == 'rejected_queue':
                    query = f"""SELECT COUNT(*) AS COUNT
                        FROM QUEUE_LIST ql
                        JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                        WHERE ql.queue in ('rejected_queue')
                        AND {region_filter}
                        AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI')
                            AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') AND pq.LAST_UPDATED > (pq.created_date + INTERVAL '5' HOUR + INTERVAL '35' MINUTE)"""



                elif type(queue[i]) == str:
                    # query = f"SELECT COUNT(*) AS COUNT FROM `{table}` WHERE {column_name} = '{queue[i]}' AND LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')"
                    query = f"""SELECT COUNT(*) AS COUNT
                                FROM QUEUE_LIST ql
                                JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id
                                WHERE ql.queue = '{queue[i]}'
                                AND {region_filter}
                                AND pq.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')
                                """
                
                elif type(queue[i]) == list:
                    queues_list = tuple(queue[i])
                    
                    query = f"SELECT COUNT(*) AS COUNT FROM `{table}` WHERE {column_name} in {queues_list} AND LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')"
                count = database.execute_(query)
                count = count['COUNT'].to_list()
                logging.info(f"The count is:{count}")
                stacked_cols[queue[i]] = count[0]

            try:
                legend_data = ast.literal_eval(legend_data)
            except:
                legend_data = legend_data
            legend_data = legend_data
            logging.info(f"stacked cols are {stacked_cols}")
            logging.info(f"legend data is {legend_data}")

            chart_options = {
                'legend': {
                    'show': True,
                    'customLegendItems': legend_data
                }
            }
            data = {
                "axiscolumns": legend_data,
                "barname": "Cases",
                "axisvalues": [],
                "stackedcolumns": stacked_cols,
                "heading": 'Documents in QUeue',
                "subheading": '',
                "chart_type": "stacked_column",
                "filename": "Documents in QUeue" 
            }
            response_data = stacked_bar_to_apex(data)
            logging.info(
                f"response data for chart card bar is {response_data}")
            st, en = change_dt_fmt(start_date), change_dt_fmt(end_datetime_)
            if st == en:
                response_data['name'] = f'{card_name}'
            else:
                response_data['name'] = f'{card_name} ({st} - {en})'

            return response_data

        except Exception as e:
            response_data = {
                'flag': False,
                'message': 'Something went wrong'
            }
            logging.exception(f"failed exception is- {e}")




@app.route('/weekly_case_processing_performance', methods=['POST', 'GET'])
def weekly_case_processing_performance():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    

    data = request.json
    logging.info(f'data######{data}')
    tenant_id = data.get('tenant_id', '')
        
    
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
        data = request.json
        logging.info(f"data for weekly case-----{data}")
        tenant_id = data.get('tenant_id', None)
        db_config["tenant_id"] = tenant_id
        start_date = data['fromDate']
        end_date = data['toDate']
        queuee = data.get('queue', '')
        table = data.get('table', '')
        database = data.get('database', '')
        card_name = data.get('header', '')
        column = data.get('column', '')
        column = ast.literal_eval(column)
        column = column[0]
        start_date_str = data.get('fromDate', '')
        end_date_str = data.get('toDate', '')
        extraction_db = DB('extraction', **db_config)
        group_access_db = DB('group_access', **db_config)
        username=data.get('user','')
        # try:
        #     region=data['filters']['Regions']
        # except:
        #     region=''
        region = ''
        #try:
            # Safe SQL (basic quote escape)
        #    filters_query = f"SELECT all_filters FROM active_directory WHERE username = '{username}'"
            
        #    result_df = group_access_db.execute_(filters_query)

        #    if not result_df.empty:
        #        all_filters_str = result_df.iloc[0]['all_filters']
        #        all_filters = json.loads(all_filters_str)
        #        logging.info(f'all_filters: {all_filters}')

                # Safely access nested keys
        #        region = all_filters.get('Regions', '')
        #    else:
        #        region = data['filters']['Regions']

        #except Exception as e:
        #    logging.warning(f"Could not retrieve region: {str(e)}")
         #   region = ''

        #logging.info(f"Region got is: {region}")

        #if len(region)>1:
        #    reg_column="('"+region+"')"
        #else:
        #    reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai')

        #if region == 'NA':
        #    region_command= 'is Null'
        #else:
        #    region_command=f'in {reg_column}'
        # Convert strings to datetime objects
        region_map = {'West i': ['West i', 'West I'],'West ii': ['West ii', 'West II'],'South i': ['South i', 'South I'],'South ii': ['South ii', 'South II'],'North': ['North'],'East': ['East'],'Mumbai': ['Mumbai'],'NA': ['NA']}
        try:
            try:
                #print(f"emtereddd tryyy")
                # Safe SQL (basic quote escape)
                filters_query = f"SELECT all_filte FROM active_directory WHERE username = '{username}'"
                
                result_df = group_access_db.execute_(filters_query)

                logging.info(f'result_df:{result_df}')
            except Exception as e:
                #logging.info("entered the second try except")
                error_str = str(e)
                logging.info(f'error_str:{str(e)}')   
                result_df=None
                

            #if not result_df.empty:
            #if result_df is not None and not result_df.empty:
            if result_df is not None and hasattr(result_df, "empty") and not result_df.empty:
                #print("Regionssssss")
                all_filters_str = result_df.iloc[0]['all_filters']
                all_filters = json.loads(all_filters_str)
                logging.info(f'all_filters: {all_filters}')

                # Safely access nested keys
                region = all_filters.get('Regions', '')
            else:
                #print("entered elseeee")
                region = data['filters']['Regions']
                logging.info(f"region si:{region}")

        except Exception as e:
            logging.warning(f"Could not retrieve region: {str(e)}")
            region = ''

            logging.info(f"Region got is: {region}")
        start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

        end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

        
        start_date_formatted = start_dates.strftime('%Y-%b-%d')
        end_date_formatted = end_dates.strftime('%Y-%b-%d')
        end_datetime = datetime.strptime(end_date, r"%Y-%m-%d").date()
        end_datetime_=datetime.strftime(end_datetime, r"%Y-%m-%d")
        if isinstance(region, list) and len(region) > 1:  # If multiple regions are selected
            reg_column = tuple(region)  # Convert list to tuple
        elif region in region_map:  # If the region exists in the map
            reg_column = tuple(region_map[region])
        else:  # Default case when no region is provided
            reg_column = ('North', 'East', 'West i', 'West I', 'West ii', 'West II',
                    'South i', 'South I', 'South ii', 'South II', 'Mumbai', 'NA')

        if region == 'NA':
            #region_command = "IN ('NA')"
            region_command = "IN ('NA') OR p.REGION IS NULL"
        elif isinstance(reg_column, tuple):
            region_command = f"IN ({', '.join([f"'{r}'" for r in reg_column])})"
        else:
            region_command = f"= '{reg_column}'"
        database = ast.literal_eval(database)
        database = database[0]

        logging.info(
            f"in no_of_files route database: {database} table: {table}")
        db_config["tenant_id"] = tenant_id
        database = database.split('_')[1]
        database = DB(f'{database}', **db_config)
        all_regions = tuple({r for group in region_map.values() for r in group})  # flatten into single tuple

        logging.info("Incoming Region:", region, "| Type:", type(region))

        # Normalize region to list
        if not region:
            region_list = list(all_regions)
        elif isinstance(region, str):
            region_list = [region]
        elif isinstance(region, list):
            region_list = region
        else:
            region_list = list(all_regions)

        # Expand region_list using region_map
        expanded_regions = []
        for r in region_list:
            expanded_regions.extend(region_map.get(r, [r]))

        # Remove duplicates & clean whitespace
        expanded_regions = list({r.strip() for r in expanded_regions})

        logging.info("Expanded region list:", expanded_regions)

        # -------- BUILD FILTER CONDITIONS --------
        if expanded_regions == ['NA']:  # Only NA selected
            region_filter = "(p.REGION = 'NA' OR p.REGION IS NULL)"

        elif 'NA' in expanded_regions:  # Multi-select including NA
            values = [f"'{r}'" for r in expanded_regions if r != 'NA']
            region_filter = f"((p.REGION IN ({', '.join(values)}) OR p.REGION = 'NA' OR p.REGION IS NULL))"

        elif len(expanded_regions) == 1:  # Single region, not NA
            region_filter = f"p.REGION = '{expanded_regions[0]}'"

        else:  # Multi-select without NA
            values = [f"'{r}'" for r in expanded_regions]
            region_filter = f"p.REGION IN ({', '.join(values)})"

        #print("Final region_filter:", region_filter)
        queues_db = DB('queues', **db_config)   
        logging.info(f"data for start_date_formatted-----{start_date_formatted}")
        try:
            logging.info(f"checking")
            
            
            # query = f"SELECT TRUNC(LAST_UPDATED) AS processing_date, COUNT(*) AS total_cases_processed  FROM queue_list  WHERE LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') and queue='maker_queue' GROUP BY TRUNC(LAST_UPDATED) ORDER BY TRUNC(LAST_UPDATED)"
             # query = f""" WITH date_ranges AS (
            #         SELECT
            #             TRUNC(TO_DATE('{start_date}', 'YYYY-MM-DD')) AS start_date,
            #             TRUNC(TO_DATE('{end_date}', 'YYYY-MM-DD')) AS end_date
            #         FROM
            #             dual
            #     ),
            #     week_intervals AS (
            #         SELECT
            #             start_date + ((LEVEL - 1) * 7) AS week_start,
            #             LEAST(start_date + (LEVEL * 7) - 1, end_date) AS week_end
            #         FROM
            #             date_ranges
            #         CONNECT BY
            #             start_date + ((LEVEL - 1) * 7) <= end_date
            #     )
            #     SELECT
            #         TO_CHAR(week_start, 'DD-MON-YY') || ' - ' || TO_CHAR(week_end, 'DD-MON-YY') AS processing_date,

            #         COUNT(DISTINCT CASE WHEN  {region_filter} THEN q.case_id END) AS total_cases_processed
            #     FROM
            #         week_intervals
            #     LEFT JOIN
            #         queue_list q ON TRUNC(q.LAST_UPDATED) BETWEEN week_start AND week_end AND q.queue = 'maker_queue'
            #     LEFT JOIN
            #         process_queue p ON q.case_id = p.case_id
            #     GROUP BY
            #         week_start, week_end
            #     ORDER BY
            #         week_start"""
            query=f"""WITH date_ranges AS (
                    SELECT
                        TRUNC(TO_DATE('{start_date}', 'YYYY-MM-DD')) AS start_date,
                        TRUNC(TO_DATE('{end_date}',   'YYYY-MM-DD')) AS end_date
                    FROM dual
                ),
                week_intervals AS (
                    SELECT
                        start_date + ((LEVEL - 1) * 7) AS week_start,
                        LEAST(start_date + (LEVEL * 7) - 1, end_date) AS week_end
                    FROM date_ranges
                    CONNECT BY
                        start_date + ((LEVEL - 1) * 7) <= end_date
                )
                SELECT
                    TO_CHAR(week_start, 'DD-MON-YY') || ' - ' ||
                    TO_CHAR(week_end, 'DD-MON-YY') AS processing_date,

                    COUNT(DISTINCT q.case_id) AS total_cases_processed

                FROM week_intervals wi
                JOIN queue_list q
                    ON q.queue = 'maker_queue'
                JOIN process_queue p
                    ON p.case_id = q.case_id
                    AND p.LAST_UPDATED >= wi.week_start
                    AND p.LAST_UPDATED < wi.week_end + 1
                    AND p.LAST_UPDATED > (p.created_date + INTERVAL '5' HOUR + INTERVAL '35' MINUTE)
                    AND {region_filter}
                GROUP BY
                    week_start, week_end
                ORDER BY
                    week_start""" 
            df = queues_db.execute_(query)
            
            
            date_count_list = df['total_cases_processed'].to_list()
            
            date_list = df['processing_date'].to_list()
            
            stacked_cols = {}
            stacked_cols['Processed Cases'] = date_count_list
            data = {
                "axiscolumns": date_list,
                "barname": "Cases",
                "axisvalues": [],
                "stackedcolumns": stacked_cols,
                "heading": 'Weekly Case Processing Performance',
                "subheading": '',
                "chart_type": "stacked_column",
                "filename": "Weekly Case Processing Performance"
            }
            data = stacked_bar_to_apex_data(data)
            chart_options = {
                'plotoptions': {
                    'bar': {
                        'borderRadius': 5,
                        'distributed': False
                    }
                }
            }
            response_data={**data, **chart_options}
            st, en = change_dt_fmt(start_date), change_dt_fmt(end_datetime_)
            if st == en:
                response_data['name'] = f'{card_name}'
            else:
                response_data['name'] = f'{card_name} ({st} - {en})'


            
            response = {'flag': True}
        except Exception as e:
            logging.info(e)
            response = {'flag': False}
            return response
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
                    "api_service": "weekly_case_processing_performance", "service_container": "stats",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(response_data), "trace_id": trace_id,
                    "session_id": "","status":json.dumps(response['flag'])}
    # try:
    #     insert_into_audit(audit_data)
    # except:
    #     logging.info(f"issue in the query formation")
    return response_data
    




def replace_empty_with_none(value):
    return None if value == "" else value

def create_dataframe_from_json(json_data_list):
    columns_data = {}

    try:
        for idx, json_data in enumerate(json_data_list):
            if json_data is not None and isinstance(json_data, dict):  # Check if json_data is not None and is a dictionary
                for key, value in json_data.items():
                    if value is not None and value != 'null':
                        try:
                            data_dict = json.loads(value)
                            for nested_key, nested_value in data_dict.items():
                                # nested_key_cleaned = re.sub(r'\W', '_', nested_key)  # Replace non-alphanumeric characters with underscore
                                column_name = f"{nested_key}"
                                if column_name not in columns_data:
                                    columns_data[column_name] = []
                                numeric_value = extract_numeric_value(replace_empty_with_none(nested_value))
                                columns_data[column_name].append(numeric_value)
                        except json.JSONDecodeError as json_error:
                            print(f"Error decoding JSON at index {idx}: {json_error}")
                            # Handle the error if needed
                    else:
                        # Handle the case where value is None
                        for nested_key in columns_data.keys():
                            columns_data[nested_key].append(None)
            else:
                # Handle the case where json_data is None or not a dictionary
                print(f"Skipping invalid data at index {idx}: {json_data}")
                for nested_key in columns_data.keys():
                    columns_data[nested_key].append(None)

        # Fill missing keys with None
        if len(columns_data) !=0:
            max_len = max(len(v) for v in columns_data.values())
            for key, values in columns_data.items():
                values.extend([None] * (max_len - len(values)))

        df = pd.DataFrame(columns_data)
        return df
    except Exception as e:
        print(f"Error: {e}")
        return None
def extract_numeric_value(value):
    if isinstance(value, str):
        cleaned_value = re.sub(r'[^\d.]', '', value)  # Remove non-numeric characters except dot
        return pd.to_numeric(cleaned_value, errors='coerce')
    return None

def create_dataframe_from_jsondata(json_data_list):
    columns_data = {}
    try:
        for json_data in json_data_list:
            if json_data is None:
                continue  # Skip to the next iteration if json_data is None
            
            for key, value in json_data.items():
                if isinstance(value, str) and value:
                    try:
                        data_dict = json.loads(value)
                        if isinstance(data_dict, dict):
                            for nested_key, nested_value in data_dict.items():
                                column_name = f"{key}_{nested_key}"
                                if column_name not in columns_data:
                                    columns_data[column_name] = []
                                numeric_value = extract_numeric_value(nested_value)
                                columns_data[column_name].append(numeric_value if numeric_value is not None else 0)
                    except json.JSONDecodeError:
                        # Handle invalid JSON strings here if needed
                        pass
                else:
                    column_name = key
                    if column_name not in columns_data:
                        columns_data[column_name] = []
                    columns_data[column_name].append(0)

        # Find the maximum length among all columns
        max_length = max(len(column_data) for column_data in columns_data.values())

        # Fill missing values with None
        for key in columns_data.keys():
            while len(columns_data[key]) < max_length:
                columns_data[key].append(0)

        df = pd.DataFrame(columns_data)
        return df

    except Exception as e:
        print(f"Error: {e}")
        return None


    

@app.route('/no_of_clients', methods=['POST', 'GET'])
def no_of_clients():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    data = request.json
    tenant_id = data.get('tenant_id', None)

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
            data = request.json
            tenant_id = data.get('tenant_id', None)
            db_config["tenant_id"] = tenant_id
            start_date = data.get('fromDate', '')
            end_date = data.get('toDate', '')
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')
            table = data.get('table', '')
            table = ast.literal_eval(table)
            table = table[0]
            column = data.get('column', '')
            column = ast.literal_eval(column)
            column = column[0]
            group_access_db = DB('group_access', **db_config)
            username=data.get('user','')
            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')
            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            extraction_db = DB("extraction", **db_config)
            queues_db=DB("queues",**db_config)
            end_datetime = datetime.strptime(end_date, r"%Y-%m-%d").date()
            end_datetime_=datetime.strftime(end_datetime, r"%Y-%m-%d")
            #try:
            #    region=data['filters']['Regions']
            #except:
            #    region=''

            #if len(region)>1:
            #    reg_column="('"+region+"')"
            #else:
            #    reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai')
            #if region == 'NA':
            #    region_command= 'is Null'
            #else:
            #    region_command=f'in {reg_column}'
            region_map = {'West i': ['West i', 'West I'],'West ii': ['West ii', 'West II'],'South i': ['South i', 'South I'],'South ii': ['South ii', 'South II'],'North': ['North'],'East': ['East'],'Mumbai': ['Mumbai'],'NA': ['NA']}
            try:
                try:
                    #print(f"emtereddd tryyy")
                    # Safe SQL (basic quote escape)
                    filters_query = f"SELECT all_filte FROM active_directory WHERE username = '{username}'"

                    result_df = group_access_db.execute_(filters_query)

                    logging.info(f'result_df:{result_df}')
                except Exception as e:
                    #print("entered the second try except")
                    error_str = str(e)
                    logging.info(f'error_str:{str(e)}')
                    result_df=None


                #if not result_df.empty:
                #if result_df is not None and not result_df.empty:
                if result_df is not None and hasattr(result_df, "empty") and not result_df.empty:
                    #print("Regionssssss")
                    all_filters_str = result_df.iloc[0]['all_filters']
                    all_filters = json.loads(all_filters_str)
                    logging.info(f'all_filters: {all_filters}')

                    # Safely access nested keys
                    region = all_filters.get('Regions', '')
                else:
                    #print("entered elseeee")
                    region = data['filters']['Regions']
                    logging.info(f"region iss:{region}")

            except Exception as e:
                logging.warning(f"Could not retrieve region: {str(e)}")
                region = ''

                logging.info(f"Region got is: {region}")
            if end_date == start_date:
                end_date = (datetime
                        .strftime(end_datetime + timedelta(days=1), r'%Y-%m-%d'))
            if isinstance(region, list) and len(region) > 1:  # If multiple regions are selected
                reg_column = tuple(region)  # Convert list to tuple
            elif region in region_map:  # If the region exists in the map
                reg_column = tuple(region_map[region])
            else:  # Default case when no region is provided
                reg_column = ('North', 'East', 'West i', 'West I', 'West ii', 'West II',
                        'South i', 'South I', 'South ii', 'South II', 'Mumbai', 'NA')

            if region == 'NA':
                #region_command = "IN ('NA')"
                region_command = "IN ('NA') OR p.REGION IS NULL"
            elif isinstance(reg_column, tuple):
                region_command = f"IN ({', '.join([f"'{r}'" for r in reg_column])})"
            else:
                region_command = f"= '{reg_column}'"
            all_regions = tuple({r for group in region_map.values() for r in group})  # flatten into single tuple

            logging.info("Incoming Region:", region, "| Type:", type(region))

            # Normalize region to list
            if not region:
                region_list = list(all_regions)
            elif isinstance(region, str):
                region_list = [region]
            elif isinstance(region, list):
                region_list = region
            else:
                region_list = list(all_regions)

            # Expand region_list using region_map
            expanded_regions = []
            for r in region_list:
                expanded_regions.extend(region_map.get(r, [r]))

            # Remove duplicates & clean whitespace
            expanded_regions = list({r.strip() for r in expanded_regions})

            logging.info("Expanded region list:", expanded_regions)

            # -------- BUILD FILTER CONDITIONS --------
            if expanded_regions == ['NA']:  # Only NA selected
                region_filter = "(p.REGION = 'NA' OR p.REGION IS NULL)"

            elif 'NA' in expanded_regions:  # Multi-select including NA
                values = [f"'{r}'" for r in expanded_regions if r != 'NA']
                region_filter = f"((p.REGION IN ({', '.join(values)}) OR p.REGION = 'NA' OR p.REGION IS NULL))"

            elif len(expanded_regions) == 1:  # Single region, not NA
                region_filter = f"p.REGION = '{expanded_regions[0]}'"

            else:  # Multi-select without NA
                values = [f"'{r}'" for r in expanded_regions]
                region_filter = f"p.REGION IN ({', '.join(values)})"

            logging.info("Final region_filter:", region_filter)
            if end_date == start_date:
                end_date = (datetime
                        .strftime(end_datetime + timedelta(days=1), r'%Y-%m-%d'))

            #query=f"SELECT {column},COUNT({column}) AS count FROM {table} WHERE {column} IS NOT NULL AND REGION {region_command} AND LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI') GROUP BY {column}"
            #query=f"""SELECT p.{column}, COUNT(p.{column}) AS count FROM  process_queue p  WHERE p.{column} IS NOT NULL AND {region_filter} AND p.LAST_UPDATED BETWEEN TO_DATE('{start_date_formatted} 00:00','YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59','YYYY-MON-DD HH24:MI') GROUP BY p.{column}"""
            query=f"""SELECT region_normalized AS region, COUNT(*) AS count FROM (SELECT CASE WHEN UPPER(p.region) LIKE 'WEST I' THEN 'West I' WHEN UPPER(p.region) LIKE 'WEST II' THEN 'West II' WHEN UPPER(p.region) LIKE 'SOUTH I' THEN 'South I' WHEN UPPER(p.region) LIKE 'SOUTH II' THEN 'South II' WHEN UPPER(p.region) LIKE 'NORTH' THEN 'North' WHEN UPPER(p.region) LIKE 'EAST' THEN 'East' WHEN UPPER(p.region) LIKE 'MUMBAI' THEN 'Mumbai' ELSE NULL END AS region_normalized FROM process_queue p JOIN queue_list q ON q.case_id = p.case_id  WHERE p.region IS NOT NULL AND {region_filter} AND p.last_updated BETWEEN TO_DATE('{start_date_formatted} 00:00','YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59','YYYY-MON-DD HH24:MI') and q.queue IN ('maker_queue','waiting_queue','accepted_queue','rejected_queue')) WHERE region_normalized IS NOT NULL GROUP BY region_normalized ORDER BY region_normalized"""
            logging.info(f"columns is :{column}")
            logging.info(f"columns is :{column}")
            df = queues_db.execute_(query)
            logging.info(f"df{df}")
            segment_list = df['region'].tolist()
            segment_count_list = df['count'].tolist()
            response_data={"series":[{"name":"clients","data":segment_count_list}],"chart":{"type":"bar","height":"350"},"plotOptions":{"bar":{"horizontal":True}},"dataLabels":{"enabled":False},"xaxis":{"categories":segment_list}}
            logging.info(f"---------{response_data}")
            response = {'flag': True}
        except Exception as e:
            logging.info(f"---------{e}")
            response = {'flag': False}
            return jsonify(response)
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
                    "api_service": "no_of_clients", "service_container": "stats",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed),
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data),
                    "response_data": json.dumps(response_data), "trace_id": trace_id,
                    "session_id": "","status":json.dumps(response['flag'])}
    # try:
    #     insert_into_audit(audit_data)
    # except:
    #     logging.info(f"issue in the query formation")
    return response_data

    
@app.route('/due_date_status', methods=['POST', 'GET'])
def due_date_status():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    tenant_id = data.get('tenant_id', None)
        
    
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
            data = request.json
            logging.info(f"request data is {data}")
            tenant_id = data.get('tenant_id', None)
            start_date_str = data['fromDate']
            end_date_str = data['toDate']
            extraction_db = DB("extraction", **db_config)
            stats_db = DB("stats", **db_config)
            queues_db = DB('queues', **db_config)
            
            start_date = datetime.strptime(start_date_str, r"%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, r"%Y-%m-%d").date()
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')
            # try:
            #     region=data['filters']['Regions']
            # except:
            #     region=''
            group_access_db = DB('group_access', **db_config)
            username=data.get('user','')
            
            region = ''
            try:
                # Safe SQL (basic quote escape)
                filters_query = f"SELECT all_filters FROM active_directory WHERE username = '{username}'"
                
                result_df = group_access_db.execute_(filters_query)

                if not result_df.empty:
                    all_filters_str = result_df.iloc[0]['all_filters']
                    all_filters = json.loads(all_filters_str)
                    logging.info(f'all_filters: {all_filters}')

                    # Safely access nested keys
                    region = all_filters.get('Regions', '')
                else:
                    region = data['filters']['Regions']

            except Exception as e:
                logging.warning(f"Could not retrieve region: {str(e)}")
                region = ''

            logging.info(f"Region got is: {region}")

            if len(region)>1:
                reg_column="('"+region+"')"
            else:
                reg_column=('North', 'East', 'West i', 'West ii','South i','South ii','Mumbai')

            if region == 'NA':
                region_command= 'is Null'
            else:
                region_command=f'in {reg_column}'
            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            try:
            
                current_date = datetime.now().strftime('%d/%m/%Y')
            except Exception as e:
                logging.info(f'#format changed{e}')
                current_date = datetime.now()


            # Define dictionaries to store cases in different categories

            row_data=[]
            
            query= f"""SELECT case_id FROM QUEUE_LIST where last_updated BETWEEN TO_DATE('{start_date_formatted} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                        AND TO_DATE('{end_date_formatted} 23:59:59', 'YYYY-MM-DD HH24:MI:SS')"""
            case_df = queues_db.execute_(query)
            if len(case_df):
                case_ids = case_df['case_id'].tolist()
                chunk_size = 1000
                #case_id_tuple = tuple(case_df['case_id'])
                for start in range(0, len(case_ids), chunk_size):
                    case_id_chunk = case_ids[start:start+chunk_size]
                    case_id_tuple = tuple(case_id_chunk)
                    for i in ['Standard', 'Script', 'Jewellers', 'STL', 'Broker', 'NBFC']:   
                    
                        query_1 = f"""SELECT case_id, due_date
                                    FROM ocr 
                                    WHERE region {region_command} and case_id IN {case_id_tuple}
                                    AND last_updated BETWEEN TO_DATE('{start_date_formatted} 00:00:00', 'YYYY-MM-DD HH24:MI:SS') 
                                    AND TO_DATE('{end_date_formatted} 23:59:59', 'YYYY-MM-DD HH24:MI:SS') 
                                    AND SEG LIKE '%{i}%'"""
                        result = extraction_db.execute_(query_1)
                    
                    
                        # Execute the query
                        nearing_cases = []
                        missing_cases = []
                        within_cases = []
                        tobefetched_cases = []
                        if not result.empty:
                        
                        # Process the results
                            for index, row in result.iterrows():
                                case_id = row['case_id']
                                due_date = row['due_date']
                                queue_query=f"""SELECT queue FROM QUEUE_LIST where case_id like '{case_id}'"""
                                queue_df = queues_db.execute_(queue_query)
                                queue = queue_df['queue']

                                # queue = row['queue']
                                #print(case_id)
                                if due_date is not None:
                                    if due_date <= current_date:
                                        #print(due_date)
                                        if queue == 'Maker':
                                            if case_id not in missing_cases:
                                                missing_cases.append((case_id, due_date))
                                        elif queue == 'accepted_queue':
                                            if case_id not in [case[0] for case in within_cases]:
                                                within_cases.append((case_id, due_date))
                                    try:

                                        due_date = datetime.strptime(due_date, '%d/%m/%Y')
                                    except:
                                        try:
                                            due_date = datetime.strptime(due_date, '%b')
                                        except Exception as e:
                                            logging.info(f"##formatmissed {e}")
                                            due_date = datetime.now()


                                    if due_date is not None and due_date > datetime.strptime(current_date, '%d/%m/%Y'):
                        #                 days_until_due = (due_date - current_date).days
                        #                 if days_until_due <= 7:
                                            nearing_cases.append(case_id)
                                    elif queue == 'Maker' and case_id not in ['Completed', 'Rejected']:
                                        tobefetched_cases.append((case_id, due_date))
                        row_data.append({'Category':i,'Missed':len(missing_cases),'Nearing':len(nearing_cases),'Within_Cases':len(within_cases),'To Be Fetched':len(tobefetched_cases)})
            
            
            else:
                row_data=[{'Category':'Standard','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0},
                          {'Category':'Script','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0},
                          {'Category':'Jewellers','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0},
                          {'Category':'STL','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0},
                          {'Category':'Broker','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0},
                          {'Category':'NBFC','Missed':0,'Nearing':0,'Within_Cases':0,'To Be Fetched':0}]
            response_data={"heading":"Due Date Status",
                        "table":{
                            "header": [
                                "Category",
                                "Missed",
                                "Nearing",
                                "Within_Cases",
                                "To Be Fetched"
                                
                                
                            ],
                            "rowData": row_data
                            }}
            #return response_data
            response = {'flag': True}
        except Exception as e:
            logging.info(e)
            response = {'flag': False}
            return response
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
                    "api_service": "due_date_status", "service_container": "stats",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(response_data), "trace_id": trace_id,
                    "session_id": "","status":json.dumps(response['flag'])}
    # try:
    #     insert_into_audit(audit_data)
    # except:
    #     logging.info(f"issue in the query formation")
    return response_data
    
@app.route('/tat_report', methods=['POST', 'GET'])
def tat_report():
    data = request.json
    logging.info(f"request data is {data}")
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
            
            start_date_str = data['fromDate']
            end_date_str = data['toDate']
            extraction_db = DB("extraction", **db_config)
            stats_db = DB("stats", **db_config)
            queues_db = DB('queues', **db_config)
            start_date = datetime.strptime(start_date_str, r"%Y-%m-%d").date()
            end_date = datetime.strptime(end_date_str, r"%Y-%m-%d").date()
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')
            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            current_date = datetime.now().strftime('%d-%m-%Y')
        

            # Get the current date
            current_date = datetime.now()

            # Extract the current month name from the current date
            current_month_name = current_date.strftime("%B")

            # Get the number of days in the current month
            days_in_month = calendar.monthrange(current_date.year, current_date.month)[1]

            # Initialize the rowData list
            rowData = []

            # Define the date ranges based on the number of days in the month
            date_ranges = [
                (1, 7),
                (8, 14),
                (15, 21),
                (22, 28),
                (29, days_in_month)
            ]

            # Generate the rowData based on the date ranges
            for start, end in date_ranges:
                date_range_str = f"{current_month_name} {start}-{end}"
                rowData.append({
                    "Date Range": date_range_str,
                    "Total Interfaced": 0,
                    "Processed Within TAT": 0,
                    "% within TAT": 0,
                    "Processed After TAT": 0
                })

            # Print the rowData
            print("rowData:", rowData)
            
            response_data={"heading":"TAT Report for Current Month",
                        "table":{
                            "header": [
                                "Date Range",
                                "Total Interfaced",
                                "Processed Within TAT",
                                "% within TAT",
                                "Processed After TAT"
                                
                                
                            ],
                            "rowData": rowData
                            }}

            #return response_data
            response = {'flag': True}
        except Exception as e:
            logging.info(e,'exception')
            logging.debug(e)
            response = {'flag': False}
            return response
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
                    "api_service": "tat_report", "service_container": "stats",
                    "changed_data": "New file received","tables_involved": "","memory_usage_gb": str(memory_consumed), 
                    "time_consumed_secs": time_consumed, "request_payload": json.dumps(data), 
                    "response_data": json.dumps(response_data), "trace_id": trace_id,
                    "session_id": "","status":json.dumps(response['flag'])}
    # try:
    #     insert_into_audit(audit_data)
    # except:
    #     logging.info(f"issue in the query formation")
    return response_data


@app.route('/abbyy_pages', methods=['POST', 'GET'])
def abbyy_pages():
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    data = request.json
    logging.info(f"data is {data}")
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
            service_name='stats',
            span_name='abbyy_pages',
            transport_handler=http_transport,
            zipkin_attrs=attr,
            port=5010,
            sample_rate=0.5):
        try:
            start_date = data.get('fromDate', '')
            end_date = data.get('toDate', '') 
            database = data.get('database', '')
            table = data.get('table', '')
            queue = data.get('queue', '')
            header = data.get('header', '')
            # Extract dates from data dictionary
            start_date_str = data.get('fromDate', '')
            end_date_str = data.get('toDate', '')
            card_name = data.get('card_name', '')
            queues_db=DB("queues",**db_config)
            total_pages=os.environ['ABBYY_PAGES']

            # Convert strings to datetime objects
            start_dates = datetime.strptime(start_date_str, '%Y-%m-%d')

            end_dates = datetime.strptime(end_date_str, '%Y-%m-%d')

            # Format datetime objects to the desired format
            start_date_formatted = start_dates.strftime('%Y-%b-%d')
            end_date_formatted = end_dates.strftime('%Y-%b-%d')
            logging.info(f'start_date_formatted,{start_date_formatted}')
            filters=data.get('filters',{})
            card_name = data.get('card_name', '')
            end_datetime = datetime.strptime(end_date, r"%Y-%m-%d").date()
            end_datetime_=datetime.strftime(end_datetime, r"%Y-%m-%d")
            if end_date == start_date:
                end_date = (datetime
                            .strftime(end_datetime + timedelta(days=1), r'%Y-%m-%d'))
            
            get_page_consumed_cnt_qry=f"""select pages_remaining as pages_remains from ocr_info order by created_date DESC"""
            # where created_date between TO_DATE('{start_date_formatted} 00:00', 'YYYY-MON-DD HH24:MI') AND TO_DATE('{end_date_formatted} 23:59', 'YYYY-MON-DD HH24:MI')"""
            consumed_df=queues_db.execute_(get_page_consumed_cnt_qry)
            if not consumed_df.empty:
                pages_remains=consumed_df['pages_remains'].to_list()[0]
                if pages_remains=='' or pages_remains is None:
                    pages_remains=0
                else:
                    pages_remains=pages_remains
            else:
                pages_remains=0
            
            response_data = {
                "value": pages_remains,
                "name":f'{card_name}'
            }
            return_data = {
                "flag": True, "message": "Successfully got count", "data": response_data}


        except Exception as e:
            return_data = {'flag': False, 'message': 'Something went wrong'}
            logging.exception(f"failed {e}")
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
        logging.info(
            f"## Stats no_of_files Time and Ram checkpoint, Time consumed: {time_consumed}, Ram Consumed: {memory_consumed}")
        return return_data

        


@app.route('/clims_request', methods=['POST', 'GET'])
def clims_request():
    data = request.json
    logging.info(f"request data is {data}")
    tenant_id = data.get('tenant_id', None)
    userid=data.get('user','')
    case_id=data.get('case_id','')
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass

    try:
        passcode = os.environ['CLIMS_PASSCODE']
        location = os.environ['CLIMS_LOCATION']
        location_desc = os.environ['CLIMS_LOCDESC']
    except:
        logging.info("Failed to get the variables from env")
    
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
            service_name='stats',
            zipkin_attrs=attr,
            span_name='clims_request',
            transport_handler=http_transport,
            sample_rate=0.5
    ) as zipkin_context:
        try:
            db_config['tenant_id']=tenant_id
            extraction_db = DB("extraction", **db_config)
            stats_db = DB("stats", **db_config)
            queues_db = DB('queues', **db_config)

            query=f'''select STOCK_SEC_REFERENCE_ID,DOCUMENT_ID,PARTY_ID,DUE_DATE,STOCK_DOC_MONTH,STOCK_DOC_YEAR,BANK_SHARE,REMARKS from ocr where case_id='{case_id}'
            '''
            logging.info(f'query:{query}')
            ocr_vals = extraction_db.execute_(query)

            party_id = ocr_vals.iloc[0]['party_id']
            ocr_vals = ocr_vals.drop(columns=['PARTY_ID'])
            ocr_vals = ocr_vals.drop(columns=['party_id'])

            #query=f'''select HUB_CODE, BRANCH_CODE from process_queue where case_id='{case_id}'
            #'''
            #pq_vals = queues_db.execute_(query)
            #hub_code = pq_vals.iloc[0]['hub_code']
            #branch_code = pq_vals.iloc[0]['branch_code']

            query=f'''
                select STOCKS,ADVANCES,DEBTORS,CREDITORS from ocr where case_id='{case_id}'
            '''
            logging.info(f'query:{query}')
            ocr_vals__ = extraction_db.execute_(query)

            stocks = ocr_vals__['stocks'].tolist()
            debtors = ocr_vals__['debtors'].tolist() 
            creditors = ocr_vals__['creditors'].tolist() 
            advances = ocr_vals__['advances'].tolist() 

            stocksss=json.loads(stocks[0]) if stocks[0] else {}
            debtorsss=json.loads(debtors[0]) if debtors[0] else {}
            creditorsss=json.loads(creditors[0]) if creditors[0] else {}
            advancesss=json.loads(advances[0]) if advances[0] else {}

            logging.info(f'stocks:{stocksss}')

            stocks_view=stocksss.get('tab_view',{}).get('rowData',{})

            debtors_view=debtorsss.get('tab_view',{}).get('rowData',{})
            creditors_view=creditorsss.get('tab_view',{}).get('rowData',{})
            advances_view=advancesss.get('tab_view',{}).get('rowData',{})

            logging.info(f'stocks_view:{stocks_view}')
            logging.info(f'debtors_view:{debtors_view}')
            logging.info(f'creditors_view:{creditors_view}')
            logging.info(f'advances_view:{advances_view}')
            

            try:
                dynamic_query = f"select duplicated_fields from dynamic_fields where party_id='{party_id}'"
                dynamic_vals = extraction_db.execute_(dynamic_query)
                #component_fields = json.loads(dynamic_vals.iloc[0]['component_fields'])
                duplicated_fields = json.loads(dynamic_vals.iloc[0]['duplicated_fields'])
            except Exception as e:
                logging.info(f'No dynamic fields set for party_id {party_id}: {e}')
                #component_fields = ''
                duplicated_fields = ''

            try:
                fileName_query = f"select file_name from process_queue where case_id='{case_id}'"
                file_name = queues_db.execute_(fileName_query)
                file_name= file_name.iloc[0].get('file_name','')
                logging.info(f"file_name is {file_name}")
            except Exception as e:
                logging.info(f"Exception occured while fetching the file_name {e}")
                file_name = ''
            try:
                doc_item_id = file_name.split('_')[3]
                logging.info(f"doc_item_id is {doc_item_id}")
            except Exception as e:
                logging.info(f"Exception occured while fetching the doc_item_id {e}")
                doc_item_id = ''

            logging.info(f'ocr_vals:{ocr_vals}')
            unique_key_records = []
            stock_sec_reference_id=""
        
            def component_code_extraction(field_name):
                """
                component_mapping = {"Raw Materials":["RAW MATERIALS INSURED","RAW MATERIALS-IMP INSURED","RAW MATERIALS - IND. INSURED","RAW MATERIAL (LOCAL) INSURED","RAW MATERIAL (EXPORTS) INSURED"],
                                     "Finished Goods":["FINISHED GOODS INSURED","FINISH GOODS - EXPORTS INSURED","FINISHED GOODS (BLUK DRUG) INSURED","FINISHED GOODS ( FORMULATION) INSURED","FINISHED GOODS(EXPORT/FORMULATION  B.D)INSURED"],
                                     "Total Stock":["TOTAL STOCKS INSURED","TOTAL STOCK (PC) INSURED"],
                                     "Work in Process":["WORK IN PROGRESS INSURED","Semi Finished Goods","STOCK IN PROCESS INSURED","WORK IN PROGRESS (LOCAL) INSURED","WORK IN PROGRESS (EXPORTS) INSURED"],
                                     "Stores and Spares":["STORES & SPARES INSURED","STOCK & STORES INSURED"],
                                     "Stock in Transit":["STOCK IN TRANSIT"],
                                     "Goods in Transit":["GOODS IN TRANSIT INSURED"],"Domestic Stock":["DOMESTIC STOCKS INSURED","Domestic Stock"],
                                     "Export Stock":["EXPORT STOCKS INSURED"],"Sales":["SALES"],
                                     "Debtors <30 days":["Debtors <30 days","BOOK DEBTS UPTO 30 DAYS"],"Debtors <60 days":["Debtors <60 days","BOOK DEBTS UPTO 60 DAYS"],"Debtors <90 days":["Debtors <90 days","BOOK DEBTS UPTO 90 DAYS"],"Debtors <120 days":["Debtors <120 days","BOOK DEBTS UPTO 120 DAYS"],"Debtors <150 days":["Debtors <150 days","BOOK DEBTS UPTO 150 DAYS"],"Debtors <180 days":["Debtors <180 days","BOOK DEBTS UPTO 180 DAYS","EXPORT DEBTORS<180 DAYS INSURED"],"Debtors - Exports":["BOOK DEBTS -EXPORTS"],"Debtors - Domestic":["BOOK DEBTS -DOMESTIC"],"Debtiors of Group Companies":["DEBTORS OF GROUP COMPANIES"],
                                     #"Receivables":["RECEIVABLES"],"Domestic receivables":["DOMESTIC RECEIVABLES"],
                                     "Export receivables":["EXPORT RECEIVABLES"],
                                     #"Total Creditors":["CREDITORS","Trade Creditors"],
                                     "Creditors PC":["CREDITORS (PC)"],
                                     #"Unpaid Stocks":["UNPAID STOCKS"],"DALC":["LESS : DALC"],
                                     "Advances paid to suppliers":["ADD : ADVANCE TO SUPPLIER"],
                                     #"Debtors PC":["DEBTORS (PC)"],
                                     "Advances from Debtors/Customers":["LESS: ADVANCE RECIEVED"]}
                component_name_list = []
                for key, values in component_mapping.items():
                    if field_name.strip().lower() == key.lower():
                        component_name_list = values
                        logging.info(f"Matched field_name: {field_name} to component names: {component_name_list}")
                        break

                if not component_name_list:
                    logging.info(f"No matching component names found for field_name: {field_name}")
                    component_name_list = field_name.split(',')

                # Convert component list to SQL string with proper quoting
                component_name_sql = ",".join(f"'{x.upper().strip()}'" for x in component_name_list)
                logging.info(f"Component names for SQL: {component_name_sql}")"""
                def amp_to_chr38(value):
                    parts = value.split('&')
                    converted = f"'{parts[0]}'"

                    for part in parts[1:]:
                        converted += " || CHR(38) || "
                        converted += f"'{part}'"

                    return converted
                if '&' in field_name:
                    field_name = amp_to_chr38(field_name)
                else:
                    field_name = f"'{field_name}'" 
                # Construct the SQL query
                component_code_query = f"""
                SELECT DISTINCT TO_CHAR(cm.component_code) AS comp_code
                FROM component_master cm
                WHERE TRIM(TO_CHAR(cm.component_name)) IN (
                    SELECT TRIM(TO_CHAR(am.component_name))
                    FROM age_margin_working_uat am
                    WHERE am.party_id = '{party_id}'
                    AND TRIM(TO_CHAR(am.component_name)) = {field_name} AND TRIM(am.is_active) = 1
                )
                AND UPPER(TRIM(TO_CHAR(cm.status))) = 'ACTIVE'
                """

                print(component_code_query)
                component_code_df = extraction_db.execute_(component_code_query)
                if not component_code_df.empty:
                    component_code = component_code_df.iloc[0]['comp_code']
                    logging.info(f"Extracted component_code: {component_code} for field_name: {field_name}")
                else:
                    component_code = "000"
                return component_code
                    

            def generate_single_reference_id():
                timestamp = int(time.time() * 1000)  # Get current timestamp in milliseconds
                random_num = random.randint(1000, 9999)  # Generate a random 4-digit number
                reference_id = f"{timestamp}{random_num}"
                return reference_id

            def GetItemRestRequestDTOList(s_view, sub_word,dup_fields):
                aa=[]
                for sto in s_view:
                    dictt = {}
                    # Skip if fieldName is in duplicate or dynamic fields
                    if sto.get('fieldName') in dup_fields:
                        logging.info(f"Skipping fieldName: {sto.get('fieldName')} as it is in duplicate or dynamic fields")
                        continue
                    # Check if 'margin' key exists and is not empty or null
                    if sto.get('margin') and sto.get('value'):
                        component_code = component_code_extraction(sto.get('fieldName', ''))
                        if component_code != "000":
                            dictt = {
                                f'value{sub_word}UniqueId': str(generate_single_reference_id())[:15],
                                'component': component_code,  # Set from fieldName
                                'applicationDp': "YES"
                            }
                            if sto.get('value'):
                                dictt['amount'] = sto['value'].replace(',','')
                            if sto.get('margin'):
                                dictt['margin'] = sto['margin']
                            aa.append(dictt)
                return aa

            if isinstance(ocr_vals, pd.DataFrame):
                ocr_vals.columns = [col.lower() for col in ocr_vals.columns]
                ocr_vals = ocr_vals.loc[:, ~ocr_vals.columns.duplicated(keep='first')]
                ocr_vals = ocr_vals.rename(columns={
                    'stock_sec_reference_id': 'stockSecUniqueId',
                    'document_id': 'docItemId',
                    'due_date': 'dueDate',
                    'stock_doc_month': 'stockDocMonth',
                    'stock_doc_year': 'stockDocYear',
                    'bank_share': 'dpShare',
                    'remarks': 'remark'
                })
                ocr_vals = ocr_vals.fillna("").to_dict(orient='records')

                # List to store cleaned records
                
                for record in ocr_vals:
                    normalized_keys = set()
                    cleaned_record = {}
                    for original_key, value in record.items():
                        lower_key = original_key.lower()
                        if lower_key not in normalized_keys:
                            cleaned_record[original_key] = value
                            normalized_keys.add(lower_key)
                    unique_key_records.append(cleaned_record)
                    logging.info(f'unique_key_records:{unique_key_records}')
                    stock_sec_reference_id=unique_key_records[0].get('stockSecUniqueId')
                    unique_key_records[0].update({'stockSecUniqueId': int(str(unique_key_records[0].get('stockSecUniqueId')).replace('-','')[:15])})
                    logging.info(f'stock_sec_reference_id:{stock_sec_reference_id}')
                    #unique_key_records[0].update({'docItemId': int(doc_item_id)})
                    if str(doc_item_id).isdigit():
                        unique_key_records[0].update({'docItemId': int(doc_item_id)})
                    else:
                        unique_key_records[0].update({'docItemId': 0})
                    unique_key_records[0].update({'stockDocMonth': str(unique_key_records[0].get('stockDocMonth'))[:3]})
                    unique_key_records[0].update({'stockRestRequestDTOList':[{}],'stockBankRestRequestDTOList':[{}]})
                    unique_key_records[0]['stockRestRequestDTOList'][0].update({"stockUniqueId": str(generate_single_reference_id())[:15],
                    "location": int(location),
                    "locationDesc": location_desc,
                    "valueStockRestRequestDTOList":[{}] ,
                    "valueCreditorsItemRestRequestDTOList":[],
                    "valueDebtorsRestRequestDTOList": [{}],
                    "valueAdvancesItemRestRequestDTOList": [],
                    "adhocInsCovAmt": ""})
                    unique_key_records[0]['stockBankRestRequestDTOList'][0].update({"stockBankUniqueId": str(generate_single_reference_id())[:13],
                    "dpLeadBank": "",
                    "allocation": "",
                    "stockAmt": "",
                    "CreditorsAmt": "",
                    "debtorAmt": "",
                    "stockMargin": "",
                    "debtorMargin": "",
                    "CreditorsMargin": ""})
                    types = ['Stock','Debtors']
                    for i in types:
                        unique_key_records[0]['stockRestRequestDTOList'][0][f'value{i}RestRequestDTOList'][0].update({"marginType": "VARIABLE",
                        "margin": "",
                        f"value{i}ItemRestRequestDTOList": []})
                    valss=['stocks_view','debtors_view','creditors_view','advances_view']
                    for i in valss:
                        if i=='stocks_view':
                            duplicate_stocks= duplicated_fields.get('stocks', [])
                            #component_stocks = component_fields.get('stocks', [])
                            data__1=GetItemRestRequestDTOList(stocks_view, 'Stock',duplicate_stocks)
                            unique_key_records[0]['stockRestRequestDTOList'][0]['valueStockRestRequestDTOList'][0]['valueStockItemRestRequestDTOList']=data__1

                        if i=='debtors_view':
                            duplicate_debtors= duplicated_fields.get('debtors', [])
                            #component_debtors = component_fields.get('debtors', [])
                            data__2=GetItemRestRequestDTOList(debtors_view, 'Debtors',duplicate_debtors)
                            unique_key_records[0]['stockRestRequestDTOList'][0]['valueDebtorsRestRequestDTOList'][0]['valueDebtorsItemRestRequestDTOList']=data__2

                        if i=='creditors_view':
                            duplicate_creditors= duplicated_fields.get('creditors', [])
                            #component_creditors = component_fields.get('creditors', [])
                            data__3=GetItemRestRequestDTOList(creditors_view, 'Creditors',duplicate_creditors)
                            unique_key_records[0]['stockRestRequestDTOList'][0]['valueCreditorsItemRestRequestDTOList']=data__3


                        if i=='advances_view':
                            duplicate_advances= duplicated_fields.get('advances', [])
                            #component_advances = component_fields.get('advances', [])
                            data__4=GetItemRestRequestDTOList(advances_view, 'Advances',duplicate_advances)
                            unique_key_records[0]['stockRestRequestDTOList'][0]['valueAdvancesItemRestRequestDTOList']=data__4
                        
                    # for sto in stocks_view:
                    #     dictt={}
                    #     for key,val in sto.items():
                    #         dictt['valueStockUniqueId']=str(generate_single_reference_id(stock_sec_reference_id))
                    #         dictt["component"]="SEC0001084"
                    #         dictt["applicationDp"]="YES"
                    #         if key == 'value':
                    #             dictt['amount']=val
                    #         if key == 'margin':
                    #             dictt['margin']=val
            


            else:
                ocr_vals = []
                unique_key_records={}
            
            logging.info(f'ocr_vals:{ocr_vals}')
            logging.info(f'unique_key_records:{unique_key_records}')

            requestId = str(generate_single_reference_id())
            req_data={
                "sessionContext": {
                    "channel": "CDACE",
                    "userId": userid,
                    "externalReferenceNo": str(generate_single_reference_id()),
                    "bankCode": "08",
                    "transactionBranch": "089999"
                },
                "StockStatementSummaryRequestDTO": {
                    "requestString": {
                        "headerDetails": [
                                {
                                    "channelCode": "FCUBS",
                                    "requestId": requestId,
                                    "passCode": passcode
                                }
                            ],
                        "bodyDetails": [
                                {
                                    "partyId": party_id,
                                    "abStockRestRequestDTOList": unique_key_records  
                                }
                            ]
                }
            }
            }
            response_data = json.dumps(req_data)
            
        except Exception as e:
            logging.info(e,'exception')
            logging.debug(e)
            response = {'flag': False}
            return response
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

    
    return response_data
