import json
import argparse
import psutil
import os
import requests
import copy
from app import app
import time
import random
import shutil
from fuzzywuzzy import fuzz
from db_utils import DB
import traceback

import difflib
import stat
import math

import joblib
from difflib import SequenceMatcher
import pandas as pd
import re
from time import time as tt
from flask import Flask, request, jsonify
import numpy as np
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, classification_report
from collections import defaultdict
import pyodbc
import pandas as pd

# from ace_logger import Logging
# logging = Logging(name="extraction_api")

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}


def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes4


def insert_into_audit(case_id, data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit_')
    return True


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



def line_wise_ocr_data(words):
    """
    Forms the values in each line and creates line-wise OCR data.

    Args:
        words: List of dictionaries containing OCR data.

    Returns:
        list: List of lists where each inner list represents words on the same horizontal line.
    """
    ocr_word = []

    # Sort words based on their 'top' coordinate
    sorted_words = sorted(filter(lambda x: isinstance(x, dict) and "pg_no" in x and "top" in x, words), key=lambda x: (x["pg_no"], x["top"]))

    # sorted_words = sorted(words, key=lambda x: (x["pg_no"], x["top"]))

    # Group words on the same horizontal line
    line_groups = []
    current_line = []

    for word in sorted_words:
        if not current_line:
            # First word of the line
            current_line.append(word)
        else:
            diff = abs(word["top"] - current_line[0]["top"])
            if diff < 5:
                # Word is on the same line as the previous word
                current_line.append(word)
            else:
                # Word is on a new line
                current_line=sorted(current_line, key=lambda x: x["left"])
                line_groups.append(current_line)
                current_line = [word]

    # Add the last line to the groups
    if current_line:
        current_line=sorted(current_line, key=lambda x: x["left"])
        line_groups.append(current_line)
        
    for line in line_groups:
        line_words = [word["word"] for word in line]
        # print(" ".join(line_words))

    return line_groups


def row_wise_table(words):
    """
    Extracts structured tabular data from OCR results by grouping words into rows and aligning them into columns.

    Args:
        words (list): List of dictionaries containing OCR data with keys: 'word', 'left', 'top', 'width', 'height', 'pg_no'.

    Returns:
        list: A structured list of lists, where each inner list represents a row in the table.
    """
    print("\n🔹 Step 1: Sorting words by page number and top position...\n")
    words = sorted(words, key=lambda x: (x["pg_no"], x["top"]))

    height=min(words, key=lambda x: x["height"])['height']
    # print(f"height is {height} \n")
    thr=4

    table_rows = []
    current_row = []

    # print("\n🔹 Step 3: Grouping words into rows...\n")
    for word in words:

        if word['bottom']-word['top']>10:
            print(f"Skipping word {word['word']} due to abnormal height")
            continue

        if not current_row:
            # print(f"Starting new row with: {word['word']}")
            current_row.append(word)
        else:
            last_word = min(current_row, key=lambda w: w["top"])
            # print(f"Last word in current row: {last_word['word']} and {word['word']}")
            # print(f"Current word: {word} {last_word}")
            if (abs(word["top"] - last_word["top"]) < thr) or (abs((word["top"]+word['bottom'])/2 - (last_word["top"]+last_word["bottom"])/2) < thr) or (last_word["top"] < word["top"] < last_word["bottom"]) or (last_word["top"] < word["bottom"] < last_word["bottom"]):
                current_row.append(word)
            else:
                # print("\n🚀 New row detected. Saving current row and starting a new one.")
                current_row = sorted(current_row, key=lambda x: x["left"])
                table_rows.append(current_row)
                current_row = [word]
    
    # Add last processed row
    if current_row:
        current_row = sorted(current_row, key=lambda x: x["left"])
        table_rows.append(current_row)

    for line in table_rows:
        line_words = [word["word"] for word in line]
        print(" ".join(line_words))

    return table_rows


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

def preparing_ocr_parsed_data_1(data,api=False,api_2=False,api_3=False):

    if not data:
        return data

    data = sorted(data, key=lambda x: x["left"])
    combined_word_dicts = {}
    distances=[]
    for i,dict1 in enumerate(data):
        if (i+1)==len(data):
            break
        dict2=data[i+1]
        distance = abs(dict2["left"] - dict1["right"])
        distances.append(distance)
    # print(f"{distances} \n")
    data = sorted(data, key=lambda x: x["left"])
    # print(f"data is {data} \n")
    threshold=5
    if api:
        threshold=4
    if api_2:
        threshold=3
    if api_3:
        threshold=9
    
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
        # print(dict2,temp)
        # print(distance)
        if not api_3:
            check=char_check(temp,dict2)
        else:
            check=True

        if (distance <= threshold or temp["left"]<=dict2["left"]<=temp["right"]) and check:
            temp_l.append(dict2)
            temp=dict2
        else:
            # print(f"gone her 3")
            combine.append(temp_l)
            temp_l=[]
            temp_l.append(dict2)
            temp=dict2
    # print(temp_l)
    if temp_l:
        combine.append(temp_l) 
    # print(f"combine list from the horizontal line words is {combine} \n")
    return combine


def classify_word(word):
    """Classify word into type: date, amount, code, desc, etc."""
    if re.match(r'\d{2}/\d{2}/\d{2,4}', word):
        return 'date'
    elif re.match(r'^\$?\d+\.\d{2}$', word):
        return 'amount'
    elif re.match(r'^\$+$', word):
        return 'dollar'
    elif re.match(r'^[A-Z0-9]{5}$', word):  # like D0274
        return 'code'
    elif word.isalpha():
        return 'desc'
    return 'other'


def preparing_ocr_parsed_data_table(data, api=False, api_2=False):
    if not data:
        return []

    # Sort left to right
    data = sorted(data, key=lambda x: x["left"])
    
    # Set threshold
    threshold = 5
    if api:
        threshold = 4
    if api_2:
        threshold = 3

    combined = []
    current_group = []
    temp = data[0]
    temp['x-space'] = threshold
    current_group.append(temp)

    for i in range(1, len(data)):
        prev = current_group[-1]
        current = data[i]
        current['x-space'] = threshold
        distance = abs(current["left"] - prev["right"])

        # classify both
        prev_type = classify_word(prev['word'])
        
        # print(prev_type,prev['word'])
        curr_type = classify_word(current['word'])
        # print(curr_type,current['word'])

        # new group if spacing is large or type shift (amount/code)
        if distance > threshold:
            combined.append(current_group)
            current_group = [current]
        else:
            # if (prev_type in ['amount', 'code','date']):
            #     combined.append(current_group)
            #     current_group = [current]
            # else:
            current_group.append(current)

    if current_group:
        combined.append(current_group)

    return combined


def get_ocr_word_table_ll(words,row=''):
    if not row:
        ocr_lines=line_wise_ocr_data(words)
    else:
        ocr_lines=[row]

    out_lines=[]
    for line in ocr_lines:
        print(line)
        temp=[]
        out_line=(preparing_ocr_parsed_data_1(line))
        for lists in out_line:
            combined_result = combine_dicts(lists)
            temp.append(combined_result)
        out_lines.append(temp)
        print(out_lines)
    
    if not row:
        return out_lines
    else:
        tem=[]
        for word in out_lines:
            tem.append(combine_dicts(word))
        return tem


def get_ocr_word_table(words,row=''):
    if not row:
        ocr_lines=line_wise_ocr_data(words)
    else:
        ocr_lines=[row]

    out_lines=[]
    for line in ocr_lines:
        print(line)
        temp=[]
        out_line=(preparing_ocr_parsed_data_1(line))
        for lists in out_line:
            combined_result = combine_dicts(lists)
            temp.append(combined_result)
        out_lines.append(temp)
        print(out_lines)
    
    if not row:
        return out_lines
    else:
        tem=[]
        for word in out_lines:
            for wo in word:
                tem.append(wo)
        return tem
    

def table_col_crop(col_ocr,table,table_cord,col_index):

    table_headers=table['headers']
    table_headers_cord=table_cord['headers']
    print(F" table_headers is {table_headers}")
    print(F" table_headers_cord is {table_headers_cord}")

    removed='removed'
    table_rows=table['rows']
    for row in table_rows:
        row.pop(removed,None)
    table_rows_cord=table_cord['rows']
    for row in table_rows_cord:
        row.pop(removed,None)

    columns=get_ocr_word_table(col_ocr)
    print(F" columns is {columns}")

    temp_head=table_headers[0]
    print(F" temp_head is {temp_head}")
    
    index=-1
    for row in table_rows_cord:
        print(f"row is {row}")
        index=index+1
        flg=False
        for key,item in row.items():
            print(F"######### item is {item}")
            try:
                if item['word'] in table_headers:
                    continue
                if key == item['word']:
                    continue
                row_top=item['top']
                row_bottom=item['bottom']
                flg=True
                break
            except:
                continue
        if not flg:
            table_rows[index].update({table_headers[col_index]:''})
            print(f"table_rows after inerr=tionis {table_rows[index]}")
            row.update({table_headers[col_index]:{}})  
            print(f"row after inerstionis {row}")
            continue
            
        flag=False
        for column in columns:
            column=combine_dicts(column)
            print(f"column is {column}, row_top is {row_top} ,row_bottom is {row_bottom}")
            if row_top-2 <= column['top'] <= row_bottom+2 or row_top-2 <= column['bottom'] <= row_bottom+2 or row_top <= abs(column['top']+column['bottom'])/2 <= row_bottom:   
                table_rows[index].update({table_headers[col_index]:column['word']})
                print(f"table_rows after inerr=tionis {table_rows[index]}")
                row.update({table_headers[col_index]:column})  
                print(f"row after inerstionis {row}")
                flag=True
                break
            
    print(F" table_rows is {table_rows}")
    print(F" table_rows_cord is {table_rows_cord}")

    table['rows']=table_rows
    table_cord['rows']=table_rows_cord

    table['headers']=table_headers
    table_cord['headers']=table_headers_cord

    return table_cord,table


def table_row_crop(row_ocr,table,table_cord,row_index):

    table_headers=table_cord['headers']

    table_rows=table['rows']
    table_rows_cord=table_cord['rows']

    headers = [item["word"] for item in table_headers]
    headers_cord = [item for item in table_headers]
    print(F'headers and headers_cord are {headers_cord} and {headers}')
    row=get_ocr_word_table([],row_ocr)
    print(F'row  get_ocr_word_table {row}')
    temp,temp_word=get_row_table(headers_cord,row)
    print(F'temp and temp_word are {temp} and {temp_word}')

    try:
        table_rows.pop(row_index)
        table_rows_cord.pop(row_index)
    except:
        pass

    table_rows.insert(row_index, temp_word)
    table_rows_cord.insert(row_index, temp)
    print(F'table_rows_cord and table_rows are {table_rows} and {table_rows_cord}')

    table['rows']=table_rows
    table_cord['rows']=table_rows_cord

    return table_cord,table


def column(words_list):
    columns=get_ocr_word_table_ll(words_list)
    col_head=columns[0]
    columns.pop(0)

    all_column=[]
    for col in col_head:
        temp=[col]
        for row in columns:
            for word in row:
                if col['left']-2 <= word['right'] <= col['right']+2 or col['left']-2 <= word['left'] <= col['right']+2 or col['left'] <= abs(word['left']+word['right'])/2 <= col['right']:   
                    temp.append(word)
                elif col['left']-20 <= word['right'] <= col['right']+20 or col['left']-20 <= word['left'] <= col['right']+20 or col['left'] <= abs(word['left']+word['right'])/2 <= col['right']:   
                    temp.append(word)
        all_column.append(temp)
    
    print(all_column)
    return all_column


def get_restructed_col(table,trained_map=[]):

    print(f"table is {table}")
    print(f"trained_map is {trained_map}")

    updated_rows = []
    for row in table["rows"]:
        print(F"row is {row}")
        try:
            updated_row = {trained_map[key]: value for key, value in row.items()}
        except:
            updated_row=row
        updated_rows.append(updated_row)
    
    table["rows"] = updated_rows

    print(f"renamed_headers is {trained_map}")
    
    return table,trained_map


def reverse_table(data):
    # Step 1: Reverse the mapping
    reverse_mapping = {v: k for k, v in data["renamed_headers"].items()}
    
    # Step 2: Remap each row's keys
    remapped_rows = []
    for row in data["rows"]:
        remapped_row = {reverse_mapping.get(k, k): v for k, v in row.items()}
        remapped_rows.append(remapped_row)
    
    # Step 3: Remap headers too (to match the original names)
    remapped_headers = [reverse_mapping.get(h, h) for h in data["headers"]]
    
    # Output the result
    output = {
        "headers": remapped_headers,
        "rows": remapped_rows
    }
    return output


@app.route("/cropped_table", methods=['POST', 'GET'])
def cropped_table():
    data=request.json
    print(f"Data recieved in cropped_table {data}")
    if data is None:
        return {'flag': False}

    tenant_id = data['tenant_id']
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
   
    if (user is None) or (session_id is None):
        ui_data = data.get('ui_data', {'user':None,'session_id':None})
        user = ui_data['user']
        session_id = ui_data['session_id']

    # try:

    db_config['tenant_id'] = tenant_id

    row_ocr=data.get('row_ocr',None)
    table=data.get('table',None)
    table_cord=table.get('cor_table',None)
    row_index=data.get('row_index',None)
    col_ocr=data.get('col_ocr',None)
    col_index=data.get('col_index',None)
    case_id=data.get('case_id',None)
    claim_id=data.get('claim_id',None)
    check_no=data.get('check_no',None)
    renamed_headers=table.get('renamed_headers',None)
    renamed_headers_got=copy.deepcopy(renamed_headers)

    db_config['tenant_id']=tenant_id
    template_db=DB('template_db',**db_config)

    headers_table=table['headers']
    table=reverse_table(table)

    if row_ocr:
        row_ocr=get_ocr_data_lines(row_ocr)
        print('row_ocr',row_ocr)
        i=0
        for ind in row_index:
            row=row_ocr[i]
            i=i+1
            print(ind,row)
            table_cord,table=table_row_crop(row,table,table_cord,ind)
            # print(table_cord)

        swapped_dict = {v: k for k, v in renamed_headers.items()}
        header=renamed_headers.values()
        table,renamed_headers=get_restructed_table(table,template_db,trained_map=swapped_dict,header_raw=header)
        print(renamed_headers)
        table_high=get_table_high(table_cord,renamed_headers)

        new_rows=[]
        for row in table['rows']:
            print(f'row here is {row}')
            temp_row={}
            for key,value in row.items():
                if key in headers_table:
                    temp_row[key]=value
                else:
                    try:
                        temp_row[swapped_dict[key]]=value
                    except:
                        temp_row[key]=value

            new_rows.append(temp_row)

        # update_highlights_table(tenant_id,case_id,table_high,claim_id,check_no)
        table['headers']=headers_table
        table['cor_table']=table_cord
        table['rows']=new_rows
        table['renamed_headers']=renamed_headers_got
        response={"flag":True,"data":{"modified_table":table}}
                
    elif col_ocr:
        col_ocr=column(col_ocr)
        print("col_ocr",col_ocr)
        i=0
        for ind in col_index:
            col=col_ocr[i]
            i=i+1
            print(ind,col)
            table_cord,table=table_col_crop(col,table,table_cord,ind)
            # print(table)

        swapped_dict = {v: k for k, v in renamed_headers.items()}
        header=renamed_headers.values()
        table,renamed_headers_=get_restructed_table(table,template_db,trained_map=renamed_headers,header_raw=header)
        print(renamed_headers)
        table_high=get_table_high(table_cord,renamed_headers)

        new_rows=[]
        for row in table['rows']:
            temp_row={}
            print(f'row here is {row}')
            for key,value in row.items():
                if key in headers_table:
                    temp_row[key]=value
                else:
                    try:
                        temp_row[swapped_dict[key]]=value
                    except:
                        temp_row[key]=value
            print(f'row here is {temp_row}')
            new_rows.append(temp_row)

        # update_highlights_table(tenant_id,case_id,table_high,claim_id,check_no)
        table['headers']=headers_table
        table['rows']=new_rows
        table['cor_table']=table_cord
        table['renamed_headers']=renamed_headers_got
        response={"flag":True,"data":{"modified_table":table}}

    # except Exception as e:
    #     print(f'## Exception Occured in Cropped_table UI !@#$%^&* {e}')
    #     # return {'flag': False, 'data':{'message': 'Incorrect Data in request'}}
    #     response={"data":{"msg":"Error in cropped_data"},"flag":True}
    #     final_table_ui=[{}]
            
    return jsonify(response)



def update_highlights_table(tenant_id,case_id,table_high,claim_id,check_no):

    #updating all the extracted highlights into database

    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    query=f"select * from ocr_post_processed where case_id='{case_id}' and check_no ='{check_no}' and patient_name = '{claim_id}'"
    highlight_df=extraction_db.execute(query)
    highlight=highlight_df.to_dict(orient='records')
    highlight=highlight[0]['highlights']

    if highlight:
        highlight=json.loads(highlight)[0]
        highlights_table=highlight['table']

        print(f"highlights_table before is {highlights_table}")

        for table_name in highlights_table:
            highlights_table[table_name]=table_high['table']

        print(f"highlights_table after is {highlights_table}")

        highlight['table']=highlights_table

        highlight=json.dumps([highlight])
        query = "UPDATE `ocr_post_processed` SET `highlights`= %s WHERE `case_id` = %s and check_no = %s  and patient_name =  %s"
        params = [highlight, case_id,check_no,claim_id]
        extraction_db.execute(query, params=params)

    return True


def get_table_high(data,renamed_headers=[]):

    row_data=data["rows"]
    header=data["headers"]
    high_table={}
    dit=[]
    for row in row_data:
        temp={}
        print(f'row is',row)
        for key_,val in row.items():
            try:
                out=from_highlights(val,table=True)
                temp[key_]=out
            except:
                temp[key_]={}
        
        print(f'temp is',temp)
        dit.append(temp)
    high_table['table']=dit

    final_dict=[]
    if renamed_headers:
        print(dit)
        for row in dit:
            temp={}
            for key,value in row.items():
                if key in renamed_headers:
                    temp[renamed_headers[key]]=value
                else:
                    temp[key]=value
            final_dict.append(temp)
        high_table['table']=final_dict
        print(final_dict)
        return high_table
    else:
        return high_table
    

def get_no_header_cord(no_header_columns,headers_cord):

    print()

    for head in no_header_columns:
        left_header=head['leftHeader']
        right_header=head['rightHeader']
        header=head['header']
        print(f'finding for the head {head}')

        for i in range(len(headers_cord)):
            if i == len(headers_cord)-1:
                break

            matcher = SequenceMatcher(None, headers_cord[i]['word'].lower(), left_header.lower())
            left_match = matcher.ratio()

            matcher = SequenceMatcher(None, headers_cord[i+1]['word'].lower(), right_header.lower())
            right_match = matcher.ratio()

            print(f'left_match {left_match} for left_header is {headers_cord[i]["word"] , left_header}')
            print(f'right_match {right_match} for right_header is {headers_cord[i+1]["word"] , right_header}')

            if right_match>0.9 and left_match>0.9:

                print(f'found the left and right headers {headers_cord[i]["word"],headers_cord[i+1]["word"]}')

                new_header=copy.deepcopy(headers_cord[i])
                new_header['left']=headers_cord[i]['right']+10
                new_header['right']=headers_cord[i+1]['left']-10
                new_header['word']=header

                print(f'New Header that is created is {new_header}')

                headers_cord.append(new_header)

    headers_cord=sorted(headers_cord, key=lambda x: x["left"])
    print(f'New Header_cords are {headers_cord}')

    return headers_cord



def get_row_table(headers_cord,row,row_data=[],api=False,second=False):

    print(f'row here is {row}')
    print(f'headers_cord here is {headers_cord}')

    temp={}
    temp_word={}
    index=-1
    picked=[]
    for item in row:

        print(f'item here is {item}')

        flag=False 
 
        for header in headers_cord:
            if item in picked:
                continue
            index=index+1
            if header['left']<(item['right']+item['left'])/2<header['right']:
                if header['word'] in temp:
                    temp[header['word']].append(item)
                    temp_word[header['word']].append(item)
                else:
                    temp[header['word']]=[item]
                    temp_word[header['word']]=[item['word']]
                picked.append(item)
                flag=True
            elif item['left']<(header['right']+header['left'])/2<item['right']:
                if header['word'] in temp:
                    temp[header['word']].append(item)
                    temp_word[header['word']].append(item)
                else:
                    temp[header['word']]=[item]
                    temp_word[header['word']]=[item['word']]
                flag=True
                picked.append(item)

        if not flag:
            for header in headers_cord:
                if item in picked:
                    continue

                index=index+1
                if header['left']<item['left']<header['right']:
                    if header['word'] in temp:
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    picked.append(item)
                    flag=True
                elif item['left']<header['right']<item['right']:
                    if header['word'] in temp:
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    flag=True
                    picked.append(item)
        
        if not flag:
            for header in headers_cord:
                if item in picked:
                    continue

                index=index+1
                if header['left']<item['right']<header['right']:
                    if header['word'] in temp:
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    picked.append(item)
                    flag=True
                elif item['left']<header['left']<item['right']:
                    if header['word'] in temp:
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    flag=True
                    picked.append(item)

        if not flag:
            for header in headers_cord:
                if item in picked:
                    continue
                index=index+1
                if header['left']<item['left']<header['right']+10:
                    if header['word'] in temp:
                        item['confidance']=60
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        item['confidance']=60
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    picked.append(item)
                    flag=True

        if not flag:
            for header in headers_cord:
                if item in picked:
                    continue
                index=index+1
                if header['left']-10<item['right']<header['right']:
                    if header['word'] in temp:
                        item['confidance']=60
                        temp[header['word']].append(item)
                        temp_word[header['word']].append(item)
                    else:
                        item['confidance']=60
                        temp[header['word']]=[item]
                        temp_word[header['word']]=[item['word']]
                    picked.append(item)
                    flag=True

    
    print(f'row is',temp)
    new_row={}
    new_row_word={}
    for head,values in temp.items():

        if head not in new_row:
            new_row[head]=''
            new_row_word[head]=''
        temp_head=[]
        lines=line_wise_ocr_data(values)
        # print(f'lines is',lines)
        for lin in lines:
            lin = sorted(lin, key=lambda x: x["left"])
            temp_head.append(combine_dicts(lin))

        # print(f'temp_head is',temp_head)
        sorted_words = sorted(temp_head, key=lambda x: x["top"])
        temP_word=combine_dicts(sorted_words)

        # print(f'temP_word is',temP_word)
        new_row[head]=temP_word
        new_row_word[head]=temP_word['word']
    if not second:
        for header in headers_cord:
            new_row.setdefault(header['word'], header)
            new_row_word.setdefault(header['word'], "")
    return new_row,new_row_word


def convert_to_table_format_from_word(list_of_lists,no_header_columns=[]):
    """
    Converts a nested list of dictionaries into a table format where the first sublist serves as headers,
    and the remaining sublists represent row data.

    Args:
        list_of_lists (list): A list of lists, where each inner list contains dictionaries with a "word" key.

    Returns:
        dict: A table in the format {"headers": [header1, header2, ...], "rowData": [{header1: value1, ...}, ...]}.
    """
    if not list_of_lists:
        return {"headers": [], "rows": []}
    
    # Extract headers from the "word" values of the first list
    headers = [item["word"] for item in list_of_lists[0]]
    headers_cord = [item for item in list_of_lists[0]]


    if no_header_columns:
        print(f'here we have {no_header_columns}')
        print(f'here  headers_cord {headers_cord}')
        headers_cord=get_no_header_cord(no_header_columns,headers_cord)

    # Extract row data from the remaining lists
    row_data = []
    row_data_main=[]
    for row in list_of_lists[1:]:  # Skip the first sublist (headers)
        
        temp_row=[]

        out_line=(preparing_ocr_parsed_data_table(row,api_2=True))
        for lists in out_line:

            temp_lines=[]
            lines=line_wise_ocr_data(lists)
            # print(f'lines is',lines)
            for lin in lines:
                lin = sorted(lin, key=lambda x: x["left"])
                temp_lines.append(combine_dicts(lin))

            # print(f'temp_head is',temp_lines)
            sorted_words = sorted(temp_lines, key=lambda x: x["top"])
            temP_word=combine_dicts(temp_lines)

            print(f'temp_head is',temp_lines)
            temp_row.append(temP_word)
        temp,temp_word=get_row_table(headers_cord,temp_row,row_data=row_data,api=True)

        if not temp:
            for head in headers:
                temp[head]=''

        if not temp_word:
            for head in headers:
                temp_word[head]=''

        # print(F"temp is {temp}")
        row_data.append(temp)
        row_data_main.append(temp_word)
        
    # Build the table format
    table = {
        "headers": headers_cord,
        "rows": row_data
    }
    table_word = {
        "headers": headers,
        "rows": row_data_main,
        "cor_table":table
    }
    
    return table,table_word


def get_ranges(headers):
    headers=sorted(headers, key=lambda x: x["left"])
    ranges=[]
    ranges_left=[]
    for head in headers:
        ranges.append(abs(head['left']+head['right'])/2)
        ranges_left.append(head['left'])
        print(head,abs(head['left']+head['right'])/2)

    ranges=sorted(ranges) 
    ranges_left=sorted(ranges_left) 

    return ranges,ranges_left


def check_line(ranges,ranges_left,line,current_line):

    avoid=['pageof']

    #need incoprate header checking using some other here for now spacing is used
    corected_lines=[]
    out_line=preparing_ocr_parsed_data_1(line)
    for lists in out_line:
        combined_result = combine_dicts(lists)
        corected_lines.append(combined_result)

    if  not current_line and len(corected_lines)==1:
        print(f" ############### table line in one word no current_line {current_line ,corected_lines}")
        return False
    
    suspect=0
    for word in corected_lines:

        temp=re.sub(r'[^a-zA-Z]', '', word['word'])
        if temp in avoid:
            print(f" ############### table line in avoid {temp}")
            return False
        
        found=False
        print(f" ############### table line validity check {word} with ranges {ranges}")
        for i in range(len(ranges)):
            if word['left']<=ranges[i]<=word['right']:
                if not found:
                    found=True
                    print(f"found this found in the range of {ranges[i]}")
                    # if i==len(ranges)-1:
                    #     pass
                    # else:
                    #     print(f" ############### table line validity ranges {ranges_left} with ranges {ranges_left[i+1]} and {word['right']}")
                    #     if ranges_left[i+1]< word['right']:
                    #         print(f" ############### falied check line fro this word returing  {word['word']}")
                    #         return False
                else:
                    print(f"found again found in the range of {ranges[i]} so returning ")
                    return False
        if not found:
            suspect=suspect+1
        
    return True


def get_table_row_headers(headers_cord,row,final_out):
    print(headers_cord)
    index=-1
    final_out={}
    for header in headers_cord:
        final_out[header['word']+str(header['left'])]=[header]
        flag=False
        for item in row:
            index=index+1
            # print(F" headers_cord is {header['word']} left - {header['left']} right - {header['right']}")
            # print(F" item got is {item['word']} left - {item['left']} right - {item['right']}")
            if header['left']-5<item['left']<header['right']+5 or header['left']-5<item['right']<header['right']+5 or header['left']<(item['right']+item['left'])/2<header['right']:
                final_out[header['word']+str(header['left'])].append(item)
            elif item['left']-5<header['left']<item['right']+5 or item['left']-5<header['right']<item['right']+5 or item['left']<(header['right']+header['left'])/2<item['right']:
                final_out[header['word']+str(header['left'])].append(item)
        
    return final_out


def is_word_present(word, word_list, threshold=0.95):

    temp_word=re.sub(r'[^a-zA-Z0-9 ]', '', word)
    if not word_list:
        return False
    if not temp_word:
        return True
    
    word_lower = temp_word.lower()

    word_list_lower = [re.sub(r'[^a-zA-Z0-9 ]', '', w).lower() for w in word_list] 

    # print(f'word_list_lower is {word_list_lower}')
    # print(f'word_lower is {word_lower}')

    closest_matches = difflib.get_close_matches(word_lower, word_list_lower, n=1, cutoff=threshold)
    return closest_matches[0] if closest_matches else False



def detect_table_header_2(table_all_header,ocr_data):

    temp_words=[]
    for wo in table_all_header:
        if re.sub(r'[^a-zA-Z]', '', wo['word']):
            temp_words.append(wo['word'])
    print(f'temp_words',temp_words)
    table_all_header_word=' '.join(temp_words)

    table_lines=[]
    for ocr in ocr_data:
        sorted_words = sorted(ocr, key=lambda x: x["top"])
        line_ocr=line_wise_ocr_data(sorted_words)
        for line in line_ocr:
            valid_flag=True
            for word in line:
                if re.sub(r'[^a-zA-Z]', '', word['word']):
                    if not is_word_present(word['word'], temp_words, threshold=0.75):
                        valid_flag=False
                        break   
                else:
                    continue
                
            if valid_flag:
                table_lines.append(line)
                print(f"table_lineis {line} and table header is {temp_words}")

    if not table_lines:
        print("No table header detected.")
        return []
    
    all_groups=[]
    group=[table_lines[0]]
    for lines in table_lines[1:]:
        prev_bottom=group[-1][0]['bottom']
        current_top=lines[0]['top']

        if abs(current_top-prev_bottom)<5:
            group.append(lines)
        else:
            all_groups.append(group)
            group=[lines]

        prev_bottom=lines[0]['bottom']

    if group:
        all_groups.append(group)
    print(f"all_groups is {all_groups}")
    
    max_match=0
    final_group=[]
    returned_group=[]
    print(f"table_all_header_word is {table_all_header_word}")
    for group in all_groups:
        temp_words=[]
        print(f"group is {group}")
        for li in group:
            for wo in li:
                if re.sub(r'[^a-zA-Z]', '', wo['word']):
                    temp_words.append(wo['word'])
        group_word=' '.join(temp_words)
        print(f"group_word is {group_word}")

        similarity = fuzz.token_sort_ratio(group_word, table_all_header_word) / 100
        if similarity > max_match and similarity>0.65:
            max_match = similarity
            final_group = group


    if not final_group:
        return []
    else:
        for lin in final_group:
            for wo in lin:
                returned_group.append(wo)

    return returned_group



def extract_table_header(ocr_data,top, table_headers, foot=False,table_header_text=[],header_check=False):
    """
    Extracts table data from OCR words starting from a predicted header. The function looks for the header
    and then traverses down, collecting rows of table data until it encounters an empty line, a large gap,
    or other stopping conditions.

    Args:
        words_ (list): List of dictionaries containing OCR data for words.
        div (str): Division identifier.
        seg (str): Segment identifier.

    Returns:
        list: Extracted table rows.
    """
    
    table_headers_line=line_wise_ocr_data(table_headers)
    table_line_words=[]
    for table_head_line in table_headers_line:
        temp=''
        for word in table_head_line:
            temp=temp+" "+word['word']
        table_line_words.append(temp)

    table_lines=[]
    print(F'table_headers is {table_line_words}')
    # Sort words by their "top" position to process lines vertically
    found=[]
    for table_line_word in table_line_words:
        temp_line=[]
        max_match=0
        for ocr in ocr_data:
            sorted_words = sorted(ocr, key=lambda x: x["top"])
            line_ocr=line_wise_ocr_data(sorted_words)
            for line in line_ocr:
                if line[0]['top']<top:
                    continue
                if foot:
                    line_words = [word["word"] for word in line]
                    line_words=" ".join(line_words)
                    line_words_temp=re.sub(r'[^a-zA-Z]', '', line_words)
                    table_line_word_temp=re.sub(r'[^a-zA-Z]', '', table_line_word)
                    matcher = SequenceMatcher(None, line_words_temp, table_line_word_temp)
                    similarity_ratio_col = matcher.ratio()
                    if similarity_ratio_col>max_match and similarity_ratio_col>0.65:
                        max_match=similarity_ratio_col
                        temp_line=line
                        print(f"table_lineis {line_words} and table header is {table_line_word} and {similarity_ratio_col}")
                else:
                    line_words = [word["word"] for word in line]
                    line_words=" ".join(line_words)
                    matcher = SequenceMatcher(None, line_words, table_line_word)
                    similarity_ratio_col = matcher.ratio()
                    if similarity_ratio_col>max_match and similarity_ratio_col>0.65:
                        max_match=similarity_ratio_col
                        temp_line=line
                        print(f"table_lineis {line_words} and table header is {table_line_word} and {similarity_ratio_col}")
            print(f"temp_line got for this page is {temp_line}")
            if temp_line:
                table_lines.extend(temp_line)
        if temp_line:
            found.append(table_line_word)  

    print(F'table_line is {table_lines}')
    if not table_lines or len(found)!=len(table_line_words):
        table_lines_=detect_table_header_2(table_headers,ocr_data)
        if not table_lines and not table_lines_:
            print("No table header detected.")
            if header_check:
                return []
            return [],[]
        else:
            table_lines=table_lines_

    table_header_box=combine_dicts(table_lines)

    final_line=[]
    for ocr in ocr_data:
        sorted_words = sorted(ocr, key=lambda x: x["top"])
        line_ocr=line_wise_ocr_data(sorted_words)
        for line in line_ocr:
            line_box=combine_dicts(line)
            if line_box and table_header_box and table_header_box['top']<=line_box['top']<=table_header_box['bottom'] and line_box['pg_no'] == table_header_box['pg_no']:
                print(F'final table lines are is {line_box}')
                final_line.extend(line)

    if header_check:
        return final_line

    #need incoprate header checking using some other here for now spacing is used
    print(F'table_header_text is {table_header_text}')

    new_lines=[]
    for word in final_line:
        new_lines.append(word)

    table_lines=new_lines

    if not table_header_text:
        headers=group_words_into_columns(table_lines, col_threshold=5)
    else:

        headers=[]
        for header in table_header_text:
            words=[word for word in header.split() if re.sub(r'[^a-zA-Z]', '', word)]
            found_header=find_accurate_table_header([table_lines],words)
            print(f'found_header is {found_header} for the header is {header}')
            headers.append(found_header)

        # soert_lines=sorted(table_lines, key=lambda x: x["top"])
        # final_header,table_heads=group_words_into_columns(soert_lines,api=True)
        # print(f"table_heads is" ,table_heads)

        # new_dict={}
        # picked=[]

        # remove_if_pciked={}
        # for he in table_header_text:
        #     remove_if_pciked[he]=he.lower().split()

        # header_list=list(table_heads.keys())
        # table_header_text=list(table_header_text)
        # print(f"header_list is" ,header_list)
        # print(F'table_header_text is {table_header_text}')

        # #'need to remove'
        # if len(header_list) == len(table_header_text):
        #     flag=True
        #     for i in range(len(header_list)):
        #         matcher = SequenceMatcher(None, header_list[i], table_header_text[i])
        #         print(f'here headers are same length so comparing similarity for {header_list[i],table_header_text[i]}') 
        #         similarity_ratio_con = matcher.ratio()
        #         if similarity_ratio_con<0.9:
        #             flag=False
        #             break
        #     if flag:
        #         return final_header,table_line_words

        # ind=0
        # for head in table_header_text:
        #     print(f"head  is" ,head)
        #     start=0
        #     if len(header_list) > len(table_header_text):
        #         end=len(header_list)
        #     else:
        #         end=ind+2
        #     # print(f"header_list  is" ,header_list[start:end])
        #     # print(f"remove_if_pciked[head])  is" ,remove_if_pciked[head])
        #     for head_ in header_list[start:end]:
        #         # print(f"table_heads[head_]  is" ,table_heads[head_])
        #         for word in table_heads[head_]:
        #             if not remove_if_pciked[head]:
        #                 continue
        #             if word in picked:
        #                 continue
        #             if head not in new_dict:
        #                 new_dict[head]={}
        #             temp_word=re.sub(r'[^a-zA-Z0-9 ]', '', word['word'])
        #             if temp_word and is_word_present(word['word'], remove_if_pciked[head]):
        #                 new_dict[head][word['word']]=word
        #                 print(f"new_dict ,remove_if_pciked" ,new_dict[head],remove_if_pciked[head])
        #                 if word['word'].lower() in remove_if_pciked[head]:
        #                     remove_if_pciked[head].remove(word['word'].lower())
        #                 picked.append(word)

        #     ind=ind+1
                
        # print(f"new_dict is" ,new_dict[head])
        # headers=[]     
        # for head,dic in new_dict.items():
        #     temp_head=[]
        #     lines=line_wise_ocr_data(list(dic.values()))
        #     for lin in lines:
        #         lin = sorted(lin, key=lambda x: x["left"])
        #         temp_head.append(combine_dicts(lin))
        #     sorted_words = sorted(temp_head, key=lambda x: x["top"])
        #     headers.append(combine_dicts(sorted_words))

    final_head=[]
    for head in headers:
        if head:
            final_head.append(head)

    return final_head,table_line_words


def group_words_into_columns(ocr_boxes, col_threshold=5,api=False):
    """
    Groups OCR words into columns based on their left and right positions.

    :param ocr_boxes: List of OCR word dictionaries containing 'left', 'right', 'word'.
    :param col_threshold: Maximum horizontal overlap distance to consider words in the same column.
    :return: Dictionary where keys are column indices and values are lists of words in each column.
    """
    
    # Sort words by left position
    ocr_boxes.sort(key=lambda box: box['left'])
    
    columns = []
    
    for box in ocr_boxes:
        left, right, word = box['left'], box['right'], box
        
        added_to_column = False
        
        for column in columns:
            first_word = column[0]
            
            # If this word overlaps significantly with an existing column
            if abs(first_word['left'] - left) <= col_threshold or (left <= first_word['right'] and right >= first_word['left']):
                column.append(box)
                added_to_column = True
                break
        
        # If no suitable column found, create a new column
        if not added_to_column:
            columns.append([box])


    columns_=[]
    for column_ in columns:
        temp=[]
        for col in column_:
            if col not in temp:
                temp.append(col)
        columns_.append(temp)
    
    final_columns=[]
    all_column={}
    for column in columns_:
        final_columns.append(combine_dicts(column))
        all_column[combine_dicts(column)['word']+str(combine_dicts(column)['left'])]=column

    print(all_column)

    final=[]
    final_all={}
    out=preparing_ocr_parsed_data_1(final_columns,True)
    for ind in out:
        if ind:
            final.append(combine_dicts(ind))
            final_all[combine_dicts(ind)['word']]=[]

            for head_i in ind:
                final_all[combine_dicts(ind)['word']].extend(all_column[head_i['word']+str(head_i['left'])])

    print(final_all)
    
    all_heads_final=[]
    for temp_head,words in final_all.items():
        head_line=line_wise_ocr_data(words)
        temp=[]
        for line in head_line:
            line = sorted(line, key=lambda x: x["left"])
            temp.append(combine_dicts(line))
        
        all_heads_final.append(combine_dicts(temp))
    print(all_heads_final)

    if api:
        return all_heads_final,final_all
    return all_heads_final


def check_rows(line):

     # File paths for the trained model and vectorizer
    model_filename = f"/var/www/extraction_api/app/extraction_folder/table_rows_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/table_rows_count_vectorizer.joblib"
    
    try:
        # print(F"file found and file name is {vectorizer_filename},{model_filename}")
        # Load the trained model
        clf = joblib.load(model_filename)

        # Load the vectorizer used to convert words into numerical features
        vectorizer = joblib.load(vectorizer_filename)
    except:
        # print(F"file not found and file name is {vectorizer_filename},{model_filename}")
        return {}
    try:
        # Transform the single word into numerical features
        X_inference = vectorizer.transform([line])  # Model expects a list

        # Predict the label for the word
        prediction = clf.predict(X_inference)

        print(f"Predicted label for '{line}' is: {prediction[0]}")
        if prediction == 'rows':
            return True  # Return the predicted label
        else:
            return False 
    except Exception as e:
        print(f"Prediction error: {e}")
        return False


# Function to find the best matching standard header using SequenceMatcher
def get_standard_header(header, standard_map, threshold=0.8):
    best_match = None
    best_score = 0.0

    for standard, variations in standard_map.items():
        for variation in variations:
            # print(f"variation is {variation} and {header}")
            score = SequenceMatcher(None, header.lower(), variation.lower()).ratio()
            if score > best_score and score >= threshold:
                best_score = score
                best_match = standard

    return best_match if best_match else header  # Return best match or original if no match found
    

def get_restructed_table(table,template_db,trained_map=[],header_raw=[]):

    print(f"table is {table}")
    print(f"trained_map is {trained_map}")

    query =f"SELECT * FROM blue_keywords"
    data=template_db.execute_(query).to_dict(orient='records')
    
    standard_header_map={}
    for re in data:
        if re['keywords']:
            standard_header_map[re['column_name']]=json.loads(re['keywords'])
        else:
            standard_header_map[re['column_name']]=[]

    # Iterate through all headers and find the best match
    if not trained_map:
        renamed_headers = {}
        used_standard_headers = set()
        
        for header in table["headers"]:
            standard_header = get_standard_header(header, standard_header_map)
            
            if standard_header in used_standard_headers:
                renamed_headers[header] = header  # Handle conflict (you can assign a different value)
            else:
                renamed_headers[header] = standard_header
                used_standard_headers.add(standard_header)
    else:
        standard_header_map={}
        for header,mapped in trained_map.items():
            standard_header_map[header]=[mapped]
        if header_raw:
            renamed_headers= trained_map
        else:
            renamed_headers= {header: get_standard_header(header, standard_header_map) for header in table["headers"]}
    
    print(F'renamed_headers is {renamed_headers}')

    # Updating the table headers
    if header_raw:
        for header in header_raw:
            try:
                table["headers"] = renamed_headers[header]
            except:
                table["headers"]=header
    else:
        table["headers"] = [renamed_headers[header] for header in table["headers"]]

    # Updating the rows with renamed header
    updated_rows = []
    for row in table["rows"]:
        print(F"row is {row}")
        updated_row={}
        for key, value in row.items():
            try:
                updated_row[renamed_headers[key]]=value
            except:
                updated_row[key]=value
            
        updated_rows.append(updated_row)
    
    table["rows"] = updated_rows

    print(f"renamed_headers is {renamed_headers}")
    
    return table,renamed_headers


def find_second_lines(tables,ocr_rows):

    rows=tables['rows']
    headers=tables['headers']
    header_words=[header['word'] for header in headers]
    print(F"rows for checking is {rows}")

    new_rows_created=[]
    for i in range(len(rows)):
        new_row={}
        
        rows[i] = {k: rows[i][k] for k in header_words if k in rows[i]}

        current_row_=list(rows[i].values())

        current_row=[]
        for f in range(len(current_row_)):
            if current_row_[f] not in headers:
                current_row.append(current_row_[f])
            else:
                current_row.append(current_row_[f])

        if i+1>=len(rows):
            next_row=[]
        else:
            next_row_=list(rows[i+1].values())
            next_row=[]
            for f in range(len(next_row_)):
                # print(F"next_row_ is {next_row_[f]} and {headers}")
                if next_row_[f] not in headers:
                    next_row.append(next_row_[f])

        temp_current=[]
        for word in current_row:
            if word not in headers:
                temp_current.append(word)
        print(F"temp_current is  without headers is {temp_current}")
        if temp_current and isinstance(temp_current[0], dict):
            current_page = temp_current[0].get('pg_no')
            current_top = max((w for w in temp_current if isinstance(w, dict) and "top" in w), key=lambda w: w["top"])["top"]

        # if temp_current:
        #     current_page=temp_current[0]['pg_no']
        #     current_top = max(
        #             (w for w in temp_current if "top" in w), 
        #             key=lambda w: w["top"]
        #         )["top"]
        
            print(F"current_row is {current_row} and next_row is {next_row}")
            if next_row and isinstance(next_row, list) and isinstance(next_row[0], dict) and 'pg_no' in next_row[0]:
                if next_row[0]['pg_no'] != temp_current[0]['pg_no']:
                    next_top = 1000
                else:
                    try:
                        next_top = min(
                            (w for w in next_row if isinstance(w, dict) and "top" in w and w not in headers),
                            key=lambda w: w["top"]
                        )["top"]
                    except (ValueError, TypeError):
                        next_top = 1000
            else:
                next_top = 1000

            # if next_row:
            #     if next_row[0]['pg_no']!=temp_current[0]['pg_no']:
            #         next_top=1000
            #     else:
            #         next_top = min(
            #                 (w for w in next_row if "top" in w  and w not in  headers), 
            #                 key=lambda w: w["top"]
            #             )["top"]
            # else:
            #     next_top=1000
            
            print(F"current_top is {current_top} and next_top is {next_top}")

            for found_lines_ in ocr_rows:
                
                if not found_lines_:
                    continue

                if found_lines_[0]['pg_no']!=current_page:
                    print(F"page number is not same so skipping {found_lines_[0]['pg_no']} and {current_page}")
                    continue

                sorted_words = sorted(found_lines_, key=lambda x: (x["pg_no"], x["top"]))
                if not sorted_words:
                    continue

                found_lines=row_wise_table(sorted_words)

                for line in found_lines:
                    # print('line next_top current_top',line[0],'next_top',next_top,'current_top',current_top)
                    if next_top>line[0]['top']>current_top:
                        print('line',line)
                        out_line=(preparing_ocr_parsed_data_1(line,api_3=True))
                        print('out_line',out_line)
                        out_lines=[]
                        for lists in out_line:
                            combined_result = combine_dicts(lists)
                            out_lines.append(combined_result)
                        print('out_lines',out_lines)

                        for j in range(len(current_row)):

                            current_row_word=current_row[j]
                            if not current_row_word:
                                continue

                            temp_cu=re.sub(r'[^a-zA-Z-/]', '', current_row_word['word'])
                            if not temp_cu:
                                # print(F"new_row is {new_row} {headers[j]}")
                                new_row[headers[j]['word']] = current_row_word
                                continue

                            if j+1==len(current_row):
                                next_left=1000
                            else:
                                next_left=current_row[j+1]['left']
                            if j==0:
                                previous_right=current_row_word['left']

                            else:
                                previous_right=current_row[j-1]['right']

                            print(f"previous_right is {previous_right} and next_left is {next_left}")

                            temp_word=[current_row_word]

                            for h in range(len(out_lines)):
                                current_word=out_lines[h]
                                if current_word['left']>=previous_right and current_word['right']<=next_left:
                                    print(f"current_word is {current_word} add on for current_row_word is {current_row_word}")
                                    check=re.sub(r'[^a-zA-Z0-9]', '', current_word['word'])
                                    if abs(temp_word[-1]['bottom']-current_word['top'])<=3 and check:
                                        temp_word.append(current_word)

                            current_row[j]=combine_dicts(temp_word)
                            new_row[headers[j]['word']]=combine_dicts(temp_word)
                            # print(F"new_row is {new_row}")

                if not new_row:
                    new_row=rows[i]
                    print(F"new_row is {new_row}")


            new_rows_created.append(new_row)
            print(F"new_row is {new_row}")        
                    
    table={
        'headers': headers,
        'rows': new_rows_created
    }

    headers=[]
    for head in table['headers']:
        headers.append(head['word'])
    print(F"headers is {headers}")
    
    new_rows_final=[]
    for row in table['rows']:
        temp_row={}
        for head in headers:
            if head in row and row[head]['word'] not in headers:
                temp_row[head]=row[head]['word']
            else:
                temp_row[head]=''
        new_rows_final.append(temp_row)  
    print(F"new_rows_final is {new_rows_final}")

    table_word={
        'headers': headers,
        'rows': new_rows_final,
        'cor_table':table
    }       

    return table,table_word


def group_words_by_proximity_extra(words):
    
    words = sorted(words, key=lambda w: (w['top'], w['left']))

    grouped_words = []
    picked=[]
    lines=line_wise_ocr_data(words)
    for line in lines:
        grouped_words.append(line)

    return grouped_words


def get_grouped_headers_extra(page_ocr_data,headers_got):

    print(F"headers_got is {headers_got}")
    headers=[]
    for header_word in headers_got:
        cleaned_header_word=re.sub(r'[^a-zA-Z]', '', header_word)
        for word in page_ocr_data:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            matcher = SequenceMatcher(None, cleaned_header_word, cleaned_word)
            # print(f'cleaned_header_word is matcher is {matcher} {cleaned_header_word} and cleaned_word is {cleaned_word}')
            similarity_ratio_con = matcher.ratio()
            if similarity_ratio_con>0.8:
                headers.append(word)
    print(F"headers is {headers}")
    if len(headers)>1:
        grouped_headers=group_words_by_proximity_extra(headers)
    else:
        grouped_headers=[headers]

    # print(f" ############ here Grouped headers are {grouped_headers}")

    final_head_groups=[] 
    head_join=' '.join(headers_got)
    for group in grouped_headers:
        if not group:
            continue
        group_words=[]
        for gr_word in group:
            group_words.append(gr_word["word"])
        # print(f" ############ here group_words is {group_words}")

        group_join=' '.join(group_words)
        group_dict=combine_dicts(group)
        group_dict['word']=group_join
        final_head_groups.append(group_dict)
        # print(f" ############ here group_dict is {group_dict}")

    # print(f" ############ here head_join is {head_join}")
    # print(f" ############ here final_head_groups is {final_head_groups}")

    return head_join,final_head_groups


def find_extra_columns(mapping,ocr_rows,table,table_word):

    print(F"mapping got is {mapping}") 

    table_rows=table['rows']
    table_head=table['headers']
    table_word_rows=table_word['rows']
    print(F"table_word_rows got is {table_word_rows}") 
    table_word_head=table_word['headers']

    found_column=[]
    found_head={}
    for word,position in mapping.items():

        posiz=position['position']
        map_col=position['mapped_column']
        found_column.append(map_col)
        found_words=[]
        for page in ocr_rows:
            postion_ocr_words=[word['word'] for word in position['ocr'] if re.sub(r'[^a-zA-Z]', '', word['word'])]
            head_join,final_head_groups=get_grouped_headers_extra(page,postion_ocr_words)
            print(F"final_head_groups is {final_head_groups} {combine_dicts(position['ocr'])['word']}") 
            lines_words=line_wise_ocr_data(final_head_groups)
            for lin_word in lines_words:
                matcher = SequenceMatcher(None, combine_dicts(lin_word)['word'],re.sub(r'[^a-zA-Z]', '', combine_dicts(position['ocr'])['word']))
                similarity_ratio_col = matcher.ratio()
                if similarity_ratio_col>0.9:
                    found_words.append(combine_dicts(lin_word))
        print(F"found_words is {found_words}") 

        for found_word in found_words:
            found_head[map_col]=found_word
            min_thr=100
            temp_line={}
            for found_lines_ in ocr_rows:
                sorted_words = sorted(found_lines_, key=lambda x: (x["pg_no"], x["top"]))
                if not sorted_words:
                    continue
                found_lines=line_wise_ocr_data(sorted_words)
                for line in found_lines:
                    line_box=combine_dicts(line)
                    if abs(line_box['top'] - found_word['top'])<min_thr and line_box['pg_no'] == found_word['pg_no']:
                        min_thr=abs(line_box['top'] - found_word['top'])
                        temp_line=line
            print(F"temp_line is {temp_line} forthe kwy  {found_word['word']}") 

            if temp_line:
                temp_line=sorted(temp_line, key=lambda x: x["left"])

                postion_words=[word['word'] for word in position['ocr']]
                tem=[]
                for wor in temp_line:
                    if wor['left'] > found_word['right'] and wor['word'] not in postion_words:
                        if tem: 
                            if abs(tem[-1]['right']-wor['left'])<20:
                                tem.append(wor)
                        else:
                            tem.append(wor)

                print(F"tem is {tem}") 
                val=combine_dicts(tem)

                temp_row='None'
                index=0
                min_thr = 10000
                for index, row in enumerate(table_rows):
                    temp_r=[]
                    for r in row.values():
                        if r not in table_head:
                            temp_r.append(r)
                    row_box = combine_dicts(temp_r)
                    print(f'row_box is {row_box} and val is {val}')
                    if posiz == 'top':
                        if abs(row_box['bottom'] - val['top']) < min_thr and row_box['top'] > found_word['top'] and row_box['pg_no'] == found_word['pg_no'] :
                            min_thr = abs(row_box['bottom'] - val['top'])
                            temp_row = index
                    else:
                        if abs(row_box['bottom'] - val['top']) < min_thr and row_box['top'] < found_word['top']  and row_box['pg_no'] == found_word['pg_no'] :
                            min_thr = abs(row_box['bottom'] - val['top'])
                            temp_row = index
                print(F"temp_row is {temp_row}")
                if temp_row != 'None':
                    table_rows[temp_row][map_col]=val
                    table_word_rows[temp_row][map_col]=val['word']
                    print(F"temp_row after adding new able_rows[temp_row] {table_rows[temp_row]}") 

    print(F"final after adding new columns table_rows {table_rows}")

    for column in found_column:
        if column not in table_word_head:
            table_word_head.append(column)
        for row in table_word_rows:
            if column not in row:
                row[column]=''

    table_cord={
        'headers':table_head,
        'rows':table_rows
    }
    
    table_word={
        'headers':table_word_head,
        'rows':table_word_rows,
        'cor_table':table_cord
    }

    print(F"final after adding new columns table_word_head {table_word_head}")
    print(F"final after adding new columns table_word_rows {table_word_rows}")
    
    return table,table_word


from itertools import product
from difflib import SequenceMatcher

def is_close(w1, w2, x_thresh=50, y_thresh=20):
    """
    Returns True if w1 and w2 are spatially close to each other, 
    considering their bounding boxes and a given threshold.
    """
    # Calculate widths and heights
    width1 = w1['right'] - w1['left']
    width2 = w2['right'] - w2['left']
    height1 = w1['bottom'] - w1['top']
    height2 = w2['bottom'] - w2['top']
    
    # Expand bounding boxes by threshold
    expanded_left1 = w1['left'] - x_thresh
    expanded_right1 = w1['right'] + x_thresh
    expanded_top1 = w1['top'] - y_thresh
    expanded_bottom1 = w1['bottom'] + y_thresh

    # Check for overlap with w2's bounding box
    horizontal_overlap = not (w2['right'] < expanded_left1 or w2['left'] > expanded_right1)
    vertical_overlap = not (w2['bottom'] < expanded_top1 or w2['top'] > expanded_bottom1)

    return horizontal_overlap and vertical_overlap


def all_words_close(word_list, x_thresh=50, y_thresh=20):
    for i in range(len(word_list)):
        if not any(is_close(word_list[i], word_list[j], x_thresh, y_thresh) for j in range(len(word_list)) if i != j):
            return False  # if a word has no close neighbors
    return True

def form_valid_clusters(matched_words_dict):
    master_keys = list(matched_words_dict.keys())

    # Generate all possible combinations: one from each master
    combinations = list(product(*(matched_words_dict[k] for k in master_keys)))

    valid_clusters = []
    for combo in combinations:
        if all_words_close(combo):
            valid_clusters.append(list(combo))
        elif len(combo) == 1:
            valid_clusters.append(list(combo))

    return valid_clusters

def get_grouped_headers_head(page_ocr_data,headers_got):

    print(F"headers_got is {headers_got}")
    headers={}
    for header_word in headers_got:
        headers[header_word]=[]
        cleaned_header_word=re.sub(r'[^a-zA-Z]', '', header_word)
        for word in page_ocr_data:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            matcher = SequenceMatcher(None, cleaned_header_word, cleaned_word)
            # print(f'cleaned_header_word is matcher is {matcher} {cleaned_header_word} and cleaned_word is {cleaned_word}')
            similarity_ratio_con = matcher.ratio()
            if similarity_ratio_con>0.8:
                headers[header_word].append(word)
    print(F"headers is {headers}")
   
    grouped_headers=form_valid_clusters(headers)
   
    # print(f" ############ here Grouped headers are {grouped_headers}")

    final_head_groups=[] 
    head_join=' '.join(headers_got)
    for group in grouped_headers:
        if not group:
            continue

        lines=line_wise_ocr_data(group)
        temp_head=[]
        for lin in lines:
            lin = sorted(lin, key=lambda x: x["left"])
            temp_head.append(combine_dicts(lin))
        sorted_words = sorted(temp_head, key=lambda x: x["top"])
        final_head_groups.append(combine_dicts(sorted_words))

    # print(f" ############ here head_join is {head_join}")
    print(f" ############ here final_head_groups is {final_head_groups}")

    return head_join,final_head_groups


def get_smallest_area_box(word_boxes):
    """
    Returns the word box with the smallest area from the list.
    Area is calculated as width * height.
    """
    if not word_boxes:
        return None

    # Calculate area for each box and find the smallest one
    smallest_box = min(word_boxes, key=lambda box: box['width'] * box['height'])
    return smallest_box


def find_accurate_table_header(ocr_data,words):

    min_thr=0.85
    possible_header=[]
    for page in ocr_data:

        # print('page',page)
        fullheader,groups=get_grouped_headers_head(page,words)
        print(f'found extra header groups are {groups}')

        for group in groups:
            if 'word' not in group:
                continue
            cleaned_word=re.sub(r'[^a-zA-Z]', '', group['word'])
            joined_column_clean=re.sub(r'[^a-zA-Z]', '', fullheader)
            matcher = SequenceMatcher(None, cleaned_word, joined_column_clean)
            similarity_ratio_con = matcher.ratio()

            if similarity_ratio_con>min_thr:
                possible_header.append(group)
    
    return get_smallest_area_box(possible_header)


def find_extra_headers(second_headers,ocr_data):

    found_extra_headers={}

    for extra_head in second_headers:

        print(f'finding extra header is {extra_head}')

        ocr=extra_head['croppedOcrAreas']
        mapped_head=extra_head['value']

        words=[word['word'] for word in ocr if re.sub(r'[^a-zA-Z]', '', word['word'])]
        print(f'header words aree {words}')

        possible_header=find_accurate_table_header(ocr_data,words)

        if possible_header:
            found_extra_headers[mapped_head]=possible_header

    print(f'final extra header groups are {found_extra_headers}')

    
    second_header_box= combine_dicts(list(found_extra_headers.values()))

    return found_extra_headers,second_header_box


def contains_amount(line):
    """
    Check if the line contains at least one amount.
    Recognizes formats like: $123.45 or 123.45
    """
    return bool(re.search(r'(?<![\w/])\$?\d+(\.\d{2})?(?![\w/])', line))


def add_table_rows(temp,temp_word,table,table_word,second_headers_cord,second_headers,second_header_rows):

    table_rows=table['rows']
    table_rows_word=table_word['rows']
    table_header=table['headers']
    print(f'table_header is {table_header}')
    print(f'second_headers_cord is {second_headers_cord}')
    table_header_word=table_word['headers']

    row_boxes=[]
    for row in table_rows:
        tem=[]
        for key,val in row.items():
            if key != val['word']:
                tem.append(val)
        row_boxes.append(combine_dicts(tem))

    print(f'table_rows is {table_rows}')
    print(f'table_rows_word is {table_rows_word}')
    print(f'row_boxes is {row_boxes}')

    for i in range(len(temp)):  
        temP_box=combine_dicts(list(temp[i].values()))
        print(f'temP_box is {temP_box}')

        if not temP_box:
            continue

        for j in range(len(row_boxes)):

            print(f'row_box is {row_boxes[j]}')
            print(f'temp is {temp[i]}')
            print(f'temp_word is {temp_word[i]}')
            print(f'table_rows_word is {table_rows_word[j]}')
            print(f'table_rows is {table_rows[j]}')

            if j+1>=len(row_boxes):
                table_rows[j].update(temp[i])
                table_rows_word[j].update(temp_word[i])
                break

            row_top=row_boxes[j]['top']
            if second_header_rows == 'even':
                if temP_box['pg_no'] == row_boxes[j+1]['pg_no']:
                    next_row_top=row_boxes[j+1]['top']
                else:
                    next_row_top=1000
            else:
                if j==0:
                    prev_row_top=0
                elif temP_box['pg_no'] == row_boxes[j-1]['pg_no']:
                    prev_row_top=0
                else:
                    prev_row_top=row_boxes[j-1]['top']

            if second_header_rows == 'odd':
                if row_boxes[j]['top']>temP_box['top']>prev_row_top and temP_box['pg_no']==row_boxes[j]['pg_no']:
                    table_rows[j].update(temp[i])
                    table_rows_word[j].update(temp_word[i])
                    break
            else:
                if row_boxes[j]['top']<temP_box['top']<next_row_top and temP_box['pg_no']==row_boxes[j]['pg_no']:
                    table_rows[j].update(temp[i])
                    table_rows_word[j].update(temp_word[i])
                    break

    table_header_word.extend(second_headers)
    table_header.extend(second_headers_cord)

    print(f'table_header is {table_header}')
    print(f'second_headers_cord is {second_headers_cord}')

    print(f'new_header_cord is {table_header_word}')
    print(f'new_header is {table_header}')

    print(f'new_header_cord is {table_rows}')
    print(f'new_header is {table_rows_word}')

    new_table = {
        "headers": table_header,
        "rows": table_rows
    }
    new_table_word = {
        "headers": table_header_word,
        "rows": table_rows_word,
        "cor_table":new_table
    }

    return new_table,new_table_word




def contains_date(text: str) -> bool:
    """
    Checks if a string contains any common date formats.
    Returns True if a date-like pattern is found.
    """
    # Common regex patterns for dates (with optional whitespace around separators)
    date_patterns = [
        r"\b\d{1,2}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{2,4}\b",  # 06 / 25 / 25 or 12-05-2025
        r"\b\d{4}\s*[/-]\s*\d{1,2}\s*[/-]\s*\d{1,2}\b",    # 2025-08-20
        r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},?\s+\d{2,4}\b",  # Aug 20, 2025
        r"\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?,?\s+\d{2,4}\b",  # 20 Aug 2025
        r"\b\d{8}\b",                                      # 20250820 or 20052025
    ]
    
    for pattern in date_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return True
    return False


def extract_table_from_header(extraction_db,template_db,case_id,ocr_data, final_head, table_headers_line, top, bottom=1000,cord=[],trained_map=[],api = False,bottom_page=False,extra_variables=[],no_header_columns=[],found_extra_headers=[],second_header_box={}):
    """
    Extracts table data from OCR words starting from a predicted header. The function looks for the header
    and then traverses down, collecting rows of table data until it encounters an empty line, a large gap,
    or other stopping conditions.

    Args:
        words_ (list): List of dictionaries containing OCR data for words.
        div (str): Division identifier.
        seg (str): Segment identifier.

    Returns:
        list: Extracted table rows.
    """
    table_rows=[]
    second_table_rows=[]

    print(f"bottom found is" ,bottom)
    print(f"found_extra_headers found is" ,found_extra_headers)
    print(f"second_header_box found is" ,second_header_box)
    print(f"top found is" ,top)

    final_head=sorted(final_head, key=lambda x: x["left"])
    print(f"the final head is:::::::{final_head}")

    # Step 2: Process rows below the header
    table_rows.append(final_head)
    previous_bottom = combine_dicts(final_head)["top"]
    print(f"the prevous bottom is:::::{previous_bottom}")

    second_header_rows='odd'
    if second_header_box and previous_bottom < second_header_box['top']:
        second_header_rows='even'

    current_line = []
    current_page=combine_dicts(final_head)["pg_no"]
    start_page=combine_dicts(final_head)["pg_no"]

    ranges,ranges_left=get_ranges(final_head)

    rows_count=0

    for found_lines_ in ocr_data:

        sorted_words = sorted(found_lines_, key=lambda x: (x["pg_no"], x["top"]))
        if not sorted_words:
            continue
        found_lines=line_wise_ocr_data(sorted_words)
        exe_range=0
        for line in found_lines:

            need_skip=False

            # if line[0]['pg_no'] < current_page:
            #     print(F"page is samller so breaeking here current_page {line[0]['pg_no']} and pre {current_page}")
            #     break

            if line[0]['pg_no'] != current_page:
                previous_bottom=0
                top=0
                print(F"New page started so setting previous bottom {previous_bottom} here current_page {line[0]['pg_no']} and pre {current_page}")
                current_page=line[0]['pg_no']

            if line[0]['top']>bottom+10:
                if bottom_page and line[0]['pg_no'] ==bottom_page:
                    print(F"its exceding the bottom of the last page {line[0]} bottom_page {bottom_page} so we are ending the table")
                    break
                elif not bottom_page:
                    print(F"its exceding the bottom of the last page {line[0]} so we are ending the table")
                    break
            elif bottom_page and line[0]['pg_no'] > bottom_page:
                print(F"its exceding the bottom of the last page {line[0]} bottom_page {bottom_page} so we are ending the table")
                break  

            
            if line[0]['top']<top:
                print(F"here is line[0]['top'] {line[0]['top']} is less than header top {top}")
                if (start_page and line[0]['pg_no'] == start_page):
                    print(f"skiip if ad start_page {start_page} and line[0][pg_no] {line[0]['pg_no']}")
                    need_skip=False
                elif not start_page:
                    print(f"breaking if ad start_page {start_page} and line[0][pg_no] {line[0]['pg_no']}")
                    need_skip=True

            line_box=combine_dicts(line)

            if not line_box:
                continue
            

            for head in table_headers_line:
                matcher = SequenceMatcher(None, line_box['word'], head)
                similarity_ratio_col = matcher.ratio()
                if similarity_ratio_col>0.8:
                    print(F"we found simialr like {head} so skiping {line_box['word']}")
                    need_skip=True
                    break

            if line in table_rows:
                print("_______________________")
                need_skip=True

            if not current_line and len(line)<2:
                print("#########################")
                need_skip=True

            if need_skip:
                print("$$$$$$$$$$$$$$$$$$$$")
                continue
            # print("\n--- Checking line position ---")
            # print(f"Line text           : {line_box['word']}")
            # print(f"Line top            : {line_box['top']}")
            # print(f"Header bottom       : {combine_dicts(final_head)['bottom']}")
            # print(f"Threshold (bottom-1): {combine_dicts(final_head)['bottom'] - 1}")
            # print(f"Condition check     : {line_box['top']} >= {combine_dicts(final_head)['bottom'] - 1}")

            print(F"current line is{line_box['word']} and {line_box['top']} and  previous_bottom is {previous_bottom}")

            if line_box["top"] >= previous_bottom:
                print(F"line is in if condition {line_box['word']}")
                
                if not check_rows(line_box['word']) or not contains_amount(line_box['word']) or (not contains_date(line_box['word']) and second_header_box): 

                    if second_header_box:
                        print(f'we are here for lable {second_header_box},{line_box}')
                        if second_header_box['left']-20<line_box['left'] and second_header_box['right']+20>line_box['right']:
                            print(F"it is a second line here {line}")
                            second_table_rows.append(line)
                            rows_count =rows_count+1
                    print(F"avoiding not a proper lable for this is {line}")
                    continue
                
                rows_count =rows_count+1

                print(F"found is second_header_rows odd_or_even {second_header_rows,odd_or_even(rows_count)}")

                if second_header_box and second_header_rows == 'even' and odd_or_even(rows_count) == 'even':
                    print(F"second_header_box is {line_box['word']}")
                    second_table_rows.append(line)
                    continue
                elif second_header_box and second_header_rows == 'odd' and odd_or_even(rows_count) == 'odd':
                    print(F"second_header_box is {line_box['word']}")
                    second_table_rows.append(line)
                    continue

                # Otherwise, append the word to the current line
                exe_range=exe_range+1
                print(F"current line is{line_box['word']} appending to current line")
                current_line.append(line)
                previous_bottom = line_box["top"] ###added line bottom replaced to top

        if current_line:
            table_rows.extend(current_line)
            current_line=[]

    # Add the last line to the table if it exists
    if current_line:
        table_rows.extend(current_line)
    print(f"table_rows {table_rows}")

    print(f"second_table_rows are total {second_table_rows}")

    temp_table_rows=table_rows[:2]
    for row in table_rows[2:]:
        contains_total = any('total' in item['word'].lower() for item in row)
        contains_claim=any('claim' in item['word'].lower() for item in row)
        print(f"contains_total {contains_total} for {row}")
        if contains_total:
            continue
        elif contains_claim:
            break
        else:
            temp_table_rows.append(row)
    table_rows=temp_table_rows
    print(f"table_rows after removing total {table_rows}")

    table,table_word=convert_to_table_format_from_word(table_rows,no_header_columns)

    table,table_word=find_second_lines(table,ocr_data)

    if extra_variables:
        column_keys=extra_variables['colHeaderMapping']
        if column_keys:  
            smaple_row=combine_dicts(extra_variables['tableExtraRow']['croppedOcrAreas'])

            mapping={}
            for column_ke in column_keys:
                mapping[column_ke['key']]={'position':'','ocr':column_ke['croppedOcrAreas'],'mapped_column':column_ke['value']}

                ocr_box=combine_dicts(column_ke['croppedOcrAreas'])
                if smaple_row['top']<ocr_box['top']:
                    mapping[column_ke['key']]['position']='bottom'
                elif smaple_row['top']>ocr_box['top']:
                    mapping[column_ke['key']]['position']='top'
                
            table,table_word=find_extra_columns(mapping,ocr_data,table,table_word)

    if second_table_rows:
        all_temp=[]
        all_temp_word=[]

        second_headers = [item["word"] for item in list(found_extra_headers.values())]
        print(f"second_headers are {second_headers}")
        second_headers_cord = [item for item in list(found_extra_headers.values())]
        print(f"second_headers_cord are {second_headers_cord}")

        if trained_map:
            for mapped_sec,sec_header in found_extra_headers.items():
                trained_map[mapped_sec]=sec_header['word']
            print(f"updated trained map is {trained_map}")
    
        for second_row in second_table_rows:
            temp,temp_word=get_row_table(second_headers_cord,second_row,second=True)
            all_temp.append(temp)
            all_temp_word.append(temp_word)

        print(f"all_temp_word is {all_temp_word}")
        print(f"all_temp is {all_temp}")

        table,table_word=add_table_rows(all_temp,all_temp_word,table,table_word,second_headers_cord,second_headers,second_header_rows)
    
    if not api:
        table_word,renamed_headers=get_restructed_table(table_word,template_db,trained_map)

        print(renamed_headers)

        table_high=get_table_high(table,renamed_headers)

        swapped_dict = {v: k for k, v in renamed_headers.items()}

        table_word['renamed_headers'] = swapped_dict

        return table_word,table_high
    else:
        table_word,renamed_headers=get_restructed_table(table_word,template_db,trained_map)

        print(renamed_headers)

        table_high=get_table_high(table,renamed_headers)

        swapped_dict = {v: k for k, v in renamed_headers.items()}

        table_word['renamed_headers'] = renamed_headers

        return table_word,table_high,swapped_dict


def odd_or_even(num):
    return "even" if num % 2 == 0 else "odd"




@app.route("/extract_trained_table", methods=['POST', 'GET'])
def extract_trained_table():

    data=request.json
    
    # dynamic_fields = data.get("dynamic_fields_comp_data", {}).get("fields", {})
    # dynamic_fields_comp_data = data.get('dynamic_fields_comp_data',{})
    # training_data = dynamic_fields_comp_data.get('training_data',{})
    print(f"Data recieved in cropped_table {data}")
    if data is None:
        return {'flag': False}

    tenant_id = data['tenant_id']
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    check_no=data.get('check_no','')
    claim_id=data.get('claim_id','')
    session_id = data.get('session_id', None)
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    template_db=DB('template_db',**db_config)
    stats_db = DB('stats', **db_config)
    queue_db = DB('queues', **db_config)
    row_ocr=data.get('page_ocr',None)
    table_header=data.get('table_header',None)
    table_fotter=data.get('table_footer',None)
    update_training_data = data.get('update_training_data', False)
    
    top=table_header[0]['top']-20
    found_header,table_headers_line=extract_table_header([row_ocr],top, table_header)
    header_map={}
    found_footer=None
    if table_fotter:
        found_footer,table_foot_line=extract_table_header([row_ocr],top, table_fotter,True)
    if found_footer:
        bottom=found_footer[0]['top']
    else:
        bottom=1000

    table_word,table_high,renamed_headers=extract_table_from_header(extraction_db,template_db,case_id, [row_ocr], found_header, table_headers_line, top, bottom,api=True)
    response={"data":{"modified_table":table_word,"cor_table":table_high,"header_mapping":renamed_headers},"flag":True}
    #Add table_header and table_footer into training_data
    table_data_ = {}
    try:
        table_data_["table_header"] = data.get("table_header", [])
        table_data_["table_footer"] = data.get("table_footer", [])
        print(f"########### These are Table Header ----> {table_data_['table_header']}")
        print(f"########### These are Table Footer ----> {table_data_['table_footer']}")
    except Exception as e:
        print(f"the issue has occured::::::{e}")   
     
    if update_training_data:     
        try:
            query = f"SELECT TOP 1 case_id,table_trained_data FROM t2_audit WHERE case_id = '{case_id}' ORDER BY id DESC"
            res = stats_db.execute_(query)  
        except Exception as e:
            print(f"issue raied:::{e}")


        print("$$$$$$$$$$$$$$$$$$")

        table_getting_query = f"select fields from ocr_post_processed where case_id = %s and check_no = %s and patient_name = %s"
        params = [case_id, check_no, claim_id]
        table_getting_res = extraction_db.execute_(table_getting_query, params = params)

        try:
            final_data = {}
            fields_json = table_getting_res["fields"].iloc[0]
            fields = json.loads(fields_json)
            table_data = fields[0].get("table",{})
            if "table_1" in table_data:
                final_data = table_data["table_1"]
            elif "table_2" in table_data:
                final_data = table_data["table_2"]

            if "table" in final_data:
                final_data = final_data["table"]


        except Exception as e:
            final_data = {}


        if not res.empty:
            print(f"Updating existing t2_audit for case_id: {case_id}")
            print(f"The res is:::::{res}")
            # row_id = res["id"].iloc[0]

            # row_id = res.index[0]

            # Extract and parse the existing payload
            # existing_payload_str = res["table_trained_data"].iloc[0]
            # existing_payload = json.loads(existing_payload_str)
            existing_payload_str = res["table_trained_data"].iloc[0]
            if existing_payload_str:  # Check if it's not None
                existing_payload = json.loads(existing_payload_str)
            else:
                existing_payload = {}  # Initialize as empty if no prior data

            # Ensure check_no exists in the payload
            if check_no not in existing_payload:
                print(f" check {check_no} nor present")
                existing_payload[check_no] = {}

            # Ensure claim_id exists
            if claim_id not in existing_payload[check_no]:
                print(f"claim {claim_id} nor present")
                final_data_final = {"table":final_data,"header_mapping": renamed_headers}
                existing_payload[check_no][claim_id] = {"training_table": [final_data_final]}

            final_table_word = {"table":table_word,"header_mapping": renamed_headers}
            # Simple, safe way to ensure it's a list before appending
            if not isinstance(existing_payload[check_no][claim_id]["training_table"], list):
                existing_payload[check_no][claim_id]["training_table"] = [existing_payload[check_no][claim_id]["training_table"]]

            existing_payload[check_no][claim_id]["training_table"].append(final_table_word)
            # existing_payload[check_no][claim_id]["training_table"].append(table_word)

            # Convert back to JSON and update the database
            json_str = json.dumps(existing_payload)
            update_query = f"UPDATE t2_audit SET table_trained_data = %s WHERE case_id = %s"
            update_params = [json_str, case_id]
            stats_db.execute_(update_query, params=update_params)
            print(f"Updated table_trained_data for case_id: {case_id}")

        else:
            print("Entered the else condition ---> No existing record, creating new...")

            # Create new payload structure
            final_data_final = {"table":final_data,"header_mapping": renamed_headers}
            final_table_word = {"table":table_word,"header_mapping": renamed_headers}

            request_payload = {
                check_no: {
                    claim_id: {
                        "training_table": [final_data_final, final_table_word]
                    }
                }
            }

            print(f"request_payload::{request_payload}")
            insert_query = f"INSERT INTO t2_audit (table_trained_data, case_id) VALUES (%s, %s)"
            insert_params = [json.dumps(request_payload), case_id]
            stats_db.execute_(insert_query, params=insert_params)
            print(f"Inserted into t2_audit for case_id: {case_id}")


            ##########Adding the new queue into the systen that is t2 autit queue
            inserting_t2_queue = f"""INSERT INTO queue_list (case_id, document_id, parent_case_id, queue, task_id, parallel)
                            SELECT '{case_id}', '{case_id}', '{case_id}', 't2_audit_queue', '', 0
                            WHERE NOT EXISTS (
                                SELECT 1 FROM queue_list 
                                WHERE case_id = '{case_id}' AND queue = 't2_audit_queue'
                            )"""
            inserting_t2_queue_res = queue_db.execute_(inserting_t2_queue)
            print(f"########inserting the queue into the table -> {inserting_t2_queue_res}")
    else:
        print(f"######No training data updating only H, F and S performing")

    return jsonify(response)

    
    # query = f"SELECT request_payload FROM t2_audit WHERE case_id = '{case_id}'"
    # res = stats_db.execute(query)
    # query = "SELECT request_payload FROM t2_audit WHERE case_id = %s"
    # params = [case_id]
    # res = stats_db.execute(query, params=params)
    # if res:
    #     print(f"the res is:::::{res}")
    #     # Parse existing payload
    #     existing_payload_str = res["request_payload"].iloc[0]
    #     existing_payload = json.loads(existing_payload_str)
    
    # # Initialize the top-level key if not present
    #     if check_no not in existing_payload:
    #         existing_payload[check_no] = {
    #             "main_fields": {},
    #             "patients": {}
    #         }

    # # Ensure 'patients' exists
    #     if "patients" not in existing_payload[check_no]:
    #         existing_payload[check_no]["patients"] = {}

    #     # Update or insert the claim_id data
    #     if claim_id in existing_payload[check_no]["patients"]:
    #         # Only update training_table part
    #         existing_payload[check_no]["patients"][claim_id][0]["training_table"] = table_data
    #     else:
    #         # Insert new claim_id info
    #         existing_payload[check_no]["patients"][claim_id] = [{
    #             "fields": {},
    #             "training_table": table_data
    #         }]

    # # Save only the updated JSON back
    #     json_str = json.dumps(existing_payload)
    #     update_query = "UPDATE t2_audit SET request_payload = %s WHERE case_id = %s"
    #     params = [json_str, case_id]
    #     stats_db.execute(update_query, params=params)
    #     # update_query = f"UPDATE t2_audit SET request_payload = ? WHERE case_id = ?"
    #     # stats_db.execute(update_query, (json_str, case_id))
    # else:
    #     print(f"entered the elso coditions---> No existing record, create new:::::")
    #     # No existing record, create new
    #     request_payload = {
    #         check_no: {
    #             "main_fields": {},
    #             "patients": {
    #                 claim_id: [{
    #                     "fields": {},
    #                     "training_table": table_data
    #                 }]
    #             }
    #         }
    #     }

    #     print(f"request_payload::{request_payload}")
    #     query = "INSERT INTO t2_audit (request_payload, case_id) VALUES (%s, %s)"
    #     params = [json.dumps(request_payload), case_id]
    #     stats_db.execute(query, params=params)
    #     print(f"Inserted into t2_audit for case_id: {case_id}")

        
    # return jsonify(response)




# @app.route("/extract_trained_table", methods=['POST', 'GET'])
# def extract_trained_table():

#     data=request.json
#     print(f"Data recieved in cropped_table {data}")
#     if data is None:
#         return {'flag': False}

#     tenant_id = data['tenant_id']
#     case_id = data.get('case_id', None)
#     user = data.get('user', None)
#     session_id = data.get('session_id', None)
#     db_config['tenant_id']=tenant_id
#     extraction_db=DB('extraction',**db_config)
#     template_db=DB('template_db',**db_config)

#     db_config['tenant_id'] = tenant_id
#     row_ocr=data.get('page_ocr',None)
#     table_header=data.get('table_header',None)
#     table_fotter=data.get('table_footer',None)
    
#     top=table_header[0]['top']-20
#     found_header,table_headers_line=extract_table_header([row_ocr],top, table_header)
#     print(f"the found header is:::{found_header}")
#     header_map={}
#     found_footer=None
#     if table_fotter:
#         found_footer,table_foot_line=extract_table_header([row_ocr],top, table_fotter,True)
#     if found_footer:
#         bottom=found_footer[0]['top']
#     else:
#         bottom=1000

#     table_word,table_high,renamed_headers=extract_table_from_header(extraction_db,template_db,case_id, [row_ocr], found_header, table_headers_line, top, bottom,api=True)
#     response={"data":{"modified_table":table_word,"cor_table":table_high,"header_mapping":renamed_headers},"flag":True}

#     return jsonify(response)




def get_ocr_data_lines(words):
    
    # Sort words based on their 'top' coordinate
    sorted_words = sorted(words, key=lambda x: x["top"])

    # Group words on the same horizontal line
    line_groups = []
    current_line = []

    for word in sorted_words:
        if not current_line:
            # First word of the line
            current_line.append(word)
        else:
            diff = abs(word["top"] - current_line[0]["top"])
            if diff < 5:
                # Word is on the same line as the previous word
                current_line.append(word)
            else:
                # Word is on a new line
                line_groups.append(current_line)
                current_line = [word]

    # Add the last line to the groups
    if current_line:
        line_groups.append(current_line)

    for line in line_groups:
        line_words = [word["word"] for word in line]
        print(" ".join(line_words))

    return line_groups


def finding_possible_values(header,header_int,value_page_ocr_data):
    
    possibe_values=[]
    sorted_words = sorted(value_page_ocr_data, key=lambda x: x["top"])
    value_thr=header_int["value_thr_hor"]
    multi_line=header_int.get("multi_line",False)
    # print(F" value_thr is {value_thr}")
    postion_down=False
    try:
        if '~' in value_thr:
            postion_down=True
            value_thr=float(value_thr.replace('~', ''))
    except:
        pass
    min_col=100
    # print(f"line is ",value_page_ocr_data)
    sorted_words=get_ocr_data_lines(value_page_ocr_data)
    # print(F" postion_down is {postion_down}")
    for line in sorted_words:
        if line[0]['pg_no']!=header['pg_no']:
            continue
        if postion_down:
            if line[0]["top"] <= header["top"]+5:
                if header in line:
                    line.remove(header) 
                if line:
                    for wor in line:
                        value_column_thr_cal=abs(abs((wor["top"]+wor["bottom"])/2) - abs((header["top"]+header["bottom"])/2))
                        col_diff=abs(value_column_thr_cal-value_thr)
                        if min_col>col_diff:
                            possibe_values=[]
                            min_col=col_diff
                            for word in line:
                                possibe_values.append(word)
                        
        else:
            print(f'current line is {line[0]} and {header}')
            if line[0]["top"] >= header["top"]-5:
                if header in line:
                    line.remove(header) 
                if line:
                    for wor in line:
                        value_column_thr_cal=abs(abs((wor["top"]+wor["bottom"])/2) - abs((header["top"]+header["bottom"])/2))
                        print(F" value_column_thr_cal is {value_column_thr_cal}")
                        col_diff=abs(value_column_thr_cal-value_thr)
                        print(F" col_diff is {col_diff}")
                        if min_col>col_diff:
                            possibe_values=[]
                            min_col=col_diff
                            for word in line:
                                possibe_values.append(word)

    return possibe_values,multi_line


def is_valid_single_amount_text(text):

    text = re.sub(r'\$\s+', '$', text)
    
    # Match patterns like $123.45, ₹1000, or 123.45
    amount_pattern = re.compile(r'^[\$\₹]?\d+(?:\.\d+)?$')

    tokens = text.strip().split()
    valid_amounts = [token for token in tokens if amount_pattern.match(token)]

    # Case 1: No amount → Valid
    if not valid_amounts:
        return True

    # Case 2: Only one amount and all tokens match it → Valid
    return len(valid_amounts) == 1 and len(tokens) == 1


def from_highlights(words,confindance=100,table=False):
    try:
        temp = {}
        temp['word'] = words['word']
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
        temp['confidance']=words.get('confidance',100)

        if table:
            word=temp['word']
            if is_valid_single_amount_text(word):
                temp['confidance']=words.get('confidance',100)
            else:
                temp['confidance']=60
        else:
            temp['confidance']=confindance

        return  temp
    except:
        return {}


def horizontal_distance(word1, word2):
    if word2['left'] > word1['right']:
        return word1['right'] - word2['left']
    else:
        return word2['right'] - word1['left']


def vertical_distance(word1, word2):
    if word2['top']>word1['top']:
        return word2['top'] - word1['bottom']
    else:
        return word1['top'] - word2['bottom']


def calculate_value_distance(point1, point2):
    # print(point1,point2)
    x1 = (point1['right']+point1['left'])/2
    x2= (point2['right']+ point2['left'])/2
    distance = abs(x1-x2)
    return distance


def calculate_distance(point1, point2):
    # print(point1,point2)
    x1, y1 = point1['left'], point1['top']
    x2, y2 = point2['left'], point2['top']
    distance = math.sqrt((x2 - x1)**2 + (y2 - y1)**2)
    return distance


def group_words_by_proximity(words, y_threshold=15):
    
    words = sorted(words, key=lambda w: (w['top'], w['left']))

    grouped_words = []
    picked=[]
    for i in range(len(words)):
        if i+1 > len(words):
            break
        current_group=[]
        if words[i] not in picked:
            current_group.append(words[i])
            picked.append(words[i])
        for j in range(i+1,len(words)-1):
            if (words[i]["left"] <= words[j]["left"]<= words[i]["right"] or words[i]["left"] <= words[j]["right"] <= words[i]["right"]) and vertical_distance(words[i], words[j]) < y_threshold:
                if words[j] not in picked:
                    current_group.append(words[j])
                    picked.append(words[j])

        grouped_words.append(current_group)

    return grouped_words


def get_grouped_headers(page_ocr_data,headers_got):

    print(F"headers_got is {headers_got}")
    headers=[]
    for header_word in headers_got:
        cleaned_header_word=re.sub(r'[^a-zA-Z]', '', header_word)
        for word in page_ocr_data:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            matcher = SequenceMatcher(None, cleaned_header_word, cleaned_word)
            # print(f'cleaned_header_word is matcher is {matcher} {cleaned_header_word} and cleaned_word is {cleaned_word}')
            similarity_ratio_con = matcher.ratio()
            if similarity_ratio_con>0.8:
                headers.append(word)
    print(F"headers is {headers}")
    if len(headers)>1:
        grouped_headers=group_words_by_proximity(headers)
    else:
        grouped_headers=[headers]

    # print(f" ############ here Grouped headers are {grouped_headers}")

    final_head_groups=[] 
    head_join=' '.join(headers_got)
    for group in grouped_headers:
        if not group:
            continue
        group_words=[]
        for gr_word in group:
            group_words.append(gr_word["word"])
        # print(f" ############ here group_words is {group_words}")

        group_join=' '.join(group_words)
        group_dict=combine_dicts(group)
        group_dict['word']=group_join
        final_head_groups.append(group_dict)
        # print(f" ############ here group_dict is {group_dict}")

    # print(f" ############ here head_join is {head_join}")
    print(f" ############ here final_head_groups is {final_head_groups}")

    return head_join,final_head_groups


def finalise_headers(row_header_g,column_header_g,context_g,ocr_data_all,ocr_raw_pages):

    column_header={}
    row_header={}
    context={}

    final_headers={}
    min_thr=0.5

    if column_header_g:
        column_header_words=column_header_g['word']
    else:
        column_header_words=[]
    column_max_thr=0
    possible_column={}
    if row_header_g:
        row_header_words=row_header_g['word']
    else:
        row_header_words=[]
    row_max_thr=0
    possible_row_head={}
    if context_g:
        contexts_words=context_g['word']
        context_row_thr=context_g['con_row_thr']
        context_column_thr=context_g['con_col_thr']
        context_max_thr=0
    else:
        contexts_words=[]
    possible_context={}

    all_pairs_row_con=[]
    all_pairs_col_con=[]

    ind=-1
    for page_ocr_data in ocr_data_all:
        ind=ind+1

        # print(F"searching in page_ocr_data is {page_ocr_data}")
        
        column_headers=[]
        row_headers=[]
        contexts=[]
        tem_pair=[]

        if contexts_words:
            if 'ocr_data' not in context_g:
                joined_context,grouped_context=get_grouped_headers(page_ocr_data,contexts_words)
            else:
                joined_context,grouped_context=get_grouped_headers(ocr_raw_pages[ind],contexts_words)
        else:
            grouped_context=[]

        if row_header_words:
            if 'ocr_data' not in row_header_g:
                joined_row,grouped_row=get_grouped_headers(page_ocr_data,row_header_words)
            else:
                joined_row,grouped_row=get_grouped_headers(ocr_raw_pages[ind],row_header_words)
        else:
            grouped_row=[]
        if column_header_words:
            if 'ocr_data' not in column_header_g:
                joined_column,grouped_column=get_grouped_headers(page_ocr_data,column_header_words)
            else:
                joined_column,grouped_column=get_grouped_headers(ocr_raw_pages[ind],column_header_words)
        else:
            grouped_column=[]

        print(F" ################ \n")
        print(F" ################## grouped_column is {grouped_column}")
        for word in grouped_column:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            joined_column_clean=re.sub(r'[^a-zA-Z]', '', joined_column)
            if joined_column:
                matcher = SequenceMatcher(None, cleaned_word, joined_column_clean)
                similarity_ratio_con = matcher.ratio()
                # print(f"joined_context {joined_column} and cleaned_word is {cleaned_word} and similarity_ratio_con {similarity_ratio_con}")
                if similarity_ratio_con>min_thr:
                    if similarity_ratio_con>row_max_thr:
                        row_max_thr=similarity_ratio_con
                        possible_column=word
                    column_headers.append(word) 
        # print(F" ################## column_headers is {column_headers}")
        print(F" ################ \n")
        print(F" ################## grouped_row is {grouped_row}")
        for word in grouped_row:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            joined_row_clean=re.sub(r'[^a-zA-Z]', '', joined_row)
            if joined_row:
                matcher = SequenceMatcher(None, cleaned_word, joined_row_clean)
                similarity_ratio_col = matcher.ratio()
                print(f"joined_context {joined_row} and cleaned_word is {cleaned_word} and similarity_ratio_con {similarity_ratio_col}")
                if similarity_ratio_col>min_thr:
                    if similarity_ratio_col>column_max_thr:
                        column_max_thr=similarity_ratio_col
                        possible_row_head=word
                    row_headers.append(word)

        # print(F" ################## row_headers is {row_headers}")
        print(F" ################ \n")
        print(F" ################## grouped_context is {grouped_context}")
        for word in grouped_context:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            joined_context_clean=re.sub(r'[^a-zA-Z]', '', joined_context)
            if joined_context:
                matcher = SequenceMatcher(None, cleaned_word, joined_context_clean)
                similarity_ratio_con = matcher.ratio()
                print(f"joined_context {joined_context} and cleaned_word is {cleaned_word} and similarity_ratio_con {similarity_ratio_con}")
                if similarity_ratio_con>min_thr:
                    if similarity_ratio_con>context_max_thr:
                        context_max_thr=similarity_ratio_con
                        possible_context=word
                    contexts.append(word)
        # print(F" ################## contexts is {contexts}")
        print(F" ################ \n")
        

        if not row_headers and possible_row_head:
            row_headers.append(possible_row_head)

        if not column_headers and possible_column:
            column_headers.append(possible_column)

        if not contexts and possible_context:
            contexts.append(possible_context)

        tem_pair=None
        tem_pair_col=None
        if contexts:
            tem_col=1000
            tem_row=1000
            for context in contexts:
                # print(f"context is {context}")
                print(F" ################ \n")
                if row_headers:
                    for row in row_headers:
                        # print(f"row is {row}")
                        # print(F" ################ \n")
                        distance_calculated = calculate_distance(row, context)
                        # print(F"column__row_thr is {context_row_thr} and distance_calculated is {distance_calculated}")
                        if abs(distance_calculated-context_row_thr)<tem_row:
                            tem_row=abs(distance_calculated-context_row_thr)
                            tem_pair=[row,tem_row,context]
                if column_headers: 
                    for column in column_headers:   
                        # print(f"column is {column}")
                        # print(F" ################ \n")
                        distance_calculated = calculate_distance(column, context)
                        # print(F"context_column_thr is {context_column_thr} and distance_calculated is {distance_calculated}")
                        if abs(distance_calculated-context_column_thr)<tem_col:
                            tem_col=abs(distance_calculated-context_column_thr)
                            tem_pair_col=[column,tem_col,context]
        if tem_pair:
            all_pairs_row_con.append(tem_pair)
        if tem_pair_col:
            all_pairs_col_con.append(tem_pair_col)
        #here need to write the logic
    # print(F" ################ \n")
    # print(F" ################ \n")
    # print(f" all_pairs_row_con are {all_pairs_row_con}")
    # print(f" all_pairs_col_con are {all_pairs_col_con}")
    # print(F" ################ \n")
    # print(F" ################ \n")

    if all_pairs_row_con:
        sorted_list = sorted(all_pairs_row_con, key=lambda x: x[1])
        row_header=sorted_list[0][0]
        context=sorted_list[0][2]
    if all_pairs_col_con:
        column_header=all_pairs_col_con[0][0]

    final_headers['column_header']=column_header
    final_headers['row_header']=row_header
    final_headers['context']=context

    return final_headers



def calculate_confidence(actual, current):
    """
    Calculates confidence based on word position difference.
    The further the current position from the actual, the lower the confidence.
    Minimum confidence is capped at 40%.
    """
    if actual == 0:
        return 40  # Avoid division by zero

    max_offset = 20 # Offset at which penalty reaches max_penalty
    max_penalty = 37  # Max confidence drop allowed
    offset = abs(current - actual)
    
    # Proportional penalty, scaled to max 60% drop
    penalty = min((offset / max_offset) * max_penalty, max_penalty)

    confidence = 100 - penalty

    return round(max(confidence, 40), 2)


def calculate_cont_confidence(actual, current):
    """
    Calculates confidence based on word position difference.
    The further the current position from the actual, the lower the confidence.
    Minimum confidence is capped at 40%.
    """
    if actual == 0:
        return 40  # Avoid division by zero

    max_offset = 25 # Offset at which penalty reaches max_penalty
    max_penalty = 37  # Max confidence drop allowed
    offset = abs(current - actual)
    
    # Proportional penalty, scaled to max 60% drop
    penalty = min((offset / max_offset) * max_penalty, max_penalty)

    confidence = 100 - penalty

    return round(max(confidence, 40), 2)


def get_angle(target,origin):
    angleInRadians = math.atan2(target["top"] - origin["top"], target["left"] - origin["left"])
    angleInDegrees = angleInRadians * 180 / math.pi
    return angleInDegrees


def get_template_extraction_values(ocr_data_all_got,ocr_raw_pages,process_trained_fields,fields,section_fields=False,common_section=False,sub_section=False,common_section_fields=[]):

    row_headers = process_trained_fields['row_headers']
    column_headers = process_trained_fields['column_headers']
    contexts = process_trained_fields['contexts']

    row_headers_t2 = process_trained_fields['row_headers_t2']
    column_headers_t2 = process_trained_fields['column_headers_t2']
    contexts_t2 = process_trained_fields['contexts_t2']

    # row_headers,column_headers,contexts=process_trained_fields['row_headers',{}],process_trained_fields['column_headers',{}],process_trained_fields['contexts',{}]

    if column_headers:
        column_headers=json.loads(column_headers)
    else:
        column_headers={}
    if row_headers:
        row_headers=json.loads(row_headers)
    else:
        row_headers={}
    if contexts:
        contexts=json.loads(contexts)
    else:
        contexts={}

    if column_headers_t2:
        column_headers_t2=json.loads(column_headers_t2)
    else:
        column_headers={}
    if row_headers_t2:
        row_headers_t2=json.loads(row_headers_t2)
    else:
        row_headers_t2={}
    if contexts_t2:
        contexts_t2=json.loads(contexts_t2)
    else:
        contexts_t2={}
    
    print(f" ########### all contexts_t2 got are  {contexts_t2}")
    print(f" ###########mall row_headers_t2 got are  {row_headers_t2}")
    print(f" ########### all column_headers_t2 got are  {column_headers_t2}")

    print(f" ########### all contexts got are  {contexts}")
    print(f" ########### all row_headers got are  {row_headers}")
    print(f" ###########all  column_headers got are  {column_headers}")
    
    extracted_fields={}
    fields_highlights={}

    for field in fields:

        if field in common_section_fields and sub_section:
            continue

        if field not in common_section_fields and common_section:
            continue 

        ocr_data_all=copy.deepcopy(ocr_data_all_got)
        print(F" #####################")
        print(F" #####################")
        print(F" #####################")
        print(F" ##################### fiels is {field}")
        print(F" #####################")
        print(F" #####################")
        print(F" #####################")

        extracted_fields[field]=''

        row_header_int=row_headers.get(field,[])
        column_header_int=column_headers.get(field,[])
        context_int=contexts.get(field,[])
        
        row_header_int_t2=[]
        if row_headers_t2:
            row_header_int_t2=row_headers_t2.get(field,[])
        column_header_int_t2=[]
        if column_headers_t2:
            column_header_int_t2=column_headers_t2.get(field,[])
        context_int_t2=[]
        if contexts_t2:
            context_int_t2=contexts_t2.get(field,[])

        print(f" ########### row_header_int_t2 got are  {row_header_int_t2}")
        print(f" ########### column_header_int_t2 got are  {column_header_int_t2}")
        print(f" ########### context_int_t2 got are  {context_int_t2}")


        if row_header_int_t2 and context_int_t2:
            row_header_int=row_header_int_t2
            column_header_int=column_header_int_t2
            context_int=context_int_t2
        
        print(f" ########### row_header_int got are  {row_header_int}")
        print(f" ########### column_header_int got are  {column_header_int}")
        print(f" ########### context_int got are  {context_int}")

        context_box={}
        if 'context_box' in context_int:
            context_box=context_int['context_box']
        row_box={}
        if 'row_box' in row_header_int:
            row_box=row_header_int['row_box']
        value_box={}
        if 'value_box' in row_header_int:
            value_box=row_header_int['value_box']

        # print(F" ##################### len of ocr_data_all is {len(ocr_data_all)}")
        finalised_headers=finalise_headers(row_header_int,column_header_int,context_int,copy.deepcopy(ocr_data_all_got),copy.deepcopy(ocr_raw_pages))

        column_header=finalised_headers['column_header']
        row_header=finalised_headers['row_header']
        context=finalised_headers['context']
        print(f" ########### finalised_headers got are  {finalised_headers}")
        if not row_header:
            continue
    
        for ocr_word in ocr_data_all_got:
            if not ocr_word:
                continue
            if ocr_word[0]['pg_no'] == row_header['pg_no']:
                value_page_ocr_data=ocr_word
        
        if row_header:
            possible_values_row_got,multi_line=finding_possible_values(row_header,row_header_int,value_page_ocr_data)
            print(f" ################ possible_values_row got are {possible_values_row_got}")

        print(f" ################ value_box got are {value_box}")
        print(f" ################ row_box got are {row_box}")
        print(f" ################ context_box got are {context_box}")
        if value_box:
            possible_values_row=[]
            for value in possible_values_row_got:
                if value_box['left']>=row_box['left'] and value['left']+10>=row_header['left']:
                    possible_values_row.append(value)
                else:
                    possible_values_row.append(value)
            
            trained_angle_rv=get_angle(value_box,row_box)
            trained_angle_cv=get_angle(value_box,context_box)

        else:
            possible_values_row=possible_values_row_got
        print(f" ################ possible_values_row got are {possible_values_row}")

        max_calculated_diff=10000
        predicted={}

        for value in possible_values_row:

            calculated_diff=0
            if row_header:
                value_row_diff_cal=calculate_distance(row_header,value)
                print(f'value is {value["word"]}')
                row_act_confidence=value_row_diff_cal
                row_base_diff=row_header_int['value_thr']
                row_conf=calculate_confidence(row_base_diff,row_act_confidence)
                print(f'row_act_confidence is {row_act_confidence}')
                print(f'row_base_diff is {row_base_diff}')
                print(f'row_conf is {row_conf}')
                # print(f"################ calculated_col_diff is  {row_header_int['value_thr']} for {value_row_diff_cal}")
                calculated_row_diff=abs(row_header_int['value_thr']-value_row_diff_cal)
                calculated_diff=calculated_diff+calculated_row_diff
                # print(f"################ calculated_diff is  {calculated_row_diff} for {value}")

            if context:
                
                column_act_confidence=calculate_value_distance(context,value)
                con_base_diff=context_int['value_thr']
                con_conf=calculate_cont_confidence(con_base_diff,column_act_confidence)
            
                print(f'value is {value["word"]}')
                print(f'column_confidence is {column_act_confidence}')
                print(f'con_base_diff is {con_base_diff}')
                print(f'con_conf is {con_conf}')

                value_con_diff_cal=calculate_value_distance(context,value)
                calculated_con_diff=abs(context_int['value_thr']-value_con_diff_cal)
                calculated_diff=calculated_diff+calculated_con_diff
                # print(f"################ calculated_diff is  {calculated_con_diff} for {value}")

            # print(f"################ calculated_diff is  {calculated_diff} for {value}")
            if max_calculated_diff>calculated_diff:

                angle_cv=get_angle(value,context)
                if value_box:
                    confidence = 1 - (min(abs(trained_angle_cv - angle_cv), 360 - abs(trained_angle_cv - angle_cv)) / 180)
                    confidence_percent_cv = round(confidence * 100, 2)
                    print(f"here angle_cv {angle_cv} and cal diff is {trained_angle_cv}")
                    print(f"here confidence_percent is {confidence_percent_cv}")

                angle_rv=get_angle(value,row_header)
                if value_box:
                    confidence = 1 - (min(abs(trained_angle_rv - angle_rv), 360 - abs(trained_angle_rv - angle_rv)) / 180)
                    confidence_percent_rv = round(confidence * 100, 2)
                    print(f"here angle_cv {angle_rv} and cal diff is {trained_angle_rv}")
                    print(f"here confidence_percent is {confidence_percent_rv}")
                # print(f"################ final predict is  {predicted}")
                # print(f"################ multi_line is  {multi_line}")
                try:
                    if value_box:
                        confindance= min([row_conf,con_conf,confidence_percent_rv,confidence_percent_cv])
                    else:
                        confindance= min([row_conf,con_conf])
                except Exception as e:
                    print("exception occured:::::{e}")

                print(f"here base_diff {calculated_diff} and cal diff is {row_conf,con_conf}a and {confindance}")
                max_calculated_diff=calculated_diff
                predicted=value

        if multi_line:
            predicted=find_y_values(predicted,value_page_ocr_data,True)

        print(f"################ final predict is  {predicted}")
        print('#################')
        print('\n')
        if predicted:
            extracted_fields[field]=predicted['word']
            fields_highlights[field]=from_highlights(predicted)     

    return extracted_fields,fields_highlights


def check_headers(row_header_g,context_g,ocr_data_all,ocr_raw_pages):

    row_header={}
    context={}

    final_headers={}
    min_thr=0.85

    if row_header_g:
        row_header_words=row_header_g['word']
    else:
        row_header_words=[]
    possible_row_head={}

    if context_g:
        contexts_words=context_g['word']
        context_row_thr=context_g['con_row_thr']
    else:
        contexts_words=[]
    possible_context={}

    all_pairs_row_con=[]

    ind=-1
    for page_ocr_data in ocr_data_all:

        row_max_thr=0
        context_max_thr=0

        ind=ind+1

        # print(F"searching in page_ocr_data is {page_ocr_data}")
        
        row_headers=[]
        contexts=[]
        tem_pair=[]

        if contexts_words:
            if 'ocr_data' not in context_g:
                joined_context,grouped_context=get_grouped_headers(page_ocr_data,contexts_words)
            else:
                joined_context,grouped_context=get_grouped_headers(ocr_raw_pages[ind],contexts_words)
        else:
            grouped_context=[]

        if row_header_words:
            if 'ocr_data' not in row_header_g:
                joined_row,grouped_row=get_grouped_headers(page_ocr_data,row_header_words)
            else:
                joined_row,grouped_row=get_grouped_headers(ocr_raw_pages[ind],row_header_words)
        else:
            grouped_row=[]

        print(F" ################ \n")
        print(F" ################## grouped_row is {grouped_row}")
        for word in grouped_row:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            joined_row_clean=re.sub(r'[^a-zA-Z]', '', joined_row)
            if joined_row:
                matcher = SequenceMatcher(None, cleaned_word, joined_row_clean)
                similarity_ratio_col = matcher.ratio()
                print(f"joined_context {joined_row} and cleaned_word is {cleaned_word} and similarity_ratio_con {similarity_ratio_col}")
                if similarity_ratio_col>min_thr:
                    if similarity_ratio_col>row_max_thr:
                        row_max_thr=similarity_ratio_col
                        possible_row_head=word
        print(F" ################## row_headers is {row_headers}")
        
        print(F" ################ \n")
        print(F" ################## grouped_context is {grouped_context}")
        for word in grouped_context:
            cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
            joined_context_clean=re.sub(r'[^a-zA-Z]', '', joined_context)
            if joined_context:
                matcher = SequenceMatcher(None, cleaned_word, joined_context_clean)
                similarity_ratio_con = matcher.ratio()
                print(f"joined_context {joined_context} and cleaned_word is {cleaned_word} and similarity_ratio_con {similarity_ratio_con}")
                if similarity_ratio_con>min_thr:
                    if similarity_ratio_con>context_max_thr:
                        context_max_thr=similarity_ratio_con
                        possible_context=word
        print(F" ################## contexts is {contexts}")
        print(F" ################ \n")
        
        if possible_row_head:
            row_headers.append(possible_row_head)

        if possible_context:
            contexts.append(possible_context)

        tem_pair=None
        if contexts:
            tem_row=1000
            for context in contexts:
                # print(f"context is {context}")
                print(F" ################ \n")
                if row_headers:
                    for row in row_headers:
                        # print(f"row is {row}")
                        # print(F" ################ \n")
                        distance_calculated = calculate_distance(row, context)
                        # print(F"column__row_thr is {context_row_thr} and distance_calculated is {distance_calculated}")
                        if abs(distance_calculated-context_row_thr)<tem_row:
                            tem_row=abs(distance_calculated-context_row_thr)
                            tem_pair=[row,tem_row,context]
       
        if tem_pair:
            all_pairs_row_con.append(tem_pair)
    
        #here need to write the logic
    print(F" ################ \n")
    print(F" ################ \n")
    print(f" all_pairs_row_con are {all_pairs_row_con}")
    print(F" ################ \n")
    print(F" ################ \n")

    return all_pairs_row_con


def predict_mutli_checks(ocr_data_all_got,ocr_raw_pages,process_trained_fields,end):

    row_headers = process_trained_fields.get('row_headers', None)
    column_headers = process_trained_fields.get('column_headers', None)
    contexts = process_trained_fields.get('contexts', None)

    # row_headers,column_headers,contexts=process_trained_fields['row_headers'],process_trained_fields['column_headers'],process_trained_fields['contexts']

    if column_headers:
        column_headers=json.loads(column_headers)
    else:
        column_headers={}
    if row_headers:
        row_headers=json.loads(row_headers)
    else:
        row_headers={}
    if contexts:
        contexts=json.loads(contexts)
    else:
        contexts={}
    
    extracted_fields={}

    fields=['Check Number','Check Amount']
    page_field_data={}

    for field in fields:

        print(F" #####################")
        print(F" #####################")
        print(F" #####################")
        print(F" ##################### fiels is {field}")
        print(F" #####################")
        print(F" #####################")
        print(F" #####################")

        extracted_fields[field]=''

        row_header_int=row_headers.get(field,[])
        context_int=contexts.get(field,[])
        
        print(f" ########### row_header_int got are  {row_header_int}")
        print(f" ########### context_int got are  {context_int}")

        context_box={}
        if 'context_box' in context_int:
            context_box=context_int['context_box']
        row_box={}
        if 'row_box' in row_header_int:
            row_box=row_header_int['row_box']
        value_box={}
        if 'value_box' in row_header_int:
            value_box=row_header_int['value_box']

        # print(F" ##################### len of ocr_data_all is {len(ocr_data_all)}")
        all_pairs_row_con=check_headers(row_header_int,context_int,copy.deepcopy(ocr_data_all_got),copy.deepcopy(ocr_raw_pages))

        for pair in all_pairs_row_con:

            row_header=pair[0]
            context=pair[2]

            print(f" ########### finalised_headers got are  {pair}")
            if not row_header:
                continue
        
            for ocr_word in ocr_data_all_got:
                if not ocr_word:
                    continue
                if ocr_word[0]['pg_no'] == row_header['pg_no']:
                    value_page_ocr_data=ocr_word
            
            if row_header:
                possible_values_row_got,multi_line=finding_possible_values(row_header,row_header_int,value_page_ocr_data)
                print(f" ################ possible_values_row got are {possible_values_row_got}")

            if value_box:
                possible_values_row=[]
                for value in possible_values_row_got:
                    if value_box['left']>=row_box['left'] and value['left']+10>=row_header['left']:
                        possible_values_row.append(value)
                    else:
                        possible_values_row.append(value)
            else:
                possible_values_row=possible_values_row_got
            print(f" ################ possible_values_row got are {possible_values_row}")

            max_calculated_diff=10000
        
            for value in possible_values_row:

                calculated_diff=0
                if row_header:
                    value_row_diff_cal=calculate_distance(row_header,value)
                    row_act_confidence=value_row_diff_cal
                    row_base_diff=row_header_int['value_thr']
                    row_conf=calculate_confidence(row_base_diff,row_act_confidence)
                
                    calculated_row_diff=abs(row_header_int['value_thr']-value_row_diff_cal)
                    calculated_diff=calculated_diff+calculated_row_diff

                if context:
                    
                    column_act_confidence=calculate_value_distance(context,value)
                    con_base_diff=context_int['value_thr']
                    con_conf=calculate_cont_confidence(con_base_diff,column_act_confidence)

                    value_con_diff_cal=calculate_value_distance(context,value)
                    calculated_con_diff=abs(context_int['value_thr']-value_con_diff_cal)
                    calculated_diff=calculated_diff+calculated_con_diff

            
                if max_calculated_diff>calculated_diff:
                    max_calculated_diff=calculated_diff
                    predicted=value

            print(f" ################ predicted value is {predicted['word']}")

            if predicted:
                pg = predicted['pg_no']
                page_field_data.setdefault(pg, {})[field] = predicted['word']

   # Now build ranges based on pages with required field count
    pages_with_checks = sorted([pg for pg, data in page_field_data.items()])
    print(f"pages_with_checks is {pages_with_checks}")

    ranges = []
    if pages_with_checks:
        # Filter out pages with no Check Number AND no Check Amount
        filtered_pages = []
        for pg in sorted(pages_with_checks):
            check_number = page_field_data.get(pg, {}).get("Check Number", "")
            check_amount = page_field_data.get(pg, {}).get("Check Amount", "")
            if check_number or check_amount:  # Keep if at least one is present
                filtered_pages.append(pg)

        if filtered_pages:
            start_page = filtered_pages[0]

            prev_vals = (
                page_field_data.get(start_page, {}).get("Check Number", ""),
                page_field_data.get(start_page, {}).get("Check Amount", "")
            )
            print(f"pages_with_checks is {pages_with_checks}")

            for page in filtered_pages[1:]:
                curr_vals = (
                    page_field_data.get(page, {}).get("Check Number", ""),
                    page_field_data.get(page, {}).get("Check Amount", "")
                )

                print(f"curr_vals is {curr_vals}")
                print(f"prev_vals is {prev_vals}")
                # Priority: Check Number → Check Amount → Check Date
                check_number_changed = curr_vals[0] != prev_vals[0]
                check_amount_changed = curr_vals[1] != prev_vals[1]

                if check_number_changed or check_amount_changed:
                    ranges.append([start_page-1, page-2])
                    start_page = page
                    prev_vals = curr_vals

            # Add last range
            ranges.append([start_page - 1, end])

    print(f"Final ranges (filtered & based on value changes): {ranges}")

    return ranges



# def predict_mutli_checks(ocr_data_all_got, ocr_raw_pages, process_trained_fields, end, min_fields_per_page=3):
#     import json, copy

#     row_headers = process_trained_fields.get('row_headers')
#     column_headers = process_trained_fields.get('column_headers')
#     contexts = process_trained_fields.get('contexts')

#     row_headers = json.loads(row_headers) if row_headers else {}
#     column_headers = json.loads(column_headers) if column_headers else {}
#     contexts = json.loads(contexts) if contexts else {}

#     fields = ['Check Number', 'Check Amount']

#     # Dict to store extracted fields per page
#     page_field_data = {}  # { page_num: {field: value} }

#     for field in fields:
#         print(f"\n{'#' * 20} Field: {field} {'#' * 20}")
#         row_header_int = row_headers.get(field, {})
#         context_int = contexts.get(field, {})

#         context_box = context_int.get('context_box', {})
#         row_box = row_header_int.get('row_box', {})
#         value_box = row_header_int.get('value_box', {})

#         all_pairs_row_con = check_headers(
#             row_header_int,
#             context_int,
#             copy.deepcopy(ocr_data_all_got),
#             copy.deepcopy(ocr_raw_pages)
#         )

#         for pair in all_pairs_row_con:
#             row_header = pair[0]
#             context = pair[2]

#             if not row_header:
#                 continue

#             for ocr_word in ocr_data_all_got:
#                 if not ocr_word:
#                     continue
#                 if ocr_word[0]['pg_no'] == row_header['pg_no']:
#                     value_page_ocr_data = ocr_word
#                     possible_values_row_got, _ = finding_possible_values(
#                         row_header, row_header_int, value_page_ocr_data
#                     )

#                     if value_box:
#                         possible_values_row = []
#                         for value in possible_values_row_got:
#                             if value_box['left'] >= row_box['left'] and value['left'] + 10 >= row_header['left']:
#                                 possible_values_row.append(value)
#                             else:
#                                 possible_values_row.append(value)
#                     else:
#                         possible_values_row = possible_values_row_got

#                     max_calculated_diff = 10000
#                     predicted = None
#                     for value in possible_values_row:
#                         calculated_diff = 0

#                         if row_header:
#                             value_row_diff_cal = calculate_distance(row_header, value)
#                             calculated_row_diff = abs(row_header_int['value_thr'] - value_row_diff_cal)
#                             calculated_diff += calculated_row_diff

#                         if context:
#                             value_con_diff_cal = calculate_value_distance(context, value)
#                             calculated_con_diff = abs(context_int['value_thr'] - value_con_diff_cal)
#                             calculated_diff += calculated_con_diff

#                         if calculated_diff < max_calculated_diff:
#                             max_calculated_diff = calculated_diff
#                             predicted = value

#                     if predicted:
#                         pg = predicted['pg_no']
#                         page_field_data.setdefault(pg, {})[field] = predicted['word']

#     # Now build ranges based on pages with required field count
#     pages_with_checks = sorted([pg for pg, data in page_field_data.items() if len(data) >= min_fields_per_page])
#     print(f"pages_with_checks is {pages_with_checks}")

#     ranges = []
#     if pages_with_checks:
#         # Filter out pages with no Check Number AND no Check Amount
#         filtered_pages = []
#         for pg in sorted(pages_with_checks):
#             check_number = page_field_data.get(pg, {}).get("Check Number", "")
#             check_amount = page_field_data.get(pg, {}).get("Check Amount", "")
#             if check_number or check_amount:  # Keep if at least one is present
#                 filtered_pages.append(pg)

#         if filtered_pages:
#             start_page = filtered_pages[0]
#             prev_page = start_page

#             prev_vals = (
#                 page_field_data.get(start_page, {}).get("Check Number", ""),
#                 page_field_data.get(start_page, {}).get("Check Amount", "")
#             )
#             print(f"pages_with_checks is {pages_with_checks}")

#             for page in filtered_pages[1:]:
#                 curr_vals = (
#                     page_field_data.get(page, {}).get("Check Number", ""),
#                     page_field_data.get(page, {}).get("Check Amount", "")
#                 )

#                 print(f"curr_vals is {curr_vals}")
#                 print(f"prev_vals is {prev_vals}")
#                 # Priority: Check Number → Check Amount → Check Date
#                 check_number_changed = curr_vals[0] != prev_vals[0]
#                 check_amount_changed = curr_vals[1] != prev_vals[1]

#                 if check_number_changed or check_amount_changed:
#                     ranges.append([start_page - 1, prev_page - 1])
#                     start_page = page
#                     prev_vals = curr_vals

#                 prev_page = page

#             # Add last range
#             ranges.append([start_page - 1, prev_page - 1])

#     print(f"Final ranges (filtered & based on value changes): {ranges}")

#     return ranges



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


def group_words_by_proximity_in_line(words, distance_threshold=3):
    """
    Groups words that are right next to each other based on their 'left' coordinate proximity.

    Args:
        words: List of dictionaries containing OCR data for words on the same line.
        distance_threshold: Maximum allowed horizontal distance between consecutive words (default is 5).

    Returns:
        list: List of grouped words that are close to each other.
    """
    # Sort words by 'left' to ensure they are processed from left to right
    sorted_words = sorted(words, key=lambda x: x["left"])

    if not sorted_words:
        return []

    grouped_words = [sorted_words[0]]  # Start with the first word
    previous_right = sorted_words[0]["right"]

    for word in sorted_words[1:]:
        current_left = word["left"]
        # Check if the distance between the current word and the previous word is less than the threshold
        if (current_left - previous_right) <= distance_threshold:
            grouped_words.append(word)
            previous_right = word["right"]  # Update the right edge of the current word
        else:
            break  # Stop grouping if the distance exceeds the threshold
    
    return combine_dicts(grouped_words)



def find_x_values(key,column_header,ocr_data,thr=0):
    
    next_values=[]
    k_right=key["right"]
    min=100
    sorted_words=line_wise_ocr_data(ocr_data)
    for line in sorted_words:
            if key in line:
                line.remove(key) 
            if line:
                diff_ver=abs(abs((line[0]["top"]+line[0]["bottom"])/2) - abs((key["top"]+key["bottom"])/2))
                dif=abs(diff_ver-thr)
                if min>dif:
                    # print(F" here diff for values are {min  , dif}")
                    next_values=[]
                    min=dif
                    for word in line:
                        if k_right<word['left']:
                            next_values.append(word)
    print(f"  we have for next values as {next_values} where thr is {thr}")
    if min>10:
        return []
    else:
        if column_header:
            out_value={}
            min_diff=100
            for value in next_values:
                diff=calculate_value_distance(value, column_header)
                if diff<min_diff:
                    min_diff=diff
                    out_value=value
            return out_value
        else:
            return group_words_by_proximity_in_line(next_values) 


def group_vertical_words_by_diff_and_left(words,initial_diff_threshold=20, diff_tolerance=5, left_tolerance=10):
    """
    Groups words in a vertical line based on the 'top' coordinate difference and proximity of 'left' values.

    Args:
        words: List of dictionaries containing OCR data sorted vertically.
        initial_diff_threshold: Maximum allowed difference between the first two words.
        diff_tolerance: Tolerance range for subsequent differences to maintain grouping.
        left_tolerance: Maximum allowed deviation in 'left' values from the first word.

    Returns:
        list: List of words that fit within the vertical difference and left proximity criteria.
    """
    # Sort words by 'top' to ensure they are processed from top to bottom
    sorted_words = sorted(words, key=lambda x: x["top"])

    if not sorted_words:
        return []

    grouped_words = [sorted_words[0]]  # Start with the top word
    previous_bottom = sorted_words[0]["bottom"]
    initial_diff = None
    initial_left = sorted_words[0]["left"]  # Use the first word's 'left' value as reference

    for word in sorted_words[1:]:
        current_top = word["top"]
        current_left = word["left"]
        top_diff = current_top - previous_bottom

        print(word ,'word')
        print(top_diff ,'top_diff')
        print(initial_diff ,'initial_diff')

        if initial_diff is None:
            # Calculate the initial difference
            if top_diff < initial_diff_threshold:
                grouped_words.append(word)
                initial_diff = top_diff  # Set the initial difference
                initial_left=current_left
            else:
                break  # Stop if the first difference is too large or left is not close enough
        else:
            # Compare the top difference and left proximity
            if abs(top_diff - initial_diff) <= diff_tolerance and abs(current_left - initial_left) <= left_tolerance:
                grouped_words.append(word)
            else:
                break  # Stop grouping if the top difference or left proximity exceeds the tolerance

        # Update previous top for the next iteration
        previous_bottom = word["bottom"]
    
    final_word=combine_dicts(grouped_words)
    return final_word

    

def find_y_values(each_value, ocr_data,api=''):
    if api:
        next_values = [each_value]
    else:
        next_values=[]
    # Sort the OCR data by the "top" attribute to maintain vertical order
    sorted_words = sorted(ocr_data, key=lambda x: x["top"])
    
    k_bottom = each_value["bottom"]  # Get the bottom value of the key to find values below it
    k_right = each_value["right"]
    centorid=(each_value['left']+each_value['right'])/2
    
    min_diff = 100  # Initialize a large value for the minimum difference
    sorted_words=line_wise_ocr_data(sorted_words)
    for line in sorted_words:

        if line[0]['bottom']<each_value['top'] or each_value in line:
            continue
        # print(F" ########## line is {line[0]['word']} and {each_value}")

        flag=False
        for word in line:
            if (word['left']<=centorid<=word['right'] or word['left']<=each_value['right']<=word['right'] or word['left']<=each_value['left']<=word['right'] ):
                next_values.append(word)
                flag=True
            elif (word['left']<=each_value['left']<=word['right'] ) :
                next_values.append(word)
                flag=True
            elif (word['left']<=each_value['right']<=word['right'] ):
                next_values.append(word)
                flag=True
        # if flag == False:
        #     sorted_left = sorted(line, key=lambda x: x["left"],reverse=True)
        #     min=100
        #     tem={}
        #     for wo in sorted_left:
        #         diff=abs(wo['left']-each_value['left'])
        #         if diff<min:
        #             min=diff
        # #             tem=wo
        #     if tem:
        #         next_values.append(tem)
    print(f" ##################  next_values got from y are {next_values}")
    if not next_values:
        return []
    sorted_list_top = sorted(next_values, key=lambda x: x["top"])
    diff_top=abs(sorted_list_top[0]['top']-each_value['top'])
    if diff_top>100:
        return []
    
    return group_vertical_words_by_diff_and_left(sorted_list_top)

    

def predict_possible_values(ocr_pages,row_headers_all,column_headers_all,main_fields):

    predicted_values={}
    for page,row_headers in row_headers_all.items():
    
        column_headers=column_headers_all[page]

        for predict,row_header in row_headers.items():
            if not row_header:
                continue
            print(f"  row_header her eare {row_header}")
            if predict not in predicted_values:
                predicted_values[predict]=[]
            column_header={}
            if predict in column_headers:
                column_header=column_headers[predict]
            y_value={}
            x_value={}
            for page in ocr_pages:
                if not page:
                    continue
                if page[0]['pg_no']==row_header['pg_no']:
                    x_value=find_x_values(row_header,column_header,page)
                    if x_value:
                        if predict not in main_fields:
                            main_fields[predict]=row_header
                        predicted_values[predict].append(x_value)
                    if not column_header:
                        y_value=find_y_values(row_header,page)
                        if y_value:
                            if predict not in main_fields:
                                main_fields[predict]=row_header
                            predicted_values[predict].append(y_value)
                    break

    return predicted_values,main_fields


def predict_possible_section_values(ocr_pages,section,column_headers_all):

    predicted_values={}
    for page,row_headers in section.items():
    
        column_headers=column_headers_all[page]

        for predict,row_header in row_headers.items():
            if not row_headers:
                continue
            print(f"  row_header her eare {row_header}")
            if predict not in predicted_values:
                predicted_values[predict]=[]
            column_header={}
            if predict in column_headers:
                column_header=column_headers[predict]
            y_value={}
            x_value={}
            for page in ocr_pages:
                if not page:
                    continue
                if page[0]['pg_no']==row_header['pg_no']:
                    x_value=find_x_values(row_header,column_header,page)
                    if x_value:
                        predicted_values[predict].append(x_value)
                    if not column_header:
                        y_value=find_y_values(row_header,page)
                        if y_value:
                            predicted_values[predict].append(y_value)
                    break

    return predicted_values


def find_best_fit(ocr_data, desired_label,process,format,case_id,header,api=False):
    """
       THis function is for choosing the final value from x tilt or y tilt, 
       word_list contains the words from x and y tilts,
       desired_label is the label we want to determine the words,
       file_type is file_types availbe ,we use this to load the files from db based on query,
       tenant_id : used to load the files present in the path

       this code reads the fv filenames from the database and loads the model files based on label and determines which tilt 
       contains the value between X tilt and Y tilt
    """
    try:

        word_list=[]
        for word in ocr_data:
            word_list.append(word['word'])

        # File paths for the trained model and vectorizer
        model_filename = f"/var/www/extraction_api/app/extraction_folder/{process}_{format}_{header}_logistic_regression_model.joblib"
        vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/{process}_{format}_{header}_count_vectorizer.joblib"
    
        loaded_model = joblib.load(model_filename)
        loaded_vectorizer = joblib.load(vectorizer_filename)
        print(f"got model files from in find_values{model_filename},{vectorizer_filename}")
        print(f"words list got for find_values is {word_list}")
        print(f"desired label in find_values is {desired_label}")

        # Convert the list of words to lowercase for processing
        word_list_lower = [word.lower() for word in word_list if word]

        # Preprocess the list of words using the loaded vectorizer
        word_list_vectorized = loaded_vectorizer.transform(word_list_lower)

        # Get the probability estimates for the classes
        proba_estimates = loaded_model.predict_proba(word_list_vectorized)

        model_classes = loaded_model.classes_

        # Check if the desired label is in the model's classes
        if desired_label not in model_classes:
            print(f"Desired label '{desired_label}' not found in the model's classes.")
            return None

        # Find the index of the desired label in the model's classes
        desired_label_index = list(loaded_model.classes_).index(desired_label)

        # Initialize variables to track the best word and highest confidence score
        best_word = []
        highest_confidence = -1.0  # Initialize with a very low value

        # Iterate through the words and find the one with the highest confidence
        for i, (word, word_lower) in enumerate(zip(word_list, word_list_lower)):
            confidence_score = proba_estimates[i][desired_label_index]
            print(f"the word in find values word is {word} and confidence is {confidence_score} and {desired_label_index}")
            if confidence_score > highest_confidence:
                best_word.append(ocr_data[i])  # Return the original word with original case
                highest_confidence = confidence_score
        
        for i, (word, word_lower) in enumerate(zip(word_list, word_list_lower)):
            confidence_score = proba_estimates[i][desired_label_index]
            if confidence_score-highest_confidence < 0.075:
                print(f"the word with similar confidence is {confidence_score} and {desired_label_index}")
                best_word.append(ocr_data[i])  # Return the original word with original case

        if api:
            # Return the word with the highest confidence for the given label
            print(f"output got from value (find_values) svm is {best_word}")
            return best_word
        else:
            return best_word[0]

    except Exception as e:
        print(f"Exception occurred in find_word_with_highest_confidence: {e}")
        return None


def finalize_values(predicted,process,format,case_id):

    final_predict={}
    final_highlights={}
    final_values={}
    for predcit,word_list in predicted.items():
        if predcit not in final_predict:
            final_predict[predcit]={}
        if predcit not in final_highlights:
            final_highlights[predcit]={}
        temp=''
        other=[]
        best_value=find_best_fit(word_list, predcit,process,format,case_id,'values')
        if best_value:
            final_highlights[predcit]=from_highlights(best_value)
            final_values[predcit]=best_value
            temp=best_value['word']
            for word in word_list:
                if word !=best_value:
                    # final_highlights[word['word']]=from_highlights(word)
                    other.append(word['word'])
        elif word_list:
            final_highlights[predcit]=from_highlights(word_list[0])
            final_values[predcit]=word_list[0]
            temp=word_list[0]['word']

        # final_predict[predcit]['av']=temp
        # final_predict[predcit]['rv']=other
        final_predict[predcit]=temp

    return final_predict,final_highlights,final_values



def predict_with_svm(ocr_data, process,format,case_id,header):
    """
    Predicts labels for words using Support Vector Machine (SVM) classifier.

    Args:
        word_list: List of words to be classified.
        div: Division or category for which the model is trained.

    Returns:
        field_keywords: Predicted labels for the words.
    """

    word_list=[]
    for word in ocr_data:
        word_list.append(word['word'])


    # File paths for the trained model and vectorizer
    model_filename = f"/var/www/extraction_api/app/extraction_folder/{process}_{format}_{header}_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/{process}_{format}_{header}_count_vectorizer.joblib"
    
    try:
        print(F"file found and file name is {vectorizer_filename},{model_filename}")
        # Load the trained model
        clf = joblib.load(model_filename)

        # Load the vectorizer used to convert words into numerical features
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(F"file not found and file name is {vectorizer_filename},{model_filename}")
        return {}
    try:
        # Convert the input list of words into a bag-of-words (BoW) representation
        X_inference = vectorizer.transform(word_list)

        # Make predictions using the loaded model
        predictions = clf.predict(X_inference)

        field_keywords = {}
        # Store predicted labels for each word
        for i, item in enumerate(ocr_data):
            print(F" predictions[i] is {predictions[i]} and predicted label is {item}")
            if predictions[i]  not in field_keywords and 'other' not in predictions[i]:
                field_keywords[predictions[i]]=[]
            if 'other' not in predictions[i]:
                field_keywords[predictions[i]].append(item)

        # Return the predicted labels
        return field_keywords
    except Exception as e:
        print(F"error is {e}")
        return {}



def filter_headers(row_headers_all,column_headers_all,context_all,process,format,case_id,common_fields):

    final_row_headers={}
    final_col_headers={}
    final_con_headers={}

    final_row_headers_sec={}
    final_col_headers_sec={}
    final_con_headers_sec={}

    for page,row_headers in row_headers_all.items():

        if page not in final_row_headers:
            final_row_headers[page]={}
        if page not in final_col_headers:
            final_col_headers[page]={}
        if page not in final_con_headers:
            final_con_headers[page]={}

        if page not in final_row_headers_sec:
            final_row_headers_sec[page]={}
        if page not in final_col_headers_sec:
            final_col_headers_sec[page]={}
        if page not in final_con_headers_sec:
            final_con_headers_sec[page]={}

        for predict,row_header in row_headers.items():

            if predict in common_fields:

                if predict not in final_row_headers[page]:
                    final_row_headers[page][predict]={}
                if predict not in final_col_headers[page]:
                    final_col_headers[page][predict]={}
                if predict not in final_con_headers[page]:
                    final_con_headers[page][predict]={}

                if page in column_headers_all and predict in column_headers_all[page]:
                    final_col_headers[page][predict]=find_best_fit(column_headers_all[page][predict],predict,process,format,case_id,'column_headers')
                if page in context_all and predict in context_all[page]:
                    final_con_headers[page][predict]=find_best_fit(context_all[page][predict],predict,process,format,case_id,'context')

                final_row_headers[page][predict]=find_best_fit(row_header,predict,process,format,case_id,'row_headers')
            else:

                if predict not in final_row_headers_sec[page]:
                    final_row_headers_sec[page][predict]=[]
                if predict not in final_col_headers_sec[page]:
                    final_col_headers_sec[page][predict]=[]
                if predict not in final_con_headers_sec[page]:
                    final_con_headers_sec[page][predict]=[]

                if page in column_headers_all and predict in column_headers_all[page]:
                    final_col_headers_sec[page][predict]=find_best_fit(column_headers_all[page][predict],predict,process,format,case_id,'column_headers')
                if page in context_all and predict in context_all[page]:
                    final_con_headers_sec[page][predict]=find_best_fit(context_all[page][predict],predict,process,format,case_id,'context')

                final_row_headers_sec[page][predict]=find_best_fit(row_header,predict,process,format,case_id,'row_headers',True)

    return final_row_headers,final_col_headers,final_con_headers,final_row_headers_sec,final_col_headers_sec,final_con_headers_sec



def predict_section(row_headers):

    sections=[]
    temp_section={}
    track_words=[]
    for page,row_headers in row_headers.items():
        if page not in temp_section:
            temp_section[page]={}

        page_wise_headers=[]
        for head,predcit in row_headers.items():
            for item in predcit:
                item['predict']=head
                if item not in page_wise_headers:
                    page_wise_headers.append(item)

        sorted_headers=sorted(page_wise_headers, key=lambda x: x["top"])

        index=0
        print(f" ################ temp_section is {temp_section}")
        print(f" ################ section srated with word {sorted_headers}")
        for head in sorted_headers:
            predict=str(head['predict'])
            if predict not in temp_section[page]:
                print(f" ################ section {predict} started")
                temp_section[page][predict]={}

            if temp_section and is_word_present(head['word'], track_words, threshold=0.95):
                print(f" ################ section repeated with word {sorted_headers[index]} and {temp_section[page][predict]}")
                section_end=False
                temp_index=index
                for head_ in sorted_headers[temp_index+1:temp_index+4]:
                    print(f" ################ section end cehcking with word {head_}")
                    if is_word_present(head_['word'], track_words, threshold=0.95):
                        print(f" ################ section end found here with word {head_}")
                        section_end=True
                        break
                if section_end:
                    print(f" ################ new is getting created")
                    if temp_section:
                        sections.append(copy.deepcopy(temp_section))
                        temp_section={}
                        temp_section[page]={}
                        track_words=[]
            print(f" ################ new is temp_section {sections}")
            print(f" ################ head is assigned {head} ,{track_words}")
            temp_section[page][predict]=head
            track_words.append(head['word'])
            index=index+1

    if temp_section:
        print(f" ################ temp_section is assigned end is {temp_section}")
        sections.append(temp_section)

    print(f" ################ section is {sections}")
    return sections
            

def get_master_extraction_values(process,format,case_id,ocr_pages,nedded_fields,common_fields):
    
    common_fields=json.loads(common_fields)
    
    row_headers_all={}
    column_hearders_all={}
    context_all={}
    for document_id_df in ocr_pages:
        try:
            page=document_id_df[0]['pg_no']

            print(F" ################ ")
            print(F" ################ ")
            print(f" ################ page is {page}")
            print(F" ################ ")
            print(F" ################ ")
         
            row_headers=predict_with_svm(document_id_df,process,format,case_id,'row_headers')
            print(f"  ################ row_headers predicted is {row_headers}")
            row_headers_all[str(page)]=row_headers

            column_hearders=predict_with_svm(document_id_df,process,format,case_id,'column_headers')
            print(f"  ################ column_hearders predicted is {column_hearders}")
            column_hearders_all[str(page)]=column_hearders

            context=predict_with_svm(document_id_df,process,format,case_id,'context')
            print(f"  ################ context predicted is {context}")
            context_all[str(page)]=context

        except Exception as e:
            print(F"Exception is {e}")
            continue

    print(F" ################ ")
    print(F" ################ ")
    print(f" ################ row_headers got is  {row_headers_all}")
    print(f" ################ column_hearders is {column_hearders_all}")
    print(f" ################ context is {context_all}")
    print(f" ################ ")
    print(f" ################ ")

    extarcted={}
    extracted_highlights={}
    extracted_headers={}

    row_headers_all,column_hearders_all,context_all,final_row_headers_sec,final_col_headers_sec,final_con_headers_sec=filter_headers(row_headers_all,column_hearders_all,context_all,process,format,case_id,common_fields)
    print(f" ################ filtered headers are  {row_headers_all,column_hearders_all,context_all}")

    row_headers={}
    predicted,row_headers=predict_possible_values(ocr_pages,row_headers_all,column_hearders_all,row_headers)
    print(f" ################ predicted  are  {predicted}")
    extracted_headers['main_fields']=row_headers

    final_predict,final_highlights,final_values=finalize_values(predicted,process,format,case_id)
    print(f" ################ final_predict  are  {final_predict}")
    extarcted['main_fields']=final_predict
    extracted_headers['main_values']=final_values
    extracted_highlights['main_fields']=final_highlights

    if format == 'eob':
        sections=predict_section(final_row_headers_sec)

        index=0
        name='section'
        extarcted[name]=[]
        extracted_highlights[name]=[]
        section_headers={}
        section_values={}
        for section in sections:

            print(f" ################ section is  {index}")
            section_predicted,section_headers=predict_possible_values(ocr_pages,section,column_hearders_all,section_headers)
            print(f" ################ predicted  are  {section_predicted}")

            section_predict,section_highlights,section_values=finalize_values(section_predicted,process,format,case_id)
            print(f" ################ section predcition are  {section_predict}")
            for key,value in section_values.items():
                if key not in section_values:
                    section_values[key]=value

            extarcted[name].append({'fields':section_predict,'table':{'table_1':{}}})
            extracted_highlights[name].append({'fields':section_highlights})


            #need to integarte table here

            index=index+1

        extracted_headers['sections']=section_headers
        extracted_headers['section_values']=section_values

    for field in common_fields:
        possible_val=[]
        val={}
        if field not in final_predict:
            print(f" ################ val prediction for  {field}")
            for page in ocr_pages:
                first_filter=find_best_fit(page, field,process,format,case_id,'values')
                if first_filter:
                    possible_val.append(first_filter)
            print(f" ################ possible_val  are  {possible_val}")
            if len(possible_val)>1:
                val=find_best_fit(possible_val,field,process,format,case_id,'values')
                print(f" ################ final value is {val}")
                final_predict[field]=val['word']
                final_highlights[field]=from_highlights(val)
            elif len(possible_val)==1:
                val=possible_val[0]
                print(f" ################ final value is {val}")
                final_predict[field]=val['word']
                final_highlights[field]=from_highlights(val)
            elif possible_val==0:
                final_predict[field]=''
                final_highlights[field]=''

    print(f" ################ all predited  are  {extarcted}")

    return extarcted,extracted_highlights,extracted_headers


def generate_variations(value):
    """
    Generate at least 5 variations of the input string by altering the capitalization.

    Args:
        value (str): The input string.

    Returns:
        list: A list containing 5 variations of the input string.
    """
    variations = [
        value.lower(),       # All lowercase
        value.upper(),       # All uppercase
        value.title(),       # Title case (each word capitalized)
        value.capitalize(),  # First letter capitalized
        ''.join(random.choice([c.upper(), c.lower()]) for c in value)  # Random capitalization
    ]
    
    # Ensure 5 variations (if the value is very short, ensure unique variations)
    while len(set(variations))<10:
        variations.append(''.join(random.choice([c.upper(), c.lower()]) for c in value))

    return variations[:5]  # Ensure exactly 5 variations



def create_json_files(input_dict,others, output_file,case_id,flag=False):
    """
    Converts a dictionary where values are lists into a list of lists where each sublist contains
    a variation of the value from the dictionary and its corresponding key.

    Args:
        input_dict (dict): The input dictionary where keys map to lists of values.
        output_file (str): The file path where the JSON file will be saved.
    
    Returns:
        list: A list of lists in the form [[variation, key], ...].
    """
    result = []
    # Loop through the dictionary to create the desired list structure
    print(F" create_json_files has started ")
    print(F" input_dict is {input_dict} for {output_file}")
    for key, values in input_dict.items():
        for value in values:
            # # Generate 5 variations for each value
            # if not flag:
            #     variations = generate_variations(value['word'])
            #     for variation in variations:
            #         result.append([variation, key])  # Create sublists with variation and corresponding key
            # else:
            for i in range(10):
                result.append([value['word'], key])

    # print(F" result here 1 {result}")
    i=1   
    temp_count=0  
    for val in others:
        if temp_count==0: 
            label=f"other"+str(i)
            temp_count=10
        result.append([val,label ])
        temp_count=temp_count-1

    # print(F" result here 1 {result}")

    out_path=f"/var/www/extraction_api/app/extraction_folder/{output_file}"
    # Write the result to a JSON file
    with open(out_path, 'w') as json_file:
        json.dump(result, json_file, indent=4)
    
    return result



def train_and_save_model(json_file_path, model_filename, vectorizer_filename,case_id):
    """
    Train a Logistic Regression model using a corpus from a JSON file and save the trained model and vectorizer.

    Args:
        json_file_path (str): Path to the JSON file containing the corpus.
        model_filename (str): The output filename for saving the trained model (joblib format).
        vectorizer_filename (str): The output filename for saving the vectorizer (joblib format).
    
    Returns:
        None
    """
    path=f"/var/www/extraction_api/app/extraction_folder/"
    model_filename=path+model_filename
    vectorizer_filename=path+vectorizer_filename

    # Function to read data from a JSON file
    def read_corpus_from_json(json_file):
        with open(json_file, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    
    # Read the corpus from the JSON file
    corpus = read_corpus_from_json(f"/var/www/extraction_api/app/extraction_folder/{json_file_path}")

    # Split data into features (X) and labels (y)
    X = [text for text, label in corpus]
    y = [label for text, label in corpus]

    # Text preprocessing: Convert text to a bag of words (BoW) representation
    vectorizer = CountVectorizer()
    X = vectorizer.fit_transform(X)

    # Split data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Initialize and train the Logistic Regression classifier
    clf = LogisticRegression()
    clf.fit(X_train, y_train)

    # Make predictions on the test data
    y_pred = clf.predict(X_test)

    # Evaluate the classifier
    accuracy = accuracy_score(y_test, y_pred)
    print(f"Accuracy: {accuracy:.2f}")

    # Print classification report (includes precision, recall, F1-score, and support)
    report = classification_report(y_test, y_pred, zero_division=1)
    print("Classification Report:\n", report)

    # Save the trained model and vectorizer for later inference
    joblib.dump(clf, model_filename)
    joblib.dump(vectorizer, vectorizer_filename)
    print("Model and vectorizer files saved successfully.")

    return True


def form_others(others,other_list):

    for other in other_list:
        for values in other.values():
            for val in values:
                others.append(val['word'])

    return others


import os
import json
import stat

def create_models_real_time(process, format, process_trained_fields, case_id):
    column_header = json.loads(process_trained_fields['column_headers'])
    row_header = json.loads(process_trained_fields['row_headers'])
    context = json.loads(process_trained_fields['contexts'])
    values = json.loads(process_trained_fields['values'])
    others = json.loads(process_trained_fields['others'])

    dir_path = os.path.join("/var/www/extraction_api/app/extraction_folder/")
    os.makedirs(dir_path, exist_ok=True)
    print(f"Directory '{dir_path}' created successfully.")

    def process_part(part_data, others_input, file_prefix, is_value=False):
        try:
            json_file = f"{file_prefix}.json"
            model_file = f"{file_prefix}_logistic_regression_model.joblib"
            vectorizer_file = f"{file_prefix}_count_vectorizer.joblib"
            json_path = os.path.join(dir_path, json_file)
            model_path = os.path.join(dir_path, model_file)
            vectorizer_path = os.path.join(dir_path, vectorizer_file)

            if os.path.exists(json_path) and os.path.exists(model_path) and os.path.exists(vectorizer_path):
                print(f"Files for '{file_prefix}' already exist. Skipping...")
                return

            others_part = form_others(others, others_input)
            create_json_files(part_data, others_part, json_file, case_id, is_value)
            train_and_save_model(json_file, model_file, vectorizer_file, case_id)
            print(f"Creation of trained model for '{file_prefix}' is done")
        except Exception as e:
            print(f"Exception while processing '{file_prefix}' is: {e}")

    process_part(column_header, [row_header, context, values], f"{process}_{format}_column_headers")
    process_part(row_header, [column_header, context, values], f"{process}_{format}_row_headers")
    process_part(context, [row_header, column_header, values], f"{process}_{format}_context")
    process_part(values, [row_header, context, column_header], f"{process}_{format}_values", is_value=True)

    return True



def update_into_db(tenant_id,predicted_fields,table,extraction_db,case_id,format,process,needed_fields,version,document_id,extarction_from):

    print(F" ############# fields that  are extracted are {predicted_fields}")
    print(F" ############# fields that  are needed are {needed_fields}")
    print(F" ############# table that is extracted are {table}")

    needed_fields=[]

    for key in needed_fields:
        if key not in predicted_fields:
            predicted_fields[key]=''

    db_config['tenant_id']=tenant_id
    queues_db=DB('queues',**db_config)

    query="UPDATE `ocr` SET `extracted` = %s ,extracted_from = %s WHERE `case_id` = %s and document_id =%s and format_type=%s"
    extraction_db.execute_(query,params=[json.dumps(predicted_fields),json.dumps(extarction_from),case_id,document_id,format])

    percentage='100'
    extarcted_fields=[]
    for key,value in predicted_fields.items():
        if value:
            extarcted_fields.append(key)
    
    # queues_db.insert_dict({"total_fields_extracted":json.dumps(extarcted_fields),'total_fields':json.dumps(needed_fields),"format":format,"process":process,"case_id":case_id,"percentage":percentage,'used_model_version':version},'field_accuracy')

    return True
    

def update_highlights(tenant_id,case_id,highlights,format,document_id):

    #updating all the extracted highlights into database

    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    query=f"select * from ocr where case_id='{case_id}' and format_type ='{format}' and document_id = '{document_id}'"
    highlight_df=extraction_db.execute(query)
    highlight=highlight_df.to_dict(orient='records')
    highlight=highlight[0]['highlight']

    if highlight:
        highlight=json.loads(highlight)
        #highlight.update({key: value for key, value in highlights.items() if key not in highlight})
        highlight.update(highlights)
        highlight=json.dumps(highlight)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `case_id` = %s and format_type= %s and document_id= %s"
        params = [highlight, case_id,format,document_id]
        extraction_db.execute(query, params=params)
    else:
        word_highlight=json.dumps(highlights)
        query = "UPDATE `ocr` SET `highlight`= %s WHERE `case_id` = %s and  format_type= %s and document_id= %s"
        params = [word_highlight, case_id,format,document_id]
        extraction_db.execute(query, params=params)
    return True


def valid_identifier(all_page,distances,base_word):
    if not distances:
        return False
    print(f" ################# we are here to validate section identifier")
    found=[]
    current_page=base_word['pg_no']
    top=base_word['top']
    for page in all_page:
        sorted_words = sorted(page, key=lambda x: x["top"])
        if current_page>page[0]['pg_no']:
            continue
        if current_page<page[0]['pg_no']:
            top=0
        for word in sorted_words:
            for word_id,dis in distances.items():
                cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
                base_identifier_word=re.sub(r'[^a-zA-Z]', '', word_id)
                if base_identifier_word:
                    matcher = SequenceMatcher(None, cleaned_word, base_identifier_word)
                    similarity_ratio_con = matcher.ratio()
                    if similarity_ratio_con>0.85 and top<word['top']:
                        # print(f" ################# we are here to {base_identifier_word} {cleaned_word} {similarity_ratio_con}")
                        dis_now=base_word['right']-word['left']
                        # print(f"here we are validating the words",word,word_id,dis_now)
                        if abs(dis_now-dis)<100:
                            found.append(word)
    # print(" ############ Other section words are",found)
    found=sorted(found, key=lambda x: (x["pg_no"],x["top"]))
    if found:
        if len(found) == len(list(distances.keys())) or len(found) == len(list(distances.keys()))-1 or len(found)>len(list(distances.keys())):
            return found[0]
        else:
            return {}
    else:
        return {}


def euclidean_distance(w1, w2):
        return math.sqrt((w2["left"] - w1["left"])**2 + (w2["top"] - w1["top"])**2)


def extract_section_header(ocr_data,table_headers,start_pg):
    
    table_headers_line=line_wise_ocr_data(table_headers)
    table_line_words=[]
    for table_head_line in table_headers_line:
        temp=''
        for word in table_head_line:
            temp=temp+" "+word['word']
        table_line_words.append(temp)

    table_lines=[]
    print(F'table_headers is {table_line_words}')

    for table_line_word in table_line_words:
        temp_line=[]
        max_match=0
        for ocr in ocr_data:
            if ocr[0]['pg_no']!=start_pg:
                continue
            sorted_words = sorted(ocr, key=lambda x: x["top"])
            line_ocr=line_wise_ocr_data(sorted_words)
            for line in line_ocr:
                line_words = [word["word"] for word in line]
                line_words=" ".join(line_words)
                line_words_temp=re.sub(r'[^a-zA-Z]', '', line_words)
                table_line_word_temp=re.sub(r'[^a-zA-Z]', '', table_line_word)
                matcher = SequenceMatcher(None, line_words_temp, table_line_word_temp)
                similarity_ratio_col = matcher.ratio()
                if similarity_ratio_col>max_match and similarity_ratio_col>0.75:
                    max_match=similarity_ratio_col
                    temp_line=line
                    print(f"table_lineis {line_words} and table header is {table_line_word} and {similarity_ratio_col}")
    
            if temp_line:
                table_lines.extend(temp_line)
                break

    print(F'table_line is {table_lines}')
    if not table_lines:
        print("No table header detected.")
        return []

    return table_lines


def is_fuzzy_subsequence(phrase, main_phrase, threshold=0.95):
    phrase_words = phrase.split()
    main_words = main_phrase.split()

    it = iter(main_words)
    matches = []

    # print(f'phrase_words',phrase_words)
    # print(f'main_words',main_words)

    for word in phrase_words:
        best_match = max(main_words, key=lambda w: SequenceMatcher(None, word, w).ratio(), default="")
        match_ratio = SequenceMatcher(None, word, best_match).ratio()
        
        if match_ratio >= threshold and best_match in it:
            matches.append(best_match)
        else:
            return False  # If a word doesn't match well enough or is out of order

    print(f" ################# matches",matches)
    return True


def check_remark_row(line):

     # File paths for the trained model and vectorizer
    model_filename = f"/var/www/extraction_api/app/extraction_folder/table_rows_logistic_regression_model.joblib"
    vectorizer_filename = f"/var/www/extraction_api/app/extraction_folder/table_rows_count_vectorizer.joblib"
    
    try:
        print(F"file found and file name is {vectorizer_filename},{model_filename}")
        # Load the trained model
        clf = joblib.load(model_filename)

        # Load the vectorizer used to convert words into numerical features
        vectorizer = joblib.load(vectorizer_filename)
    except:
        print(F"file not found and file name is {vectorizer_filename},{model_filename}")
        return {}
    try:
        # Transform the single word into numerical features
        X_inference = vectorizer.transform([line])  # Model expects a list

        # Predict the label for the word
        prediction = clf.predict(X_inference)

        print(f"Predicted label for '{line}' is: {prediction[0]}")
        if prediction == 'remark':
            return True  # Return the predicted label
        else:
            return False 
    except Exception as e:
        print(f"Prediction error: {e}")
        return False


def extract_remarks(headers,footers,ocr_word_all):
    try:
        headers=headers['ocrAreas']
    except:
        headers=headers
    footers=[]

    all_head_identifiers=[]
    for head in headers:
        all_head_identifiers.append(combine_dicts(head))
    # print(f" ################# all_head_identifiers",all_head_identifiers)

    base_head={}
    if all_head_identifiers:
        base_head=max(all_head_identifiers, key=lambda w: w["top"])
    # print(f" ################# base_head",base_head)

    all_foot_identifiers=[]
    for foot in footers:
        all_foot_identifiers.append(combine_dicts(foot))
    # print(f" ################# all_foot_identifiers",all_foot_identifiers)

    base_foot={}
    if all_foot_identifiers:
        base_foot=min(all_foot_identifiers, key=lambda w: w["top"])
    # print(f" ################# base_foot",base_foot)

    if not base_head:
        return {},{}

    remarks_ocr_data_temp=[]
    start_header={}
    matched_word=''
    end_matcher=False
    start_matcher=False
    for page in ocr_word_all:
        sorted_words = sorted(page, key=lambda x: (x["top"]))
        word_lines=line_wise_ocr_data(sorted_words)
        current_index=-1
        # print(f'############### word_lines for oage {word_lines[0]} are',word_lines)
        for line in word_lines:
            # print(f'############### line is',line)
            current_index=current_index+1
            for word in line:
                base_identifier_word=re.sub(r'[^a-zA-Z ]', '', base_head['word'])
                if base_identifier_word and not start_matcher:
                    start_matcher = is_fuzzy_subsequence(base_identifier_word, word['word'])
                   
                    if start_matcher:
                        matched_word=word
                        if len(all_head_identifiers)>1:
                            words=[]
                            valid_start=True
                            print(f'############### word_lines next lines {current_index} is',word_lines[current_index-4:current_index+1])
                            for word_ in word_lines[current_index-4:current_index+1]:
                                for wo in word_:
                                    words.append(wo['word'])

                            if not words:
                                for word_ in word_lines[current_index:current_index+3]:
                                    for wo in word_:
                                        words.append(wo['word'])
                            print(f'############### here words to consider for nextline',words)

                            for ident in all_head_identifiers:
                                if base_head['word'] != ident['word']:
                                    if not is_word_present(ident['word'], words, threshold=0.95):
                                        print(f'############### identifier not found in this line and the ident is',ident)
                                        valid_start=False
                                        break
                            if not valid_start:
                                start_matcher=False
                                continue

                    if start_matcher:
                        print(f'first line strated is {line}')
                        # temp_line=[]
                        # for word_ in line:
                        #     if word == word_:
                        #         continue
                        #     if word not in temp_line:
                        #         temp_line.append(word)
                        line.remove(matched_word)
                        start_header=matched_word
                        if line:
                            if line not in remarks_ocr_data_temp:
                                remarks_ocr_data_temp.append(line)
                        break

                if start_matcher and base_foot:
                    end_matcher = is_fuzzy_subsequence(base_foot['word'], word['word'])
                    break

                if start_matcher:
                    if line not in remarks_ocr_data_temp:
                        remarks_ocr_data_temp.append(line)
                    break

            if end_matcher:
                break

    # print(f'############### remarks_ocr_data_temp is',remarks_ocr_data_temp)

    remarks_ocr_data=remarks_ocr_data_temp
    # for temp_line in remarks_ocr_data_temp:
    #     rmk_line=combine_dicts(temp_line)
    #     if check_remark_row(rmk_line['word']):
    #         remarks_ocr_data.append(temp_line)

    # print(f'############### remarks_ocr_data is',remarks_ocr_data)

    for lst in remarks_ocr_data:
        lst.sort(key=lambda x: x["top"])

    if remarks_ocr_data:
        # Sort the outer list based on the smallest "top" value of each inner list
        remarks_ocr_data=sorted(remarks_ocr_data, key=lambda group: (group[0]['pg_no'], group[0]['top']))

    print(f'############### remarks_ocr_data is',remarks_ocr_data)

    Remarks=[]
    start_word=[]
    other_word=[]
    has_start=False
    i=0
    for i in  range(len(remarks_ocr_data)):
        remark_line=sorted(remarks_ocr_data[i], key=lambda x: (x["left"]))
        print(f'############### remark_line is',remark_line)
        for j in range(len(remark_line)):

            if j == len(remark_line)-1 and len(remark_line)>1:
                break
            
            words = remark_line[j]['word']

            if not is_remark_code_present(remark_line[j]['word']):
                continue
            
            has_start=True
            start_word=[remark_line[j]]
            if len(remark_line) == 1:
                other_word=None
            else:
                other_word=remark_line[j+1]
            print(f'############### start_word is',start_word)
            print(f'############### other_word is',other_word)
            break
        if start_word:
            break

        if i>3 and not start_word:
            start_word=remarks_ocr_data[0]
            i=0
            break
    
    if start_word:
        final_remarks={}
        final_high={}
        Remarks.append(start_word)
        print(f'############### Remarks is',Remarks)
        if not other_word:
            flag_stop=False
            prev_bootom=start_word[0]
            for remark_line in remarks_ocr_data[i+1:]:
                remark_line_box=combine_dicts(remark_line)
                for word in remark_line:
                    if start_word[0]['pg_no']!=word['pg_no']:
                        prev_bootom['bottom']=0
                    print(f'############### start_word',start_word[0],word)

                    if prev_bootom['bottom']<word['top']:

                        if abs(prev_bootom['bottom']-word['top'])>50:
                            if not is_remark_code_present(remark_line_box['word']):
                                continue

                        if remark_line not in Remarks:
                            prev_bootom=word
                            Remarks.append(remark_line)

            print(f'############### Remarks is',Remarks)
            
            te=0
            sorted_line = sorted(Remarks[0], key=lambda x: (x["left"]))
            combine_line=combine_dicts(sorted_line)
            prev_length=combine_line['right']-combine_line['left']
            temp_remark=[combine_dicts(Remarks[0])]

            print(f'############### temp_remark is',temp_remark)
            
            for re_line in Remarks[1:]:

                sorted_line = sorted(re_line, key=lambda x: (x["left"]))
                current_line=combine_dicts(sorted_line)
                current_length=current_line['right']-current_line['left']

                print(f'############### current_line is',current_line)
                print(f'############### prev_length is',prev_length)
                print(f'############### current_length is',current_length)

                check_start=False
                if has_start:
                    check_start=is_remark_code_present(current_line['word'])

                if check_start:
                    lines=line_wise_ocr_data(temp_remark)
                    all_words=[]
                    for line in lines:
                        # line[-1]['word']=line[-1]['word']+'//n'
                        all_words.extend(line)
                    final_remarks[str(te+1)+'**']=combine_dicts(all_words)['word']
                    final_high[str(te+1)+'**']=from_highlights(combine_dicts(temp_remark))
                    temp_remark=[current_line]
                    te=te+1
                else:
                    temp_remark.append(current_line)

                prev_length=current_length
                print(f'############### temp_remark is',temp_remark)

            if temp_remark:
                lines=line_wise_ocr_data(temp_remark)
                all_words=[]
                for line in lines:
                    # line[-1]['word']=line[-1]['word']+'//n'
                    all_words.extend(line)
                final_remarks[str(te+1)+'**']=combine_dicts(all_words)['word']
                final_high[str(te+1)+'**']=from_highlights(combine_dicts(temp_remark))

        elif other_word:
            try:
                prev_bootom = start_word['bottom']
                prev_page = start_word['pg_no']
            except Exception as e:
                try:
                    start_word = start_word[0] 
                    prev_bootom = start_word['bottom']
                    prev_page = start_word['pg_no']
                except Exception as e:
                    print(f"the exception is::::{e}")  # or handle differently if needed

            # prev_bootom=start_word['bottom']
            prev_page=start_word['pg_no']
            prev_bootom = start_word['bottom']
            break_flag=False
            for remark_line in remarks_ocr_data[i+1:]:
                if prev_page != remark_line[0]['pg_no']:
                    prev_bootom=remark_line[0]['top']

                remark_line=sorted(remark_line, key=lambda x: x["left"])
                print(f'############### start_word checking in ',remark_line)

                for word in remark_line:

                    if word['height']>10:
                        continue

                    if start_word['right']+5>word['left']>start_word['left']-5 or start_word['right']+5>word['right']>start_word['left']-5 or start_word['right']+5>(word['left']+word['right'])/2>start_word['left']-5:
                        temP_word = word['word'].split()  # Step 1: split into words
                        cleaned_words = [re.sub(r'[^a-zA-Z0-9]', '', word_) for word_ in temP_word if re.sub(r'[^a-zA-Z0-9]', '', word_)]

                        if is_remark_code_present(word['word']):
                            print(f'############### start_word found in ',word)
                            if len(cleaned_words)>1:
                                temp=copy.deepcopy(word)
                                temp['left']=0
                                temp['right']=0
                                temp['word']=cleaned_words[0]
                                print(f'############### modified strat in ',temp)
                                Remarks.append(temp)
                            else:
                                Remarks.append(word)
                            break
                        # elif len(cleaned_words) == 1 and word not in Remarks and (not re.sub(r'[^a-zA-Z]', '', cleaned_words[0]) or cleaned_words[0].isupper()):
                        #     Remarks.append(word)
                    elif word['right']+5>start_word['left']>word['left']-5 or word['right']+5>start_word['right']>word['left']-5 or word['right']+5>(start_word['left']+start_word['right'])/2>word['left']-5:
                        temP_word = word['word'].split()  # Step 1: split into words
                        cleaned_words = [re.sub(r'[^a-zA-Z0-9]', '', word_) for word_ in temP_word if re.sub(r'[^a-zA-Z0-9]', '', word_)]

                        if is_remark_code_present(word['word']):
                            print(f'############### start_word found in ',word)
                            if len(cleaned_words)>1:
                                temp=copy.deepcopy(word)
                                temp['left']=0
                                temp['right']=0
                                temp['word']=cleaned_words[0]
                                print(f'############### modified strat in ',temp)
                                Remarks.append(temp)
                            else:
                                Remarks.append(word)
                            break
                        # elif len(cleaned_words) == 1 and word not in Remarks and (not re.sub(r'[^a-zA-Z]', '', cleaned_words[0]) or cleaned_words[0].isupper()):
                        #     Remarks.append(word)
                    elif word['left']<start_word['left']:
                        temP_word = word['word'].split()  # Step 1: split into words
                        cleaned_words = [re.sub(r'[^a-zA-Z0-9]', '', word_) for word_ in temP_word if re.sub(r'[^a-zA-Z0-9]', '', word_)]

                        if is_remark_code_present(word['word']):
                            print(f'############### start_word found in ',word)
                            if len(cleaned_words)>1:
                                temp=copy.deepcopy(word)
                                temp['left']=0
                                temp['right']=0
                                temp['word']=cleaned_words[0]
                                print(f'############### modified strat in ',temp)
                                Remarks.append(temp)
                            else:
                                Remarks.append(word)
                            break

                prev_bootom=word['bottom']
                prev_page=word['pg_no']

                if break_flag:
                    break
        
            # print(f'############### Remarks is',Remarks)
            print(f'############### Remarks is', Remarks)

            remarks_code = {}
            if Remarks:
                last_bottom = next((r for r in Remarks[:2] if isinstance(r, dict) and 'bottom' in r), None)
                # last_bottom = next((r for r in Remarks[:2] if isinstance(r, dict) and 'bottom' in r), None)
                for i in range(len(Remarks)):
                    current = Remarks[i][0] if isinstance(Remarks[i], list) else Remarks[i]
                    current_top = current.get('top')
                    current_bottom = current.get('bottom')
                    current_page = current.get('pg_no')

                    if len(Remarks) - 1 == i:
                        next_top = 1000
                    else:
                        next_item = Remarks[i + 1][0] if isinstance(Remarks[i + 1], list) else Remarks[i + 1]
                        if next_item.get('pg_no') == current_page:
                            next_top = next_item.get('top', 1000)
                        else:
                            next_top = 1000

                    if isinstance(last_bottom, dict) and last_bottom.get('bottom', 0) > current_top:
                        last_bottom = Remarks[i]

                    print(f'############### current_top is', current_top)
                    print(f'############### next_top is', next_top)
                    print(f'############### last_bottom is', last_bottom)

                    code_line = []

                    for remark_data in remarks_ocr_data:

                        print(f'############### lone is',remark_data)

                        if current_page != remark_data[0]['pg_no']:
                            continue

                        top=max(remark_data, key=lambda w: w["top"])
                        left=max(remark_data, key=lambda w: w["left"])

                        # print(f'############### top is',top)
                        if (last_bottom and abs(top['top'] - (last_bottom[0]['bottom'] if isinstance(last_bottom, list) else last_bottom['bottom']) ) > 10 
                            and top['top'] > (last_bottom[0]['bottom'] if isinstance(last_bottom, list) else last_bottom['bottom']) 
                            and code_line):
                            break
  
                        if (last_bottom and (last_bottom[0]['left'] if isinstance(last_bottom, list) else last_bottom['left']) > left['left'] 
                            and code_line):
                            break

                        code_line=[]

                        sorted_data = sorted(remark_data, key=lambda x: (x["left"]))
                        key = Remarks[i][0]['word'] if isinstance(Remarks[i], list) else Remarks[i]['word']
                        if next_top>top['top']>=current_top  or current_bottom>top['bottom']>=current_top:
                            for word in sorted_data:
                                if word ['word']== key:
                                    continue
                                code_line.append(word)
                                last_bottom=sorted_data[0]

                        if code_line:
                            if key not in remarks_code:
                                remarks_code[key] = []
                            remarks_code[key].append(combine_dicts(code_line))

                        print(f'############### code_line is',code_line)

                    print(f'############### remarks_code is',remarks_code)

                for remark_code,value in  remarks_code.items(): 
                    temp=combine_dicts(value)   
                    final_remarks[remark_code]=temp['word']
  
        try:    
            final_high['Remark_default']=from_highlights(start_header)
        except:
            pass

        return final_remarks,final_high
    else:
        return {}, {}



# def is_remark_code_present(line):
#     """
#     Detects standard US EOB remark codes and payer-specific codes:
#     - M + 1–3 digits (e.g., M15)
#     - MA + 2 digits (e.g., MA01)
#     - N + 1–3 digits (e.g., N24)
#     - L + 2–3 digits (e.g., L123)
#     - C + 1–3 digits (e.g., C36)
#     - CO, PR, OA, PI + 1–4 digits (e.g., CO45)
#     - 1–3 digit standalone numerics (e.g., 23)
#     - Custom alphanumeric codes like 6TZ (3 chars, mix of digits & letters)
#     - 2-letter + 3–6 digits (e.g., EL00057)
#     - 3-letter codes with no digits (e.g., FNO, ABC)
#     """

#     valid_code_pattern = r"""
#         (?<![A-Z0-9])                             # No prefixing alphanumerics
#         (?:                                       # Start group of valid codes
#             MA\d{2} |                             # MA01
#             M\d{1,3} |                            # M15, M1
#             N\d{1,3} |                            # N24
#             L\d{2,3} |                            # L123
#             C\d{1,3} |                            # C36
#             (?:CO|OA|PR|PI)\d{1,4} |              # CO45, OA23, etc.
#             [A-Z]{2}\d{3,6} |                     # EL00057, AB123, XX98765
#             (?<!\w)\d{1,3}(?!\w) |                # Standalone numeric codes
#             (?<!\w)(?=[A-Z0-9]{3})(?=[A-Z0-9]*\d)(?=[A-Z0-9]*[A-Z])[A-Z0-9]{3}(?!\w) |  # 3-char alphanumeric (6TZ)
#             (?<!\w)[A-Z]{3}(?!\w)                  # 3-letter-only codes (FNO, ABC)
#         )
#         (?![A-Z0-9])                              # No suffixing alphanumerics
#     """

#     line_clean = re.sub(r"[^\w\s]", " ", line)
#     matches = re.findall(valid_code_pattern, line_clean, re.VERBOSE | re.IGNORECASE)

#     if matches:
#         print(f"✅ Remark code(s) found: {matches} in line: '{line}'")
#         return True
#     return False


def is_remark_code_present(line):
    """
    Detects standard US EOB remark codes and payer-specific codes:
    Works even if codes are wrapped in parentheses, e.g., (PD), (M15).
    """

    valid_code_pattern = r"""
        ^                                          # Must be at start of line
        \(?\s*   
        (?:                                        # Start group
            (?:CO|OA|PR|PI)\d{1,4} |               # CARC codes
            MA\d{2} |                              # MA01
            M\d{1,3} |                             # M15
            N\d{1,3} |                             # N24
            L\d{2,3} |                             # L123
            C\d{1,3} |                             # C36
            (?:CO|OA|PR|PI)\d{1,4} |              # CO45, OA23, etc.
            [A-Z]{2}\d{3,6} |                     # EL00057, AB123, XX98765
            D\d{3,4} |                             # Dental CDT codes
            [A-Z]{2}\d{3,6} |                      # Payer-specific 2L+digits
            \d{1,3} |                              # Standalone numeric
            (?=[A-Z0-9]{3})(?=[A-Z0-9]*\d)(?=[A-Z0-9]*[A-Z])[A-Z0-9]{3} | # 3-char alphanumeric
            [A-Z]{2,3} |                           # 3-letter-only codes
            [A-Z0-9]{2,6}                          # Catch-all payer-specific short codes
        )
        \s*\)?                                     # Optional closing parenthesis
        (?![A-Z0-9])                               # No trailing letters/numbers
    """

    matches = re.findall(valid_code_pattern, line, re.VERBOSE)

    if matches:
        print(f"✅ Remark code(s) found: {matches} in line: '{line}'")
        return True
    return False


def get_structure(line):
    parts = line.strip().split()
    structure = []
    for part in parts:
        if re.match(r'^\d{2}/\d{2}/\d{4}$', part):
            structure.append('DATE')
        elif re.match(r'^\d+\.\d+$', part):
            structure.append('FLOAT')
        elif re.match(r'^\d+$', part):
            structure.append('INT')
        elif re.match(r'^[A-Z0-9]+$', part):
            structure.append('WORD')
        else:
            structure.append('OTHER')
    return structure

def is_similar_with_tolerance(struct1, struct2, allowed_mismatches=2):
    # print(struct1,struct2)
    i = j = mismatches = 0
    while i < len(struct1) and j < len(struct2):
        if struct1[i] == struct2[j]:
            i += 1
            j += 1
        else:
            mismatches += 1
            if mismatches > allowed_mismatches:
                return False
            # Try to skip one in either struct1 or struct2
            if i + 1 < len(struct1) and struct1[i+1] == struct2[j] and len(struct1) > len(struct2):
                # print(struct1[i+1],struct2[j],'1')
                i += 1
            elif j + 1 < len(struct2) and struct1[i] == struct2[j+1] and len(struct1) < len(struct2):
                # print(struct1[i],struct2[j+1],'2')
                j += 1
            else:
                # print('nothing')
                if len(struct1) > len(struct2):
                # If skipping didn't help, count as mismatch and move both
                    i += 1
                elif len(struct1) < len(struct2):
                    j += 1
                else:
                    i += 1
                    j += 1
                    
    # Remaining parts can also cause mismatch
    mismatches += abs((len(struct1) - i) + (len(struct2) - j))
    return mismatches <= allowed_mismatches
    

def get_filtered_section(section_got,section_format_identification):

    first_format_dict=section_format_identification[0]
    word_struture=[wor['word'] for wor in first_format_dict]
    word_format=' '.join(word_struture)
    print(f'first_format structure for {word_format}')
    rest_formats=section_format_identification[1:]
    first_structure=get_structure(word_format)
    section={}

    flag=False
    for page,ocr in section_got.items():
        print(f'iterating through {page}')
        lines=line_wise_ocr_data(ocr)
        for i in range(len(lines)):
            if not flag:
                line_structure=get_structure(' '.join([wor['word'] for wor in lines[i]]))
                print(f"similar structure matching btw {' '.join([wor['word'] for wor in lines[i]])}")
                print(f'and {word_format}')
                if is_similar_with_tolerance(first_structure, line_structure):
                    print(f'similar structure matched for {lines[i]}')
                    tem_flag=True
                    index=0
                    for tem_li in lines[i+1:]:
                        if index==len(rest_formats):
                            break
                        next_str=get_structure(' '.join([wor['word'] for wor in rest_formats[index]]))
                        temp_structure=get_structure(' '.join([wor['word'] for wor in tem_li]))
                        if is_similar_with_tolerance(next_str, temp_structure):
                            index=index+1
                        else:
                            tem_flag=False
                            break

                    if tem_flag:
                        flag=True

            if flag:
                if page not in section:
                    section[page]=[]
                section[page].extend(lines[i])    
        
    return section



def predict_sub_sub_templates(section_identifier,ocr_word_all,ocr_data_all):
    
    print(f" ################# identifiers['section_identifiers']",section_identifier)
    if not section_identifier:
        return None,None,None,None
    
    start=section_identifier

    all_identifiers=[]
    other_identifers={}
    for identifiers in start:
        if not identifiers:
            continue
        lines_identifiers=line_wise_ocr_data(identifiers)
        temp_ident=combine_dicts(lines_identifiers[0])
        all_identifiers.append(temp_ident)
        other_identifers[temp_ident['word']]=[]
        for other_ident in lines_identifiers[1:]:
            other_identifers[temp_ident['word']].append(combine_dicts(other_ident)['word'])
    print(f" ################# all_identifiers",all_identifiers)

    base_identifier=min(all_identifiers, key=lambda w: w["top"])
    print(f" ################# base_identifier",base_identifier)

    cordinates=[]
    end_point={}

    for page in ocr_word_all:
        sorted_words = sorted(page, key=lambda x: (x["top"]))
        word_lines=line_wise_ocr_data(sorted_words)
        current_index=-1
        for line in word_lines:
            current_index=current_index+1
            for word in line:
                base_identifier_word=re.sub(r'[^a-zA-Z]', '', base_identifier['word'])
                if base_identifier_word:

                    if end_point and ((end_point['pg_no'] > word['pg_no']) or (end_point['pg_no'] == word['pg_no'] and word['top'] < end_point['bottom'])):
                        continue
                    
                    # start_matcher = is_fuzzy_subsequence(base_identifier['word'], word['word'])

                    match_ratio = SequenceMatcher(None, word['word'], base_identifier['word']).ratio()
                    
                    if match_ratio >= 0.85:
                        start_matcher=True
                    else:
                        start_matcher=False

                    if start_matcher:

                        if len(all_identifiers)>1:
                            words=[]
                            valid_start=True
                            for word_ in word_lines[current_index:current_index+4]:
                                for wo in word_:
                                    words.extend(wo['word'].split())
                            print(f'############### here words to consider for nextline',words)

                            for ident in all_identifiers:
                                if base_identifier['word'] != ident['word']:
                                    temp_ids=ident['word'].split()
                                    for temp_id in temp_ids:
                                        if not is_word_present(temp_id, words, threshold=0.95):
                                            print(f'############### identifier not found in this line and the ident is',ident)
                                            valid_start=False
                                            break
                            
                            if not valid_start:
                                continue

                        if cordinates and not end_point and len(cordinates[-1])<3:
                            print(f'############### another line is found but append this start as last end',word)
                            cordinates[-1].extend([word['pg_no'],word['top']])

                        valid=True
                        if other_identifers and len(other_identifers[base_identifier['word']])>0:
                            words=[]
                            for word_ in word_lines[current_index:current_index+4]:
                                for wo in word_:
                                    words.append(wo['word'])
                            print(f'############### here words to consider for nextline',words)
                            for ident in other_identifers[base_identifier['word']]:
                                if not is_word_present(ident, words, threshold=0.95):
                                    print(f'############### identifier not found in this line and the ident is',ident)
                                    valid=False
                                    break

                        if not valid:
                            continue

                        elif end_point:
                            print(f'################ end_point',end_point,'for',word)
                            cordinates.append([word['pg_no'],word['top'],end_point['pg_no'],end_point['bottom']])
                                
                        elif not end_point:
                            print(f'################ no end_point',end_point,'for',word)
                            cordinates.append([word['pg_no'],word['top']])
                            
    print(f'################ cordinates',cordinates)
    for page in reversed(ocr_word_all):
        if page:  # non-empty list
            last_page_words = page
            break

    # Sort if there are words
    last_page = sorted(last_page_words, key=lambda x: x["top"]) if last_page_words else []

    print(f'################ last_page',last_page)
    if cordinates and len(cordinates[-1])<4:
        # print(f'############### we have didnt find any end point in the last so appending last wrd of last page',last_page[-1])
        cordinates[-1].extend([last_page[-1]['pg_no'],last_page[-1]['bottom']])

    # print(f'cordinates',cordinates)
    reamark_cordinates=[]
    for i in range(len(cordinates)):

        current=cordinates[i]
        if i == len(cordinates)-1:
            next_=[last_page[-1]['pg_no'],last_page[-1]['bottom']]
        else:
            next_=cordinates[i+1]
        merged = current[:2] + next_[:2] 
        reamark_cordinates.append(merged)

    remarks_section_ocr=[]
    for cordinate in reamark_cordinates:
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_word_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word=word_line[0]
                if word['top'] >= end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top'] >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    section[word['pg_no']].extend(word_line)
            if stop:
                break

        remarks_section_ocr.append(section)

    print(f'section ocr satrting here')
    all_sections=[]
    index=-1
    all_headers=[] 
    for cordinate in cordinates:
        index=index+1
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_word_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word_line=sorted(word_line, key=lambda x: (x["pg_no"], x["top"]))
                word=word_line[0]
                if word['bottom'] > end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top']+5 >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    # print(f"start word is {word}")
                    section[word['pg_no']].extend(word_line)
            if stop:
                break
        # print(f'one section ending here',section.keys())
        all_sections.append(section)


    print(f'section ocr 2 satrting here')
    all_headers=[]  
    all_ocr_sections=[]
    index=-1
    for cordinate in cordinates:
        index=index+1
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_data_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            if not page:
                continue
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word_line=sorted(word_line, key=lambda x: (x["pg_no"], x["top"]))
                word=word_line[0]
                if word['bottom'] > end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top']+5 >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    # print(f"start word is {word}")
                    section[word['pg_no']].extend(word_line)
            if stop:
                break

        all_ocr_sections.append(section)
                
    return all_sections,cordinates,all_ocr_sections,remarks_section_ocr


def predict_paragraph(para_fields,ocr_word_all):

    para_field,para_high={},{}

    for field,section_identifier in para_fields.items():

        print(f" ################# identifiers['section_identifiers']",section_identifier)
        if not section_identifier:
            return None,None,None,None
        
        start=section_identifier[0]
        print(f" ################# start",start)

        start_idenitfier={}
        if start:
            start_idenitfier=combine_dicts(start)
            print(f" ################# start_identifiers",start_idenitfier)

        if not start:
            continue

        try:
            end=section_identifier[1]
        except:
            end=[]
        print(f" ################# end",end)
        end_identifiers={}
        if end:
            end_identifiers=combine_dicts(end)
            print(f" ################# end_identifiers",end_identifiers)

        start_matcher=False
        end_matcher=False
        stop=False
        needed_lines=[]
        for page in ocr_word_all:
            sorted_words = sorted(page, key=lambda x: (x["top"]))
            word_lines=line_wise_ocr_data(sorted_words)
            current_index=-1
            for line in word_lines:
                current_index=current_index+1
                for word in line:
                    cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
                    base_identifier_word=re.sub(r'[^a-zA-Z]', '', start_idenitfier['word'])
                    if base_identifier_word and not start_matcher:
                        start_matcher = is_fuzzy_subsequence(base_identifier_word, cleaned_word)
                        if start_matcher:
                            print(f" ################# start_matcher found at",word['word'])
                            break
                    
                    base_end_identifier_word=re.sub(r'[^a-zA-Z]', '', end_identifiers['word'])
                    if base_end_identifier_word:
                        end_matcher = is_fuzzy_subsequence(base_end_identifier_word,cleaned_word)
                        if end_matcher:
                            print(f" ################# end_matcher found at",word['word'])
                            break

                if start_matcher and not end_matcher:
                    needed_lines.append(line)
                elif start_matcher and end_matcher:
                    needed_lines.append(line)
                    stop=True

                if stop:
                    break
            
            if stop:
                break
            
        print(f" ################# found needed_lines are",needed_lines)

        if needed_lines:
            needed_para=''
            needed_para_high=[]
            for line in needed_lines:
                sorted_para_line = sorted(line, key=lambda x: (x["left"]))
                for wor in sorted_para_line:
                    needed_para=needed_para+' '+wor['word']
                needed_para_high.extend(line)
            print(f" ################# found needed_para is",needed_para)
            para_field[field]=needed_para
            para_high[field]=from_highlights(combine_dicts(needed_para_high))

    return para_field,para_high

                    
def predict_sub_templates(section_identifier,ocr_word_all,ocr_data_all,section_header):

    section_heads=[]
    section_format=[]
    if section_header:
        section_heads=section_header['identifiers']
        section_format=section_header['formats']

    try:
        print(f" ################# identifiers['section_identifiers']",section_identifier)
        if not section_identifier:
            return None,None,None,None
        
        start=section_identifier[0]
        # print(f" ################# start",start)
        # end=section_identifier[1]
        try:
            end=section_identifier[1]
        except:
            end=[]
        # print(f" ################# end",end)


        all_identifiers=[]
        other_identifers={}
        for identifiers in start:
            if not identifiers:
                continue
            lines_identifiers=line_wise_ocr_data(identifiers)
            temp_ident=combine_dicts(lines_identifiers[0])
            all_identifiers.append(temp_ident)
            other_identifers[temp_ident['word']]=[]
            for other_ident in lines_identifiers[1:]:
                other_identifers[temp_ident['word']].append(combine_dicts(other_ident)['word'])
        print(f" ################# all_identifiers",all_identifiers)
        # print(f"other identifiers got are {other_identifers}")

        base_identifier=min(all_identifiers, key=lambda w: w["top"])
        print(f" ################# base_identifier",base_identifier)

        all_end_identifiers=[]
        if end:
            for identifiers in end:
                if identifiers:
                    all_end_identifiers.append(combine_dicts(identifiers))
            print(f" ################# all_end_identifiers",all_end_identifiers)

        base_end_identifier={}
        if all_end_identifiers:
            base_end_identifier=max(all_end_identifiers, key=lambda w: w["top"])
            print(f" ################# base_end_identifier",base_end_identifier)

        # Compute distances
        distances = {}
        for word in all_end_identifiers:
            if word != base_identifier and word:
                dist = base_identifier['right']-word['left']
                distances[word["word"]]= dist
        #         print(f" ################# word and dist",word ,dist)
        # print(f" ################ distances",distances)

    except Exception as e:
        print(f"here we have an exception {e}")
        all_identifiers=[]
        other_identifers={}
        base_end_identifier={}
        all_end_identifiers=[]
        for identifiers in section_identifier:
            all_identifiers.append(combine_dicts(identifiers))
        print(f" ################# all_identifiers",all_identifiers)

        base_identifier=min(all_identifiers, key=lambda w: w["top"])
        print(f" ################# base_identifier",base_identifier)

        # Compute distances
        distances = {}
        for word in all_identifiers:
            if word != base_identifier:
                dist = base_identifier['right']-word['left']
                distances[word["word"]]= dist
                print(f" ################# word and dist",word ,dist)
        print(f" ################ distances",distances)

    found_flag=False
    cordinates=[]
    end_point={}

    Claim_countinuation_word=['CONTINUED ON NEXT PAGE']

    try:
        start_page=ocr_word_all[0][0]['pg_no']
    except:
        for page in ocr_word_all:
            for words in page:
                for word in words:
                    start_page=word['pg_no']
    skip_count=0

    for page in ocr_word_all:
        sorted_words = sorted(page, key=lambda x: (x["top"]))
        word_lines=line_wise_ocr_data(sorted_words)
        current_index=-1

        print(f'################ we are at validating word {page[0]["pg_no"]}')

        for line in word_lines:
            current_index=current_index+1

            line_words = [word["word"] for word in line]
            line_words=' '.join(line_words)
            for wo in Claim_countinuation_word:
                if is_fuzzy_subsequence(wo, line_words):
                    skip_count=skip_count+1
                    break

            for word in line:
                cleaned_word=re.sub(r'[^a-zA-Z]', '', word['word'])
                base_identifier_word=re.sub(r'[^a-zA-Z]', '', base_identifier['word'])
                if base_identifier_word:

                    if end_point and ((end_point['pg_no'] > word['pg_no']) or (end_point['pg_no'] == word['pg_no'] and word['top'] < end_point['bottom'])):
                        continue
                    
                    start_matcher = is_fuzzy_subsequence(base_identifier['word'], word['word'])
                    print(f'################ start_matcher word {word["word"]} is {start_matcher}')

                    similarity_ratio_con_end=0
                    end_matcher=False
                    if base_end_identifier:
                        end_matcher = is_fuzzy_subsequence(base_end_identifier['word'], word['word'])

                    if skip_count >0 and (start_matcher or end_matcher):
                        start_matcher=False
                        end_matcher=False
                        skip_count=skip_count-1

                    if start_matcher:

                        if len(all_identifiers)>1:
                            words=[]
                            valid_start=True
                            for word_ in word_lines[current_index:current_index+4]:
                                for wo in word_:
                                    words.extend(wo['word'].split())
                            print(f'############### here words to consider for nextline',words)

                            for ident in all_identifiers:
                                if base_identifier['word'] != ident['word']:
                                    temp_ids=ident['word'].split()
                                    for temp_id in temp_ids:
                                        if not is_word_present(temp_id, words, threshold=0.95):
                                            print(f'############### identifier not found in this line and the ident is',ident)
                                            valid_start=False
                                            break
                            
                            if not valid_start:
                                continue

                        if cordinates and not end_point and len(cordinates[-1])<3:
                            print(f'############### another line is found but append this start as last end',word)
                            cordinates[-1].extend([word['pg_no'],word['top']])

                        valid=True
                        if other_identifers and len(other_identifers[base_identifier['word']])>0:
                            words=[]
                            for word_ in word_lines[current_index:current_index+4]:
                                for wo in word_:
                                    words.append(wo['word'])
                            print(f'############### here words to consider for nextline',words)
                            for ident in other_identifers[base_identifier['word']]:
                                if not is_word_present(ident, words, threshold=0.95):
                                    print(f'############### identifier not found in this line and the ident is',ident)
                                    valid=False
                                    break

                        if not valid:
                            continue

                        end_point=valid_identifier(ocr_word_all,distances,word)
                        print(f'################ sub template is found at line',word ,end_point)

                        if end_point and len(all_end_identifiers)>1:
                            words=[]
                            valid_end=True
                            ind=0
                            for page_end in ocr_word_all:
                                if end_point['pg_no']== page_end[0]['pg_no']:
                                    word_lines_ed=line_wise_ocr_data(page_end)
                                    for word_ in word_lines_ed:
                                        if ind>5:
                                            break
                                        if end_point['top']<=word_[0]['top']:
                                            for wo in word_:
                                                words.append(wo['word'])
                                            ind=ind+1
                            print(f'############### here words to consider for 1',words)

                            for ident in all_end_identifiers:
                                if base_end_identifier['word'] != ident['word']:
                                    if not is_word_present(ident['word'], words, threshold=0.95):
                                        print(f'############### identifier not found in this line and the ident is',ident)
                                        valid_end=False
                                        break

                            if valid_end:
                                if end_point:
                                    print(f'################ end_point',end_point,'for',word)
                                    cordinates.append([word['pg_no'],word['top'],end_point['pg_no'],end_point['bottom']])
                                
                        elif end_point:
                            print(f'################ end_point',end_point,'for',word)
                            cordinates.append([word['pg_no'],word['top'],end_point['pg_no'],end_point['bottom']])
                                
                        elif not end_point:
                            print(f'################ no end_point',end_point,'for',word)
                            cordinates.append([word['pg_no'],word['top']])
                            print(f'################ cordinates',cordinates)

                    elif end_matcher:

                        print(f'############## validatig end identifier for nextline')

                        words=[]
                        valid_end=True
                        for word_ in word_lines[current_index:current_index+4]:
                            for wo in word_:
                                words.extend(wo['word'].split())
                        print(f'############### here words to consider for nextline',words)

                        for ident in all_end_identifiers:
                            if base_end_identifier['word'] != ident['word']:
                                base_end=ident['word'].split()
                                for end_wo in base_end:
                                    if not is_word_present(end_wo, words, threshold=0.95):
                                        print(f'############### identifier not found in this line and the ident is',ident)
                                        valid_end=False
                                        break

                        if valid_end:
                            if cordinates:
                                if len(cordinates[-1])<3:
                                    print(f'############### another line is found but append this start as last end',word)
                                    cordinates[-1].extend([word['pg_no'],word['bottom']])
                                    continue

                                print(f'################ direct end_point',end_point,'for',word)
                                cordinates.append([cordinates[-1][2],cordinates[-1][3],word['pg_no'],word['bottom']])
                                print(f'################ cordinates',cordinates)
                            else:
                                cordinates.append([start_page,0,word['pg_no'],word['bottom']])
                                print(f'################ cordinates',cordinates)

    last_page=sorted(ocr_data_all[-1], key=lambda x: (x["top"]))
    if cordinates and len(cordinates[-1])<4:
        # print(f'############### we have didnt find any end point in the last so appending last wrd of last page',last_page[-1])
        cordinates[-1].extend([last_page[-1]['pg_no'],last_page[-1]['bottom']])

    # print(f'cordinates',cordinates)
    reamark_cordinates=[]
    for i in range(len(cordinates)):

        current=cordinates[i]
        if i == len(cordinates)-1:
            next_=[last_page[-1]['pg_no'],last_page[-1]['bottom']]
        else:
            next_=cordinates[i+1]
        merged = current[:2] + next_[:2] 
        reamark_cordinates.append(merged)

    remarks_section_ocr=[]
    for cordinate in reamark_cordinates:
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_word_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word=word_line[0]
                if word['top'] >= end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top'] >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    section[word['pg_no']].extend(word_line)
            if stop:
                break
        # print(f'one section ending here',section.keys())
        for head in section_heads:
            detected_header=extract_section_header(ocr_pages,head,start_pg)
            # print(f'section_headers here',detected_header)
            for head in detected_header:
                if head['pg_no'] in section:
                    section[head['pg_no']].extend(detected_header)
                    break
        remarks_section_ocr.append(section)

    print(f'section ocr satrting here')
    all_sections=[]
    index=-1
    all_headers=[] 
    for cordinate in cordinates:
        index=index+1
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_word_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word_line=sorted(word_line, key=lambda x: (x["pg_no"], x["top"]))
                word=word_line[0]
                if word['bottom'] > end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top']+5 >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    # print(f"start word is {word}")
                    section[word['pg_no']].extend(word_line)
            if stop:
                break
        # print(f'one section ending here',section.keys())
        if section_heads:
            section_format_identification=section_format

            if index==0:
                for head in section_heads:
                    detected_header_got=extract_section_header(ocr_pages,head,start_pg)
                    detected_header=copy.deepcopy(detected_header_got)
                    if detected_header:
                        all_headers.extend(detected_header)
                print(f'all_headers here',all_headers)
                if all_headers:
                    temp_section=get_filtered_section(section,section_format_identification)

                    if temp_section:
                        section=temp_section

                    print(f'index 0 here',detected_header)
                    for he in all_headers:
                        if he in section[start_pg]:
                            section[start_pg].remove(he)

                    header_box=combine_dicts(all_headers)
                    print(f'header_box here',header_box)

                    # print(f'section[word[pg_no]] here',section[word['pg_no']])
                    actual_start=combine_dicts(section[start_pg])
                    print(f'actual_start here',actual_start)
                    actual_start=actual_start['top']
                    # print(f'actual_start here',actual_start)
                    second_header_diff=header_box['bottom']-actual_start
                    # print(f'second_header_diff here',second_header_diff)
                    
                    for head in all_headers:
                        if head['pg_no'] in section:
                            section[head['pg_no']].extend(all_headers)
                            break

            else:

                if section and all_headers:
                    header_box=[]

                    temp_section=get_filtered_section(section,section_format_identification)
                    if temp_section:
                        section=temp_section
                        temp_start_pg=list(temp_section.keys())[0]
                    else:
                        temp_start_pg=start_pg

                    temp_headers=copy.deepcopy(all_headers)
                    header_box=combine_dicts(temp_headers)
                    
                    if section[temp_start_pg]:
                        start_cord=combine_dicts(section[temp_start_pg])
                    else:
                        start_cord=combine_dicts(section[start_pg])
                    print(f'start_cord here',start_cord)

                    if start_cord:
                        start_cord=start_cord['top']
                        current_header_diff=header_box['bottom']-start_cord
                        print(f'current_header_diff here',current_header_diff)
                        print(f'second_header_diff here',second_header_diff)
                        overall_diff=current_header_diff-second_header_diff
                        print(f'overall_diff here',overall_diff)
                        
                        for head in temp_headers:
                            if head['pg_no']!= temp_start_pg:
                                head['pg_no']=temp_start_pg
                            head['top']=head['top']-overall_diff
                            head['bottom']=head['bottom']-overall_diff

                        # print(f'section_headers after modfication here',detected_header)
                        for head in temp_headers:
                            if head['pg_no'] in section:
                                section[head['pg_no']].extend(temp_headers)
                                break

        all_sections.append(section)


    print(f'section ocr 2 satrting here')
    all_headers=[]  
    all_ocr_sections=[]
    index=-1
    for cordinate in cordinates:
        index=index+1
        # print(f'cordinate',cordinate)
        section={}
        start_pg=cordinate[0]
        start_cord=cordinate[1]
        end_pg=cordinate[2]
        end_cord=cordinate[3]
        ocr_pages=[]
        for ocr_word in ocr_data_all:
            if not ocr_word:
                continue
            if start_pg <= ocr_word[0]['pg_no'] <= end_pg:
                ocr_pages.append(ocr_word)
            if ocr_word[0]['pg_no']>end_pg:
                break
        stop=False
        start=False
        for page in ocr_pages:
            if not page:
                continue
            word_lines=line_wise_ocr_data(page)
            section[page[0]['pg_no']]=[]
            for word_line in word_lines:
                word_line=sorted(word_line, key=lambda x: (x["pg_no"], x["top"]))
                word=word_line[0]
                if word['bottom'] > end_cord and end_pg == word['pg_no']:
                    # print(f"stoping word is {word}")
                    stop=True 
                    break
                if word['top']+5 >= start_cord and start_pg == word['pg_no'] and not start:
                    start=True
                    
                if start:
                    # print(f"start word is {word}")
                    section[word['pg_no']].extend(word_line)
            if stop:
                break
        # print(f'one section ending here',section.keys())
        if section_heads:
            section_format_identification=section_format

            if index==0:
                for head in section_heads:
                    detected_header_got=extract_section_header(ocr_pages,head,start_pg)
                    detected_header=copy.deepcopy(detected_header_got)
                    if detected_header:
                        all_headers.extend(detected_header)
                print(f'all_headers here',all_headers)
                if all_headers:
                    temp_section=get_filtered_section(section,section_format_identification)

                    if temp_section:
                        section=temp_section

                    print(f'index 0 here',detected_header)
                    for he in all_headers:
                        if he in section[start_pg]:
                            section[start_pg].remove(he)

                    header_box=combine_dicts(all_headers)
                    print(f'header_box here',header_box)

                    # print(f'section[word[pg_no]] here',section[word['pg_no']])
                    actual_start=combine_dicts(section[start_pg])
                    print(f'actual_start here',actual_start)
                    actual_start=actual_start['top']
                    # print(f'actual_start here',actual_start)
                    second_header_diff=header_box['bottom']-actual_start
                    # print(f'second_header_diff here',second_header_diff)
                    
                    for head in all_headers:
                        if head['pg_no'] in section:
                            section[head['pg_no']].extend(all_headers)
                            break

            else:
            
                if section and all_headers:
                    header_box=[]

                    temp_section=get_filtered_section(section,section_format_identification)
                    if temp_section:
                        section=temp_section
                        temp_start_pg=list(temp_section.keys())[0]
                    else:
                        temp_start_pg=start_pg

                    temp_headers=copy.deepcopy(all_headers)
                    header_box=combine_dicts(temp_headers)
                    if section[temp_start_pg]:
                        start_cord=combine_dicts(section[temp_start_pg])
                    else:
                        start_cord=combine_dicts(section[start_pg])
                    print(f'start_cord here',start_cord)

                    if start_cord:
                        start_cord=start_cord['top']
                        current_header_diff=header_box['bottom']-start_cord
                        print(f'current_header_diff here',current_header_diff)
                        print(f'second_header_diff here',second_header_diff)
                        overall_diff=current_header_diff-second_header_diff
                        print(f'overall_diff here',overall_diff)
                        
                        for head in temp_headers:
                            if head['pg_no']!= temp_start_pg:
                                head['pg_no']=temp_start_pg
                            head['top']=head['top']-overall_diff
                            head['bottom']=head['bottom']-overall_diff

                        # print(f'section_headers after modfication here',detected_header)
                        for head in temp_headers:
                            if head['pg_no'] in section:
                                section[head['pg_no']].extend(temp_headers)
                                break
                        
        all_ocr_sections.append(section)
                
    return all_sections,cordinates,all_ocr_sections,remarks_section_ocr


def relevent_checks_claims_calc(extraction_db,case_id,ocr_proccessed):
    total_checks = 0
    total_claims = 0
    try:
        query=f"select * from ocr where case_id = '{case_id}' and format_type is not NULL"
        df_frame_got=extraction_db.execute_(query).to_dict(orient='records')
        generic_pages=[]
        for ocr_rec in df_frame_got:
            if not ocr_rec['template_name']:
                generic_pages.append(ocr_rec['format_type'])

        check_datas={}
        irelevant_datas={}

        for check in ocr_proccessed:
            pages=json.loads(check['page_no'])
            print(f'here template pages are {pages}')

            generic=True
            for page in pages:
                if page not in generic_pages:
                    generic= False

            print(f'here generic detection for template pages is {generic}')

            if not generic:
                if check['check_no'] not in check_datas:
                    check_datas[check['check_no']]={'name':check['check_no'],'items': []}

                claim_status_is = ""
                approved=True
                if check['claim_status'] not in ('approve','done'):
                    approved=False

                if check['claim_status'] in ('approve','done'):
                    claim_status_is = 'approve'
                elif check['claim_status'] == 'hold':
                    claim_status_is = 'hold'

                if check['patient_name']:
                    check_datas[check['check_no']]['items'].append({'name':check['patient_name'],'approved':approved,'status':claim_status_is})
        

        ##### Sorting the claims based on the hold and approve and empty
        check_dropdown_data = list(check_datas.values())

        print(check_dropdown_data)
        total_checks = len(check_dropdown_data)
        for check in check_dropdown_data:
            items = check.get("items", [])
            total_claims += len(items)

        print(F"#####Total checks -> {total_checks} and Total claims -> {total_claims}")

    except Exception as e:
        print(f"#####Error at Calculating the relevent checks and claim --> {e}")

    return total_checks,total_claims


def update_process_queue_data(extraction_db,queue_db,case_id):


    query = f"SELECT * FROM `ocr_post_processed` WHERE case_id= '{case_id}' order by id asc"
    ocr_data = extraction_db.execute_with_decryption(query).to_dict(orient='records')
    print(f"temp_dict{ocr_data}")

    total_relevent_checks, total_relevent_claims = relevent_checks_claims_calc(extraction_db,case_id,ocr_data)
    
    added_check=[]
    added_claim=[]
    check_amount=0
    # payer_name = '' 
    payer_names = set() 

    total_fields=[]
    applicable_fields=[]
    for check in ocr_data:
        if check['main_fields']:
            main_fields=json.loads(check['main_fields'])
            check_amount=main_fields.get('Check Amount',0)
             # Collect payer name if present
            payer_name_from_main = main_fields.get('Payer Name', '').strip()
            if payer_name_from_main:
                payer_names.add(payer_name_from_main)
            print(f"the payer names are:::{payer_names}")    
        # # NEW: Get payer_name if present
        #     if not payer_name:  # take only the first one found
        #         payer_name = main_fields.get('Payer Name', '')
        #         print(f"the payer_name is:::{payer_name}")
            for fie in main_fields:
                if fie not in total_fields:
                    total_fields.append(fie)
                if fie not in applicable_fields:
                    applicable_fields.append(fie)

        if check['fields']:
            fields=json.loads(check['fields'])[0].get('fields',[])
            for field in fields:
                total_fields.append(field)
                if fields[field]:
                    applicable_fields.append(field)

        if check['check_no'] and check['check_no'] not in added_check:
            added_check.append(check['check_no'])

        if check['patient_name'] and check['patient_name'] not in added_check:
            added_claim.append(check['patient_name'])
    # Determine payer_name based on the collected payer_names
    if len(payer_names) == 1:
        final_payer_name = list(payer_names)[0]
    elif len(payer_names) > 1:
        final_payer_name = 'Multi Payer'
    else:
        final_payer_name = ''

    extracted_fields=f'{len(applicable_fields)}/{len(total_fields)}'
    percentage_value = '0'
    try:
        if len(applicable_fields) == 0 or len(total_fields) == 0:
            percentage_value = '0'
        else:
            percentage_value = (len(applicable_fields)/len(total_fields)) * 100
            percentage_value = round(percentage_value, 2)
        percentage_value = str(percentage_value)+'%'
    except Exception as e:
        percentage_value = '0%'
        print(f"#######Percentage issue {e}")
    
    qurey='update process_queue set no_of_checks = %s ,no_of_claims=%s ,check_amount=%s , extracted_fields=%s,extracted_fields_percentage = %s,payer_name = %s  where case_id = %s'
    queue_db.execute_(qurey,params=[str(total_relevent_checks),str(total_relevent_claims),str(check_amount),extracted_fields,percentage_value,final_payer_name,str(case_id)])

    total_fields_dumbs = json.dumps(total_fields)
    query='insert into field_accuracy (case_id,total_fields) values (%s,%s)'
    queue_db.execute_(query,params=[case_id,total_fields_dumbs])

    return True



def post_processing(extraction_db,queue_db,case_id):

    renamed_fields_all={}
    renamed_fields_all_high={}

    query = f"SELECT * FROM `ocr` WHERE case_id= '{case_id}' and format_type is not NULL order by id asc"
    ocr_data = extraction_db.execute_with_decryption(query).to_dict(orient='records')
    print(f"temp_dict{ocr_data}")

    query = f"Delete FROM `ocr_post_processed` WHERE case_id= '{case_id}'"
    extraction_db.execute_(query)

    check_no=0
    patient_no=0
    check_found=[]

    previous_check=''
    previous_format=''
    previous_main_fields={}
    previous_main_fields_high={}

    for doc in ocr_data:

        main_fields={}
        patients={}
        main_fields_high={}
        patients_high={}
        remarks={}
        remark_high={}

        check_no=check_no+1 

        format=doc['format_type']
        template=doc['template_name']

        if doc['highlight']:
            highlight=json.loads(doc['highlight'])
        exracted={}
        if doc['extracted']:
            exracted=json.loads(doc['extracted'])

        if not exracted:
            continue

        print(f" doc is {doc['format_type']}")
        
        for section,fields_data in exracted.items():

            print(f" section is {section}")

            if section == 'main_fields':
                main_fields.update(fields_data)
                main_fields_high.update(highlight[section])
                continue

            if section == 'remarks':
                remarks.update(fields_data)
                remark_high.update(highlight[section])
                continue
            
            ind=0
            for sub_section in fields_data:

                # print(f" sub_section is {sub_section}")

                patient_no=patient_no+1
                fields=sub_section.get('fields',{})
                patient_name=fields.get('Claim Id','')
                if not patient_name:
                    patient_name=f'Unknown Claim {patient_no}'
                else:
                    patient_name=f'{patient_name}_{patient_no}'

                print(f" claim is {patient_name}")
                
                if patient_name not in patients:
                    patients[patient_name]=[]
                    patients_high[patient_name]=[]

                patients[patient_name].append(sub_section)
                patients_high[patient_name].append(highlight[section][ind])
                ind=ind+1

        check_number=main_fields.get('Check Number','')
        print(f'check_number is {check_number}')
        print(f'previous_format is {previous_format}')
        print(f'previous_check is {previous_check}')
        print(f'template is {template}')
        if not check_number:
            if previous_format.lower() == 'vcc' or previous_format.lower() == 'check':
                check_number=previous_check
                main_fields=previous_main_fields
                main_fields_high=previous_main_fields_high
            else:
                check_number=f'Unknown Check {check_no}'
        print(f'check_number is {check_number}')

        if check_number not in renamed_fields_all:
            renamed_fields_all[check_number]={}
            renamed_fields_all[check_number]['main_fields']={}
            renamed_fields_all[check_number]['patients']={}
            renamed_fields_all[check_number]['formats']=[]

            renamed_fields_all_high[check_number]={}
            renamed_fields_all_high[check_number]['main_fields']={}
            renamed_fields_all_high[check_number]['patients']={}

        if check_number not in check_found:
            renamed_fields_all[check_number]['main_fields'].update(main_fields)
            renamed_fields_all_high[check_number]['main_fields'].update(main_fields_high)
        else:
            for key, value in main_fields.items():
                if key not in renamed_fields_all[check_number]['main_fields'] or not renamed_fields_all[check_number]['main_fields'][key]:
                    renamed_fields_all[check_number]['main_fields'][key] = value
            for key, value in main_fields_high.items():
                if key not in renamed_fields_all_high[check_number]['main_fields'] or not renamed_fields_all_high[check_number]['main_fields'][key]:
                    renamed_fields_all_high[check_number]['main_fields'][key] = value
            

        if check_number not in check_found and 'check' in format:
            check_found.append(check_number)

        renamed_fields_all[check_number]['patients'].update(patients)
        renamed_fields_all[check_number]['formats'].append(format)

        
        renamed_fields_all_high[check_number]['patients'].update(patients_high)

        if template:
            previous_check=check_number
            previous_format=format.split('_')[0]
            previous_main_fields=main_fields
            previous_main_fields_high=main_fields_high
        
    
    for check,data in renamed_fields_all.items():

        main_fields=data['main_fields']
        patients=data['patients']
        formats=data['formats']

        if not patients:
            print(f"Inserting case_id: {case_id}, check: {check}")

            query='insert into ocr_post_processed (case_id,check_no,page_no,main_fields,main_fields_highlights,remarks) values (%s,%s,%s,%s,%s,%s)'
            extraction_db.execute_(query,params=[case_id,str(check),json.dumps(formats),json.dumps(main_fields),json.dumps(renamed_fields_all_high[check]['main_fields']),json.dumps(remarks)])

        for patient,data in patients.items():
            
            patinet_high=renamed_fields_all_high[check]['patients'][patient]
            temp_page={'start':'','end':''}
            if patinet_high:
                max_v=''
                min_v=''
                valid_fields=True
                valid_table=True

                for sub_patinet_high in patinet_high:

                    if not sub_patinet_high['fields']:
                        valid_fields=False

                    for key,value in sub_patinet_high['fields'].items():
                        if 'page' in value:
                            if min_v and int(min_v)>int(value['page']):
                                min_v=int(value['page'])
                                print(f"found smaller valuer {value['page']} ")
                            elif not min_v:
                                min_v=int(value['page'])
                                print(f"found smaller first valuer {value['page']} ")

                            if max_v and int(max_v)<int(value['page']):
                                max_v=int(value['page'])
                                print(f"fouund bigger first valuer {value['page']} ")
                            elif not max_v:
                                max_v=int(value['page'])
                                print(f"fouund bigger valuer {value['page']} ")

                    if 'table' in sub_patinet_high:

                        for table,table_rows in sub_patinet_high['table'].items():
                            if not table_rows:
                                valid_table=False

                            for row in table_rows:
                                for row_head,value in row.items():
                                    if 'page' in value:
                                        if min_v and int(min_v)>int(value['page']):
                                            print(f"fouund samller valuer {value['page']} ")
                                            min_v=int(value['page'])
                                        if max_v and int(max_v)<int(value['page']):
                                            max_v=int(value['page'])
                                            print(f"fouund bigger valuer {value['page']} ")
                        
                if max_v:
                    temp_page['end']=max_v+1
                    temp_page['start']=min_v+1


                if not valid_fields and not valid_table:
                    print(f'skkiping patient as it does not have fields or table')
                    continue

            try:        
                # 1. Fetch client_name from DB
                print(f"here came for the to fetch from the db cleint name")
                client_query = f"SELECT client_name FROM process_queue WHERE case_id = '{case_id}'"
                client_result = queue_db.execute_(client_query)
                print(f"the query executeed:::{client_result}")
                if not client_result.empty and 'client_name' in client_result.columns:
                    client_name = client_result['client_name'].to_list()[0]
                    print(f"the client name is::::###:{client_name}")
                    main_fields['Client'] = client_name
                    print(f"after the process main_fields are::####:::::{main_fields}")
                else:
                    print(f"Warning: No client_name found for case_id: {case_id}")    
            except Exception as e:
                print(f"the issue is::::::{e}")
                
            print(f"Inserting case_id: {case_id}, check: {check}, patient: {patient}, claim_page {temp_page} ")

            query='insert into ocr_post_processed (claim_page,case_id,check_no,patient_name,page_no,fields,highlights,main_fields,main_fields_highlights,remarks) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            extraction_db.execute_(query,params=[json.dumps(temp_page),case_id,str(check),str(patient),json.dumps(formats),json.dumps(data),json.dumps(renamed_fields_all_high[check]['patients'][patient]),json.dumps(main_fields),json.dumps(renamed_fields_all_high[check]['main_fields']),json.dumps(remarks)])
            
    update_process_queue_data(extraction_db,queue_db,case_id)

    return True


def checks_stored_field_acccuracy(tenant_id,case_id):
    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)

    try:
        delete_query = f"delete from field_accuracy where case_id = '{case_id}'"
        delete_query_result = extraction_db.execute_(delete_query)
        print(f"########delete_query_result is {delete_query_result}")
    except Exception as e:
        print(f"######Error at deletein {e}")
        
    case_id_query = f"select * from ocr_post_processed where case_id = '{case_id}'"
    claims_results = extraction_db.execute_(case_id_query).to_dict(orient='records')
    
    grouped = {}
    
    for item in claims_results:
        if item['check_no'] not in grouped:
            grouped[item['check_no']] = []
        grouped[item['check_no']].append(item)

    print(f' ########## grouped is {grouped}')
    
    accuracy_dict={}
    for check_name,claims in grouped.items():

        try:

            print(f' ########## claims is {claims}')

            accuracy_dict[check_name]={}

            main_fields = json.loads(claims[0].get('main_fields','{}'))
            total_check_fields=len(main_fields)
            main_high = json.loads(claims[0].get('main_fields_highlights','{}'))
            total_check_error_fields=0
            for field,value in main_high.items():
                if value['confidance'] < 80:
                    total_check_error_fields=total_check_error_fields+1

            accuracy_dict[check_name]['total_check_fields']=total_check_fields
            accuracy_dict[check_name]['total_check_error_fields']=total_check_error_fields
            accuracy_dict[check_name]['claims_fields_count']={}

            claim_accuracy_dict={}
            for claim in claims:

                remarks=claim.get('remarks',{})
                if remarks:
                    remarks = json.loads(claim.get('remarks',{}))
                patient_name = claim.get('patient_name','')
                claim_accuracy_dict[patient_name]={}
                claim_fields=claim.get('fields',{})
                if claim_fields:
                    claim_fields = json.loads(claim_fields)
                    claim_highlights = json.loads(claim.get('highlights',''))

                ind=0
                total_claim_fields=0
                table_fields_count=0
                total_claim_error_fields=0
                table_fields_error_count=0

                if claim_fields:
                    for sub_claim in claim_fields:

                        sub_claim_fields=sub_claim.get('fields',{})
                        total_claim_fields=total_claim_fields+len(sub_claim_fields)
                        sub_claim_highlights=claim_highlights[ind].get('fields',{})

                        for field,value in sub_claim_highlights.items():
                            if value['confidance'] < 80:
                                print(f' ########## field high accuracy is {field}')
                                total_claim_error_fields=total_claim_error_fields+1

                        sub_claim_table=sub_claim.get('table',{})
                        sub_claim_table_highlight=claim_highlights[ind].get('table',{})

                        for table_name,table_data in sub_claim_table.items():
                            if 'rows' in table_data:
                                rows=table_data['rows']
                                for row in rows:
                                    for head,val in row.items():
                                        table_fields_count=table_fields_count+1

                        for table_name,table_data in sub_claim_table_highlight.items():
                            rows=table_data
                            for row in rows:
                                for head,val in row.items():
                                    if val['confidance'] < 80:
                                        table_fields_error_count=table_fields_error_count+1
                                    
                        ind=ind+1

                claim_accuracy_dict[patient_name]['total_claim_fields']=total_claim_fields
                claim_accuracy_dict[patient_name]['total_claim_error_fields']=total_claim_error_fields
                claim_accuracy_dict[patient_name]['total_claim_table_fields']=table_fields_count
                claim_accuracy_dict[patient_name]['total_claim_table_error_fields']=table_fields_error_count

            print(f' ########## claim_accuracy_dict is {claim_accuracy_dict}')
            accuracy_dict[check_name]['claims_fields_count']=claim_accuracy_dict
        
        except Exception as e:
            print(f"######Error at the checks calculation ---> {e}")

    print(f' ########## accuracy_dict is {accuracy_dict}')

    for check_name,claims in accuracy_dict.items():

        try:

            total_check_fields=claims['total_check_fields']
            total_check_error_fields=claims['total_check_error_fields']
            claims_fields_count=claims['claims_fields_count']

            all_claim_fields=0
            all_claim_error_fields=0
            all_claim_table_fields=0
            all_claim_table_error_fields=0
            
            for claim_name,claim in claims_fields_count.items():

                total_claim_fields=claim['total_claim_fields']
                all_claim_fields=all_claim_fields+total_claim_fields

                total_claim_error_fields=claim['total_claim_error_fields']
                all_claim_error_fields=all_claim_error_fields+total_claim_error_fields

                total_claim_table_fields=claim['total_claim_table_fields']
                all_claim_table_fields=all_claim_table_fields+total_claim_table_fields

                total_claim_table_error_fields=claim['total_claim_table_error_fields']
                all_claim_table_error_fields=all_claim_table_error_fields+total_claim_table_error_fields

            print(f' ########## all_claim_fields is {all_claim_fields}')
            print(f' ########## all_claim_error_fields is {all_claim_error_fields}')
            print(f' ########## all_claim_table_fields is {all_claim_table_fields}')
            print(f' ########## all_claim_table_error_fields is {all_claim_table_error_fields}')

            total_fields=total_check_fields+all_claim_fields+all_claim_table_fields
            total_error_fields=total_check_error_fields+all_claim_error_fields+all_claim_table_error_fields

            print(f' ########## total_fields is {total_fields}')
            print(f' ########## total_error_fields is {total_error_fields}')

            accuracy=((total_fields-total_error_fields)/total_fields)*100
            accuracy=round(accuracy,2)

            print(f' ########## here accuracy is {accuracy}')
                
            query='insert into field_accuracy (case_id,check_no,total_check_fields,initial_check_error_fields,all_claim_fields,initial_claim_error_fields,all_claim_table_fields,initial_claim_table_error_fields,total_fields,total_error_fields,initial_accuracy) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
            extraction_db.execute_(query,params=[case_id,check_name,total_check_fields,total_check_error_fields,all_claim_fields,all_claim_error_fields,all_claim_table_fields,all_claim_table_error_fields,total_fields,total_error_fields,accuracy])

        except Exception as e:
            print(f"######Error at the checks calculation ---> {e}")
            
    response = {"flag":True,"message":"successfull"}

    # except Exception as e:
    #     print(f"###Error at data insert into the field accuracy table {e}")
    #     response = {"flag":False,"message":f"error occured {e}"}

    return jsonify(response)


def extract_page_list(s):
    if "@" in s and "_" in s:
        _, page_range = s.split("_")
        start, end = map(int, page_range.split("@"))
        return list(range(start, end + 1))
    return []


@app.route('/ionic_extraction', methods=['POST', 'GET'])
def ionic_extraction():

    data = request.json
    print(f"Data recieved: {data}")
    try:
        tenant_id = data['tenant_id']
        case_id = data.get('case_id', None)
        process = data.get('tenant_id', None)
    except Exception as e:
        print(f'## TE Received unknown data. [{data}] [{e}]')
        return {'flag': False, 'message': 'Incorrect Data in request'}

    db_config['tenant_id'] = tenant_id
    extraction_db = DB('extraction', **db_config)
    queue_db = DB('queues', **db_config)    

    container_name = data.get('container', None)

    try:

        query = f"SELECT `document_id`  from  `process_queue` where `case_id` = '{case_id}';"
        document_ids=queue_db.execute_(query)["document_id"].to_list()
        # print(f"The result is {document_ids}")
        
        for document_id in document_ids:

            query = f"SELECT format_type,template_name,format_pages from  `ocr` where `case_id` = '{case_id}' and document_id ='{document_id}'"
            ocr_data=extraction_db.execute_(query)
            # print(f"The result is {ocr_data}")
            
            queue_db = DB('queues', **db_config)
            template_db = DB('template_db', **db_config)

            query = f"SELECT `ocr_word`,ocr_data from  `ocr_info` where `case_id` = '{case_id}'and document_id ='{document_id}'"
            ocr_data_all = queue_db.execute_(query)['ocr_word'].to_list()[0]
            ocr_data_all=json.loads(ocr_data_all)
            ocr_all = queue_db.execute_(query)['ocr_data'].to_list()[0]
            ocr_all=json.loads(ocr_all)

            format_types=ocr_data.to_dict(orient='records')

            for format_data in format_types:

                act_format_data=format_data['format_type']
                # print(f"format extarction is going start for {act_format_data}")

                format_page=format_data['format_type']
                identifier=format_data['template_name']

                extracted_table={}
                extracted_fields={}
                fields_highlights={}

                if not format_page or not identifier:
                    continue

                # print(F"foramt got is {format_page.rsplit('_', 1)}")
                format= format_page.rsplit('_', 1)[0]
                pages=format_page.rsplit('_', 1)[1]
                start=int(pages.split('@')[0])
                end=int(pages.split('@')[1])
                # print(f'start is {start} and end is {end}')
                ocr_pages=[]
                for pag in ocr_data_all:
                    if not pag:
                        continue
                    page_no=int(pag[0]['pg_no'])-1
                    if start<=page_no<=end:
                        print(start,page_no,end)
                        ocr_pages.append(pag)
                
                ocr_data_pages=[]
                for pag in ocr_all:
                    if not pag:
                        continue
                    page_no=int(pag[0]['pg_no'])-1
                    if start<=page_no<=end:
                        ocr_data_pages.append(pag)

                query = f"SELECT * from  `trained_info` where format='{format}' and identifier='{identifier}'"
                process_trained_fields = template_db.execute_(query).to_dict(orient='records')

                if process_trained_fields:
                    process_trained_fields=process_trained_fields[0]
                    # print(f"ocr_pages sending are {ocr_pages}")
                    start_pages=predict_mutli_checks(ocr_pages,ocr_data_pages,process_trained_fields,end)

                    if len(start_pages)>1:

                        del_rec=f'delete from ocr where case_id ="{case_id}" and format_type ="{format_page}"'
                        extraction_db.execute_(del_rec)

                        for pair in start_pages:
                            if len(pair)==1:
                                end_page=end
                            else:
                                end_page=pair[1]
                            start_page=pair[0]

                            formated=format+'_'+str(start_page)+'@'+str(end_page)
                            ne_pages=extract_page_list(formated)
                            query = "insert into `ocr` (template_name,case_id,format_type,document_id,format_pages) values (%s,%s,%s,%s,%s)"
                            params = [identifier,case_id, formated,case_id,json.dumps(ne_pages)]
                            extraction_db.execute_(query,params=params)

            query = f"SELECT format_type,template_name,format_pages from  `ocr` where `case_id` = '{case_id}' and document_id ='{document_id}'"
            case_data=extraction_db.execute_(query)

            format_types=case_data.to_dict(orient='records')
            
            for format_data in format_types:

                act_format_data=format_data['format_type']
                # print(f"format extarction is going start for {act_format_data}")

                format_page=format_data['format_type']
                identifier=format_data['template_name']

                extracted_table={}
                extracted_fields={}
                fields_highlights={}

                if not format_page:
                    continue

                # print(F"foramt got is {format_page.rsplit('_', 1)}")
                format= format_page.rsplit('_', 1)[0]
                pages=format_page.rsplit('_', 1)[1]
                start=int(pages.split('@')[0])
                end=int(pages.split('@')[1])
                # print(f'start is {start} and end is {end}')
                ocr_pages=[]
                for pag in ocr_data_all:
                    if not pag:
                        continue
                    page_no=int(pag[0]['pg_no'])-1
                    if start<=page_no<=end:
                        print(start,page_no,end)
                        ocr_pages.append(pag)
                
                ocr_data_pages=[]
                for pag in ocr_all:
                    if not pag:
                        continue
                    page_no=int(pag[0]['pg_no'])-1
                    if start<=page_no<=end:
                        ocr_data_pages.append(pag)

                if not ocr_pages or not ocr_data_pages:
                    continue

                print(f'len of ocr pages {len(ocr_pages)}')
                print(f'len of ocr pages {len(ocr_data_pages)}')

                # print(F" ########## exarction starting for foramt {format} and start and end pages got are {start} {end}")

                query = f"SELECT * from  `process_training_data` where format='{format}' and model_usage = 'yes'"
                master_process_trained_fields = template_db.execute_(query)

                if not master_process_trained_fields.empty:
                    master_process_trained_fields=master_process_trained_fields.to_dict(orient='records')[0]
                else:
                    master_process_trained_fields={}
                
                query = f"SELECT * from  `trained_info` where format='{format}' and identifier='{identifier}'"
                process_trained_fields = template_db.execute_(query)
                
                used_version=''
                if process_trained_fields.empty:
                    extarction_from={}

                    if not master_process_trained_fields:
                        continue

                    process_trained_fields=master_process_trained_fields
                    trained=process_trained_fields['trained']
                    process_trained_fields['values']=process_trained_fields['values_trained']
                    common_fields=process_trained_fields['fields_common']
                    
                    if trained:
                        print(F" ################# creating models real_time")
                        create_models_real_time(process,format,process_trained_fields,case_id)

                        extracted_fields,fields_highlights,extracted_headers=get_master_extraction_values(process,format,case_id,ocr_pages,json.loads(master_process_trained_fields['fields']),common_fields)
                        used_version='1'

                        query=f'update ocr set extracted_headers = %s where case_id = %s and format_type = %s'
                        extraction_db.execute_(query,params=[json.dumps(extracted_headers),case_id,format_page])

                    else:
                        if process_trained_fields['fields']:
                            fields=json.loads(process_trained_fields['fields'])
                            print(F" we dont ave any master template so fields are not extarcted so {fields}")
                            for field in fields:
                                extracted_fields[field]=''
                                
                else:
                    process_trained_fields=process_trained_fields.to_dict(orient='records')
                    extracted_table={}
                    extracted_fields={}
                    extarction_from={}
                    extracted_remarks={}
                    remarks_high={}

                    if process_trained_fields:
                        process_trained_fields= process_trained_fields[0]
                        #here we will extract any over all thing
                        fields_data = process_trained_fields['fields'] if process_trained_fields['fields'] else ''
                        if fields_data:
                            print(f'fields_data is {fields_data}')
                            extracted_fields['main_fields'], fields_highlights['main_fields'] = get_template_extraction_values(ocr_pages, ocr_data_pages, process_trained_fields, json.loads(fields_data))
                            # extracted_fields['main_fields'],fields_highlights['main_fields']=get_template_extraction_values(ocr_pages,ocr_data_pages,process_trained_fields,json.loads(process_trained_fields['fields']))
                            extarction_from['main_fields']=list(extracted_fields['main_fields'].keys())
                            print(F" #################### here table to extract is {fields_highlights['main_fields']}")
                        
                        if process_trained_fields['trained_table']:
                            process_trained_fields['trained_table']=json.loads(process_trained_fields['trained_table'])
                            for table_name,tables in process_trained_fields['trained_table'].items():
                                if len(tables)>2:
                                    table_header=tables[0]
                                    table_header_text=[]
                                    if 'table' in tables:
                                        table_header_text=tables[2]['table']['headers']
                                    elif 'headers' in tables[2]:
                                        table_header_text=tables[2]['headers']

                                    if table_header:
                                        found_header,table_headers_line=extract_table_header(ocr_data_pages,0,table_header,table_header_text=table_header_text)

                                    table_footer=tables[1]
                                    if table_footer:
                                        found_footer,table_foot_line=extract_table_header(ocr_data_pages,0,table_footer,True)

                                    if found_header:
                                        if found_footer:
                                            bottom=found_footer[0]['top']
                                        else:
                                            bottom=1000
                                        extracted_table,table_high=extract_table_from_header(extraction_db,template_db,case_id,ocr_data_pages,found_header,table_headers_line,0,bottom)
                                        # print(F" extracted_table #################### {extracted_table}")

                                        # if 'table' in table_high:
                                        #     fields_highlights['main_fields']['table']=table_high['table']
                                        #     extracted_fields['main_fields']['table']=extracted_table

                        remarks=process_trained_fields.get('remarks',{})
                        print(f' remarks need to be extracted are {remarks}')
                        if remarks:
                            print(f'remarks here are {remarks}')
                            remarks=json.loads(remarks)
                            if remarks and remarks.get('type','None') == 'check level':
                                extracted_remarks,remarks_high=extract_remarks(remarks.get('start',[]),remarks.get('end',[]),ocr_pages)

                        #here we will extract any sessions things
                        # print(F" here extract for section {process_trained_fields['sub_template_identifiers']}")
                        if process_trained_fields['sub_template_identifiers']:

                            sub_template_identifiers=json.loads(process_trained_fields['sub_template_identifiers'])
                            all_section_fields=json.loads(process_trained_fields['section_fields'])
                            table=json.loads(process_trained_fields['section_table'])

                            try:
                                table_t2=json.loads(process_trained_fields['section_table_t2'])
                            except:
                                table_t2={}

                            if table_t2:
                                print(f'this table has a t2 trained table')
                                table=table_t2
                                
                            print(F" all_section_fields {all_section_fields}")

                            for section,identifiers in sub_template_identifiers.items():

                                if not all_section_fields:
                                    continue

                                section_fields=all_section_fields[section]

                                extarction_from[section]=[]
                                # print(F" section #################### {section}")
                                extracted_fields[section]=[]
                                fields_highlights[section]=[]
                                section_header=[]
                                if 'section_exceptions' in  identifiers:
                                    section_header=identifiers['section_exceptions']

                                if 'sub_section' in  identifiers and identifiers['sub_section']:
                                    sub_section=identifiers['sub_section']
                                    sub_section_identifiers=sub_section['identifiers']
                                    sub_section_fields=sub_section['selected_fields']

                                paraFieldsTraining={}
                                if 'paraFieldsTraining' in  identifiers and identifiers['paraFieldsTraining']:
                                    paraFieldsTraining=identifiers['paraFieldsTraining']

                                # print(F"################N section_header is {section_header}")
                                sub_templates,cordinates,all_ocr_sections,remarks_section_ocr=predict_sub_templates(identifiers['section_identifiers'],ocr_pages,ocr_data_pages,section_header)
                                print(F" main section cordinates {cordinates}")
                                if not sub_templates:
                                    continue

                                section_no=-1
                                # print(F" #################### sub_templates {len(sub_templates)}")

                                for template_ocr in sub_templates:
                                    
                                    if 'sub_section' in  identifiers and identifiers['sub_section']:

                                        section_no=section_no+1

                                        sub_section=identifiers['sub_section']
                                        sub_section_identifiers=sub_section['identifiers']
                                        common_section_fields=sub_section['selected_fields']

                                        common_template_fields={}
                                        common_template_highlights={}
                                        if common_section_fields:
                                            common_template_fields['fields'],common_template_highlights['fields']=get_template_extraction_values(list(template_ocr.values()),list(all_ocr_sections[section_no].values()),process_trained_fields,section_fields,common_section=True,common_section_fields=common_section_fields)
                                        print(f'common_fields is {common_template_fields}')

                                        sub_sub_templates,sub_cordinates,sub_all_ocr_sections,sub_remarks_section_ocr=predict_sub_sub_templates(sub_section_identifiers,list(template_ocr.values()),list(all_ocr_sections[section_no].values()))
                                        print(F" sub section cordinates {sub_cordinates}")
                                        #loop for sub sub section starts here 

                                        sub_section_no=-1
                                        for sub_template_ocr in sub_sub_templates:

                                            sub_section_no=sub_section_no+1
                                            template_fields={}
                                            template_highlights={}
                                            if not sub_template_ocr:
                                                continue

                                            print(F" #################### sub_section_no is {sub_section_no}")
                                            print(F" #################### ocr_data is {list(sub_template_ocr.values())}")

                                            template_fields['fields'],template_highlights['fields']=get_template_extraction_values(list(sub_template_ocr.values()),list(sub_all_ocr_sections[sub_section_no].values()),process_trained_fields,section_fields,sub_section=True,common_section_fields=common_section_fields)

                                            if common_template_fields:
                                                template_fields['fields'].update(common_template_fields['fields'])
                                                template_highlights['fields'].update(common_template_highlights['fields'])

                                            print(f"template_fields is {template_fields['fields']}")

                                            extarction_from[section].append(list(template_fields['fields'].keys()))
                                            # print(F" #################### template_fields {template_fields}")
                                            # print(F" #################### cordinates {cordinates} and section_no {section_no}")

                                            if section in table and table[section]:
                                                print(F"section is {section}")
                                                # print(F" #################### cordinates {cordinates} and section_no {section_no}")
                                                page_no=sub_cordinates[sub_section_no][0]
                                                section_top=sub_cordinates[sub_section_no][1]
                                                trained_tables=table[section]
                                                print(F"trained_tables is {trained_tables}")
                                                template_fields['table']={}
                                                template_highlights['table']={}

                                                # print(" ################ all_ocr_sections start line is",all_ocr_sections[section_no])
                                                table_extract=False
                                                max_match=0
                                                needed_table=''
                                                for table_name,tables in trained_tables.items():

                                                    print(F"table_name is  here is to idnetify the table {table_name}")

                                                    if table_name == 'table':
                                                        continue
                                                    if tables and len(tables)>2:
                                                        print(F"tables is {tables[0]}")
                                                        table_header=tables[0]
                                                        table_header_text=tables[2]
                                                        if 'table' in table_header_text:
                                                            table_header_text=table_header_text['table'].get('headers',[])
                                                        else:
                                                            table_header_text=table_header_text.get('headers',[])
                                                        header_lines=[]
                                                        if table_header:
                                                            header_lines=extract_table_header(list(sub_all_ocr_sections[sub_section_no].values()),section_top,table_header,table_header_text=table_header_text,header_check=True)
                                                            print(F"found_header is {header_lines}")
                                                            if not header_lines:
                                                                for ocr in ocr_all:
                                                                    if not pag:
                                                                        continue
                                                                    header_lines=extract_table_header([ocr],0,table_header,table_header_text=table_header_text,header_check=True)
                                                                    print(F"found_header is {header_lines}")
                                                                    if header_lines:
                                                                        break
                                                        if header_lines:
                                                            table_headers_line=line_wise_ocr_data(table_header)
                                                            table_line_words=''
                                                            for table_head_line in table_headers_line:
                                                                for word in table_head_line:
                                                                    table_line_words=table_line_words+" "+word['word']
                                                            print(F"table_line_words is {table_line_words}")

                                                            header_lines_found=line_wise_ocr_data(header_lines)
                                                            headee_line_words=''
                                                            for header_line_found in header_lines_found:
                                                                for word in header_line_found:
                                                                    headee_line_words=headee_line_words+" "+word['word']
                                                            print(F"headee_line_words is {headee_line_words}")
                                                        
                                                            matcher = SequenceMatcher(None, table_line_words, headee_line_words)
                                                            similarity_ratio_col = matcher.ratio()
                                                            print(F"similarity_ratio_col is {similarity_ratio_col} for the tabke is {table_name}")

                                                            if similarity_ratio_col>max_match:
                                                                max_match=similarity_ratio_col
                                                                needed_table=table_name

                                                for table_name,tables in trained_tables.items():

                                                    if table_name == 'table':
                                                        continue

                                                    if needed_table and needed_table !=table_name:
                                                        continue


                                                    trained_map={}
                                                    needed_headers=[]
                                                    if tables and len(tables)>3:
                                                        trained_map_got=tables[3]
                                                        # print(F"trained_map is {trained_map_got}")
                                                        trained_map={}
                                                        for key,value in trained_map_got.items():
                                                            if key.startswith("New Column") and value.startswith("New Column"):
                                                                continue
                                                            if key.startswith("New Column"):
                                                                trained_map[value] = value
                                                            else:
                                                                trained_map[key] = value
                                                        # print(F"trained_map is {trained_map}")

                                                    extra_variables=[]
                                                    if tables and len(tables)>4:
                                                        extra_variables=tables[4]
                                                    print(F"extra_variables is {extra_variables}")

                                                    no_header_columns=[]
                                                    if tables and len(tables)>5:
                                                        no_header_columns=tables[5]
                                                    print(F"no_header_columns is {no_header_columns}")
                                                        
                                                    if tables and len(tables)>2:
                                                        print(F"tables is {tables[0]}")
                                                        table_header=tables[0]
                                                        table_header_text=tables[2]
                                                        if 'table' in table_header_text:
                                                            table_header_text=table_header_text['table'].get('headers',[])
                                                        else:
                                                            table_header_text=table_header_text.get('headers',[])

                                                        if trained_map:
                                                            trained_map = dict(
                                                                sorted(
                                                                    trained_map.items(),
                                                                    key=lambda x: (table_header_text.index(x[0]) if x[0] in table_header_text else
                                                                                table_header_text.index(x[1]) if x[1] in table_header_text else float('inf'))
                                                                )
                                                            )
                                                            table_header_text=trained_map.values()
                                                            needed_headers=trained_map.keys()

                                                        if table_header:
                                                            head_page=[]
                                                            found_header,table_headers_line=extract_table_header(list(sub_all_ocr_sections[sub_section_no].values()),section_top,table_header,table_header_text=table_header_text)
                                                            print(F"found_header is {found_header}")
                                                            if not found_header:
                                                                for ocr in ocr_all:
                                                                    if not pag:
                                                                        continue
                                                                    found_header,table_headers_line=extract_table_header([ocr],0,table_header,table_header_text=table_header_text)
                                                                    print(F"found_header is {found_header}")
                                                                    if found_header:
                                                                        break
                                                        

                                                        table_footer=tables[1]
                                                        found_footer=None
                                                        if table_footer:
                                                            found_footer,table_foot_line=extract_table_header(list(sub_all_ocr_sections[sub_section_no].values()),section_top,table_footer,True)
                                                        
                                                        if found_header:
                                                            if found_footer:
                                                                bottom=found_footer[0]['top']
                                                                bottom_page=found_footer[0]['pg_no']
                                                            else:
                                                                bottom=1000
                                                                bottom_page=False
                                                            print(F"found_header is {found_header}")
                                                            print(F"found_footer is {found_footer}")

                                                            template_extracted_table,table_high=extract_table_from_header(extraction_db,template_db,case_id,list(sub_all_ocr_sections[sub_section_no].values()),found_header,table_headers_line,section_top,bottom=bottom,cord=sub_cordinates[sub_section_no],trained_map=trained_map,bottom_page=bottom_page,extra_variables=extra_variables,no_header_columns=no_header_columns)
                                                            print(F" template_extracted_table #################### {template_extracted_table}")

                                                            # if needed_headers:
                                                            #     template_extracted_table=filter_columns(template_extracted_table, needed_headers)

                                                            if 'table' in table_high:
                                                                template_highlights['table'][table_name]=table_high['table']

                                                            template_fields['table'][table_name]=template_extracted_table
                                                        else:
                                                            template_fields['table'][table_name]={}

                                                        if template_fields['table'][table_name]:
                                                            table_extract=True
                                                            break

                                                if not table_extract:
                                                    template_fields['table']={}
                                                    template_fields['table']['table_1']={}

                                                print(f' remarks need to be extracted are {remarks}')
                                                if remarks and remarks.get('type','None') == 'claim level':
                                                    extracted_remarks,remarks_high=extract_remarks(remarks.get('start',[]),remarks.get('end',[]),list(remarks_section_ocr[section_no].values()))

                                                if extracted_remarks:
                                                    template_fields['remarks']=extracted_remarks
                                                    template_highlights['remarks']=remarks_high

                                            fields_highlights[section].append(template_highlights)
                                            extracted_fields[section].append(template_fields)

                                    else:
                                        template_fields={}
                                        template_highlights={}

                                        if not template_ocr:
                                            continue

                                        section_no=section_no+1
                                        print(F" #################### section_no is {section_no}")
                                        print(F" #################### ocr_data is {list(template_ocr.values())}")

                                        section_fields=all_section_fields[section]
                                        template_fields['fields'],template_highlights['fields']=get_template_extraction_values(list(template_ocr.values()),list(all_ocr_sections[section_no].values()),process_trained_fields,section_fields,section_fields=True)
                                        extarction_from[section].append(list(template_fields['fields'].keys()))
                                        # print(F" #################### template_fields {template_fields}")
                                        # print(F" #################### cordinates {cordinates} and section_no {section_no}")

                                        if section in table and table[section]:

                                            # print(F" #################### cordinates {cordinates} and section_no {section_no}")
                                            page_no=cordinates[section_no][0]
                                            section_top=cordinates[section_no][1]
                                            trained_tables=table[section]
                                            template_fields['table']={}
                                            template_highlights['table']={}

                                            # print(" ################ all_ocr_sections start line is",all_ocr_sections[section_no])
                                            table_extract=False
                                            max_match=0
                                            needed_table=''
                                            for table_name,tables in trained_tables.items():
                                                if table_name == 'table':
                                                    continue
                                                if tables and len(tables)>2:
                                                    print(F"tables is {tables[0]}")
                                                    table_header=tables[0]
                                                    table_header_text=tables[2]
                                                    if 'table' in table_header_text:
                                                        table_header_text=table_header_text['table'].get('headers',[])
                                                    else:
                                                        table_header_text=table_header_text.get('headers',[])
                                                    header_lines=[]
                                                    if table_header:
                                                        header_lines=extract_table_header(list(all_ocr_sections[section_no].values()),section_top,table_header,table_header_text=table_header_text,header_check=True)
                                                        print(F"found_header is {header_lines}")
                                                        if not header_lines:
                                                            for ocr in ocr_all:
                                                                if not pag:
                                                                    continue
                                                                header_lines=extract_table_header([ocr],0,table_header,table_header_text=table_header_text,header_check=True)
                                                                print(F"found_header is {header_lines}")
                                                                if header_lines:
                                                                    break
                                                    if header_lines:
                                                        table_headers_line=line_wise_ocr_data(table_header)
                                                        table_line_words=''
                                                        for table_head_line in table_headers_line:
                                                            for word in table_head_line:
                                                                table_line_words=table_line_words+" "+word['word']
                                                        print(F"table_line_words is {table_line_words}")

                                                        header_lines_found=line_wise_ocr_data(header_lines)
                                                        headee_line_words=''
                                                        for header_line_found in header_lines_found:
                                                            for word in header_line_found:
                                                                headee_line_words=headee_line_words+" "+word['word']
                                                        print(F"headee_line_words is {headee_line_words}")
                                                    
                                                        matcher = SequenceMatcher(None, table_line_words, headee_line_words)
                                                        similarity_ratio_col = matcher.ratio()
                                                        print(F"similarity_ratio_col is {similarity_ratio_col}")
                                                        if similarity_ratio_col>max_match:
                                                            needed_table=table_name


                                            for table_name,tables in trained_tables.items():

                                                if table_name == 'table':
                                                    continue

                                                print(f'needed table is {needed_table}')
                                                if needed_table and needed_table !=table_name:
                                                    continue


                                                trained_map={}
                                                needed_headers=[]
                                                if tables and len(tables)>3:
                                                    trained_map_got=tables[3]
                                                    # print(F"trained_map is {trained_map_got}")
                                                    trained_map={}
                                                    for key,value in trained_map_got.items():
                                                        if key.startswith("New Column"):
                                                            trained_map[value] = value
                                                        elif value.startswith("New Column"):
                                                            trained_map[key] = key
                                                        else:
                                                            trained_map[key] = value
                                                    # print(F"trained_map is {trained_map}")

                                                extra_variables=[]
                                                if tables and len(tables)>4:
                                                    extra_variables=tables[4]
                                                print(F"extra_variables is {extra_variables}")

                                                no_header_columns=[]
                                                if tables and len(tables)>5:
                                                    no_header_columns=tables[5]
                                                print(F"no_header_columns is {no_header_columns}")
                                                    
                                                if tables and len(tables)>2:
                                                    table_header=tables[0]
                                                    table_header_text=tables[2]
                                                    if 'table' in table_header_text:
                                                        table_header_text=table_header_text['table'].get('headers',[])
                                                    else:
                                                        table_header_text=table_header_text.get('headers',[])

                                                    if trained_map:
                                                        trained_map = dict(
                                                            sorted(
                                                                trained_map.items(),
                                                                key=lambda x: (table_header_text.index(x[0]) if x[0] in table_header_text else
                                                                            table_header_text.index(x[1]) if x[1] in table_header_text else float('inf'))
                                                            )
                                                        )
                                                        table_header_text=trained_map.values()
                                                        needed_headers=trained_map.keys()

                                                    found_header=[]
                                                    print(F"table_header is {table_header}")
                                                    if table_header:
                                                        head_page=[]
                                                        found_header,table_headers_line=extract_table_header(list(all_ocr_sections[section_no].values()),section_top,table_header,table_header_text=table_header_text)
                                                        print(F"found_header is here is{found_header}")

                                                        if found_header:
                                                            extra_headers=[]
                                                            found_extra_headers=[]
                                                            second_header_box=[]
                                                            if tables and len(tables)>6:
                                                                extra_headers=tables[6]
                                                            print(F"extra_headers is {extra_headers}")

                                                            header_page=found_header[0]['pg_no']
                                                            needed_ocr=[]
                                                            for page in list(all_ocr_sections[section_no].values()):
                                                                if page[0]['pg_no']==header_page:
                                                                    needed_ocr.append(page)

                                                            if extra_headers:
                                                                found_extra_headers,second_header_box=find_extra_headers(extra_headers,needed_ocr)

                                                        if not found_header:
                                                            for ocr in ocr_all:
                                                                if not pag:
                                                                    continue
                                                                found_header,table_headers_line=extract_table_header([ocr],0,table_header,table_header_text=table_header_text)
                                                                print(F"found_header is here is {found_header}")
                                                                if found_header:
                                                                    extra_headers=[]
                                                                    found_extra_headers=[]
                                                                    second_header_box=[]
                                                                    if tables and len(tables)>6:
                                                                        extra_headers=tables[6]
                                                                    print(F"extra_headers is here is {extra_headers}")

                                                                    found_extra_headers,second_header_box=find_extra_headers(extra_headers,[ocr])
                                                                    break
                                                    

                                                    table_footer=tables[1]
                                                    found_footer=None
                                                    print(F"table_footer is {table_footer}")
                                                    if table_footer:
                                                        found_footer,table_foot_line=extract_table_header(list(all_ocr_sections[section_no].values()),section_top,table_footer,True)
                                                    
                                                    if found_header:

                                                        if found_footer:
                                                            bottom=found_footer[0]['top']
                                                            bottom_page=found_footer[0]['pg_no']
                                                        else:
                                                            bottom=1000
                                                            bottom_page=False
                                                        print(F"found_header is {found_header}")
                                                        print(F"found_footer is {found_footer}")

                                                        template_extracted_table,table_high=extract_table_from_header(extraction_db,template_db,case_id,list(all_ocr_sections[section_no].values()),found_header,table_headers_line,section_top,bottom=bottom,cord=cordinates[section_no],trained_map=trained_map,bottom_page=bottom_page,extra_variables=extra_variables,no_header_columns=no_header_columns,found_extra_headers=found_extra_headers,second_header_box=second_header_box)
                                                        print(F" template_extracted_table #################### {template_extracted_table}")

                                                        # if needed_headers:
                                                        #     template_extracted_table=filter_columns(template_extracted_table, needed_headers)

                                                        if 'table' in table_high:
                                                            template_highlights['table'][table_name]=table_high['table']

                                                        template_fields['table'][table_name]=template_extracted_table
                                                    else:
                                                        template_fields['table'][table_name]={}

                                                    if template_fields['table'][table_name]:
                                                        table_extract=True
                                                        break


                                            if not table_extract:
                                                template_fields['table']={}
                                                template_fields['table']['table_1']={}


                                            print(f' remarks need to be extracted are {remarks}')
                                            if remarks and remarks.get('type','None') == 'claim level':
                                                extracted_remarks,remarks_high=extract_remarks(remarks.get('start',[]),remarks.get('end',[]),list(remarks_section_ocr[section_no].values()))

                                            if extracted_remarks:
                                                template_fields['remarks']=extracted_remarks
                                                template_highlights['remarks']=remarks_high


                                        if paraFieldsTraining:
                                            para_field,para_high=predict_paragraph(paraFieldsTraining,list(template_ocr.values()))
                                            print(para_field,'para_field')
                                            print(para_high,'para_high')
                                            for field,value in para_field.items():
                                                if field in template_fields['fields'] and value:
                                                    template_fields['fields'][field]=value
                                                    for field_high in template_highlights:
                                                        template_highlights['fields'][field_high]=para_high[field]

                                        fields_highlights[section].append(template_highlights)
                                        extracted_fields[section].append(template_fields)


                if extracted_fields:
                    update_into_db(tenant_id,extracted_fields,extracted_table,extraction_db,case_id,act_format_data,process,json.loads(master_process_trained_fields['fields']),used_version,document_id,extarction_from)
                    update_highlights(tenant_id,case_id,fields_highlights,act_format_data,document_id)
                
                # dir_path = os.path.join("/var/www/extraction_api/app/extraction_folder/", case_id)
                # try:
                #     # Delete the directory and its contents
                #     if os.path.exists(dir_path):
                #         shutil.rmtree(dir_path)
                #         print(f"Directory '{dir_path}' deleted successfully.")
                #     else:
                #         print(f"Directory '{dir_path}' does not exist.")
                        
                # except Exception as e:
                #     print(f"An error occurred while deleting the directory: {e}")

        #post Processing
        post_processing(extraction_db,queue_db,case_id)
    
        try:
            result_for_accuracy = checks_stored_field_acccuracy(tenant_id,case_id)
            print(f"#####result is {result_for_accuracy}")
        except Exception as e:
            print(f"##########Error is {e}")

        message="ionic_extraction api is sucessfull."

        container=load_controler(tenant_id,"business_rules")

        query = f"select no_of_process from load_balancer where container_name='{container}'"
        no_of_process = int(queue_db.execute_(query)['no_of_process'].to_list()[0])
        no_of_process=str(no_of_process+1)
        query=f"update load_balancer set no_of_process = '{no_of_process}' where container_name = '{container}'"
        queue_db.execute_(query)


        reponse = {"data":{"message":message,"container":container,'process_flag':'true'},"flag":True,"container":container,'process_flag':'true'}
    except Exception as e:
        queue_db.execute("update `process_queue` set  invalidfile_reason = %s where `case_id` = %s", params=[f"Error at Extraction Container",case_id])
    
        error_message = traceback.format_exc()
        print(f"########## Error is: {e}")
        print("########## Traceback details:")
        print(error_message)

        reponse = {"data":{"message":"Error at Extraction Container",'process_flag':'false'},"flag":True,'process_flag':'false'}

    query = f"select no_of_process from load_balancer where container_name='{container_name}'"
    no_of_process = int(queue_db.execute_(query)['no_of_process'].to_list()[0])
    no_of_process=str(no_of_process-1)
    query=f"update load_balancer set no_of_process = '{no_of_process}' where container_name = '{container_name}'"
    queue_db.execute_(query)

    return reponse


def filter_columns(data, allowed_columns):
    # Keep only the allowed headers
    data["headers"] = [header for header in data["headers"] if header in allowed_columns]
    
    # Keep only the allowed columns in rows
    for row in data["rows"]:
        keys_to_remove = [key for key in row if key not in allowed_columns]
        for key in keys_to_remove:
            del row[key]
    
    return data


def load_controler(tenant_id,service_name):
    no_of_process = int(os.environ.get('NO_OF_PROCESSES',''))

    
    db_config["tenant_id"] = tenant_id
    try:

        while True:
            queue_db = DB("queues", **db_config)
            # Query the load balancer table
            query = f"select * from load_balancer where service_name='{service_name}'"
            load_balancer_df = queue_db.execute(query)

            # If no container exists for the given service_name
            if load_balancer_df.empty:
                return None
            # Convert DataFrame to list of dictionaries for processing
            load_balancers = load_balancer_df.to_dict(orient='records')

            feasible_container_name = None

            # Iterate over the containers to find one with no processes
            for container in load_balancers:
                if int(container['no_of_process']) < 0:
                    temp_con=container['container_name']
                    query = f"Update load_balancer set no_of_process =0  where container_name='{temp_con}'"
                    load_balancer_df = queue_db.execute(query)
                    break
                    # Use the passed parameter instead of a fixed value
                if int(container['no_of_process']) < no_of_process:
                # if int(container['no_of_process']) <= 0:
                    feasible_container_name = container['container_name']
                    break
            # for container in load_balancers:
            #     if int(container['no_of_process']) <= 0:
            #         feasible_container_name = container['container_name']
            #         query = f"Update load_balancer set no_of_process =0  where container_name='{feasible_container_name}'"
            #         load_balancer_df = queue_db.execute(query)
            #         break  # Exit the loop as soon as we find a suitable container

            # If a feasible container is found, return it
            if feasible_container_name:
                return feasible_container_name

            # If no container is available, wait for a while before checking again
            time.sleep(5)  # Default is 5 seconds
                        
    except Exception as e:
        return None

        
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='Port Number', default=5099)
    parser.add_argument('--host', type=str, help='Host', default='0.0.0.0')

    args = parser.parse_args()

    host = args.host
    port = args.port

    app.run(host=host, port=port, debug=False)
