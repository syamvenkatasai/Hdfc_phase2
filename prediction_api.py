import json
import argparse
import psutil
import os
import requests
import copy
import ast
from PIL import Image, ImageDraw, ImageFont
import xml.etree.ElementTree as ET
from db_utils import DB
from time import time as tt
from flask import Flask, request, jsonify
from py_zipkin.zipkin import zipkin_span, ZipkinAttrs
from py_zipkin.util import generate_random_64bit_string
from ace_logger import Logging
from difflib import SequenceMatcher
import json
import numpy as np
import joblib  # Import joblib for model saving and loading




try:
    from cetroid_utils import *
    from ocr_block import *
    from svm_functions import *
except:
    from app.cetroid_utils import *
    from app.ocr_block import *
    from app.svm_functions import *

# try:
#     from layoutlm_preprocess import *
#     # from preprocess_train_script import *
#     #from merge_pt_Files import *
#     # from seq_labeling_train import *
# except:
# Commenting it for now
# from app.layoutlm_preprocess import *
    # from app.preprocess_train_script import *
    #from app.merge_pt_Files import *
    # from app.seq_labeling_train import *

# from app.layoutlm_preprocess import model_load,preprocess,convert_to_features
from app.utils import *
# import torch
# from torch.nn import CrossEntropyLoss

from app import app
logging = Logging(name="prediction_api")

db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT'],
}

def measure_memory_usage():
    process = psutil.Process()
    memory_info = process.memory_info()
    return memory_info.rss  # Resident Set Size (RSS) in bytes

def insert_into_audit(case_id, data):
    tenant_id = data.pop('tenant_id')
    db_config['tenant_id'] = tenant_id
    stats_db = DB('stats', **db_config)
    stats_db.insert_dict(data, 'audit')
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

def clean_data(input_list):
    input_list=input_list
    output_list = []

    for entry in input_list:
        single_doc_identifiers = entry['single_doc_identifiers']
        try:
            imagelist = entry['imagelist']
        except Exception as e:
            imagelist = json.loads(entry['imagelist'])
        for doc_identifier in single_doc_identifiers:
            if not doc_identifier['template_name']:
                start_page = doc_identifier['start_page']
                end_page = doc_identifier['end_page']
                file_type = doc_identifier['file_type'].split('_')[0]

                # Accessing the imagelist keys correctly
                image_list_key = list(imagelist.keys())[0]
                logging.info(image_list_key)
                image_list = imagelist[image_list_key]
                images_for_page_range = image_list[start_page:end_page + 1]

                sub_dict = {
                    'file_type': file_type,
                    'start_page': start_page,
                    'end_page': end_page,
                    'images': images_for_page_range
                }

                output_list.append(sub_dict)

    return output_list 

def get_image_path(tenant_id,case_id,image_list,page_num):
    # Find the image name with the given page number
    image_name = next((name for name in image_list if f"_{page_num}." in name), None)
    
            
    if image_name is None:
        logging.info(f"No image found for page {page_num}")
        return None
    image_path=f"/app/train_images/{tenant_id}/assets/pdf/{tenant_id}/{case_id}/images/{image_name}"
    logging.info(f"Image path i s{image_path}, image_name is {image_name}")
    
    
    try:
        image = Image.open(image_path)
        return image_path
    except Exception as e:
        logging.info(f"Error loading image: {e}")
        return image_path

@app.route('/prediction_api', methods=['POST', 'GET'])
def prediction_api():
    data = request.json
    case_id=data.get('case_id','')
    tenant_id=data.get('tenant_id',None)
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    extraction_db=DB('extraction',**db_config)
    logging.info(f'## TP info func_name : prediction_api \n input (data) : {data}')
    # words=['Invoice']
    # actual_boxes=[[234,78,678,89]]
    # image_path="/var/www/prediction_api/app/Changshu_Hexin_invoice.png"
    # page_num=0
    # cc=call_prediction(tenant_id,case_id,image_path,words,actual_boxes,page_num)
    

    
    try:
        query_doc_id=f"select document_id from `process_queue` where case_id='{case_id}'"
        document_id_list=queue_db.execute_(query_doc_id)['document_id'].tolist()
        logging.info(f"Document id's obtained are :{document_id_list}")
        if len(document_id_list)==0:
            logging.info(f"No document id's obtained please check")
        else:
            for document_id in document_id_list:
                query=f"SELECT single_doc_identifiers,imagelist FROM `process_queue` WHERE case_id='{case_id}' AND document_id='{document_id}' AND state IS NULL"
                query_data_df=queue_db.execute_(query)
                query_info=query_data_df.to_dict(orient="records")
                logging.info(f"Data from DB:{query_info}")
                single_doc_identifiers = ast.literal_eval(query_info[0]['single_doc_identifiers'])
                imagelist = ast.literal_eval(query_info[0]['imagelist'])

                query_info = [{
                    'single_doc_identifiers': single_doc_identifiers,

                    'imagelist': imagelist
                }]
                logging.info(f"Data received:{query_info}")
                ocr_data=get_ocr_data_parsed_cet(tenant_id,case_id,document_id)
                logging.info(f"OCr data obtained with case_id:{case_id} and document_id:{document_id}")
            try:
                data_list=clean_data(query_info)
                logging.info(f"Cleaned Data received:{query_info}")
            except Exception as e:
                logging.info(f" TP error in clean_data function {e}")

            output_data={}

            if len(data_list)==0 or data_list is None:
                logging.info(f"the templates are alredy detected or there is something wrong with data please check")
                
            for data in data_list:
                start_page = data['start_page']
                end_page = data['end_page']
                file_type = data['file_type']
                image_list = data['images']


                if start_page == end_page and file_type.lower()=='invoice':
                    
                    page_num=start_page
                    

                    actual_boxes, words = get_cords_words_nikhil(ocr_data, page_num)
                    if actual_boxes is None or not actual_boxes:
                        logging.info(f"Ocr coordintes or words were not obtained, please check page data for {page_num}")
                        continue
                    else:

                        image_path = get_image_path(tenant_id,case_id,image_list,page_num)
                        
                        logging.info(f"### calling Prediction for page num is {page_num}")
                        values,highlights=call_prediction(tenant_id,case_id,image_path,words,actual_boxes,ocr_data,page_num,file_type)    
                        try:
                            logging.info(f"## gng to update the values into db")
                            # extraction_db.update('ocr', values, where={'document_id': document_id})
                            insert_into_response_model(values,highlights,tenant_id,document_id,case_id,file_type,api='prediction_api')
                            update_highlights(tenant_id,document_id,highlights)
                        except Exception as e:
                            logging.info(f" ## Exception occured while updating the data {e}")
                        
                        # coords, words, and the loaded image
                        logging.info(f"File Type: {file_type}, Page: {page_num}")
                        logging.info(f"Coords: {actual_boxes}")
                        logging.info(f"Words: {words}")
                        logging.info(f"Image: {image_path}")
                        logging.info(f"Output_Values_Highlights: :{values,highlights}")# Displaying the loaded image
                        logging.info("========") 
                        output_data[page_num]=[values,highlights]
                        break
                else:

                    for page_num in range(start_page, end_page + 1):

                        
                        actual_boxes, words = get_cords_words_nikhil(ocr_data, page_num)
                        if actual_boxes is None or not actual_boxes:
                            logging.info(f"Ocr coordintes or words were not obtained, please check page data for {page_num}")
                            continue
                        else:
                            values,highlights={},{}
                            image_path = get_image_path(tenant_id,case_id,image_list,page_num)
                            
                            logging.info(f" ### calling Prediction for page num is {page_num}")
                            if file_type.lower()=='invoice':
                                values,highlights=call_prediction(tenant_id,case_id,image_path,words,actual_boxes,ocr_data,page_num,file_type)
                                try:
                                    logging.info(f"## gng to update the values into db")
                                    extraction_db.update('ocr', values, where={'document_id': document_id})
                                    update_highlights(tenant_id,document_id,highlights)
                                    insert_into_response_model(values,highlights,tenant_id,document_id,case_id,file_type,api='prediction_api')
                                except Exception as e:
                                    logging.info(f" ## Exception occured while updating the data {e}")

                            # coords, words, and the loaded image
                            # logging.info(f"File Type: {file_type}, Page: {page_num}")
                            # logging.info(f"Coords: {actual_boxes}")
                            # logging.info(f"Words: {words}")
                            # logging.info(f"Image: {image_path}")
                            # logging.info(f"Output_Values_Highlights: :{values,highlights}")# Displaying the loaded image
                            # logging.info("========") 
                                output_data[page_num]=[values,highlights]
                    break

            logging.info(f"Output_data for Prediction:{output_data}")
            response = {"flag": True, 'data':{"message": " in prediction","output_data":output_data}}
    
            return jsonify(response)     
            
    except Exception as e:
        logging.info(f" !@#$^%&*() An error occurred somewhere please check :{e}")
        response={"flag":False, 'data':{"message": " Errors in prediction","output_data":{}}}
        return jsonify(response)

    logging.info(f" ### Prediction Completed !!!!")
    response = {"flag": True,'data':{ "message": "Prediction Successfull","output_data":output_data}}
    return jsonify(response)



def get_labels_and_label_map(path):
    with open(path, "r") as f:
        labels = f.read().splitlines()

    logging.info(f" ### Labels got are {labels}")
    if "O" not in labels:
        labels = ["O"] + labels

    num_labels = len(labels)
    label_map = {i: label for i, label in enumerate(labels)}
    pad_token_label_id = CrossEntropyLoss().ignore_index

    label_dict = {
        item: 'green' if 'key' in item.lower() else 'orange' if item == 'O' else 'blue'
        for item in labels
    }

    return num_labels,label_dict,label_map


def iob_to_label(label):
  if label != 'O':
    return label[2:]
  else:
    return ""



def get_model_path(file_type, directory_path):
    directory_path=f"/var/www/prediction_api/app/data/model_files/"
    file_list = os.listdir(directory_path)
    
    # Filter files by the specified file_type
    selected_files = [file for file in file_list if file_type in file]
    
    # Filter out files that don't have numbers in their names
    selected_files_with_numbers = [
        file for file in selected_files if re.search(r'\d+', file)
    ]
    
    # Sort files based on the numbers in their names
    sorted_files = sorted(
        selected_files_with_numbers,
        key=lambda file: int(re.search(r'\d+', file).group())
    )
    
    if sorted_files:
        selected_file = sorted_files[-1]  # Select the file with the largest number
        full_path = os.path.join(directory_path, selected_file)
        logging.info(f" ## selected file is {selected_file}")
        return full_path
    else:
        return None


def call_prediction(tenant_id,case_id,image_path,words,actual_boxes,ocr_data,page_num,file_type):
    """
    here we call prediction with image,words and coordinates
    """
    file_type='Invoice'
    logging.info(" ### Executinggg get_model_path functionn")
    model_path=get_model_path(file_type,r'data/model_files')
    if model_path is None:
        logging.info(f"No model files present for this type {file_type} please check once")
        
    else:
        logging.info(" ### Executingg get_labels_and_label_map functionn")
        
        num_labels,label2color,label_map=get_labels_and_label_map("/var/www/prediction_api/app/data/invoice_data/labels.txt")
        logging.info(" ### Executing model_load functionn")
        model=model_load(model_path,num_labels)

        image_path=image_path

        words= words


        actual_boxes=actual_boxes

        logging.info(f" ### Executing Preprocess Function")
        
        image,words,boxes,actual_boxes=preprocess(image_path,actual_boxes,words)

        logging.info(f" ### Executing convert_to_features Function")
        word_level_predictions, final_boxes=convert_to_features(image, words, boxes, actual_boxes, model)
        
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()

        predicted_labels_dict = {}

        logging.info(f" ### Looping over prediction , predicted_labels_dict -- {predicted_labels_dict}")
        for prediction, box in zip(word_level_predictions, final_boxes):
            predicted_label = iob_to_label(label_map[prediction]).lower()
            label_color = label2color.get(predicted_label, 'blue')
            if predicted_label in predicted_labels_dict:
                predicted_labels_dict[predicted_label].append(box)
            else:
                predicted_labels_dict[predicted_label] = [box]

        output_list=[]
        output_dict={}
        logging.info(f" ####### predicted labels dict are {predicted_labels_dict}")
        for label, boxes in predicted_labels_dict.items():
            last_box = boxes[0]
            output_dict[label]=boxes[0]
            logging.info(f"Predicted data : Label: {label}, Box: {last_box}")
        
        logging.info(f" ## Executinggg modify_data function")
        output_dict=modify_data(output_dict,page_num)
        output_list.append(output_dict)
        logging.info(f"Prediction Data: {output_list}")

        logging.info(f" ## Executing get_value_highlights function")

        key_value=output_list[0]
        highlight_page_num=key_value['page_num']
        highlight_page_num=highlight_page_num
        ## get values and highlights for the predicted keys 
        key_fields,key_highlights = get_value_highlights(key_value['keys'],case_id,tenant_id,ocr_data,highlight_page_num)
        logging.info(f" ### key fields are {key_fields}  \n ## key highlights got are {key_highlights}")
        ## get values and highlights for the predicted values 
        value_fields,value_highlights = get_value_highlights(key_value['values'],case_id,tenant_id,ocr_data,highlight_page_num) 
        logging.info(f"Prediction Data: {output_dict}")

        key_value_highlights = [value_highlights,key_highlights]

        ## updating predicted columns to the ocr table
        return value_fields,value_highlights



def modify_data(input_dict, page):
    """
    This function is used to manipulate output data from prediction so that it is similar to the data we
      typically use.
      output_data_format={'keys':{'key':{'top':,'bottom':,'left':,'right':}},'values':{'top':,'bottom':,'left':,'right':}}

    """
    output_dict = {
        'keys': {},
        'values': {},
        'page_num': page
    }

    for key, value in input_dict.items():
        if key != 'others' and key !='':
            if key.startswith('key_'):
                actual_key = key[4:]
                if actual_key in input_dict:
                    modified_key = actual_key
                    # Process if the key starts with 'key_' and has a corresponding 'actual_key' in the input_dict
                    output_dict['keys'][modified_key] = {
                        'top': value[1],
                        'bottom': value[3],
                        'left': value[0],
                        'right': value[2]
                    }
                    output_dict['values'][modified_key] = {
                        'top': input_dict[actual_key][1],
                        'bottom': input_dict[actual_key][3],
                        'left': input_dict[actual_key][0],
                        'right': input_dict[actual_key][2]
                    }
            else:
                # Process for keys that do not start with 'key_'
                output_dict['keys'][key] = {
                    'top': value[1],
                    'bottom': value[3],
                    'left': value[0],
                    'right': value[2]
                }
                output_dict['values'][key] = output_dict['keys'][key]

    return output_dict


#### the below code is responsible to predict the keywords based on the cetroid method 
@app.route('/cetroid_api', methods=['POST', 'GET'])
def cetroid_api():
    data = request.json
    case_id=data.get('case_id','')
    document_id=data.get('document_id','')
    tenant_id=data.get('tenant_id','')
    seg=data.get('seg','')
    start_page=data.get('start_page',0)
    end_page=data.get('end_page',0)
    template_name=data.get('template_name','')
    db_config['tenant_id'] = tenant_id
    queue_db = DB('queues', **db_config)
    extraction_db=DB('extraction',**db_config)
    template_db=DB('template_db',**db_config)
    api=data.get('field_prediction_centroid_flag',False)
    retrain_api=data.get('retrain_api',False)
    if retrain_api:
        api=True
    update_ocr_data={}
    historicial_training={"non":"non_table_fields"}
    logging.info(f"## TP info func_name : CALLING cetroid_api FOR  input {data} : FILE TYPE is  { data['file_type']} and TEMPLATE IS {template_name}")
    try:
        file_type = data['file_type'].split('_')[0]
        file_type_lower=file_type.lower()
        logging.info(f" ########### file_type is {file_type}")
        if start_page == end_page and file_type!='Application Form':
            page_num=start_page
            
            ######## Reading ocr_word data and formating the ocr data for the centroid model
            ocr_data_word=get_ocr_data_parsed_cet(tenant_id,case_id,document_id)
            ocr_data_word=ocr_data_word[page_num]
            svm_ocr_ref=ocr_data_word
            logging.info(f" ######### ocr_data get  fro page {page_num} is \n ################ {ocr_data_word} ")

            unique_fields=data.get('ui_train_info',{})
           
            elements=key_value(unique_fields,0)
            logging.info(f" ######### elements are {elements}")
            combined_list_keys_=[]
            combined_values_ = {}
            if api and (unique_fields or elements):
                keys_to_find_=list(elements.keys())
                # keys_to_find_p={}
                keys_to_find_w=[]
                key_map={}
                for i,key in enumerate(keys_to_find_,1):
                    # ke,matched_key=key_find_(key,ocr_parsed)
                    # key_map[matched_key]=key
                    # keys_to_find_p[i]=ke
                    ke_,matched_key=key_find(key,ocr_data_word)
                    keys_to_find_w.append(ke_)
                    # cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', matched_key)
                    # cleaned_text = ' '.join(cleaned_text.split()).lower()
                    key_map[matched_key]=key

                
                #has to genearte highlights at this point for keys in api condition


                logging.info(f"################# key_to find are {keys_to_find_w} \n key_map is  {key_map}")
                ocr_data_wrd_frmt=[]
                for ocr_data in ocr_data_word:
                    ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
            #     ele_ocr_data_parse_frmt=[]
            #     for sent_,ocr_data in keys_to_find_p.items():
            # #     logging.info(f" ocr data is {ocr_data}")
            #         ele_ocr_data_parse_frmt.append({'text':ocr_data['text'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
                ele_ocr_data_wrd_frmt=[]
                for ocr_data in keys_to_find_w:
                #     logging.info(f" ocr data is {ocr_data}")
                    ele_ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
                # x_tilt,y_tilt=get_prob_ele_(ele_ocr_data_parse_frmt,ocr_data_parse_frmt, x_corrd_thr=10.0, y_corrd_thr=10.0)
                x_tilt_data_words,y_tilt_data_words=get_prob_ele_(ele_ocr_data_wrd_frmt,ocr_data_wrd_frmt,x_corrd_thr=200.0,y_corrd_thr=7)

                logging.info(f" ########### final Y_titl_values trained from wORDS GOT IS {x_tilt_data_words}")

                logging.info(f" ############ finall X_tilt_values trained from wORDS GOT IS {y_tilt_data_words}")

                x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt_data_words,y_tilt_data_words,True)
                # x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt_data_words,y_tilt_data_words,x_tilt_find=False)

                # x_tilt_values=find_lowest_value_within_margin(x_tilt_data_words,margin=2)
                # x_tilt_values = {key.lower(): value for key, value in x_tilt_values.items()}


                logging.info(f" ########### final Y_titl_values trained from wORDS GOT IS {x_tilt_values}")

                logging.info(f" ############ finall X_tilt_values trained from wORDS GOT IS {y_tilt_values}")
                # x_tilt_values,y_tilt_values=merge_and_get_unique(x_tilt_values,x_tilt_values_wrds,y_tilt_values,y_tilt_values_wrds)

                x_tilt_keys=list(x_tilt_values.keys())
                y_tilt_keys=list(y_tilt_values.keys())
                combined_list_keys_ = list(set(x_tilt_keys+ y_tilt_keys))

                logging.info(f"  ############  keys for trained keys are {combined_list_keys_} \n")
                #has to genearte highlights at this point for keys in non api condition
                result_dict_ = {key: [] for key in x_tilt_values.keys() | y_tilt_values.keys()}
            

            # Update the result dictionary with values from dict1 and dict2
                for key in result_dict_:
                    result_dict_[key].append(y_tilt_values.get(key, None))
                    result_dict_[key].append(x_tilt_values.get(key, None))

                # Remove None values if desired
                for key, values in result_dict_.items():
                    result_dict_[key] = [value for value in values if value is not None]

                logging.info(f"################# result_dict for trained keys is {result_dict_}")

                

                for key, value_list in result_dict_.items():
                    combined_values_[key] = []
                    for item in value_list:
                        if isinstance(item, list):
                            combined_values_[key].extend(item)
                        else:
                            combined_values_[key].append(item)

                logging.info(f"################# combined_values_ of trained keys is {combined_values_}")

            logging.info(f" ######## gng to EXECUTE remove_from_ocr for trained_keys that are found in ocr \n")
            ocr_blk_data=remove_from_ocr(combined_list_keys_,ocr_data_word)
        #     ocr_data_parse_frmt=[]
        #     for sent,ocr_data in ocr_parsed.items():
        # #     logging.info(f" ocr data is {ocr_data}")
        #         ocr_data_parse_frmt.append({'text':ocr_data['text'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})

        #     logging.info(f" ###### ocr_parsed is {ocr_parsed}")
            if ocr_blk_data:
                ocr_data_wrd_frmt=[]
                for ocr_data in ocr_blk_data:
                    # logging.info(f" ###### if ocr element prep {ocr_data}")
                    ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
            else:
                ocr_data_wrd_frmt=[]
                for ocr_data in ocr_data_word:
                    # logging.info(f" ###### else ocr element prep {ocr_data}")
                    ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
            logging.info(f" ######### ocr_data get  fro page {page_num} is \n ################ {ocr_data_wrd_frmt} ")

            # x_tilt,y_tilt=get_prob_ele(ocr_data_parse_frmt,x_corrd_thr=10.0,y_corrd_thr=10.0)
            x_tilt_data_words,y_tilt_data_words=get_prob_ele(ocr_data_wrd_frmt,x_corrd_thr=200.0,y_corrd_thr=7,x_tilt_find=False)
            
            logging.info(f" ########### final Y_titl_values trained from wORDS GOT IS {x_tilt_data_words}")

            logging.info(f" ############ finall X_tilt_values trained from wORDS GOT IS {y_tilt_data_words}")


            # x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt,y_tilt)
            x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt_data_words,y_tilt_data_words,x_tilt_find=True)
            # logging.info(f" ###### output  from get_prob y is {y_tilt_data_words}")
            # x_tilt_values=find_lowest_value_within_margin(x_tilt_data_words,margin=2)
            # x_tilt_values = {key.lower(): value for key, value in x_tilt_values.items()}

            logging.info(f" ########### final Y_titl_values untrained from wORDS GOT IS {x_tilt_values} \n")
            logging.info(f" ############ finall X_tilt_values untrained from wORDS GOT IS {y_tilt_values} \n")
            # x_tilt_values,y_tilt_values=merge_and_get_unique(x_tilt_values,x_tilt_values_wrds,y_tilt_values,y_tilt_values_wrds)

            try:
                get_full_train_qry=f"SELECT `training_data` FROM `field_dictionary_json` where `template_name` ='{historicial_training[file_type_lower]}'"
                get_full_train_data=template_db.execute_(get_full_train_qry)
                data=ast.literal_eval(get_full_train_data['training_data'].to_list()[0])
                x_tilt_keys=list(x_tilt_values.keys())
                y_tilt_keys=list(y_tilt_values.keys())
                combined_list_keys__ = list(set(x_tilt_keys+ y_tilt_keys))
            except:
                combined_list_keys__=[]

            logging.info(f"keys of untrained are {combined_list_keys__} \n")

            combined_list_keys=list(set(combined_list_keys__+ combined_list_keys_))
            

            update_ocr_data={}
            
            # Initialize a result dictionary with empty lists as values
            result_dict = {key: [] for key in x_tilt_values.keys() | y_tilt_values.keys()}
            

            # Update the result dictionary with values from dict1 and dict2
            for key in result_dict:
                    y_value=y_tilt_values.get(key, None)
                    x_value=x_tilt_values.get(key, None)
                    # logging.info(y_value,x_value)
                    if y_value:
                        y_value=y_value
                        result_dict[key].append(y_value)
                    if x_value:
                        result_dict[key].append(x_value)

            # Remove None values if desired
            for key, values in result_dict.items():
                result_dict[key] = [value for value in values if value is not None]

            logging.info(f"  ################# result_dict for untrained keys is {result_dict} \n")

            combined_values = {}

            for key, value_list in result_dict.items():
                combined_values[key] = []
                for item in value_list:
                    if isinstance(item, list):
                        combined_values[key].extend(item)
                    else:
                        combined_values[key].append(item)

            logging.info(f"  ################# combined_values for untraiend keys  is {combined_values} \n")


            logging.info(f"  #################  keys of both trained and untrained are {combined_list_keys__} \n")

            remove_keywords=[]
            tilt_result={}
            for key in combined_list_keys:
                
                keys_hi={}
                logging.info(f"  #################  the key is {key} \n")
                # try:
                if key in combined_values_:
                    try:
                        category=elements[key_map[key]]  
                        logging.info(f"  try block #################  the category is {category} \n") 
                    except:
                        key_=remove_all_except_al_num_prob_ele(key)
                        category=find_key_algo(key_,data)
                        logging.info(f" except #################  the category is {category} \n")
                    possible_values_x_y=combined_values_[key]
                else:
                    key_=remove_all_except_al_num_prob_ele(key)
                    category=find_key_algo(key_,data)
                    logging.info(f"  #################  the category is {category} \n")
                    possible_values_x_y=combined_values[key]
                logging.info(f"  #################  the possible_values_x_y is {possible_values_x_y} \n")
                # except:
                #     category=''
                #     pass
                if category:
                    # result=find_values(possible_values_x_y,category)
                    result=find_values(possible_values_x_y,category,file_type,tenant_id)
                    if not result:
                        pass
                    else:
                        if category in tilt_result:
                            tilt_result[category].append(result)
                        else:   
                            tilt_result[category]=[result]
                        keys_hi[key]=category
                
                ## since we can consider values of y_tilt , in result array y tilt values are at 0 th index
            logging.info(f" ########## output of the tilt function is {tilt_result} \n")
            for key,values in tilt_result.items():
                logging.info(f"key and values are {key} {values}")
                # values = sorted(values, key=lambda x: x[1],reverse=True)
                # logging.info(f"key and values are {key} {values}")
                if len(values)==1:
                    values=values[0]
                    update_ocr_data[key]=values[0]
                    remove_keywords.append(key)
                    remove_keywords.append(values[0])
                else:
                    logging.info(f"sending back to svm value for {values}")
                    first_elements = [sublist[0] for sublist in values]
                    # result=find_values(first_elements,key)
                    result=find_values(first_elements,key,file_type,tenant_id)
                    value=result[0]
                    update_ocr_data[key]=value
                    remove_keywords.append(key)
                    remove_keywords.append(value)

            key_highlights={}
            logging.info(f" ########### update_ocr_data after tilt is {update_ocr_data} \n")
            key_highlights=get_highlight(update_ocr_data,start_page,ocr_data_word)

            try:
                ocr_data_word_=[ocr_data_word]
                cpy_ocr_data=[copy.deepcopy(ocr_data_word)]
                # logging.info(f" ######## {ocr_data_word_}")
                logging.info(f" ######## gng to EXECUTE remove_from_ocr after tilt function \n")
                ocr_blk_data=remove_from_ocr(remove_keywords,ocr_data_word_[0])
                logging.info(f" ######## for this list of elements {remove_keywords} \n")
                logging.info(f" ######## gng to EXECUTE get_ocr_frmt_data for svm 1 \n")
                ocr_data_blk_frmt=get_ocr_frmt_data(ocr_blk_data)
                # logging.info(f" ######## {ocr_data_blk_frmt}")
                if ocr_data_blk_frmt:
                    print("ifffffffff")
                    blk_wrds_data=[]
                    for blk_data in ocr_blk_data:
                        if blk_data.get('top', 501) < 500: 
                            try:
                                blk_wrds_data.append(blk_data['text'])
                            except:
                                blk_wrds_data.append(blk_data['word'])

                    print(f" ######## gng to EXECUTE predict_with_svm 1 \n")
                    # predicted_labels=predict_with_svm(blk_wrds_data,file_type,
                    #                   [d for d in ocr_data_word if d.get('top', 501) < 500],
                    #                   svm_ocr_ref,"0",tenant_id,seg)
                    print(f"the bulk words data is:::::{blk_wrds_data}")
                    print(f"the ocr_word dara is::::::{ocr_data_word}")
                    predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_blk_data,svm_ocr_ref,"0",tenant_id,seg)
                else:
                    print("else::::::")
                    blk_wrds_data=[]
                    for blk_data in ocr_data_word:
                        if blk_data.get('top', 501) < 500: 
                            try:
                                blk_wrds_data.append(blk_data['text'])
                            except:
                                blk_wrds_data.append(blk_data['word'])

                    print(f" ######## gng to EXECUTE predict_with_svm 1 \n")
                    # predicted_labels=predict_with_svm(blk_wrds_data,file_type,
                    #                   [d for d in ocr_data_word if d.get('top', 501) < 500],
                    #                   svm_ocr_ref,"0",tenant_id,seg)
                    print(f"the bulk words data is:::::{blk_wrds_data}")
                    print(f"the ocr_word dara is::::::{ocr_data_word}")

                    predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_data_word,svm_ocr_ref,"0",tenant_id,seg)

                # update_ocr_data.update(predicted_labels)
                # remove_keywords_svm=[]
                # for label,values in predicted_labels.items():
                #     if len(values)==1:
                #         update_ocr_data[label]=values[0]
                #         remove_keywords_svm.append(values[0])
                #     else:
                #         for ele in values:
                #             remove_keywords_svm.append(ele)
                if predicted_labels:
                    update_ocr_data.update({key: value for key, value in predicted_labels.items() if (key not in update_ocr_data or key =='date_stat')})
                logging.info(f" ########### update_ocr_data after first svm with {predicted_labels} is {update_ocr_data} \n")
                #call highlights function on ocr_word and ocr_parsed  for keys_hi
                
                if predicted_labels:
                    remove_keywords_svm=list(predicted_labels.values())
                    ocr_blk_data=remove_from_ocr(remove_keywords_svm,cpy_ocr_data[0])

                ocr_blocks=get_ocr_block([ocr_blk_data])
                logging.info(f" ######## creation ocr_block is complete \n")

                blk_wrds_data=[]
                for blk_data in ocr_blocks:
                    try:
                        blk_wrds_data.append(blk_data['text'])
                    except:
                        blk_wrds_data.append(blk_data['word'])
                
                print(f" ######## gng to EXECUTE predict_with_svm 2nd time")

                
                predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_blocks,svm_ocr_ref,"1",tenant_id,seg)
                if predicted_labels:
                    update_ocr_data.update(predicted_labels)
                
                

                print(f" ########### update_ocr_data after second svm with {predicted_labels} is {update_ocr_data} \n")
                # ---------- FALLBACK FULL PAGE RUN IF DATE MISSING ----------
                try:
                    print("##__________##")
                    if not any(update_ocr_data.get(k) for k in ['date', 'date_stat']):
                        print("##### Date not detected — running full page scan with same processing steps as above")

                        # Copy original data
                        ocr_data_word_full = copy.deepcopy(ocr_data_word)

                        # Remove any keywords already extracted
                        ocr_blk_data_full = remove_from_ocr(remove_keywords, ocr_data_word_full)

                        # Create blocks from remaining OCR
                        ocr_blocks_full = get_ocr_block([ocr_blk_data_full])

                        # Prepare full blk_wrds_data (no top filter this time)
                        blk_wrds_data_full = []
                        for blk_data in ocr_blocks_full:
                            try:
                                blk_wrds_data_full.append(blk_data['text'])
                            except:
                                blk_wrds_data_full.append(blk_data['word'])

                        # Step 1: Get all possible candidates (mode "0")
                        predicted_labels = predict_with_svm(
                            blk_wrds_data_full, file_type, ocr_blocks_full, svm_ocr_ref, "0", tenant_id, seg
                        )
                        if predicted_labels:
                            update_ocr_data.update(predicted_labels)

                        # Step 2: Finalize best date (mode "1")
                        predicted_labels = predict_with_svm(
                            blk_wrds_data_full, file_type, ocr_blocks_full, svm_ocr_ref, "1", tenant_id, seg
                        )
                        if predicted_labels:
                            update_ocr_data.update(predicted_labels)

                        print(f"########### update_ocr_data after full page retry (0 & 1) is {update_ocr_data}")

                except Exception as e:
                    print(f"the issue is fall back logic::{e}")
                print(f"going to create highlights")
                highlights=get_highlight(update_ocr_data,start_page,ocr_data_word,{},ocr_blocks)
                logging.info(f"highlights are for values {highlights}")
                logging.info(f"highlights are for keys {key_highlights}")

                highlight=update_append_highlights(highlights,tenant_id,case_id)
                key_highlights=update_append_highlights(key_highlights,tenant_id,case_id)

                #updating into fields accuarcy table
                # db = DB('queues', **db_config)
                # logging.info("updating into the field accuarcy table")
                # query = f"select `total_fields_extracted` from `field_accuracy` where case_id ='{case_id}'"
                # query_data = db.execute_(query)['total_fields_extracted'].to_list()
                # if query_data:
                #     extracted=query_data[0]
                #     total_fields_extracted=int(extracted)+len(update_ocr_data)
                #     query = f"update `field_accuracy` set `total_fields_extracted`='{total_fields_extracted}',`total_fields`='{total_fields_extracted}'  where case_id ='{case_id}'"
                #     db.execute(query)
                # else:
                #     query = f"INSERT INTO `field_accuracy` ( `case_id`, `percentage`,`total_fields_extracted`,`total_fields`) VALUES ('{case_id}','100','{len(update_ocr_data)}','{len(update_ocr_data)}')"
                #     db.execute(query)


                # update_ocr_data[table]=json.dumps(table_data)
                
                # update_ocr_data['key_highlight']=key_highlights

                update_ocr_data['highlight']=highlight

                logging.info(f" ########### final ocr data got is {update_ocr_data}")

                logging.info(f" ###### gng to update values into the OCR table")
                try:
                    update_ocr_data.pop('customer_name',None)
                    logging.info(f"#######UPDATED")
                    extraction_db.update("ocr",update_ocr_data, where={'case_id': case_id})
                    logging.info(f"#######UPDATE_OCR_DATA GOT IS::{update_ocr_data}")

                except:
                    logging.info(f" ############ some database error please look into the query")

                ocr_data_wrd_frmt=[]
                ele_ocr_data_wrd_frmt=[]
            except Exception as e:
                logging.info(f" ########### exception occured while updating values into response data {e}")

        else:
            variable=0
            for page_num in range(start_page, end_page + 1):
                
                # ocr_parsed=get_ocr_data_parsed(tenant_id,case_id,document_id)
                # ocr_parsed=ocr_parsed[str(page_num+1)]
                
                ######## Reading ocr_word data and formating the ocr data for the centroid model
                ocr_data_word=get_ocr_data_parsed_cet(tenant_id,case_id,document_id)
                ocr_data_word=ocr_data_word[page_num]
                svm_ocr_ref=ocr_data_word
                logging.info(f" ######### ocr_data get {ocr_data_word}")

                unique_fields=data.get('ui_train_info',{})
                
                elements=key_value(unique_fields,variable)
                logging.info(f" ######### elements are {elements}")
                combined_list_keys_=[]
                combined_values_ = {}
                if api or unique_fields or elements:
                    keys_to_find_=list(elements.keys())
                    # keys_to_find_p={}
                    keys_to_find_w=[]
                    key_map={}
                    for i,key in enumerate(keys_to_find_,1):
                        # ke,matched_key=key_find_(key,ocr_parsed)
                        # key_map[matched_key]=key
                        # keys_to_find_p[i]=ke
                        ke_,matched_key=key_find(key,ocr_data_word)
                        keys_to_find_w.append(ke_)
                        # cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', matched_key)
                        # cleaned_text = ' '.join(cleaned_text.split()).lower()
                        key_map[matched_key]=key

                    
                    #has to genearte highlights at this point for keys in api condition
                    ocr_data_wrd_frmt=[]
                    for ocr_data in ocr_data_word:
                        ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})

                    logging.info(f"################# key_to find are {keys_to_find_w}  and key_map is  {key_map} \n")

                #     ele_ocr_data_parse_frmt=[]
                #     for sent_,ocr_data in keys_to_find_p.items():
                # #     logging.info(f" ocr data is {ocr_data}")
                #         ele_ocr_data_parse_frmt.append({'text':ocr_data['text'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
                    ele_ocr_data_wrd_frmt=[]
                    for ocr_data in keys_to_find_w:
                    #     logging.info(f" ocr data is {ocr_data}")
                        ele_ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
                    logging.info(f"pased from here")
                    # x_tilt,y_tilt=get_prob_ele_(ele_ocr_data_parse_frmt,ocr_data_parse_frmt, x_corrd_thr=10.0, y_corrd_thr=10.0)
                    x_tilt_data_words,y_tilt_data_words=get_prob_ele_(ele_ocr_data_wrd_frmt,ocr_data_wrd_frmt,x_corrd_thr=200.0,y_corrd_thr=15)

                    logging.info(f" ########### final Y_titl_values trained from wORDS GOT IS {x_tilt_data_words}")

                    logging.info(f" ############ finall X_tilt_values trained from wORDS GOT IS {y_tilt_data_words}")

                    
                    x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt_data_words,y_tilt_data_words,x_tilt_find=True)

                    # x_tilt_values=find_lowest_value_within_margin(x_tilt_data_words,margin=2)
                    # x_tilt_values = {key.lower(): value for key, value in x_tilt_values.items()}


                    logging.info(f" ########### final Y_titl_values for trained key from wORDS GOT IS {x_tilt_values}")

                    logging.info(f" ############ finall X_tilt_valuesfinal  for trained key  from wORDS GOT IS {y_tilt_values}")
                    # x_tilt_values,y_tilt_values=merge_and_get_unique(x_tilt_values,x_tilt_values_wrds,y_tilt_values,y_tilt_values_wrds)

                    x_tilt_keys=list(x_tilt_values.keys())
                    y_tilt_keys=list(y_tilt_values.keys())
                    combined_list_keys_ = list(set(x_tilt_keys+ y_tilt_keys))
                    logging.info(f"keys for trained keys are {combined_list_keys_} \n")
                    #has to genearte highlights at this point for keys in non api condition
                    result_dict_ = {key: [] for key in x_tilt_values.keys() | y_tilt_values.keys()}
            

                # Update the result dictionary with values from dict1 and dict2
                    for key in result_dict_:
                        result_dict_[key].append(y_tilt_values.get(key, None))
                        result_dict_[key].append(x_tilt_values.get(key, None))

                    # Remove None values if desired
                    for key, values in result_dict_.items():
                        result_dict_[key] = [value for value in values if value is not None]

                    logging.info(f"################# result_dict for trained keys is {result_dict_}")

                    

                    for key, value_list in result_dict_.items():
                        combined_values_[key] = []
                        for item in value_list:
                            if isinstance(item, list):
                                combined_values_[key].extend(item)
                            else:
                                combined_values_[key].append(item)

                    logging.info(f"################# combined_values_ trained keys  is {combined_values_}")

                logging.info(f" ######## gng to EXECUTE remove_from_ocr after tilt func")
                ocr_blk_data=remove_from_ocr(combined_list_keys_,ocr_data_word)
                
            #     ocr_data_parse_frmt=[]
            #     for sent,ocr_data in ocr_parsed.items():
            # #     logging.info(f" ocr data is {ocr_data}")
            #         ocr_data_parse_frmt.append({'text':ocr_data['text'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
            
            #     logging.info(f" ###### ocr_parsed is {ocr_parsed}")
                if ocr_blk_data:
                    logging.info(f" ###### ocr_parsed is {ocr_blk_data}")

                    ocr_data_wrd_frmt=[]
                    for ocr_data in ocr_blk_data:
                        ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})
                else:
                    logging.info(f" ###### ocr_parsed is {ocr_data_word}")

                    ocr_data_wrd_frmt=[]
                    for ocr_data in ocr_data_word:
                        ocr_data_wrd_frmt.append({'text':ocr_data['word'],'boundingBox':[ocr_data['left'],ocr_data['top'],ocr_data['right'],ocr_data['bottom']]})


                # x_tilt,y_tilt=get_prob_ele(ocr_data_parse_frmt,x_corrd_thr=10.0,y_corrd_thr=10.0)
                x_tilt_data_words,y_tilt_data_words=get_prob_ele(ocr_data_wrd_frmt,x_corrd_thr=10.0,y_corrd_thr=15,x_tilt_find=False)

                logging.info(f" ########### final Y_titl_values trained from wORDS GOT IS {x_tilt_data_words}")

                logging.info(f" ############ finall X_tilt_values trained from wORDS GOT IS {y_tilt_data_words}")


                x_tilt_values,y_tilt_values=generate_x_y_tilt_words_parse(x_tilt_data_words,y_tilt_data_words,x_tilt_find=True)

                # x_tilt_values=find_lowest_value_within_margin(x_tilt_data_words,margin=2)
                # x_tilt_values = {key.lower(): value for key, value in x_tilt_values.items()}

                logging.info(f" ########### final Y_titl_values  for untrained keys from wORDS GOT IS {y_tilt_values}")
                logging.info(f" ############ finall X_tilt_values for untained keys from wORDS GOT IS {x_tilt_values}")
                # x_tilt_values,y_tilt_values=merge_and_get_unique(x_tilt_values,x_tilt_values_wrds,y_tilt_values,y_tilt_values_wrds)


                get_full_train_qry=f"SELECT `training_data` FROM `field_dictionary_json` where `template_name` ='{historicial_training[file_type_lower]}'"
                get_full_train_data=template_db.execute_(get_full_train_qry)
                data=ast.literal_eval(get_full_train_data['training_data'].to_list()[0])
                x_tilt_keys=list(x_tilt_values.keys())
                y_tilt_keys=list(y_tilt_values.keys())
                combined_list_keys__ = list(set(x_tilt_keys+ y_tilt_keys))
                logging.info(f"untraiend keys that are found {combined_list_keys__} \n")

                combined_list_keys=list(set(combined_list_keys__+ combined_list_keys_))

                update_ocr_data={}
                
                # Initialize a result dictionary with empty lists as values
                result_dict = {key: [] for key in x_tilt_values.keys() | y_tilt_values.keys()}
            

                # Update the result dictionary with values from dict1 and dict2
                for key in result_dict:
                    y_value=y_tilt_values.get(key, None)
                    # logging.info(y_value)
                    x_value=x_tilt_values.get(key, None)
                    if y_value:
                        y_value=y_value
                        result_dict[key].append(y_value)
                    if x_value:
                        result_dict[key].append(x_value)

                # Remove None values if desired
                for key, values in result_dict.items():
                    result_dict[key] = [value for value in values if value is not None]

                logging.info(f"################# result_dict for untraiend keys is {result_dict}\n")

                combined_values = {}

                for key, value_list in result_dict.items():
                    combined_values[key] = []
                    for item in value_list:
                        if isinstance(item, list):
                            combined_values[key].extend(item)
                        else:
                            combined_values[key].append(item)

                logging.info(f"################# combined_values for untraiend keys  is {combined_values}\n")

                logging.info(f"combined trained and untraiend keys that are found {combined_list_keys} \n")

                remove_keywords=[]
                tilt_result={}
                for key in combined_list_keys:
                    
                    keys_hi={}
                    logging.info(f"  #################  the key is {key} \n")

                    if key in combined_values_:
                        try:
                            category=elements[key_map[key]]  
                            logging.info(f"  try block #################  the category is {category} \n") 
                        except:
                            key_=remove_all_except_al_num_prob_ele(key)
                            category=find_key_algo(key_,data)
                            logging.info(f" except #################  the category is {category} \n")
                        possible_values_x_y=combined_values_[key]
                    else:
                        key_=remove_all_except_al_num_prob_ele(key)
                        category=find_key_algo(key_,data)
                        logging.info(f"  #################  the category is {category} \n")
                        possible_values_x_y=combined_values[key]
                    logging.info(f"  #################  the possible_values_x_y is {possible_values_x_y} \n")
                    if category:
                        # result=find_values(possible_values_x_y,category)
                        result=find_values(possible_values_x_y,category,file_type,tenant_id)
                        if not result:
                            pass
                        else:
                            if category in tilt_result:
                                tilt_result[category].append(result)
                            else:   
                                tilt_result[category]=[result]
                            keys_hi[key]=category
                    
                    ## since we can consider values of y_tilt , in result array y tilt values are at 0 th index
                logging.info(f" ########## output of the tilt function is {tilt_result} \n")
                for key,values in tilt_result.items():
                    logging.info(f"key and values are {key} {values}")
                    # values = sorted(values, key=lambda x: x[1],reverse=True)
                    # logging.info(f"key and values are {key} {values}")
                    if len(values)==1:
                        values=values[0]
                        update_ocr_data[key]=values[0]
                        remove_keywords.append(key)
                        remove_keywords.append(values[0])
                    else:
                        logging.info(f"sending back to svm value for {values}")
                        first_elements = [sublist[0] for sublist in values]
                        # result=find_values(first_elements,key)
                        result=find_values(first_elements,key,file_type,tenant_id)
                        value=result[0]
                        update_ocr_data[key]=value
                        remove_keywords.append(key)
                        remove_keywords.append(value)

                key_highlights={}
                logging.info(f" ########### update_ocr_data after tilt is {update_ocr_data} \n")
                key_highlights=get_highlight(update_ocr_data,page_num,ocr_data_word)

                try:
                    ocr_data_word_=[ocr_data_word]
                    cpy_ocr_data=[copy.deepcopy(ocr_data_word)]
                    # logging.info(f" ######## {ocr_data_word_}")
                    logging.info(f" ######## gng to EXECUTE remove_from_ocr")
                    ocr_blk_data=remove_from_ocr(remove_keywords,ocr_data_word_[0])
                    # logging.info(f" ######## {ocr_blk_data}")
                    logging.info(f" ######## gng to EXECUTE get_ocr_frmt_data")
                    ocr_data_blk_frmt=get_ocr_frmt_data(ocr_blk_data)
                    # logging.info(f" ######## {ocr_data_blk_frmt}")
                    if ocr_data_blk_frmt:
                        blk_wrds_data = []
                        for blk_data in ocr_blk_data:
                            if blk_data.get('top', 501) < 500:   # newly added condition
                                try:
                                    blk_wrds_data.append(blk_data['text'])
                                except:
                                    blk_wrds_data.append(blk_data['word'])

                        logging.info(f" ######## gng to EXECUTE predict_with_svm 1")
                        predicted_labels = predict_with_svm(
                            blk_wrds_data, file_type, ocr_blk_data, svm_ocr_ref, "0", tenant_id
                        )

                    else:
                        blk_wrds_data = []
                        for blk_data in ocr_data_word:
                            if blk_data.get('top', 501) < 500:   # newly added condition
                                try:
                                    blk_wrds_data.append(blk_data['text'])
                                except:
                                    blk_wrds_data.append(blk_data['word'])

                        logging.info(f" ######## gng to EXECUTE predict_with_svm 1")
                        predicted_labels = predict_with_svm(
                            blk_wrds_data, file_type, ocr_data_word, svm_ocr_ref, "0", tenant_id
                        )

                    # if ocr_data_blk_frmt:
                    #     blk_wrds_data=[]
                    #     for blk_data in ocr_blk_data:
                    #         try:
                    #             blk_wrds_data.append(blk_data['text'])
                    #         except:
                    #             blk_wrds_data.append(blk_data['word'])
                    #     logging.info(f" ######## gng to EXECUTE predict_with_svm 1")
                    #     predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_blk_data,svm_ocr_ref,"0",tenant_id)

                    # else:
                    #     blk_wrds_data=[]
                    #     for blk_data in ocr_data_word:
                    #         try:
                    #             blk_wrds_data.append(blk_data['text'])
                    #         except:
                    #             blk_wrds_data.append(blk_data['word'])

                    #     logging.info(f" ######## gng to EXECUTE predict_with_svm 1")
                    #     predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_data_word,svm_ocr_ref,"0",tenant_id)
                        
                    # update_ocr_data.update(predicted_labels)
                    # remove_keywords_svm=[]
                    # for label,values in predicted_labels.items():
                    #     if len(values)==1:
                    #         update_ocr_data[label]=values[0]
                    #         remove_keywords_svm.append(values[0])
                    #     else:
                    #         for ele in values:
                    #             remove_keywords_svm.append(ele)
                    if predicted_labels:
                        update_ocr_data.update({key: value for key, value in predicted_labels.items() if key not in update_ocr_data})
                    logging.info(f" ########### update_ocr_data after first svm with {predicted_labels} is {update_ocr_data} \n")
                    # #call highlights function on ocr_word and ocr_parsed  for keys_hi
                    # print(f" ######## gng to EXECUTE predict_with_svm for for date only")

                    # # Middle SVM for date only — pass entire blk_wrds_data or filtered if you want
                    # predicted_date_labels = predict_with_svm(blk_wrds_data, file_type, ocr_blocks, svm_ocr_ref, "1", tenant_id, seg)

                    # if predicted_date_labels:
                    #     # Update only 'date_stat' key here (adjust if date key is different)
                    #     update_ocr_data.update({k: v for k, v in predicted_date_labels.items() if k not in update_ocr_data})

                    # print(f" ########### update_ocr_data after DATE svm with {predicted_date_labels} is {update_ocr_data} \n")

                    remove_keywords_svm=list(predicted_labels.values())
                    logging.info(f" ######## gng to EXECUTE remove_from_ocr 2nd time")
                    ocr_blk_data=remove_from_ocr(remove_keywords_svm,cpy_ocr_data[0])
                    ocr_blocks=get_ocr_block([ocr_blk_data])
                    if ocr_blk_data:
                        ocr_blocks=get_ocr_block([ocr_blk_data])
                    else:

                        ocr_blocks=get_ocr_block(cpy_ocr_data)

                    blk_wrds_data=[]
                    for blk_data in ocr_blocks:
                        try:
                            blk_wrds_data.append(blk_data['text'])
                        except:
                            blk_wrds_data.append(blk_data['word'])
                    
                    logging.info(f" ######## gng to EXECUTE predict_with_svm 2nd time")
                    
                    predicted_labels=predict_with_svm(blk_wrds_data,file_type,ocr_blocks,svm_ocr_ref,"1",tenant_id)

                    logging.info(f" ########### update_ocr_data after second svm with {predicted_labels} is {update_ocr_data} \n")
                    

                    # table=file_type.lower()+'_table'
                    # table_data=predict_table(case_id,tenant_id,page_num)
                    if predicted_labels:
                        update_ocr_data.update({key: value for key, value in predicted_labels.items() if key not in update_ocr_data})
                    # for label,values in predicted_labels.items():
                    #     if len(values)==1:
                    #         if label not in update_ocr_data:
                    #             update_ocr_data[label]=values[0]
                    #     else:
                    #         pass

                    # label_finals=get_invoice_labels(file_type,new_dict,ocr_blocks)
                    

                    # logging.info(f" ########### update_ocr_data after second svm with {label_finals} is {update_ocr_data} \n")
                    # update_ocr_data.update(label_finals)
                    # update_ocr_data.update(predicted_labels)
                    # key_highlights,value_highlights=get_highlight(update_ocr_data,ocr_data_word,ocr_parsed,ocr_blocks)
                    
                    # update_ocr_data[key_highlights]=json.dumps(key_highlights)
                    # update_ocr_data[value_highlights]=json.dumps(value_highlights)
                    logging.info(f"going to create highlights")
                    highlights=get_highlight(update_ocr_data,page_num,ocr_data_word,{},ocr_blocks)
                    logging.info(f"highlights are for  value {highlights} \n")
                    logging.info(f"highlights are for  key {key_highlights} \n")

                    highlight=update_append_highlights(highlights,tenant_id,case_id)
                    key_highlights=update_append_highlights(key_highlights,tenant_id,case_id)
                    # update_ocr_data[table]=json.dumps(table_data)
                    update_ocr_data['highlight']=highlight
                    update_ocr_data['key_highlight']=key_highlights


                    logging.info(f" ########### final ocr data got is {update_ocr_data}")
                
                    logging.info(f" ###### gng to update values into the OCR table")

                    try:

                        extraction_db.update("ocr",update_ocr_data, where={'case_id': case_id})

                        

                    except:
                        logging.info(f" ############ some database error please look into the query")

                    # logging.info(f" ###### gng to update values into RESPONSE MODEL TABLE")
                    # insert_into_response_model(update_ocr_data,tenant_id,document_id,case_id,file_type,api='centroid_api')
                    variable+=1
                    ocr_data_wrd_frmt=[]
                    ele_ocr_data_wrd_frmt=[]
                except Exception as e:
                    logging.info(f" ########### exception occured while updating values into response data {e}")
    except Exception as e:
        logging.info(f" !@#$%^&*() exception occured ===> {e}")
        response = {"flag":False,'data':{ "message": "Error in Prediction","output_data":{}}}
        return response

    # response = {"flag": True,'data':{ "message": "Centroid Prediction Successfull","output_data":update_ocr_data}}
    return update_ocr_data


def key_value(data,page):
    key_value_dict = {}
    keys = set()
    fields = data.get("fields", {})
    if not fields:
        # logging.info(f"############# fields are not empty {fields}")
        return {}
    pages=[]
    for key, field in fields.items():
        page_no=field.get("page", "")
        pages.append(page_no)
    temp=min(pages)
    page=page+temp 
    # logging.info(f"############# page is {page}")
    
    for key, field in fields.items():
        page_no=field.get("page", "")
        field_name = field.get("field", "")
        # logging.info(f"############# page_no is {page_no}")
        if page == page_no:
            coordinates = field.get("coordinates", [])
            if field_name not in keys:
                keys.add(field_name)
            for coord in coordinates:
                dropdown_value = coord.get("dropdown_value", "")
                if not dropdown_value:
                    word = coord.get("word", "")
                    key_value_dict[word] = field_name
    return key_value_dict
    

#project sepcific lines or function strats





#project sepcific lines or function ends here 


def get_highlight(update_ocr_data,start_page,ocr_data_word,ocr_parsed={},ocr_block=[]):
    logging.info(f"data got is {update_ocr_data}")
    value_highlights={}
    # logging.info(f"value_highlights is {value_highlights}")
    for key_,values_ in update_ocr_data.items():
        for key,value in ocr_parsed.items():
            
            # logging.info(f" value is {value} and value_ is {values_}")
            temp={}
            if values_==value['text']:
               
                temp['word']=values_
                temp['top']=value['top']
                temp['left']=value['left']
                temp['bottom']=value['bottom']
                temp['right']=value['right']
                temp['page']=start_page
                temp['x']= temp['left']
                temp['y']=temp['top']

                max_height = temp['bottom'] - temp['top']
                total_width = temp['right'] - temp['left']
                temp['height']=max_height
                temp['width']=total_width

                value_highlights[key_]=temp
                break
        for value in ocr_data_word:
            temp={}
            # logging.info(f" value is {value} and value_ is {values_}")
            if values_==value['word']:
                
                temp['word']=values_
                temp['top']=value['top']
                temp['left']=value['left']
                temp['bottom']=value['bottom']
                temp['right']=value['right']
                temp['page']=start_page
                temp['x']= temp['left']
                temp['y']=temp['top']

                max_height = temp['bottom'] - temp['top']
                total_width = temp['right'] - temp['left']
                temp['height']=max_height
                temp['width']=total_width

                value_highlights[key_]=temp
                
                # logging.info(f"value_highlights is {value_highlights}")
                break
        for value in ocr_block:
            temp={}
            # logging.info(f" value is {value} and value_ is {values_}")
            if values_==value['word']:
                
                temp['word']=values_
                temp['top']=value['top']
                temp['left']=value['left']
                temp['bottom']=value['bottom']
                temp['right']=value['right']
                temp['page']=start_page
                temp['x']= temp['left']
                temp['y']=temp['top']

                max_height = temp['bottom'] - temp['top']
                total_width = temp['right'] - temp['left']
                temp['height']=max_height
                temp['width']=total_width
                value_highlights[key_]=temp
                # logging.info(f"value_highlights is {value_highlights}")
                break
    return value_highlights

def key_find(key,ocr) :
    return_dict={}
    similarities={}
    for word in ocr:
        word=word['word']
        seq = difflib.SequenceMatcher(None, key, word)
#             logging.info(seq)
        similarity = seq.ratio()  # Ratio of similarity between the two words
        similarities[word] = similarity
# Sort the similarities by descending order
    sorted_similarities = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    matched_key=sorted_similarities[0][0]
    logging.info(f" ##### the matched_key is {matched_key} for the trained key {key}")
    for word in ocr:
        word_=word['word']
        if word_==matched_key:
            return_dict.update(word)
            break
    return return_dict,matched_key



def key_find_(key,ocr) :
    return_dict={}
    similarities={}
    for key_,word in ocr.items():
        word=word['text']
        
        seq = difflib.SequenceMatcher(None, key, word)
#             logging.info(seq)
        similarity = seq.ratio()  # Ratio of similarity between the two words
        similarities[word] = similarity
# Sort the similarities by descending order
    sorted_similarities = sorted(similarities.items(), key=lambda x: x[1], reverse=True)
    matched_key=sorted_similarities[0][0]
    logging.info(f" ##### the matched_key is {matched_key}")
    for key,word_ in ocr.items():
        word=word_['text']
        if word==matched_key:
            return_dict=word_
            break
    return return_dict,matched_key



# def get_prob_ele_(elements__,ocr, x_corrd_thr=200.0, y_corrd_thr=40.0):
#     """
#     Get probable elements from an OCR output
#     :param elements: OCR output merged
#     :param x_corrd_thr: threshold for tilt in x axis
#     :param y_corrd_thr: threshold for tilt across y axis
#     :return: dictionary of elements and probable values
#     """

#     ele_dict = {'y_tilt':{},'x_tilt':{}}
#     y_tilt={}
#     x_tilt={}
#     special_chars=[":",",",".","!","@","#","$","%","^","&","*","(","()",")","_","-","=","`","~",";","<",">","/","?"]
#     logging.info(f"elements that are recived are {elements__}")
#     for e in elements__:
#         main_word=e['text']
#         main_word=remove_all_except_al_num_prob_ele(main_word)
#         if not main_word.isnumeric():  # Get only alphanumeric values as key
#             # probable_elements = []
# #             logging.info(f" ######## boundingBox isss {e['boundingBox']}")
# #             e_coord = [int(c) for c in e['boundingBox'].split(',')]
#             e_coord = [int(c) for c in e['boundingBox']]
#             # Get centroids
#             e_cetroid_x = (e_coord[0]+e_coord[2])/2
#             e_cetroid_y = (e_coord[1]+e_coord[3])/2
# #             logging.info(f" ########### mid point for {e_text} is x {e_cetroid_x} and y is {e_cetroid_y}")
# #             logging.info(f" #########  j is {j} {elements[j+1:]}")
#             for k, n_e in enumerate(ocr):  # OCR is from left, for each element get all the next ones
# #                 logging.info(f"for {n_e}")
# #                 n_e_coord = [int(c) for c in n_e['boundingBox'].split(',')]
#                 main_prob_word=n_e['text']
#                 if main_prob_word not in special_chars:
#                     n_e_coord = [int(c) for c in n_e['boundingBox']]
#                     # Get centroid for probable values
#                     ne_cetroid_x =( n_e_coord[0] + n_e_coord[2]) / 2
#                     ne_cetroid_y = (n_e_coord[1] + n_e_coord[3])/ 2
#     #                 logging.info(f"####### text for {e_text} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} ")
#                     # if centroid difference less than threshold select as probable value
#     #                 logging.info(f" ######## check ecords1  for {n_e['text']} {e_coord[1]} and {n_e_coord[1]}")
#     #                 logging.info(f" ########## {n_e['text']} for cetnroed is {abs(ne_cetroid_x - e_cetroid_x)}")
#                     if abs(ne_cetroid_y - e_cetroid_y) < y_corrd_thr: 
#     #                     logging.info(f"######## y  tilt data got is ")
#                         if e_coord[0] < n_e_coord[0]:
#                             dist=find_distance(e_cetroid_x,e_cetroid_y,ne_cetroid_x,ne_cetroid_y)
#         # choose the least distance among the distance and consider it as the final value for that keyword 
#                             # logging.info(f" for less Y_coord_thr text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y} and distance is {dist}")
#                             if e['text'] not in y_tilt:
#                                 y_tilt[e['text']]=[[n_e['text'],dist]]
#                             else:
#                                 y_tilt[e['text']].append([n_e['text'],dist])
#     #                         ele_dict[e['text']].update([dist])
                            
#     #                 elif abs(ne_cetroid_x - e_cetroid_x) < x_corrd_thr:
#                     elif abs(ne_cetroid_x - e_cetroid_x) < x_corrd_thr:
#                         prob_word=n_e['text']
#                         main_word_cords=[e_cetroid_x,e_cetroid_y]
#                         prob_word_cords=[ne_cetroid_x,ne_cetroid_y]
#                         if e_coord[1] < n_e_coord[1]:
#                             # logging.info(f" text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y}")
#     #                         logging.info(f"######## x  tilt data got is ")
#                             vertical_spacing=find_distance(e_cetroid_x,e_cetroid_y,ne_cetroid_x,ne_cetroid_y)
#     #     multiple lines : if M is given then take the M lines with least y distance and make it as value 
#     #  if not M , then take all the lines with the delta y2  > 1.5 times delta y1
#     #                         logging.info(f"  for less x_coord_thr text is {e['text']} for {n_e['text']} with x cetroid is {ne_cetroid_x} y cetroid is {ne_cetroid_y} and dist is {dist}")
#                             if main_word not in x_tilt:
#     #                             x_tilt[e['text']]=[[n_e['text'],ne_cetroid_y]]
#                                 x_tilt[main_word]=[{main_word:main_word_cords}]
#     #                               x_tilt[main_word].append({main_word:main_word_cords})
#                                 x_tilt[main_word].append({prob_word:prob_word_cords})
#                             else:
#                                 x_tilt[main_word][1].update({prob_word:prob_word_cords})
#     #                         ele_dict[e['text']].update([n_e['text'],ne_cetroid_y])
#     #                         ele_dict[e['text']].update([dist])
#                     else:
#     #                     logging.info(f" ######## text got in else is {e['text']} with {n_e['text']}")
#                         continue
#                 # logging.info(f"x_tilt,y_tilt are {x_tilt}{y_tilt}")
#                 else:
#                     pass
#     elements__=[]
#     return x_tilt,y_tilt 


def get_prob_ele_(elements__,ocr, x_corrd_thr, y_corrd_thr,x_tilt_find=False):
    """
    Get probable elements from an OCR output
    :param elements: OCR output merged
    :param x_corrd_thr: threshold for tilt in x axis
    :param y_corrd_thr: threshold for tilt across y axis
    :return: dictionary of elements and probable values
    """

    ele_dict = {'y_tilt':{},'x_tilt':{}}
    y_tilt={}
    x_tilt={}
    special_chars=[":",",",".","!","@","#","$","%","^","&","*","(","()",")","_","-","=","`","~",";","<",">","/","?"]
#     logging.info(f"elements are {elements__}")
    for j, e in enumerate(elements__):  # parse through all the values
        
        main_word=e['text']
        main_word=remove_all_except_al_num_prob_ele(main_word)
        if not main_word.isnumeric():
            e_coord = [int(c) for c in e['boundingBox']]
            e_cetroid_x = (e_coord[0]+e_coord[2])/2
            e_coord_left=(e_cetroid_x+e_coord[0])/2
            e_coord_right=(e_cetroid_x+e_coord[2])/2
            e_cetroid_y = (e_coord[1]+e_coord[3])/2
            
            for k, n_e in enumerate(ocr):
                main_prob_word=n_e['text']
                if main_prob_word not in special_chars and e['text']!=n_e['text']:
                    n_e_coord = [int(c) for c in n_e['boundingBox']]
                    ne_cetroid_x =( n_e_coord[0] + n_e_coord[2]) / 2
                    ne_cetroid_y = (n_e_coord[1] + n_e_coord[3])/ 2
                    
#                     logging.info(f"####### text y tilt for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} {(e_coord[1]-n_e_coord[1])}")
                    if abs(e_coord[1]-n_e_coord[1])<y_corrd_thr and (e_coord[2]<n_e_coord[0]):
                    #here the thrushold should be according to the document type app=15 and remian=7
#                         logging.info(f"####### text y tilt for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} and {(e_coord[1]-n_e_coord[1])} ")
                        dist=abs(e_coord[0]-n_e_coord[2])
                        if e['text'] not in y_tilt:
                            y_tilt[e['text']]=[[n_e['text'],dist]]
                        else:
                            y_tilt[e['text']].append([n_e['text'],dist])
                            
                    elif (e_coord[1]-n_e_coord[1])<5 and (( n_e_coord[0] < e_cetroid_x < n_e_coord[2]) or ( n_e_coord[0] < e_coord[0] < n_e_coord[2]) or ( n_e_coord[0] < e_coord[2] < n_e_coord[2]) or ( n_e_coord[0] < e_coord_right < n_e_coord[2]) or ( n_e_coord[0] < e_coord_left < n_e_coord[2])):
#                         logging.info(f"####### text x titl for {e} and n_e text is {n_e['text']} \n and ne_cetroid_x is {ne_cetroid_x} \n and ne_cetroid_y is {ne_cetroid_y} ")
                        vertical_spacing=abs(e_coord[1]-n_e_coord[3])
                        
                        if e['text'] not in x_tilt:
                            
                            x_tilt[e['text']]=[[n_e['text'],vertical_spacing]]
                        else:
                            x_tilt[e['text']].append([n_e['text'],vertical_spacing])
#                         logging.info(x_tilt)
                    else:
                        continue
                else:
                    pass
    elements__=[]
    return x_tilt,y_tilt




@app.route('/hybrid_data_api', methods=['POST', 'GET'])
def hybrid_data_api():
    data = request.json
    case_id=data.get('case_id','')
    tenant_id=data.get('tenant_id',None)
    db_config['tenant_id']=tenant_id
    templates_db=DB("template_db",**db_config)
    extraction_db=DB("extraction",**db_config)
    qry=f"SELECT * FROM `response_data_models` where `case_id`='{case_id}'"
    final_data={}
    response_df=templates_db.execute_(qry)
    file_type_df=response_df['file_type'].tolist()
    final_extracted_data={}
    for file_type in file_type_df:
        model_wise_flds=get_fields_model_wise(tenant_id,file_type)
        final_data=update_models_into_ocr(tenant_id,case_id,response_df,model_wise_flds)
        final_extracted_data.update(final_data)
    logging.info(f" ######## final data is {final_data}")
    try:
        logging.info(f" ######## Going to update data into OCR")
        extraction_db.update("ocr",final_extracted_data, where={'case_id': case_id})
        response={"flag":True,"data":{"message":"Successfully updated values"}}
    except Exception as e:
        logging.info(f"########## exception occured while updating data into OCR {e}")
        response={"flag":False,"data":{"message":"Failed to updated values"}}
    return jsonify(response)


# def find_values__old(new_text,chosen_label,file_type):
        
#         file_type = file_type.lower().replace(" ", "_")
#         logging.info(f"file type got is in svm is {file_type}")

#         model_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_fv_svm_classifier_model.joblib'
#         vectorizer_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_fv_count_vectorizer.joblib'
        
#         loaded_clf = joblib.load(model_filename)
#         loaded_vectorizer = joblib.load(vectorizer_filename)

#         new_text_vectorized = loaded_vectorizer.transform(new_text)
#         predicted_labels = loaded_clf.predict(new_text_vectorized)

# #         logging.info(predicted_labels)
#         relevant_words = []
#         logging.info(f"the label_probabilities for {new_text} are {predicted_labels} \n")
#         for word, label_probabilities in zip(new_text, predicted_labels):
            
#             if label_probabilities==chosen_label:
#                 relevant_words.append(word)
#         return relevant_words



# def find_values(word_list,desired_label,file_type):
#     try:
#     # Load the SVM model and CountVectorizer
#         file_type = file_type.lower().replace(" ", "_")
#         logging.info(f"file type got is in svm is (in find values){file_type}")

#         model_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_fv_svm_classifier_model.joblib'
#         vectorizer_filename = f'/var/www/prediction_api/app/data/model_files/{file_type}_fv_count_vectorizer.joblib'
#         logging.info(f"got model files from in find_values{model_filename},{vectorizer_filename}")
#         logging.info(f"words list got for find_values is {word_list}")
#         logging.info(f"desired label in find_values is {desired_label}")
#         loaded_model = joblib.load(model_filename)
#         loaded_vectorizer = joblib.load(vectorizer_filename)

#         # Preprocess the list of words using the loaded vectorizer
#         word_list_vectorized = loaded_vectorizer.transform(word_list)

#         # Predict labels for the words
#         predicted_labels = loaded_model.predict(word_list_vectorized)

#         # Get the decision function values for the desired label
#         decision_function_values = loaded_model.decision_function(word_list_vectorized)

#         model_classes = loaded_model.classes_

#         # Check if the desired label is in the model's classes
#         if desired_label not in model_classes:
#             logging.info(f"Desired label '{desired_label}' not found in the model's classes.")
#             return []

#         # Find the index of the desired label in the model's classes
#         desired_label_index = list(loaded_model.classes_).index(desired_label)
        

#         # Initialize a list to store words with the highest confidence
#         best_words = []
#         highest_confidence = 0.3
#         best_word = ''
        
#         # Iterate through the words and find those with the highest confidence
#         for i, word in enumerate(word_list):
#             confidence_score = decision_function_values[i][desired_label_index]
#             if abs(confidence_score) > highest_confidence:
#                     best_word = word
#                     highest_confidence = confidence_score
        
#         # Return the word with the highest confidence for the given label
#         if best_word:
#             logging.info(f"output got from value svm is {best_word}")
#             return [best_word,abs(confidence_score)]
#         else:
#             logging.info(f"output got in svm is none and confidence_score is {confidence_score}")
#             return []
#     except Exception as e:
#         logging.info(f"Exception occured in find_value {e}")
#         return []


def find_values__(word_list, desired_label,file_type,tenant_id):
    try:
        model_filename = f'/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_f_v_logistic_regression_model.joblib'
        vectorizer_filename = f'/var/www/extraction_api/app/extraction_folder/non_table_fields/non_table_f_v_count_vectorizer.joblib'
        loaded_model = joblib.load(model_filename)
        loaded_vectorizer = joblib.load(vectorizer_filename)
        logging.info(f"got model files from in find_values{model_filename},{vectorizer_filename}")
        logging.info(f"words list got for find_values is {word_list}")
        logging.info(f"desired label in find_values is {desired_label}")

        # Convert the list of words to lowercase for processing
        word_list_lower = [word.lower() for word in word_list]

        # Preprocess the list of words using the loaded vectorizer
        word_list_vectorized = loaded_vectorizer.transform(word_list_lower)

        # Get the probability estimates for the classes
        proba_estimates = loaded_model.predict_proba(word_list_vectorized)

        model_classes = loaded_model.classes_

        # Check if the desired label is in the model's classes
        if desired_label not in model_classes:
            logging.info(f"Desired label '{desired_label}' not found in the model's classes.")
            return None

        # Find the index of the desired label in the model's classes
        desired_label_index = list(loaded_model.classes_).index(desired_label)

        # Initialize variables to track the best word and highest confidence score
        best_word = None
        highest_confidence = -1.0  # Initialize with a very low value

        # Iterate through the words and find the one with the highest confidence
        for i, (word, word_lower) in enumerate(zip(word_list, word_list_lower)):
            confidence_score = proba_estimates[i][desired_label_index]
            logging.info(f"the word in find values word is {word} and confidence is {confidence_score}")
            if confidence_score > highest_confidence:
                best_word = word  # Return the original word with original case
                highest_confidence = confidence_score

        # Return the word with the highest confidence for the given label
        logging.info(f"output got from value (find_values) svm is {best_word}")
        return [best_word,confidence_score]

    except Exception as e:
        logging.info(f"Exception occurred in find_word_with_highest_confidence: {e}")
        return []



def predict_table(case_id,tenant_id,page_no):

    db_config['tenant_id'] = tenant_id
    extraction_db = DB('queues', **db_config)



    query = f"SELECT `xml_abbyy` FROM `ocr_info` WHERE case_id = '{case_id}'"
    #     words_ =extraction_db.execute_(query)
    xml_data = json.loads(extraction_db.execute_(query)['xml_abbyy'].to_list()[0])

    root = ET.fromstring(xml_data)
    word_list1=[]
    colspan_list={}
    # Parse the XML string
    page=0
    for child in root.findall('{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}page'):
        if page ==page_no:
            for book in child.findall('{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}block[@blockType="Table"]'):
                word_list=[]
                for row in book.findall('{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}row'):
                    word_list_=[]
                    for cell in row.findall('{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}cell'):
                        words=[]
                        for text in cell.findall("{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}text"):
                            word=''
                            for par in text.findall("{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}par"):
                                    for line in par.findall("{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}line"):
                                        for form in line.findall("{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}formatting"):
                                            for char in form.findall("{http://www.abbyy.com/FineReader_xml/FineReader10-schema-v1.xml}charParams"):
                                                word+=char.text
    #                         if word:        
                            word_list_.append(word)

                        if 'colSpan' in cell.attrib:
                            colspan_list[word]=cell.attrib['colSpan']
                    word_list.append(word_list_)
                word_list1.append(word_list)
        page+=1
    # logging.info(word_list1,colspan_list) 

    result = []
    # result.append(word_list1[0][0])
    for data in word_list1:
        temp=[]
        header = data[0]
        temp.append(header)
        for row in data[1:]:
            combined_row = []
            for field in header:
                if field in colspan_list:
                    num_elements = int(colspan_list[field])
                    combined_value = ' '.join(row[:num_elements])
                    row = row[num_elements:]
                    combined_row.append(combined_value)
                else:
                    if row:
                        combined_row.append(row.pop(0))
                    else:
                        pass
            temp.append(combined_row)
        result.append(temp)
    return result  







# <------------------------------ Below Code is for the prediction of the fields --------------------------------------->

## single_temp_files is responsible to consider only one file type from the different types detected 
def single_temp_files(input_data):
    # Create a set to keep track of seen file types
    seen_file_types = set()

    # Initialize a list to store the filtered results
    filtered_data = []

    # Iterate through the input_data and filter out entries with the same "file_type"
    for entry in input_data:
        template=entry["template_name"]
        file_type = entry["file_type"].split('_')[0]
        # if file_type not in seen_file_types and len(template)==1:
        if file_type not in seen_file_types:
            seen_file_types.add(file_type)
            filtered_data.append(entry)

    # Logging.info the filtered data
    return filtered_data

def get_ui_train_data(template_name):
    templates_db = DB('template_db', **db_config)
    trained_info_query = f"SELECT * FROM `trained_info` WHERE `template_name` = '{template_name}'"
    template_info_df = templates_db.execute_(trained_info_query)
    if template_info_df.empty:
        logging.info(f" ####### ui train data not exist, find values through prediction")
        ui_train_data={}
    else:
        # * Get fields to extract (fte) from the trained info
        ui_train_data = json.loads(template_info_df.field_data.values[0])

    return ui_train_data


## new api integration , to call the cetroid api for detected and undetected fields
@app.route("/field_prediction_centroid", methods=['POST', 'GET'])
def field_prediction_centroid():
    data=request.json
    logging.info(f"Data recieved in field_prediction {data}")
    if data is None:
        return {'flag': False}
    try:
        memory_before = measure_memory_usage()
        start_time = tt()
    except:
        logging.warning("Failed to start ram and time calc")
        pass
    
    ## reading data from the paylod 
    tenant_id = data['tenant_id']
    case_id = data.get('case_id', None)
    user = data.get('user', None)
    session_id = data.get('session_id', None)
    documnet_splitter=data.get('document_splitter',False)
    updated_files=data.get('updated_files',False)
    ocr_data = data.get('ocr_data', [])
    template_name = data.get('template_name', None)
    document_id_df = data.get('document_id_df', [])
    api=data.get('api',False)

    ## mentioning db connections

    db_config['tenant_id'] = tenant_id
    queues_db = DB('queues', **db_config)
    extraction_db = DB('extraction', **db_config)
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
        service_name='extraction_api',
        span_name='extraction',
        transport_handler=http_transport,
        zipkin_attrs=attr,
        port=5010,
        sample_rate=0.5, ):
    
        try:
            if len(document_id_df) ==0:
                try:
                    query = f"SELECT `document_id` from  `process_queue` where `case_id` = %s and state IS NULL "
                    document_id_df = queues_db.execute_(query, params=[case_id])['document_id'].tolist()
                except:
                    return{'flag':False,'message':f'{case_id} is missing in the table'}
            else:
                pass
                
                
            logging.info(f'## TE info document_ids that we recived : {document_id_df}')
            logging.info(f'## TE info Entering for loop to extract for each documnet_id')

            for document_id in document_id_df:
                # logging.info(f'## TE info Extraction for documnet_id : {document_id}')

                # logging.info(f'## TE info func_name : get_file_data \n input (case_id,document_id, api, queues_db,documnet_splitter) : {case_id,document_id, api, queues_db,documnet_splitter}')
                file_data = get_file_data(case_id,document_id, api, queues_db,documnet_splitter)
                # logging.info(f"## TE info func_name : get_file_data \n output (file_data) : {file_data['template_name']}, {file_data['single_doc_identifiers']}")
                if file_data['flag']:
                    # if not api:
                    template_name = file_data['template_name']
                    single_template_data=file_data['single_doc_identifiers']    
                    ocr_data = file_data['ocr_data']
                else:
                    template_name = None
                    single_template_data=[]  
                    ocr_data = []
                    return file_data
            
                if documnet_splitter or documnet_splitter == "true":
                    logging.info(f'## TE info entering if condition with documnet_splitter : {documnet_splitter}')
                    for values in single_template_data:
                        single_template_name=values['template_name']
                        if len(single_template_name)>=1:
                            single_template=single_template_name[0]
                            ui_train_info=get_ui_train_data(single_template)
                            # template_name=i
                        else:
                            ui_train_info={}
                            single_template=''
                        start_page=values['start_page']
                        end_page=values['end_page']
                        file_type=values['file_type']
                        temp_ocr_data=[]
                        value_predict_params={
                            "case_id":case_id,
                            "document_id":document_id,
                            "field_prediction_centroid_flag":True,
                            "start_page":start_page,
                            "end_page":end_page,
                            "ui_train_info":ui_train_info,
                            "template_name":single_template,
                            "tenant_id":tenant_id,
                            "file_type":file_type

                        }
                        response=cetroid_api(value_predict_params)
                        # host = 'predictionapi'
                        # port = 80
                        # route = 'cetroid_api'
                        # logging.debug(f'Hitting URL: http://{host}:{port}/{route} for template {single_template}')
                        # # logging.debug(f'Sending Data: {value_predict_params}')
                        # headers = {'Content-type': 'application/json; charset=utf-8',
                        #         'Accept': 'text/json'}
                        # response = requests.post(
                        #     f'http://{host}:{port}/{route}', json=value_predict_params, headers=headers)
                        
                        # response=response.json()

                        logging.info(f" ####### Response Received from cetroid_api is {response}")

                            
                        # logging.info(f'## TE info func_name : get_field_data \n input (api, data, template_name,single_template,documnet_splitter) : {api, data, template_name,single_template,documnet_splitter}')
            response={"data":{"msg":"prediction completed"},"flag":True}
        except Exception as e:
            logging.warning(f'## TE Received unknown data. {data} \n  {e}')
            return {'flag': False, 'data':{'message': 'Incorrect Data in request'}}
    
    return jsonify(response)




def  get_file_data(case_id,document_id,api, queues_db,documnet_splitter):
    single_doc_identifiers=[]
    template_name=None
    process_file = queues_db.get_all('process_queue', condition={'document_id': document_id})
    if not process_file.empty:
        if documnet_splitter == False:
            template_name = process_file.template_name.values[0]
            if template_name is None or not template_name:
                message = 'Template name is none/empty string'
                # logging.debug(f'## TE {message}')
                if not api:
                    return {'flag': False, 'message': message}
        else:
            single_doc_identifiers=process_file.single_doc_identifiers.values[0]
            single_template_data=single_temp_files(json.loads(single_doc_identifiers))
            # logging.info(f" ### TE func get_file_data output : single_doc_identifiers {single_doc_identifiers}")

        # no need to read ocr data in this function
        ocr_info = queues_db.get_all('ocr_info', condition={'document_id': document_id})
        
        try:
            ocr_data = json.loads(json.loads(list(ocr_info.ocr_data)[0]))
        except:
            ocr_data = json.loads(list(ocr_info.ocr_data)[0])

        if ocr_data is None or not ocr_data:
            message = 'OCR data is none/empty string'
            # logging.debug(f'## TE {message}')
            return {'flag': False, 'message': message}
    else:
        message = f'document_id - {document_id} does not exist'
        return {'flag': False, 'message': message}

    return {'flag': True, 'ocr_data': ocr_data, 'template_name': template_name ,'single_doc_identifiers':single_template_data}