import os 
import base64
import requests
from flask import Flask
from flask_cors import CORS
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfFileReader, PdfFileWriter
import uuid
import os
import xml.etree.ElementTree as ET
try:
    from app import app
except Exception:
    app = Flask(__name__)
    CORS(app)

try:
    import app.xml_parser_sdk as xml_parser_sdk
except Exception:
    import xml_parser_sdk as xml_parser_sdk

from ace_logger import Logging

logging = Logging()



def ocr_sdk(file_path):
    """
    Author : Akshat Goyal
    """
    try:

        files_data = {'file': open(file_path, 'rb')}
        logging.info(files_data)
        url = os.environ['ABBYY_URL']
        logging.info("===========")
        logging.debug(url)
        response = requests.post(url, files=files_data, timeout=1000)
        xml_string=''
        pagesCount=-1
        logging.debug(type(response))
        sdk_output = response.json()
        logging.debug(type(sdk_output))
        logging.debug(sdk_output.keys())
        if 'blob' in sdk_output:
            pdf = base64.b64decode(sdk_output['blob'])
            with open(file_path, 'wb') as f:
                f.write(pdf)
        else:
            logging.info('no blob in sdk_output')
        try:
            if 'pagesCount' in sdk_output and sdk_output['pagesCount'] is not None:
                pagesCount = sdk_output['pagesCount']
                logging.info(f'pagesCount is {pagesCount}')
            else:
                logging.info('no pagesCount in sdk_output')
                pagesCount = -1
        except:
            logging.info('Got exception for pagesCount')
            pagesCount = -1
        
        try:
            if 'remaining_pages' in sdk_output:
                remaining_pages = sdk_output['remaining_pages']
                logging.info(f'remaining_pages is {remaining_pages}')
            else:
                logging.info('no remaining pages in sdk_output')
                remaining_pages = -1
        except:
            logging.info('Got exception for remaining_pages')
            remaining_pages = -1
        # shutil.copyfile(file_path, file_path+'_1')
        # os.remove(file_path)
        # os.rename(file_path+'_1', file_path)

        # shutil.copyfile(file_path, 'training_ui/' + file_parent_input + file_name)
        if len(sdk_output['xml_string'])>0:
            xml_string = sdk_output['xml_string'].replace('\r', '').replace('\n', '').replace('\ufeff', '')
        else:
            logging.info("CHECK ABBYY COUNT")
    except:
        xml_string = None
        message = f'Failed to OCR {file_path} using SDK'
        logging.exception(message)

    return xml_string,pagesCount,remaining_pages


#from concurrent.futures import ThreadPoolExecutor, as_completed
#from PyPDF2 import PdfReader, PdfWriter
#import uuid
#import os
#import xml.etree.ElementTree as ET


def merge_xml_outputs(xml_list):
    """
    Merges multiple ABBYY XML outputs into one XML.
    Assumes each XML has a single root (e.g., <document>).
    """
    if not xml_list:
        return None

    root = ET.fromstring(xml_list[0])

    for xml in xml_list[1:]:
        temp_root = ET.fromstring(xml)
        for child in temp_root:
            root.append(child)

    return ET.tostring(root, encoding="utf-8").decode("utf-8")

def ocr_sdk_parallel(file_path, chunk_size=20, max_workers=3):
    with open(file_path, "rb") as f:
        reader = PdfFileReader(f,strict=False)
        total_pages = reader.getNumPages()

    # Small file → normal OCR
    if total_pages <= chunk_size:
        return ocr_sdk(file_path)

    temp_files = []
    tasks = []
    results = []

    def process_chunk(start, end):
        with open(file_path, "rb") as fp:
            reader = PdfFileReader(fp,strict=False)
            writer = PdfFileWriter()
            for i in range(start, end):
                writer.addPage(reader.pages[i])

            #temp_path = f"/tmp/ocr_chunk_{uuid.uuid4().hex}.pdf"
            base_name = os.path.splitext(os.path.basename(file_path))[0]
            temp_path = f"/tmp/{base_name}_part_{start//chunk_size + 1}.pdf"
            with open(temp_path, "wb") as f:
                writer.write(f)

        xml, pages, remaining = ocr_sdk(temp_path)
        return temp_path, xml, pages, remaining

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:

            for start in range(0, total_pages, chunk_size):
                end = min(start + chunk_size, total_pages)
                tasks.append(executor.submit(process_chunk, start, end))

            all_xml = []
            total_pages_count = 0
            final_remaining = -1

            for future in as_completed(tasks):
                temp_path, xml, pages, remaining = future.result()

                if xml:
                    all_xml.append(xml)

                if pages != -1:
                    total_pages_count += pages

                final_remaining = remaining
                temp_files.append(temp_path)

        final_xml = merge_xml_outputs(all_xml)
        return final_xml, total_pages_count, final_remaining

    finally:
        for f in temp_files:
            try:
                os.remove(f)
            except:
                pass





def ocr_selection(file_path):
    ocr_word = []
    ocr_sen={}
    rotation=[]
    xml_string=''
    dpi_page = []
    try:
        xml_string,pagesCount,remaining_pages = ocr_sdk_parallel(file_path)
    except Exception as e:
        logging.exception(f'Error runnung OCR. Check trace.')
        return ocr_word,ocr_sen,dpi_page,rotation,xml_string

    try:
        ocr_word,ocr_sen, dpi_page,rotation= xml_parser_sdk.convert_to_json(xml_string)
        
    except Exception as e:
        logging.exception(f'Error parsing XML. Check trace.')
    return ocr_word,ocr_sen,dpi_page,rotation,xml_string,pagesCount,remaining_pages



def get_ocr(file_path):
    """
    Author : Aishwarya Jairam
    """
    logging.info(f'Processing file {file_path}')
    xml_string=''

    ocr_word,ocr_sen, dpi_page,rotation,xml_string,pagesCount,remaining_pages = ocr_selection(file_path)

    return ocr_word, ocr_sen, dpi_page,rotation,xml_string,pagesCount,remaining_pages
