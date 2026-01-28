try:
    import Lib
except:
    from . import Lib

import ast
from types import coroutine
import pandas as pd
import numpy as np
from datetime import date,timedelta,datetime
import logging
import json
import ntpath
import re
import math
import datetime
import os
import operator
import calendar
import copy
from dateutil import parser
from dateutil.relativedelta import relativedelta
import re
from difflib import SequenceMatcher
from dateutil.parser import parse
from dateutil.relativedelta import *
from babel.numbers import format_decimal
# from word2number import w2n 
from app.db_utils import DB
from datetime import date
import dateparser

import pytz
tmzone = 'Asia/Kolkata'

try:
    from ace_logger import Logging
    logging = Logging()
except:
    import logging 
    logger=logging.getLogger() 
    logger.setLevel(logging.DEBUG) 
 
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}

__methods__ = [] # self is a BusinessRules Object
register_method = Lib.register_method(__methods__)

@register_method
def evaluate_static(self, function, parameters):
    if function == 'Assign':
        return self.doAssign(parameters)
    if function == 'AssignQ':
        return self.doAssignQ(parameters)
    if function == 'AssignTable':
        return self.doAssignTable(parameters)
    if function == 'DateCompare':
        return self.doDateCompare(parameters)
    if function == 'CompareKeyValue':
        return self.doCompareKeyValue(parameters)
    if function == 'GetLength':
        return self.doGetLength(parameters)
    if function == 'GetRange':
        return self.doGetRange(parameters)
    if function == 'AmountCompare':
        return self.doAmountCompare(parameters)
    if function == 'Select':
        return self.doSelect(parameters)
    if function == 'SelectAll':
        return self.doSelectAll(parameters)
    if function == 'Transform':
        return self.doTransform(parameters)
    if function == 'Count':
        return self.doCount(parameters)
    if function == 'Contains':
        return self.doContains(parameters)
    if function == 'ProduceData':
        return self.doProduceData(parameters)
    if function == 'due_date_generate':
        return self.Dodue_date_generate(parameters)
    if function == 'Bank_due_date_generate':
        return self.BankDodue_date_generate(parameters)
    if function == 'Get_holidays_fromdatabase':
        return self.get_holidays_fromdatabase(parameters)
    if function == 'sat_and_sun_holidays':
        return self.doSat_and_sun_holidays(parameters)
    if function == 'Contains_UCIC':
        return self.doContains_UCIC(parameters)
    if function == 'DateParsing':
        return self.doDateParsing(parameters)
    if function == 'DateParsingMarch':
        return self.doDateParsingMarch(parameters)
    if function == 'Split':
        return self.doSplit(parameters)
    if function == 'Return':
        return self.doReturn(parameters)
    if function == 'RegexColumns':
        return self.doRegexColumns(parameters)
    if function == 'AlphaNumCheck':
        return self.doAlphaNumCheck(parameters)
    if function == 'DateTransform':
        return self.doDateTransform(parameters)
    if function == 'PartialMatch':
        return self.doPartialMatch(parameters)
    if function == 'FileManagerUpdate':
        return self.doFileManagerUpdate(parameters)
    if function == 'Round':
        return self.doRound(parameters)
    if function == 'Contains_string':
        return self.doContains_string(parameters)
    if function == 'Alnum_num_alpha':
        return self.doAlnum_num_alpha(parameters)
    if function == 'Regex':
        return self.doRegex(parameters)
    if function == 'Round':
        return self.doRound(parameters)
    if function == 'AmountCompare':
        return self.AmountCompare(parameters)
    if function == 'ProduceData':
        return self.doProduceData(parameters)
    if function == 'AppendDB':
        return self.doAppendDB(parameters)
    if function == 'PartialCompare':
        return self.doPartialCompare(parameters)
    if function == 'GetDateTime':
        return self.doGetDateTime(parameters)
    if function == 'Sum':
        return self.doSum(parameters)
    if function == 'DateIncrement':
        return self.doDateIncrement(parameters)
    if function == 'NtpathBase':
        return self.doNtpathBase(parameters)
    if function == 'CheckDate':
        return self.doCheckDate(parameters)
    if function == 'DateCheck':
        return self.doDateCheck(parameters)
    if function == 'DateParser':
        return self.doDateParser(parameters)
    if function == 'AmountSyntax':
        return self.doAmountSyntax(parameters)
    if function == 'ContainsMaster':
        return self.doContainsMaster(parameters)
    if function == 'TableErrorMessages':
        return self.doTableErrorMessages(parameters)
    if function == 'DateConvertion':
        return self.doDateConvertion(parameters) 
    if function == 'UserMatch':
        return self.doUserMatch(parameters)
    if function == 'Statusupdate':
        return self.doStatusupdate(parameters)
    if function == 'NumericExtract':
        return self.doNumericExtract(parameters)
    if function == 'Transform_':
        return self.doTransform_(parameters)
    if function == 'Contains_string_':
        return self.doContains_string_(parameters)
    if function == 'PartialComparison':
        return self.doPartialComparison(parameters)
    if function == 'doContain_string':
        return self.doContain_string(parameters)
    if function == 'doQueue_Percentage':
        return self.doQueue_Percentage(parameters)
    if function == 'donumword_to_number_comp':
        return self.donumword_to_number_comp(parameters)
    if function == 'doNotContain_string':
        return self.doNotContain_string(parameters)
    if function == 'doTypeConversion':
        return self.doTypeConversion(parameters)
    if function == 'ToLower':
        return self.ToLower(parameters)
    if function == 'doDates_diff':
        return self.doDates_diff(parameters)
    if function == 'is_numeric':
        return self.is_numeric(parameters)
    if function == 'duplicateCheck':
        return self.duplicateCheck(parameters)
    if function == 'QueryAndCheck':
        return self.QueryAndCheck(parameters)
    if function == 'PartiallyCompare':
        return self.PartiallyCompare(parameters)
    if function == 'doExtrayear':
        return self.Extrayear(parameters)
    if function == 'get_last_n_chars':
        return self.get_last_n_chars(parameters)
    if function == 'get_next_month_first_date':
        return self.get_next_month_first_date(parameters)
    if function == 'rb_stock_summaryTable1':
        return self.rb_stock_summaryTable1(parameters)
    if function == 'cons_stockTable':
        return self.cons_stockTable(parameters)
    if function == 'cons_crediTable':
        return self.cons_crediTable(parameters)
    if function == 'month_and_year':
        return self.month_and_year(parameters)
    if function == 'doValidation_params':
        return self.doValidation_params(parameters)
    if function == 'get_month_last_date':
        return self.get_month_last_date(parameters)
    if function == 'get_month_agri_fifteenth':
        return self.get_month_agri_fifteenth(parameters) 
    if function == 'get_data_dict':
        return self.get_data_dict(parameters)
    if function == 'merge_dict':
        return self.merge_dict(parameters)
    if function == 'date_cus':
        return self.date_cus(parameters)
    if function == 'date_out':
        return self.date_out(parameters)
    if function == 'dosummary':
        return self.dosummary(parameters)
    if function == 'dosummary_1':
        return self.dosummary_1(parameters)
    if function == 'assign_value_json':
        return self.dosummary_1(parameters)
    if function == 'checking_files':
        return self.checking_files(parameters)
    if function == 'margin_data':
        return self.margin_data(parameters)
    if function == 'add_key_value':
        return self.margin_data(parameters)
    if function == 'do_allow_4_char':
        return self.do_allow_4_char(parameters)
    if function == 'update_columns_data':
        return self.do_allow_4_char(parameters)
    if function == 'biz_rules_change_data':
        return self.biz_rules_change_data(parameters)
    if function == 'noofIngestion':
        return self.noofIngestion(parameters)
    if function == 'extractComments':
        return self.extractComments(parameters)


    


"""@register_method
def date_out(self, parameters):
    d_month_full = ['january', 'february', 'march', 'april', 'may', 'june',
                    'july', 'august', 'september', 'october', 'november', 'december']
    d_month_abbr = ['jan', 'feb', 'mar', 'apr', 'may', 'jun',
                    'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    d_month = d_month_full + d_month_abbr
    ordinal_suffixes = ['st', 'nd', 'rd', 'th']

    value = parameters['date_format']
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
                    datetime.date(year, month, day)
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

    return value  # Final fallback"""
from dateparser.search import search_dates




"""
def date_out(self, parameters):
    input_date = parameters['date_format']
    try:
        # Extract all dates from the input string
        found_dates = search_dates(input_date)

        if found_dates:
            # Pick the last parsed date
            last_parsed_date = found_dates[-1][1]
            return last_parsed_date.strftime('%d/%m/%Y')
        else:
            return "input date"
    except Exception as e:
        return "input date" 
"""

"""
####backup
@register_method
def date_out(self, parameters):
    input_date = parameters.get('date_format', '')
    try:
        # Step 1: Try to extract all dates using search_dates
        found_dates = search_dates(input_date, settings={"PREFER_DAY_OF_MONTH": "first"})
        
        if found_dates:
            # Pick the last parsed date
            last_parsed_date = found_dates[-1][1]
        else:
            # Fallback to parsing the whole string if search_dates fails
            last_parsed_date = dateparser.parse(input_date)

        if last_parsed_date:
            # Step 2: Get last day of the month
            year = last_parsed_date.year
            month = last_parsed_date.month
            last_day = calendar.monthrange(year, month)[1]

            # Step 3: Replace the day with last day of the month
            final_date = last_parsed_date.replace(day=last_day)

            return final_date.strftime('%d/%m/%Y')
        else:
            return input_date
    except Exception as e:
        return input_date
"""
@register_method
def date_out(self, parameters):
    input_date = parameters.get('date_format', '')
    try:
        # Normalize input: replace all special characters with space
        normalized_input = re.sub(r'[^A-Za-z0-9]', ' ', input_date)

        # Try dateparser's search_dates first (dayfirst=True)
        found_dates = search_dates(
            normalized_input, 
            settings={
                "PREFER_DAY_OF_MONTH": "first",
                "DATE_ORDER": "DMY"  # Ensures day is interpreted first
            }
        )
        
        last_parsed_date = None
        
        if found_dates:
            print(f" Dates found via search_dates: {found_dates}")
            last_parsed_date = found_dates[-1][1]  # Use the last matched date
        else:
            print(f" search_dates failed. Trying regex fallback...")
        
            # Regex to match full or abbreviated month names with optional trailing dot and a 4-digit year
            month_regex = r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|' \
                        r'May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|' \
                        r'Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\.?\s*\d{4}\b'
        
            # Case-insensitive match
            matches = re.findall(month_regex, normalized_input, flags=re.IGNORECASE)
            print(f"Regex matches: {matches}")
        
            for match in matches:
                cleaned = match.replace('.', '').strip()
                parsed = dateparser.parse(
                    cleaned, 
                    settings={
                        "PREFER_DAY_OF_MONTH": "last",
                        "DATE_ORDER": "DMY"
                    }
                )
                if parsed:
                    last_parsed_date = parsed  # Keep updating to get the last one
        
        # If a date is parsed, calculate last day of that month
        if last_parsed_date:
            year = last_parsed_date.year
            month = last_parsed_date.month
            last_day = calendar.monthrange(year, month)[1]
            final_date = last_parsed_date.replace(day=last_day)
            formatted = final_date.strftime('%d/%m/%Y')
            print(f"[SUCCESS] Final date (end of month): {formatted}")
            return formatted
        else:
            return input_date
    except Exception as e:
        return input_date
  







 



def compare_strings(string1, string2):
    seq_matcher = SequenceMatcher(None, string1.lower(), string2.lower())
    similarity_ratio = seq_matcher.ratio() * 100
    return similarity_ratio

@register_method
def date_cus(self, parameters):
    logging.info(f"parameters got are {parameters}")
    name_ = parameters['name__']
    logging.info(f"name_ is  {name_}")
    seg_ = parameters['seg']
    logging.info(f"seg_ is  {seg_}")
    master_tables={"Consumer":"consumer_stock_statement","Agri":"agri","RBG":"rbg_stock_statement","WBG":"wbg_consolidated_master"}
    master_customer_column={"Consumer":"Customer Name","Agri":"Client Name","RBG":"Client Name","WBG":"CRN_NAME"}
    try:
        table=master_tables[seg_]
        column=master_customer_column[seg_]
        ocr_db=DB("extraction",**db_config)
        query=f"select * from {table}"
        customer_list = ocr_db.execute_(query)[column].to_list()
        similarity_ratio=0
        out_cus=''
        for name in customer_list:
            string1=re.sub(r'[^a-zA-Z0-9]', '', name).lower()
            string2=re.sub(r'[^a-zA-Z0-9]', '', name_).lower()
            out=compare_strings(string1, string2)
            if out>similarity_ratio:
                out_cus=name
                similarity_ratio=out

        if similarity_ratio>0.98:
            return out_cus
        else:
            return name_
    except:
        return name_


    
    

@register_method
def doGetLength(self, parameters):
    """Returns the lenght of the parameter value.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'source':'input', 'value':5}
                      
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    try:
        value = len(value)
        logging.info(value)
    except Exception as e:
        logging.error(e)
        logging.error(f"giving the defalut lenght 0")
        value = 0
    return str(value)
    
@register_method
def doUserMatch(self, parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"},
        'match_ratio' : "85"
    }

    """
    logging.info(f"parameters got are {parameters}")
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    match_ratio = parameters['match_ratio']
    data = self.data_source[words_table]
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
            if ratio > int(match_ratio) and ratio > max_ratio:
                max_ratio = ratio
                match_got = word
                logging.info(match_got)
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)

### TO DO
@register_method
def doProduceData(self, parameters):
    """Updates the data that needs to be sent to next topic via kafka.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'key':{'source':'input', 'value':5},
                        'value': {'source':'input', 'value':5}
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        kafka_key = self.get_param_value(parameters['key'])
        kafka_value = self.get_param_value(parameters['value'])
        # update the self.kafka data
        self.kafka_data[kafka_key] = kafka_value
    except Exception as e:
        logging.error(e)
        logging.error(f"Unable to send the data that needs to be sent to next topic via kafka.Check rule")
    return True

@register_method
def doGetRange(self, parameters):
    """Returns the parameter value within the specific range.
    Args:
        parameters (dict): The source parameter and the range we have to take into. 
    eg:
       'parameters': {source':'input', 'value':5,
                        'start_index': 0, 'end_index': 4
                    }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Range is the python range kind of (exclusive of the end_index)
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    start_index = parameters['start_index']
    end_index = parameters['end_index']
    try:
        logging.info(f"Value got in given index is : {str(value)[int(start_index): int(end_index)]}")
        return (str(value)[int(start_index): int(end_index)])
    except Exception as e:
        logging.error(f"some error in the range function")
        logging.error(e)
    return ""


@register_method
def doSum(self, parameters):
    """retuns 
    'parameters': {source':'input_config', 'table':'ocr', 'column': 'End_date'}
        
    """
    logging.info(f"parameters got to doSum function are : {parameters}")
    input_series = self.get_param_value(parameters)
    logging.info(f"Got series is : {input_series}")
    if input_series is not None:
        try:
            total = input_series.sum(skipna = True)
            logging.info(f"Got total is : {total}")
            return total
        except Exception as e:
            logging.error("Addition of given series got failed")
            logging.error(e)
            return ""
    else:
        return ""

### TO DO
@register_method
def doSelect(self, parameters):
    """Returns the vlookup value from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    
    try:
        master_data = self.master_data_frames[from_table]
        
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data)
    

    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = lookup['compare_with']
        logging.info(f"value to compare is : {compare_value}")
        master_df = master_df[master_df[lookup_column].astype(str).str.lower() == str(compare_value).lower()]
    master_df = master_df.reset_index(drop=True)

    
    if not master_df.empty:
        try:
            return master_df[column_name_to_select][0] 
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return ""
    else:
        return ""



@register_method
def doSelectAll(self, parameters):
    """Returns all the vlookup values from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions.
    eg:
        'parameters': {
            'from_table': 'ocr',
            'select_column': 'highlight',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    column_name_to_select = parameters['select_column']
    lookup_filters = parameters['lookup_filters']

    
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data)

    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = self.get_param_value(lookup['compare_with'])
        master_df = master_df[master_df[lookup_column].astype(str) == str(compare_value)]
    master_df = master_df.reset_index(drop=True)

    
    if not master_df.empty:
        try:
            return master_df[column_name_to_select] 
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return ""
    else:
        return ""

@register_method
def doAmountTriming(self, parameters):
    """Returns the trimmed amount value data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':{
            "value":"500INR"
        }
    """
    value = parameters["value"]

    value = str(value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
    if value != "":
        value = float(value)

    return value



## TO DO
@register_method
def doTransform(self, parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    equation = ''
    logging.info(f"parameters got are {parameters}")
    for element,number_operator in parameters.items() :
        if element == 'param' :
            value = f'{number_operator}'
            value = str(value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        elif element == 'operator' :
            value = f' {number_operator} '
        equation = equation + value
    logging.info(f"equation is : {equation}")    
    logging.info('Equation is:',equation)
    try:
        equation = (eval(equation))
    except Exception as e:
        equation = 0
        logging.info(f'some value is empty in equation,so final value is 0')
    return equation

## TO DO
@register_method
def doContains(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
    
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':"5"
                        }
            
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = parameters["value"]
    logging.info(f"Value got is : {value}")
    column_values = self.data_source[table_name]
    logging.info(type(column_values),column_values)
    if value in self.data_source[table_name][column_name]:
        return True
    else :
        return False

@register_method
def doContainsMaster(self, parameters):
    """ Returns true value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            
    """
    logging.info(f"parameters got are {parameters}")
    table_name = parameters['table_name']
    column_name = parameters['column_name']
    value = parameters['value']
    logging.info(value)
    df = pd.DataFrame(self.data_source[table_name])
    logging.info(list(df[column_name]))
    if str(value) in list(df[column_name].astype(str)):
        return True
    else :
        return False

@register_method
def doCount(self, parameters):
    """Returns the count of records from the tables.
    Args:
        parameters (dict): The table from which we have to select and the where conditions. 
    eg:
        'parameters': {
            'from_table': 'ocr',
            'lookup_filters':[
                {
                    'column_name': 'Vendor GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
                {
                    'column_name': 'DRL GSTIN',
                    'compare_with':  {'source':'input', 'value':5}
                },
            ]
        }
    Note:
        1) Recursive evaluations of rules can be made for the parameter value.
        2) Its like vlook up in the dataframe and the from_table must have the primary key...case_id.
    """
    logging.info(f"parameters got are {parameters}")
    from_table = parameters['from_table']
    lookup_filters = parameters['lookup_filters']

    # convert the from_table dictionary into pandas dataframe
    try:
        master_data = self.data_source[from_table]
    except Exception as e:
        logging.error(f"data source does not have the table {from_table}")
        logging.error(e)
        master_data = {}

    master_df = pd.DataFrame(master_data) 

    # build the query
    query = ""
    for lookup in lookup_filters:
        lookup_column = lookup['column_name']
        compare_value = lookup['compare_with']
        query += f"{lookup_column} == {compare_value} & "
    query = query.strip(' & ') # the final strip for the extra &
    result_df = master_df.query(query)

    # get the wanted column from the dataframe
    if not result_df.empty:
        try:
            return len(result_df) # just return the first value of the matches
        except Exception as e:
            logging.error(f"error in selecting the required data from the result")
            logging.error(e)
            return 0
    else:
        return 0

@register_method
def doProduceData(self, parameters):
    """Updates the data that needs to be sent to next topic via kafka.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       'parameters': {'key':{'source':'input', 'value':5},
                        'value': {'source':'input', 'value':5}
                      }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        kafka_key = self.get_param_value(parameters['key'])
        kafka_value = self.get_param_value(parameters['value'])
        # update the self.kafka data
        self.kafka_data[kafka_key] = kafka_value
    except Exception as e:
        logging.error(e)
        logging.error(f"Unable to send the data that needs to be sent to next topic via kafka.Check rule")
    return True


######################################### HSBC ##################################################



@register_method
def Dodue_date_generate(self, parameters):
    """ 
        Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = {'rule_type': 'static',
                        'function': 'due_date_generate',
                        'parameters': {'Extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    holidays = self.get_param_value(parameters["holidays"])
    Extended_days = self.get_param_value(parameters["Extended_days"])
    logging.info(f'Extended_days',Extended_days)
    logging.info("=====================>")
    logging.info(holidays)
    Extended_days,_ = Extended_days.split(" ")
    Extended_days = int(Extended_days)
    try:
        today = date.today()
        due_date = today + timedelta(days = int(Extended_days))

        date_list = [(today + timedelta(days=x)).strftime('%Y-%m-%d') for x in range(Extended_days+1)]
        li_dif = [i for i in holidays if i in holidays and i in date_list]
        logging.info(f"Number of holidays are:",len(li_dif),due_date)
        due_date = due_date + timedelta(days = int(len(li_dif)))

        logging.info(f'due_date is :',due_date)
        while True:
            if str(due_date) not in holidays :
                logging.info(f'due_date2 is :',due_date)
                return due_date
            else:
                due_date = due_date + timedelta(days = 1)
                logging.info(f'due_date3 is :',due_date)
    except Exception as e:
        logging.error(f"=======> Cannot Generate DueDate_generate ERROR : in Dodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def BankDodue_date_generate(self, parameters):
    """
        Bank_Due_Date_generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        duedate_rule = { 'rule_type': 'static',
        'function': 'Bank_due_date_generate',
        'parameters': {'Due_date':{'source':'input_config','table':"ocr",'column':'Due Date(Notice)'}
        }
    }
    """
    holidays = self.get_param_value(parameters["holidays"])
    logging.info(holidays)
    logging.info(f"parameters got are {parameters}")
    Due_date = self.get_param_value(parameters["Due_date"])
    Receipt_time = self.get_param_value(parameters["Receipt_time"])
    try:
        today = date.today()
        today_time = datetime.today()
        date_format = "%Y-%m-%d"
        a = datetime.strptime(str(today), date_format)
        b = datetime.strptime(str(Due_date), date_format)
        diff_days = (b - a).days
        logging.info(f'diff_days are =====> ',diff_days)
        Due_date_date = datetime.strptime(str(Due_date),"%Y-%m-%d").date()
        if diff_days == 0 or diff_days == -1 or diff_days == 1 or diff_days == 2 or diff_days == 3 :
            Due_date1 = date.today()
            todaydate = datetime.now()
            Receipt_time = datetime.strptime(Receipt_time,"%Y-%m-%d %H:%M:%S")
            today12pm = todaydate.replace(hour=12, minute=0, second=0, microsecond=0)
            if Receipt_time > today12pm:
                Due_date1 = today + timedelta(days = 1)
            else:
                Due_date1 = date.today()
        elif diff_days == 7 :
            Due_date1 = Due_date_date - timedelta(days = 2)
        elif diff_days == 15 or diff_days == 16 :
            Due_date1 = Due_date_date - timedelta(days = 3)
        elif diff_days >= 17 :
            Due_date1 = today + timedelta(days = 2)
        else:
            Due_date1 = Due_date_date - timedelta(days = 2)
        logging.info(f'Duedate in Bank_is ===> ',Due_date1 )

        while True:
            if str(Due_date1) not in holidays :
                logging.info(f'Bank_Due_date is ======> ',Due_date1)
                return str(Due_date1)
            else:
                Due_date1 = Due_date1 - timedelta(days = 1)
        logging.info(f'Bank_Due_date is ======> ',Due_date1)
    except Exception as e:
        logging.error(f"===========>Cannot Generate BankDodue_date_generate ERROR : in BankDodue_date_generate_function")
        logging.error(e)
        return False

@register_method
def doSat_and_sun_holidays(self, parameters):

    """
        holidays generation.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        dosat_sun_rule = {'rule_type': 'static',
                        'function': 'dosat_sun_generate',
                        'parameters': {'Extended_days':{'source':'input_config','table':"ocr",'column':'Default Extension'}, 
                      }
    }   
    """
    logging.info(f"parameters got are {parameters}")
    try:
        year = int(datetime.now().year)
        logging.info(f"year is : ",year)
        def all_sundays(year):
        # January 1st of the given year
            dt1 = date(year, 1, 1)
        # First Sunday of the given year  
            logging.info("########## in all_sundays function ")     
            dt1 += timedelta(days = 6 - dt1.weekday())
            while dt1.year == year:
                yield dt1
                dt1 += timedelta(days = 7)
        years1=[]
        for s in all_sundays(year):
            years1.append(str(s))
        logging.info(f'all_sundays are',years1)

        def second_saturdays(year):
            dt2 = date(year, 1, 1) 
            dt2 += timedelta(days = 5 - dt2.weekday())
            while dt2.year == year:
                if 8 <= dt2.day <= 14 :
                    yield dt2
                dt2 += timedelta(days = 7)
        years2=[]
        for s2 in second_saturdays(year):
            years2.append(str(s2))
        logging.info(f'all_second_saturdays are',years2)

        def fourth_saturdays(year):
            dt3 = date(year, 1, 1) 
            dt3 += timedelta(days = 5 - dt3.weekday()) 
            while dt3.year == year:
                if 22 <= dt3.day <= 28 :
                    yield dt3
                dt3 += timedelta(days = 7)
        years3=[]
        for s3 in fourth_saturdays(year):
            years3.append(str(s3))
        logging.info(f'all_fourth_saturdays are',years3)
    except Exception as e:
        logging.error(f"Cannot Generate Holiday_list ERROR : in sat_sun_holiday_generate_function")
        logging.error(e)
        return False

    Holiday_list = years1+years2+years3
    logging.info(Holiday_list)
    return Holiday_list

@register_method
def get_holidays_fromdatabase(self, parameters):
    logging.info(f"parameters got are {parameters}")
    from_table1 = self.get_param_value(parameters['from_table1'])
    from_column1 = self.get_param_value(parameters['from_column1'])
    sun_sat_holidays_list = self.get_param_value(parameters['sun_sat_holidays'])
    try:
        holidays_df = (self.data_source[from_table1])
        logging.info(holidays_df)
        logging.info("============= @ ========================>")
        holidays_df = pd.DataFrame(holidays_df)
        holidays_df[from_column1] = holidays_df[from_column1].astype(str)
        holidays_df[from_column1] =  pd.to_datetime(holidays_df[from_column1],dayfirst=True,errors='coerce').dt.strftime('%Y-%m-%d')
        holidays_list = holidays_df[from_column1].tolist()
        logging.info(holidays_list)
        total_holidays = holidays_list + sun_sat_holidays_list
        logging.info(f'total_holidays are:',total_holidays)
        return total_holidays
    except Exception as e:
        logging.error(f"=========> Error in Adding Holidays ")
        logging.error(e)        

@register_method
def doContains_UCIC(self, parameters):
    """ Returns BOOLEAN value if the data is present in the data_source
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        { 'rule_type':'static',
        'function': 'Contains_UCIC',
        'parameters' : {
                        'column1_map_id': 'Customer ID',
                        'table_name_acc': 'close_account_dump',
                        'column1_acc_id': 'CUSTOMER_ID',
                        'value1':{'source':'input_config', 'table':'ocr', 'column': 'Mapping Table'},
                        }
        }
        """
    logging.info(f"parameters got are {parameters}")
    column1_map_id =  parameters['column1_map_id']
    table_name_acc =  parameters['table_name_acc']
    column1_acc_id =  parameters['column1_acc_id']
    value_map = self.get_param_value(parameters['value1'])
    logging.info(value_map)
    logging.info(table_name_acc)
    try:
        dict_list = []
        value_map = json.loads(value_map)
        map_dict = [dict_list.append(e) for e in value_map[0]["rowData"]]
        map_df = pd.DataFrame(dict_list)
        df_acc = self.data_source[table_name_acc]
        df_acc = pd.DataFrame(df_acc)
        logging.info(df_acc)
        id_list = list(df_acc[column1_acc_id])
        logging.info(id_list)
        for index, row in map_df.iterrows():
            if row[column1_map_id] in id_list :
                return 'Match Found'
        return 'Match Not Found'       
    except Exception as e:
        logging.error(f"Error in ===============> doContains_UCIC Function")
        logging.error(e)
        return False


@register_method
def doContains_string(self, parameters):
    """ Returns true value if the string is present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    strings_list = parameters['strings_list']
    try:
        for string in strings_list:
            if string in word:
                return True
        return False
    except Exception as e:
        logging.info('==========> Error in doContains_string')
        logging.info(e)
        return False

        
@register_method
def doAppendDB(self,parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        assign_table = parameters['assign_table']
        value_to_concate = parameters['assign_value']
        table_key = assign_table['table']
        column_key = assign_table['column']
        concat_value = self.data_source[table_key][column_key]
        logging.info(f'Concat Value is {concat_value}')
    except Exception as e:
        logging.error("Error In doAppend function")
        logging.error(e)
    try:
        
        if concat_value == "" or concat_value == 'NULL' or concat_value == None:
            concated_string = value_to_concate
        else:
            concated_string = concat_value+"\n"+value_to_concate
            """try:
                concat_value = json.loads(concat_value)
                concat_value.append(json.loads(value_to_concate))
                concated_string = json.dumps(concat_value)
            except json.JSONDecodeError:
                concated_string = concat_value+"\n"+value_to_concate
                concated_string = str(concated_string)"""
        
        try:
            self.data_source[table_key][column_key] = str(concated_string)
            if table_key not in self.changed_fields:
                self.changed_fields[table_key] = {}
            self.changed_fields[table_key][column_key] = str(concated_string)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in doappenddb function for : {column_key}")
            logging.error(e)
        return True
    except Exception as e:
        logging.error("Error In doAppend function")
        logging.error(e)
        return False
         
        
@register_method
def doDateParsing(self,parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        parameters:{
            "source":"input_config","table":"ocr","column":"Stock Date"
        }
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_data(parameters)
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except:
        input_date = input_date
    
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','mar','may','jul','aug','oct','dec','01','03','05','07','08','10','12','january',' march','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception as e :
        logging.error("Cannot convert date")
        logging.error(e)

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info(f"given date and month's last date are same")
            return True
        else:
            logging.info(f"given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False

@register_method
def doDateParsingMarch(self,parameters):
    """Checks whether given date is last day of that month or not, returns true if yes
        parameters:{
            "input_date":{"source":"input_config","table":"ocr","column":"Stock Date"
        }
        NOTE: works same for all months except march, if march returns true for any date
    """
    logging.info(f"parameters got are {parameters}")
    input_date = self.get_data(parameters)
    try:
        input_date = input_date.replace('.','-')
        input_date = input_date.replace('suspicious','')
        input_date = input_date.replace('/','-')
        input_date = input_date.replace(' ','-')
        input_date = input_date.replace('st','')
        input_date = input_date.replace('th','')
    except:
        input_date = input_date
        
    logging.info(f"date got is {input_date}")
    list_31 = ['jan','may','jul','aug','oct','dec','01','05','07','08','10','12','january','may','july','august','october','december','1','3','5','7','8']
    list_30 = ['apr','jun','sep','nov','04','06','09','11','april','june','september','november','4','6','9']
    try:
        input_list = input_date.split("-")
        if len(input_list) == 2:
            if input_list[0].lower() in list_31:
                input_date = "31-"+input_date
            elif input_list[0].lower() in list_30:
                input_date = "30-"+input_date
            elif(input_list[0].lower()=='march') or (input_list[0].lower()=='mar') or (input_list[0]=='03'):
                logging.info(input_date)
                return True
            else:
                feb_last = calendar.monthrange(int(input_list[1]),2)[1]
                input_date = str(feb_last)+"-"+input_date
        logging.info(f"Converted date is {input_date}")
    except Exception as e :
        logging.error("Cannot convert date")
        logging.error(e)

    try:
        input_date = parser.parse(input_date,default=datetime(2019, 10, 3))
        date_list = str(input_date).split("-")
        logging.info(f"Date list is : {date_list}")
        input_day = date_list[2][0:2]
        input_month = date_list[1]
        if (input_month.lower()=='march') or (input_month.lower()=='mar') or (input_month=='03'):
            return True
        input_year = date_list[0]
        logging.info(f"input date is: {input_day}")
        month_last = calendar.monthrange(int(input_year),int(input_month))[1]
        logging.info(f"last day of given month is {month_last}")
        if str(input_day) == str(month_last):
            logging.info(f"given date and month's last date are same")
            return True
        else:
            logging.info(f"given date and month's last date are different")
            return False
    except Exception as e:
        logging.error("Cannot compare two dates")
        logging.error(e)
        return False


@register_method
def doSplit(self,parameters):
    """ Replaces value in a string and returns the updated value
    'parameters':{
        'data':{'source':'input','value':''},
        'symbol_to_split':{'source':'input','value':''},
        'required_index':{'source':'input','value':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    symbol_to_split = parameters.get('symbol_to_split',"")
    required_index = parameters.get('required_index',"")
    data = parameters['data']
    try:
        data_split = str(data).split(symbol_to_split)
        logging.info(f"splited data is {data_split}")
        return data_split[int(required_index)]
    except Exception as e:
        logging.error("Spliting value failed")
        logging.error(e)
        return data


   

@register_method
def doAlphaNumCheck(self, parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        return bool_value
    except Exception as e:
        logging.error("Error In doAlphaNumCheck function")
        logging.error(e)
        return False

@register_method
def doRegexColumns(self, parameters):
    """ Returns a value by applying given Regex pattern on value
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'table_name':"",
                        'columns':[],
                       'regex_str':"\d{6}"
        }
        }
        NOTE: can apply for more than one column at a single time
    """
    logging.info(f"parameters got are {parameters}")
    
    table_name = parameters['table_name']
    columns = parameters['columns']
    regex_str = parameters['regex_str']
    try:
        regex = re.compile(f'{regex_str}')
    except Exception as e :
        logging.error("Error In regex pattern")
    for column in columns:
        phrase = self.data_source[table_name][column]
        logging.info(f"GOT THE VAlUE FOR COLUMN {column} IS {phrase}")
        if not phrase:
            phrase = '0'
        phrase = str(phrase).replace(",","")
        try:
            matches = re.findall(regex, str(phrase))
            if matches[0]:
                logging.info(f"LIST OF MATCHES GOT ARE : {matches}")
                matches = matches[0]
            else:
                matches = 0
        except Exception as e:
            logging.debug("REGEX MATCH GOT FAILED SO DEFAULT VALUE 0 GOT ASSIGNED")
            matches = 0
        logging.debug(f"MATCHES GOT FOR {column} COLUMN IS : {matches}")
        self.data_source[table_name][column] = str(matches)
        try:
            if table_name not in self.changed_fields:
                self.changed_fields[table_name] = {}
            self.changed_fields[table_name][column] = str(matches)
            logging.info(f"updated the changed fields\n changed_fields are {self.changed_fields}")
        except Exception as e:
            logging.error(f"error in assigning and updating the changed fields in regex function for : {column}")
            logging.error(e)
    return True

@register_method
def doReturn(self, parameters):
    """Returns the mentioned value
    'parameters':{
        'value_to_return':{"source":"input_config","table":"ocr","column":"Stock Date"}
    }
    """
    logging.info(f"parameters got are {parameters}")
    try:
        value_to_return = self.get_param_value(parameters['value_to_return'])
        return value_to_return
    except Exception as e:
        logging.error("cannot get value")
        logging.error(e)
        return ""

@register_method
def doRound(self, parameters):
    """Rounds a number to the required number of decimals
    'parameters' : {
        'value': {"source":"input_config","table":"ocr","column":""},
        'round_upto': {"source":"input_config","table":"ocr","column":""
    }
    """
    logging.info(f"parameters got are {parameters}")
    value = self.get_param_value(parameters['value'])
    round_upto = self.get_param_value(parameters['round_upto'])
    try:
        value = round(float(value), round_upto)
    except Exception as e :
        logging.error("ROUND FAILED SO RETURNING SAME VALUE")
        logging.error(e)
        value = value
    logging.info(f"Value after round is : {value}")
    return value


@register_method
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

@register_method
def doPartialMatch(self,parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"}
    }

    """
    logging.info(f"parameters got are {parameters}")
   
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    match_percent = parameters['match_percent']
    match_percent = int(match_percent)
    
    data = self.data_source.get(words_table)
    logging.info(f"data from the data_source[words_table] is {data}")
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f"words got for checking match are : {words}")
    max_ratio = 0
    match_got = ""
    for word in words:
        try:
            if match_word is not None and word is not None:
                ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
                if ratio >=match_percent and ratio > max_ratio:
                    max_ratio = ratio
                    match_got = word
        except Exception as e:
            logging.error("cannnot find match")
            logging.error(e)
 
    logging.info(f"match is {match_got} and ratio is {max_ratio}")
    logging.info(f"{type(max_ratio)}")
   
    return match_got


@register_method
def doAlnum_num_alpha(self,parameters):
    """ Returns true value if the string is Alpha or num or alnum
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
         {'rule_type':'static',
        'function': 'Alnum_num_alpha',
        'parameters' :{'word':{'source':'rule','value':get_range_rule1},
                       'option':'alpha',      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    word = self.get_param_value(parameters['word'])
    option = parameters['option']
    try:
        if option == 'alpha':
            bool_value = word.isalpha()
            logging.info(f'{word} is alpha {bool_value}')
        if option == 'numeric':
            bool_value = word.isnumeric()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'alnum':
            bool_value = word.isalnum()
            logging.info(f'{word} is numeric {bool_value}')
        if option == 'is_numeric':
            try:
                bool_value = float(word).is_integer()
                logging.info(f'{word} is numeric {bool_value}')
            except:
                return False
        return bool_value
    except Exception as e:
        logging.error("Error In doAlnum_num_alpha function")
        logging.error(e)
        return False

@register_method
def doRegex(self,parameters):
    """ Returns a value by doing Regex on given value.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'Regex',
        'parameters' :{'phrase':{'source':'input_config','table':'ocr','column':'Address_of_Vendor'},
                       'regex_str':"\d{6}",      
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    phrase = parameters['phrase']
    regex_str = parameters['regex_str']
    reg_model = parameters['reg_model']
    try:
        if len(str(phrase)) > 1:
            phrase = phrase.lower()
            #logging.info(f'regex is : {regex_str}')
            #logging.info(f'phrase is : {phrase}')
            regex= re.compile(f'{regex_str}')
            if reg_model == 'search':
                matches= re.search(regex, phrase)
                if matches:
                    matches = matches[0]
                else:
                    matches = ''
            if reg_model == 'match':
                matches= re.match(regex, phrase)
            if reg_model == 'sub':
                matches = re.sub(regex,'',phrase)
            if reg_model == 'findall':
                matches= re.findall(regex, phrase)
                if matches:
                    matches = matches[0]
                else:
                    matches = ''
            if reg_model == 'matchall':
               matches= re.findall(regex, phrase)
               if matches:
                   value=''
                   for x in matches:
                       value= value+x
                   matches =  value
               else:
                   matches = ''
            
            logging.info(f"Match got is {matches}")
            return matches
        else:
            logging.info(f'phrase is empty')
            return phrase
    except Exception as e:
        logging.error("Error In doRegex function")
        logging.error(e)
        return phrase


@register_method
def doGetDateTime(self, parameters):
    """ Returns present date and time
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        {'rule_type':'static',
        'function': 'GetDateTime',
        'parameters' :{
        }
        }
    """
    logging.info(f"parameters got are {parameters}")
    try:
        date_time = datetime.now()
        logging.info(f"Got dateand time are : {date_time}")
    except:
        logging.error("date time function failed")
        date_time = ""
    return date_time

@register_method
def doDateIncrement(self, parameters):
    """Returns date by adding given number of days to the given input
    'parameters': {
        'input_date' : {'source':'input_config', 'table':'ocr', 'column': 'End_date'},
        'input_format' :'%d-%m-%Y',
        'increment_days': {'source':'input_config', 'table':'ocr', 'column': 'End_date'}
    }
    """
    logging.info(f"parameters got to doDateIncrement are : {parameters}")
    input_date = self.get_param_value(parameters['input_date'])
    input_format = parameters['input_format']
    increment_days = self.get_param_value(parameters['increment_days'])
    increment_days = increment_days.lower().replace("days","")
    increment_days = increment_days.replace(" ","")
    try:
        converted_date = datetime.strptime(str(input_date),input_format).date()
        converted_date = converted_date + timedelta(days = int(increment_days))
        converted_date = str(converted_date)
        logging.info(f"Converted date is : {converted_date}")
        return converted_date
    except Exception as e :
        logging.error("Date incrementation failed")
        logging.error(e)
        return input_date
    

@register_method
def doNtpathBase(self, parameters):
    """Returns base word of given path
    'parameters': {
        'input_value':{'source':'input_config', 'table':'ocr', 'column': 'End_date'}
        }
    """
    logging.info(f"parameters got for doNtpathBase are : {parameters}")
    input_value = self.get_param_value(parameters['input_value'])
    try:
        extracted_base = ntpath.basename(str(input_value))
        logging.info(f"Extracted value from path is : {extracted_base}")
        return extracted_base
    except Exception as e:
        logging.error("Failed in extracting base name of the path")
        logging.error(e)
        return ""
    

@register_method
def AmountCompare(self,parameters):
    left_param, operator, right_param = parameters['left_param'], parameters['operator'], parameters['right_param'] 
    left_param_value, right_param_value = self.get_param_value(left_param), self.get_param_value(right_param)
    logging.debug(f"left param value is {left_param_value} and type is {type(left_param_value)}")
    logging.debug(f"right param value is {right_param_value} and type is {type(right_param_value)}")
    logging.debug(f"operator is {operator}")
    try:
        left_param_value = str(left_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        right_param_value = str(right_param_value).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        if operator == ">=":
            logging.info(float(left_param_value) >= float(right_param_value))
            return (float(left_param_value) >= float(right_param_value))
        if operator == "<=":
            logging.info(float(left_param_value) <= float(right_param_value))
            return (float(left_param_value) <= float(right_param_value))
        if operator == ">":
            logging.info(float(left_param_value) > float(right_param_value))
            return (float(left_param_value) > float(right_param_value))
        if operator == "<":
            logging.info(float(left_param_value) < float(right_param_value))
            return (float(left_param_value) < float(right_param_value))
        if operator == "==":
            logging.info(float(left_param_value) == float(right_param_value))
            return (float(left_param_value) == float(right_param_value))
    except Exception as e:
        logging.debug(f"error in compare key value {left_param_value} {operator} {right_param_value}")
        logging.debug(str(e))
        return False
    

@register_method
def doCheckDate(self,parameters):
    """
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        'parameters': {
            'value':{'source':'input_config', 'table':'ocr', 'column': 'End_date'},
            'input_format' :'%d-%m-%Y'
            }
    """
    logging.info(f"parameters got for doCheckDate are {parameters}")
    value = self.get_param_value(parameters['value'])
    input_format = parameters['input_format']
    if value == '':
        return False
    elif datetime.today() > datetime.strptime(value,input_format): # %H:%M:%S
        return True
    return False

        
@register_method
def doPartialCompare(self, parameters):
    """ Returns highest matched string
    'parameters':{
        'words_table' : '',
        'words_column':'',
        'match_word' : {"source":"input_config","table":"ocr","column":"Stock Date"}
    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = self.get_param_value(parameters['match_word'])
    word = self.get_param_value(parameters['word'])
    logging.info(match_word)
    logging.info(word)
    max_ratio = 0
    match_got = ""
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio > 75 and ratio > max_ratio:
            max_ratio = ratio
            match_got = word
            logging.info(match_got)
            logging.info(f"match is {match_got} and ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.error("cannnot find match")
        logging.error(e)

    logging.info(f"match is {match_got} and ratio is {max_ratio}")
    return match_got


@register_method
def doPartialComparison(self, parameters):
    """ Returns True or False Based on the matching ratio
    'parameters':{
        match_word = 'helloworld'
        word = 'hello'
    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = parameters['match_word']
    word = parameters['word']
    logging.info(match_word)
    logging.info(word)
    max_ratio = 0
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio > 75 and ratio > max_ratio:
            max_ratio = ratio
            logging.info(f"Ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.error("cannnot find match")
        logging.error(e)

    logging.info(f"Ratio is {max_ratio}")


@register_method
def doDateParser(self, parameters):
    """Accepts any type of input formate of date and returns required standard format

        'parameters' :{
            'input_date' : {'source':'input','value':''},
            'standard_format' : {'source':'input','value':''}
    }
    """
    logging.info(f"Parameters got in Date Parser function is {parameters}")
    standard_format = self.get_param_value(parameters['standard_format'])
    input_date = self.get_param_value(parameters['input_date'])

    try:
        parsed_date = parse(str(input_date), fuzzy=True, dayfirst=True).strftime(standard_format)
        logging.info(f"Date got after parsing is :{parsed_date}")
        return parsed_date
    except Exception as e:
        logging.error("DATE CONVERSION FAILED")
        logging.error(e)
        return input_date



@register_method
def doAmountSyntax(self, parameters):
    """Returns the amounts with .00 and with commas of the parameter value."""
    logging.info(f"parameters got are {parameters}")
    output_amount = self.get_data(parameters)
    logging.info(len(str(output_amount)))
    if len(str(output_amount)) >= 1:
        logging.info(len(str(output_amount)))
        try:
            output_amount2 = str(output_amount).replace(',','')
            output_amount=format_decimal(output_amount2, locale='en_IN')
            if '.' in output_amount:
                float_value = round(float(output_amount.replace(',','')),2)
                float_value = str(float_value)
                if float_value[-2] == '.':
                    logging.info('here')
                    output_amount = output_amount.split('.')[0]+'.'+float_value[-1:]+'0'
                else:
                    output_amount = output_amount.split('.')[0]+'.'+float_value[-2:]
            else:
                output_amount = output_amount+'.00'
            return output_amount    
        except Exception as e:
            logging.error(e)
            return output_amount
    else:
        return output_amount


@register_method
def doDateCompare(self, parameters):
    date_import=parameters['date_import']
    specific_date=parameters['specific_date']
    logging.info(f'specific date is {specific_date}')
    logging.info(f'date_import is {date_import}')
    try:
        logging.info(f'specific_date type is {type(specific_date)}')
        logging.info(f'date_import type is {type(date_import)}')
        delta = specific_date - date_import
        logging.info(f'Difference is {delta.days}')
        if delta.days >= 30:
            return "0"
        else:
            return "1"
    except Exception as e:
        logging.error(f"some error in the Date Compare function")
        logging.error(e)
        return "1"      

'''
@register_method
def doDateCompare(self, parameters):
    """Returns the boolean value by comparing dates.
    Args:
        parameters (dict): The parameter from which the needs to be taken. 
    eg:
       "parameters": {
                    "specific_date": {"source": "input_config","table": "ocr","column": "doc_date"},
                    "Operator1": "+",
                    "Operator2": ">",
                    "Days": "30",
                    "date_format": "%d-%m-%Y"
                  }
    Note:
        1) Recursive evaluations of rules can be made.
    
    """
    try:
        logging.info(f"parameters got are {parameters}")
        specific_date=self.get_data(parameters['specific_date'])
        date_import=self.get_data(parameters['date_import'])
        Operator1=parameters["Operator1"]
        Operator2=parameters["Operator2"]
        Days=parameters["Days"]
        date_format=parameters["date_format"]
        def get_truth(inp, relate, cut):
            ops = {'>': operator.gt,
                '<': operator.lt,
                '>=': operator.ge,
                '<=': operator.le,
                '=': operator.eq,
                '+':operator.add,
                "-":operator.sub}
            return ops[relate](inp, cut) 
        
        specific_date=datetime.strptime(specific_date,date_format)
        date_import=datetime.strptime(date_import,date_format)
        result=get_truth(date_import ,Operator1,timedelta(int(Days)))
        result=datetime.strftime(result,date_format)
        logging.info(f"result is {result}")
        fin_result=datetime.strptime(result,date_format)
        logging.info(f"The Final result is {result}")
        return get_truth(specific_date ,Operator2,fin_result)
    except Exception as e:
        logging.error(f"some error in the Date Compare function")
        logging.error(e)
        return False
'''


@register_method
def doTableErrorMessages(self, parameters):
    """
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
        'parameters': {
            'error_message':{'column1':['message'], 'column2':['message']},
            'input_fields' :[{"field":"Table"}]
            }
    """
    logging.info(f"parameters got for FailureMessages are {parameters}")
    error_message = parameters['error_message']
    input_fields = parameters['input_fields']
    description  = parameters['description']
    color = parameters['color']
    try:
        rule_data_db = DB('business_rules', tenant_id=self.tenant_id, **db_config)
        query_rule_data = f"select `validation_params` from `rule_data` where `case_id` = '{self.case_id}'"
        validation_params_ = rule_data_db.execute_(query_rule_data)
        validation_params = validation_params_['validation_params'][0]
        logging.info(f'type of validation params is {type(validation_params)}')
        try:
            if validation_params == "{}":
                case_id = 0
                validation_params = {}
                logging.info(f'validation params in if is {validation_params}')
            else:
                logging.info(f'validation params in else is {validation_params}')
                validation_params = json.loads(validation_params)
                case_id = int(list(validation_params.keys())[-1])+1
            logging.info(f'case_id is {case_id}')
            validation_params[case_id] = {}
            validation_params[case_id]["description"] = description
            validation_params[case_id]["output"] = ""
            validation_params[case_id]["error_message"] = error_message
            validation_params[case_id]["color"] = color
            validation_params[case_id]["input"] = []
            validation_params[case_id]["input"].insert(0,{})
            validation_params[case_id]["input"][0]["field"] = input_fields
        except Exception as e:
            logging.error(f"assigning of validation params failed")
            logging.exception(e)
            validation_params = {}
        logging.info(f'Final validation params is {validation_params}')
        rule_data_db = DB('business_rules', tenant_id=self.tenant_id, **db_config)
        query_rule_data = f"UPDATE `rule_data` set `validation_params` = '{json.dumps(validation_params)}' where `case_id` = '{self.case_id}'"
        rule_data_db.execute(query_rule_data)
        return True
    except Exception as e:
        logging.error(f"Error in Tableerrormessages Function")
        logging.exception(e)
        return False


@register_method
def doDateConvertion(self,parameters ):
   
    input_date =  self.get_param_value(parameters['input_date'])
    date_val=input_date[0:2]
    logging.info(f'date_val is {date_val}')
    try:
        date_val=int(date_val)
        if date_val>12:
            from_format = ["yyyy-mm-dd",'dd-mm-yyyy',"dd/mm/yyyy","dd mm yyyy","dd,mmmm,yyyy","dd,mmm,yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","dd,mmmm yyyy","dd mmm yyyy","dd/mmm/yyyy","dd mmmm yyyy","dd-Mmm-yy"]
        else:
            from_format = ['mm-dd-yyyy',"mm.dd.yyyy","mm/dd/yyyy","mm/dd/yy","mm.dd.yy","mm-dd-yy","dd,mmmm,yyyy","dd,mmm,yyyy","mmmm dd, yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","yyyy-mm-dd","dd,mmmm yyyy","mmmm dd,yyyy","mmm dd,yyyy","dd mmm yyyy","dd/mmm/yyyy","mmm dd, yyyy","m/dd/yy","m/d/yy","dd-Mmm-yy","dd mmmm yyyy"]
    except Exception as e:
            from_format = ['mm-dd-yyyy',"mm.dd.yyyy","mm/dd/yyyy","mm/dd/yy","mm.dd.yy","mm-dd-yy","mmmm dd, yyyy","dd,mmmm,yyyy","dd,mmm,yyyy","dd-mmmm-yyyy","dd-mmm-yyyy","yyyy-mm-dd","dd,mmmm yyyy","mmmm dd,yyyy","mmm dd,yyyy","dd mmm yyyy","dd/mmm/yyyy","mmm dd, yyyy","m/dd/yy","m/d/yy","dd-Mmm-yy","dd mmmm yyyy"]
            logging.info('in except')
            logging.info(e)
    to_format='dd-mm-yyyy'

    try:
        
        to_format = get_output_date_format(to_format.lower())
    except Exception as e:
        logging.error(f"Date conversion of is failed")
        logging.error(e)
    try:
        for from_format in from_format:

            date_first_formats = {"dd mm yyyy":"%d %m %Y","dd,mmm,yyyy": "%d,%b,%Y","dd,mmmm,yyyy": "%d,%B,%Y","dd,mmmm yyyy": "%d,%B %Y","dd-mmmm-yyyy":"%d-%B-%Y","dd mmmm yyyy":"%d %B %Y","dd-mm-yy":"%d-%m-%y","dd-mm-yy hh:mm:ss":"%d-%m-%y %H:%M:%S","dd-mm-yyyy hh:mm:ss":"%d-%m-%Y %H:%M:%S","dd-mm-yyyy hh:mm":"%d-%m-%Y %H:%M","dd-bbb-yyyy":"%d-%b-%Y","dd-bbb-yy":"%d-%b-%y","dd/mm/yy hh:mm:ss":"%d/%m/%y %H:%M:%S","dd-yy-mm":"%d-%y-%m","dd mm yyyy hh:mm:ss":"%d %m %Y %H:%M:%S",
                                    "dd-mmm-yy":"%d-%b-%y","dd-yy-mmm":"%d-%y-%b","dd-mmm-yyyy":"%d-%b-%Y","dd-Mmm-yyyy":"%d-%b-%Y","dd-yyyy-mm":"%d-%Y-%m","dd-yyyy-mmm":"%d-%Y-%b","dd/mm/yy":"%d/%m/%y","dd/yy/mm":"%d/%y/%m","dd/mm/yyyy":"%d/%m/%Y","dd/mmm/yy":"%d/%b/%y","dd/yy/mmm":"%d/%y/%b","dd/mmm/yyyy":"%d/%b/%Y",
                                    "dd/yyyy/mm":"%d/%Y/%m","dd/yyyy/mmm":"%d/%Y/%b","dd-mm-yyyy":"%d-%m-%Y","dd.mm.yyyy hh:mm:ss":"%d.%m.%Y %H:%M:%S","dd.mm.yyyy":"%d.%m.%Y","dd-mm-yyyy hh:mm":"%d-%m-%Y %H:%M","dd-Mmm-yyyy, hh:mm":"%d-%b-%Y, %H:%M","dd-Mmm-yy":"%d-%b-%y","dd mmm yyyy":"%d %b %Y","dd mmm yy":"%d %b %y","dd bbb yyyy":"%d %b %Y","dd bbb yy":"%d %b %Y","dd/mm/yyyy hh:mm:ss AM/PM":"%d/%m/%Y %H:%M:%S %p",
                                    "dd/mm/yyyy hh:mm:ss am/pm":"%d/%m/%Y %H:%M:%S %p","dd/mm/yyyy hh:mm":"%d/%m/%Y %H:%M","dd/mm/yyyy hh:mm:ss":"%d/%m/%Y %H:%M:%S","dd.mm.yyyy":"%d.%m.%Y","ddmmyyyy":"%d%m%Y","ddmmyy":"%d%m%y","dmmyyyy":"%d%m%Y","dd.mm.yy":"%d.%m.%y","dd-mmm-yy hh:mm am/pm":"%d-%b-%y %H:%M %p","dd-mm-yyyy hh:mm:ss am/pm":"%d-%m-%Y %H:%M:%S %p","DD-MM-YYYY HH:MM:SS AM/PM":"%d-%m-%Y %H:%M:%S %p","DD-MM-YYYY":"%d-%m-%Y",
                                    "dd-mmm-yy hh:mm:ss":"%d-%b-%y %H:%M:%S","dd-mmm-yyyy hh:mm:ss":"%d-%b-%Y %H:%M:%S"}
            
            date_last_formats = {"mmm dd,yyyy":"%b %d,%Y","mmmm dd,yyyy":"%B %d,%Y","mmmm dd, yyyy":"%B %d, %Y","mm.dd.yy": "%m.%d.%y","mm.dd.yyyy": "%m.%d.%Y","m/dd/yy": "%m/%d/%y","m/d/yy":"%m/%d/%y","mmmm-dd-yyyy":"%B-%d-%Y","mmmm dd yyyy":"%B %d %Y","mm-yy-dd":"%m-%y-%d","bbb dd, yyyy":"%b %d %Y","mmm dd, yyyy":"%b %d, %Y","mmm dd, yyyy hh:mmam/pm":"%b %d, %Y %H:%M%p","mmm dd, yyyy h:mam/pm":"%b %d, %Y %H:%M%p","yy-dd-mm":"%y-%d-%m","yy-mm-dd":"%y-%m-%d","mm-dd-yy":"%m-%d-%y","mm-yyyy-dd":"%m-%Y-%d","mm-dd-yyyy":"%m-%d-%Y","yyyy-dd-mm":"%Y-%d-%m","yyyy-mm-dd":"%Y-%m-%d","mmm-yy-dd":"%b-%y-%d",
                                    "mmm-dd-yy":"%b-%d-%y","yy-dd-mmm":"%y-%d-%b","yy-mmm-d":"%y-%b-%-d","mmm-yyyy-dd":"%b-%Y-%d","mmm-dd-yyyy":"%b-%d-%Y","yyyy-dd-mmm":"%Y-%d-%b","yyyy-mmm-dd":"%Y-%b-%d","mm/yy/dd":"%m/%y/%d","yy/dd/mm":"%y/%d/%m",
                                    "yy/mm/dd":"%y/%m/%d","mm/dd/yy":"%m/%d/%y","mm/yyyy/dd":"%m/%Y/%d","mm/dd/yyyy":"%m/%d/%Y","yyyy/dd/mm":"%Y/%d/%m","yyyy/mm/dd":"%Y/%m/%d","mmm/yy/dd":"%b/%y/%d","mmm/dd/yy":"%b/%d/%y","yy/dd/mmm":"%y/%d/%b","yy/mmm/dd":"%y/%b/%d",
                                    "mmm/yyyy/dd":"%b/%Y/%d","mmm/dd/yyyy":"%b/%d/%Y","yyyy/dd/mmm":"%Y/%d/%b","yyyy/mmm/dd":"%Y/%b/%d","yyyy-mm-dd hh:mm:ss+5:30":"%Y-%m-%d %H:%M:%S%z","yyyy-mm-dd hh:mm:ss":"%Y-%m-%d %H:%M:%S","yyyy:mm:dd":"%Y:%m:%d","yyyymmdd":"%Y%m%d",
                                    "mm/dd/yyyy hh:mm:ss AM/PM":"%m/%d/%Y %H:%M:%S %p","mm/dd/yyyy hh:mm:ss am/pm":"%m/%d/%Y %H:%M:%S %p","mm/dd/yyyy hh:mm:ss":"%m/%d/%Y %H:%M:%S","mm/dd/yyyy hh:mm":"%m/%d/%Y %H:%M","mm/dd/yyyyhh:mm":"%m/%d/%Y%H:%M","yyyymmddhhmmss":"%Y%m%d%H%M%S",
                                    "yyyy-mm-dd hh:mm:ss +0530":"%Y-%m-%d %H:%M:%S %z","yymmdd":"%y%m%d","yyyy-mm-dd hh:mm:ss +HHMM":"%Y-%m-%d %H:%M:%S %z","Mmm dd yyyy hh:mm AM/PM":"%b %d %Y %H:%M %p","Mmm dd yyyy hh:mmAM/PM":"%b %d %Y %H:%M%p","mmm dd yyyy hh:mm am/pm":"%b %d %Y %H:%M %p", "yyyy-mm-dd hh:mm:ss+H:MM":"%Y-%m-%d %H:%M:%S%z", "mmm dd yyyy hh:mmam/pm":"%b %d %Y %H:%M%p", "yyyy-mm-ddthh:mm:ss.000z":"%Y-%m-%dT%H:%M:%S.000Z", "mm-dd-yyyy hh:mm:ss am/pm":"%m-%d-%Y %H:%M:%S %p"}   
            def convert_date_(text, from_format_, dayfirst=False):
                try:
                    # Check if target column already has converted date from previous formats
                    try:
                        dtime = pd.to_datetime(str(text).strip(), exact=False, dayfirst=dayfirst, errors='coerce', format=from_format_)
                    except:
                        dtime = pd.to_datetime(str(text).strip(), exact=False, dayfirst=dayfirst, errors='coerce', utc=True, format=from_format_)

                    dtime_str = dtime.strftime(to_format)
                    return dtime_str
                except:
                    logging.error(f'date is {text}')
                    return text
            try:
                
                if from_format.lower() in date_first_formats.keys():
                    final_format = date_first_formats[from_format.lower()]
                    conv_date = convert_date_(input_date, final_format, dayfirst=True)
                elif from_format.lower() in date_last_formats.keys():
                    final_format = date_last_formats[from_format.lower()]
                    conv_date = convert_date_(input_date, final_format)
                else:
                    logging.info('in pass')
                    return input_date
            except Exception as e:
                logging.error(f"Date conversion of {input_date} is failed")
                logging.error('*@@@*'*50)
            if conv_date != input_date:
                logging.info('final date is 1')
                return conv_date
            else:
                continue
        logging.info('final date is 2')
    except Exception as e:
        logging.error(f"Date conversion in for loop failed")
        logging.error(e)
        return input_date
    return conv_date


def regex_fields(pharse,regex_str):
    try:
        matches= re.sub(regex_str,"", pharse)
    except Exception as e:
        matches = pharse
    return matches


@register_method
def doNumericExtract(self,parameters):
    """ Returns a value after extracting numeric value.
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    'parameters': {'value':{'source':'input_config','table':'','column':''}
    }
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    option=parameters['option']
    try:
        logging.info(f"got value is : {value}")
        if option == 'Digit':
            numeric_val = ''.join(filter(lambda i: i.isdigit(), value))
        elif option == 'Alnum':
            numeric_val = ''.join(filter(lambda i: i.isalnum(), value))
        elif option == 'Alpha':
            numeric_val = ''.join(filter(lambda i: i.isalpha(), value))
        elif option == 'Upper':
            list_=[]
            for each in value:
                if each.isalpha() == True:
                    list_.append(each.upper())
                else:
                    list_.append(each)
            numeric_val=''.join(list_)
        elif option == 'Lower':
            list1=[]
            for each in value:
                if each.isalpha() == True:
                    list1.append(each.lower())
                elif each.isdigit() == True:
                    list1.append(each)
            numeric_val=''.join(list1)
        logging.info(f"extracted value is : {numeric_val}")
        return numeric_val
    except Exception as e:
        logging.error("Error In doNumericExtract function")
        logging.error(e)
        return value
    
@register_method
def doTransform_(self, parameters) :
    """Returns the evalated data of given equations
    Args:
        parameters (dict): The source parameter which includes values and operators.
    eg:
        'parameters':[
            {'param':{'source':'input', 'value':5}},
            {'operator':'+'},
            {'param':{'source':'input', 'value':7}},
            {'operator':'-'},
            {'param':{'source':'input', 'value':1}},
            {'operator':'*'},
            {'param':{'source':'input', 'value':3}}
        ]
    Note:
        1) Recursive evaluations of rules can be made.
    """
    logging.info(f"parameters got are {parameters}")
    try:
        val1 = parameters['value1']
        operator = parameters['operator']
        val2 = parameters['value2']
        val1 = str(val1).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        val2 = str(val2).replace(',','').replace('INR','').replace('RUPEES','').replace('inr','').replace('rupees','').replace('rupee','').replace('RUPEE','').replace(' ','').replace(':','')
        if operator == '+':
            result = float(val1)+float(val2)
        if operator == '-':
            result = float(val1)-float(val2)
        if operator == '*':
            result = float(val1)*float(val2)
        if operator == '/':
            result = float(val1)/float(val2)
        result=round(result,2)
        result=str(result)
        return result
    except Exception as e:
        logging.error("Error In doTransform_ function")
        logging.error(e)


@register_method
def doContains_string_(self,parameters):
    """ Returns true value if the string is present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            cpt_check_rule = {'rule_type': 'static',
                'function': 'Contains',
                'parameters': { 'table_name': 'ocr','column_name': 'cpt_codes',
                                'value':{'source':'input', 'value':92610}
                        }
            }
    """
    logging.info(f"parameters got are {parameters}")
    words_table = parameters['words_table']
    words_column = parameters['words_column']
    match_word = parameters['match_word']
    #strings_list = parameters['strings_list']
    data = self.data_source[words_table]
    data = pd.DataFrame(data)
    words = list(data[words_column])
    logging.info(f'word is:',match_word)
    logging.info(words)
    match_got = ""
    try:
        if str(match_word).strip() != "":
            for string in words:
                if string.lower() in match_word.lower():
                    match_got = string
            logging.info(match_got)
        return match_got
    except Exception as e:
        logging.error('==========> Error in doContains_string_')
        logging.error(e)




@register_method
def doContain_string(self, parameters):
    """
        Blockly related
        Returns true value if the sub_string is present in the main_string
    Args:
        parameters (dict): The main_string and sub_string parameter (Both are strings).
    eg:
    
                'parameters': { 'data_source': "Sector 2 Sadiq Nagar, Bangalore,Adilabad,110049,India",
                                'data':"india"
                        }
            
    """
    try:
        logging.info(f"parameters got are {parameters}")
        main_string = parameters['main_string']
        sub_string = parameters['sub_string']
        logging.info(f"data got is : {sub_string}")
        if main_string != "" and sub_string != "":
            if sub_string.strip().lower() in main_string.strip().lower():
                return True
            else :
                return False
        else:
            return False
    except Exception as e:
        logging.error('==========> Error in doContain_string')
        logging.error(e)
        return False

@register_method
def doQueue_Percentage(self, parameters):
    try:
        logging.info(f"parameters got are {parameters}")
        queues = parameters['queues']
        queue = parameters['queue']
        flag=''
        try:
            queues = ast.literal_eval(queues)
        except:
            queues = queues
        for i in range(len(queues)):
            if type(queues[i])==str:
                if queue==queues[i]:
                    flag=i
            elif type(queues[i])==list:
                for j in queues[i]:
                    if queue==j:
                        flag=i
            try:
                percentage=(100/len(queues))*(flag+1)
                percentage=round(percentage)
            except:
                percentage=''
        
        return percentage
    except Exception as e:
        logging.info('Error in doQueue_Percentage function')
        logging.info(e)

@register_method
def donumword_to_number_comp(self, parameters):
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    
    if word != "":
        try:
            num_word  = w2n.word_to_num(word)
            return num_word
        except Exception as e:
            logging.info('Error in donumword_to_number_comp function')
            logging.info(e)
            return word
    else:
        return word



@register_method
def doNotContain_string(self, parameters):
    """ Returns true value if the string is not present in the word
    Args:
        parameters (dict): The source parameter which includes values that should be checked.
    eg:
            "word" : "india"
            "string_list" : ["country","australia"]
            }
    """
    logging.info(f"parameters got are {parameters}")
    word = parameters['word']
    string_list = parameters['string_list']
    if word != "":
        try:
            for string_ in string_list:
                if string_ in word:
                    return False
            return True
        except Exception as e:
            logging.info('==========> Error in doNotContain_string')
            logging.info(e)
            return False
    else:
        return False




@register_method
def doTypeConversion(self, parameters):
    """Returns the converted value.
    Args:
        parameters (dict): The source parameter and the datatype to be converted into. 
    eg:
        'parameters': {'value': {'source':'', 'value':''},
                        'data_type': ''
                         }
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    data_type = parameters['data_type']
    try:
        #logging.info(f"Value got is : {str(value)}")
        #logging.info(f"To be converted Data Type is :{data_type}")
        if value != "":
            if data_type == 'str':
                converted_value = str(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'list':
                converted_value = list(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'set':
                converted_value = set(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'tuple':
                converted_value = tuple(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'int':
                converted_value = int(value)
                logging.info(f"The Converted value is : {converted_value}")
            elif data_type.lower() == 'float':
                converted_value = float(value)
                logging.info(f"The Converted value is : {converted_value}")
            else :
                logging.info(f"Data Type not found {data_type}")
            return converted_value
        else:
            return value
    except Exception as e:
        logging.error(f"some error in the Type Conversion function")
        logging.error(e)
        return value
    



@register_method
def ToLower(self, parameters):
    """
        Returns false if the input is empty or else returns the lower case value of given input
    Args:
        parameters (dict): For the Given input value it checks if it is empty or not and returns False or lower case value 
        Respectively.
    eg:
    
                'parameters': { 'value' : 'INPUT_Value'}
            
    """
    logging.info(f"parameters got are {parameters}")
    value = parameters['value']
    value = str(value)
    try:
        if value != "":
            logging.info(f"To be Converted value is: {value}")
            value = value.lower()
            return value
        else:
            return value
    except Exception as e:
        logging.error("Error in converting the input to lower case")
        logging.error(e)
        return value



@register_method
def doDates_diff(self, parameters):
    """
        Returns the no. of days between the given two input dates
    Args:
        parameters (dict): For the Given input dates as dictionary, it returns the difference no of days in between
   
   eg:
             'parameters': {
                                start_date : "13-02-2023"
                                end_date : "31-08-2023"
                            }
            
    """
    logging.info(f"parameters got are {parameters}")
    start_date = parameters["start_date"]
    end_date = parameters["end_date"]
    diff = 0
    try:
        if start_date != "" and end_date != "":
            logging.info(f"start_date and end_date is : {start_date} and {end_date}")
            
            start = datetime.strptime(start_date,'%d-%m-%Y')
            end = datetime.strptime(end_date,'%d-%m-%Y')
            logging.info(f"date objects of start and end : {start},{end}")
            
            diff = end-start
            logging.info(f"Difference is {diff}")
            
            return diff.days
        else:
            return diff
    except Exception as e:
        logging.error("Error in finding the difference between the dates provided")
        logging.error(e)
        return diff
    


@register_method
def is_numeric(self,parameters):
    ''' Returns True or False Based on whether the input is numeric value or not'''
    logging.info(f"parameters got are {parameters}")
    input_val = parameters.get("input","")
    input_val = str(input_val)
    try:
        return input_val.isnumeric()
    except ValueError as e:
        logging.info(f"# error in finding whether it is numeric or not : {e}")
        return False

    
@register_method
def duplicateCheck(self, parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        column_names = parameters['column_names']
        column_values = parameters['column_values']
        db_config["tenant_id"] = self.tenant_id
        case_id = self.case_id
        ocr_db = DB('extraction', **db_config)
        queue_db = DB('queues', **db_config)

        # Construct the query to find duplicates
        query = f"SELECT * FROM `ocr` WHERE "
        conditions = []
        params_ = column_values + [case_id]

        for col in column_names:
            conditions.append(f"{col} = %s")

        conditions.append("case_id != %s")
        query += " AND ".join(conditions)

        df = ocr_db.execute_(query, params=params_)

        logging.info(f"#### Constructed Query is : {query}")
        logging.info(f"#### Length of df is : {len(df)}")
        #try:
        #    length = len(df)
        #    sql = 'UPDATE process_queue SET duplicate_count = %s WHERE case_id = %s'
        #    params = (length, case_id)
        #    result = queue_db.execute(sql, params=params)
        #except Exception as e:
        #    logging.exception("Error inserting duplicate_count into process_queue")
        #    pass


        # If no duplicates found, return False
        if df.empty:  ### CHANGED
            logging.info("No matching case_ids found. Returning False.")  ### CHANGED
            return False  ### CHANGED

        case_ids = df['case_id'].unique()
        case_ids_placeholder = ', '.join(['%s'] * len(case_ids))
        queue_query = f"SELECT case_id, queue FROM `queue_list` WHERE case_id IN ({case_ids_placeholder})"
        queue_params = tuple(case_ids)
        queue_data = queue_db.execute_(queue_query, params=queue_params)

        logging.info(f"queue_data is {queue_data}")

        # Ensure the right data types
        queue_data['case_id'] = queue_data['case_id'].astype(str)
        queue_data['queue'] = queue_data['queue'].astype(str)

        # Map of case_id to queue
        case_queue_map = {row['case_id']: row['queue'] for _, row in queue_data.iterrows()}
        logging.info(f"queue_map is {case_queue_map}")

        # Filter df where case_id is in maker_queue
        filtered_df = df[df['case_id'].apply(lambda x: case_queue_map.get(str(x)) == 'maker_queue')]
        logging.info(f"#### Length of filtered df (maker_queue) is : {len(filtered_df)}")

        if not filtered_df.empty:  ### CHANGED
            return True  ### CHANGED — Duplicate in maker_queue found
        else:
            return False  ### CHANGED — No duplicates in maker_queue
    except Exception as e:
        logging.error("Error In duplicateCheck function")
        logging.error(e)
        return False

    
@register_method
def noofIngestion(self, parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        column_names = parameters['column_names']
        column_values = parameters['column_values']
        db_config["tenant_id"] = self.tenant_id
        case_id = self.case_id
        ocr_db = DB('extraction', **db_config)
        queue_db = DB('queues', **db_config)

        # Construct the query to find duplicates
        query = f"SELECT * FROM `ocr` WHERE "
        conditions = []
        params_ = column_values + [case_id]

        for col in column_names:
            conditions.append(f"{col} = %s")

        conditions.append("case_id != %s")
        query += " AND ".join(conditions)

        df = ocr_db.execute_(query, params=params_)
        logging.info(f"#### Constructed Query is : {query}")
        logging.info(f"#### Params passed are : {params_}")
        logging.info(f"#### Length of df is : {len(df)}")
        try:
            length = len(df)
            #length=length+1
            sql = 'UPDATE process_queue SET duplicate_count = %s WHERE case_id = %s'
            params = (length, case_id)
            result = queue_db.execute(sql, params=params)
        except Exception as e:
            logging.exception("Error inserting duplicate_count into process_queue")
            pass


        # If no duplicates found, return False
        if result.empty or result==0:  ### CHANGED
            logging.info("No matching case_ids found. Returning False.")  ### CHANGED
            return False  ### CHANGED
        else:
            return True  ### CHANGED — No duplicates in maker_queue
    except Exception as e:
        logging.error("Error In noofIngestion function")
        logging.error(e)
        return False



@register_method
def extractComments(self, parameters):
    print(f"the data is received is :{parameters}")
    comment_value = ''
    try:
        data = json.loads(parameters.get("column_values", "[]"))
        comment_value = data[0]["comment"] if data else ''
    except:
        comment_value = ''
    return comment_value


@register_method
def QueryAndCheck(self,parameters):
    logging.info(f"parameters got are {parameters}")
    try:
        column_names = parameters['column_names']
        column_values = parameters['column_values']
        table_ = parameters['table_']
        db_config["tenant_id"]=self.tenant_id
        table_db = DB('extraction', **db_config)
        query = f"SELECT * FROM {table_} WHERE "
        conditions = []
        params_=[]
        
        for col,val in zip(column_names,column_values):
            conditions.append(f"{col} = %s")
            params_.append(val)
        
        query += " AND ".join(conditions)
        df = table_db.execute_(query, params=params_)
        
        logging.info(f"#### Constructed Query is : {query}")
        logging.info(f"#### Length of df is : {len(df)}")

        if len(df) == 0:
            return False
        else:
            return True
    except Exception as e:
        logging.error("Error In QueryAndCheck function")
        logging.error(e)
        return False
    



@register_method
def PartiallyCompare(self, parameters):
    """ Returns True or False Based on the matching ratio
    'parameters':{
        match_word = 'helloworld'
        word = 'hello'
        match_percent = '75'

    }

    """
    logging.info(f"parameters got are {parameters}")
    match_word = parameters['match_word']
    word = parameters['word']
    match_percent = parameters['match_percent']
    logging.info(f'match_word is {match_word} its type is {type(match_word)}')
    logging.info(f'word is {word} its type is {type(word)}')
    logging.info(f'match_percent is {match_percent} its type is {type(match_percent)}')
    max_ratio = 0
    try:
        ratio = SequenceMatcher(None,match_word.lower(),word.lower()).ratio() * 100
        if ratio >= int(match_percent) and ratio > max_ratio:
            max_ratio = ratio
            logging.info(f"Ratio is {max_ratio}")
            return True
        else:
            return False
    except Exception as e:
        logging.info(f"Ratio is {max_ratio}")
        logging.error("Error in PartiallyCompare function")
        logging.error(e)
        return False
    

@register_method
def doExtrayear(self, parameters):
    """ Takes date as input and converts it into the required format
    'parameters':{
        input_date : {"source":"input_config","table":"ocr","column":"Stock Date"},
        output_format : '%d-%b-%y',
        n : 1
        }
        'final_output': {"source":"input","value":"object"}  # can be object or string
    
    """
    logging.info(f"parameters got are {parameters}")
    input_date = parameters['input_date']
    output_format = parameters['output_format']
    n = parameters['n']
    n = int(n)
        
    try:
        if input_date != "":
            converted_date = pd.to_datetime(input_date, dayfirst=True, errors='coerce')
            logging.info(f"converted_date got are {converted_date}")

            years_to_add = converted_date.year + n
            logging.info(f"years_to_add got are {years_to_add}")

            final_output = converted_date.replace(year=years_to_add).strftime(output_format)
            logging.info(f"final_output got are {final_output}")
        
        else:
            logging.info(f"input_date is empty")
            return input_date
    
    except Exception as e:
        logging.error("cannot convert date to the given format")
        logging.info(f"Error in do Extra Year function")
        logging.error(e)
        return input_date



@register_method
def get_last_n_chars(self,parameters):
    """input parameters (dict): 
            {input : "Algonox Technologies" , n : "7"}

            output : 'ologies'
            """
    logging.info(f"parameters got are {parameters}")
    input = parameters['input']
    n = parameters['n']
    n = int(n)
    logging.info(f"input and n value is {input}, {n}")
    try:
        if n < 0:
            logging.info(f"The n value is less than zero")
            return ""
        elif n >= len(input):
            logging.info(f"The n value is grater than length of input string")
            return input
        else:
            logging.info(f"The n value is {n}")
            return input[-n:]
    except Exception as e:
        logging.info(f"error in 'get_last_n_chars' function")
        logging.error(e)
        return input


@register_method
def get_next_month_first_date(self,parameters):


    logging.info(f"parameters got are {parameters}")
    input=parameters['input']
    logging.info(f"input  is {input}")
    try:
        parsed_date = parser.parse(input, dayfirst=True)
        logging.info(f"parsed date is {parsed_date}")
        next_month_date = parsed_date + relativedelta(months=1)
        logging.info(f"next month date is {next_month_date}")
        next_month_date = next_month_date.replace(day=1)
        logging.info(f"next month date1 is {next_month_date}")
        result = next_month_date.strftime("%d-%m-%Y")
        return result
    except ValueError:
        result=''
        return result

@register_method
def rb_stock_summaryTable1(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info(f"data for extraction")
    
    try:
        logging.info(f"try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"0-5 Days","stack_id":"sub_stack_1"},{"field":"6-30 Days","stack_id":"sub_stack_2"},{"field":"31-60 Days","stack_id":"sub_stack_3"},{"field":"6-60 Days","stack_id":"sub_stack_4"},{"field":"61-90 Days","stack_id":"sub_stack_5"},{"field":"91-120 Days","stack_id":"sub_stack_6"},{"field":"121-150 Days","stack_id":"sub_stack_7"},{"field":"151-180 Days","stack_id":"sub_stack_8"},{"field":">180 Days","stack_id":"sub_stack_9"},{"field":"Total","stack_id":"sub_stack_10"}]     
        columns=[]
        logging.info(f" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info(f"after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"imported_rm","0-5_days":"rb_imported_rm_0to5","6-30_days":"rb_imported_rm_ 6to30","31-60_days":"rb_imported_rm_31to60","6-60_days":"rb_imported_rm_6to60","61-90_days":"rb_imported_rm_61to90","91-120_days":"rb_imported_rm_91to120","121-150_days":"rb_imported_rm_121to150","151-180_days":"rb_imported_rm_151to180",">180_days":"rb_imported_rm_gt180","total":""},{"particulars":"wip","0-5_days":"rb_wip_0to5","6-30_days":"rb_wip_6to30","31-60_days":"rb_wip_31to60","6-60_days":"rb_wip_6to60","61-90_days":"rb_wip_61to90","91-120_days":"rb_wip_91to120","121-150_days":"rb_wip_121to150","151-180_days":"rb_wip_151to180",">180_days":"rb_wip_gt_180","total":""},{"particulars":"fg_trading","0-5_days":"rb_fg_trading_0to5","6-30_days":"rb_fg_trading_6to30","31-60_days":"rb_fg_trading_31to60","6-60_days":"rb_fg_trading_6to60","61-90_days":"rb_fg_trading_61to90","91-120_days":"rb_fg_trading_91to120","121-150_days":"rb_fg_trading_121to150","151-180_days":"rb_fg_trading_151to180",">180_days":"rb_fg_trading_gt180","total":""},{"particulars":"vehicle_stock","0-5_days":"rb_vs_0to5","6-30_days":"rb_vs_6to30","31-60_days":"rb_vs_31to60","6-60_days":"rb_vs_6to60","61-90_days":"rb_vs_61to90","91-120_days":"rb_vs_91to120","121-150_days":"rb_vs_121to150","151-180_days":"rb_vs_151to180",">180_days":"rb_vs_gt180","total":""},{"particulars":"spares_stock","0-5_days":"rb_ss_0to5","6-30_days":"rb_ss_6to30","31-60_days":"rb_ss_31to60","6-60_days":"rb_ss_6to60","61-90_days":"rb_ss_61to90","91-120_days":"rb_ss_91to120","121-150_days":"rb_ss_121to150","151-180_days":"rb_ss_151to180",">180_days":"rb_ss_gt180","total":""},{"particulars":"consumables_packing_material","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_packing_material_total"},{"particulars":"stock_in_transit","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_stock_in_transit_total"},{"particulars":"total_stock","0-5_days":"","6-30_days":"","31-60_days":"","6-60_days":"","61-90_days":"","91-120_days":"","121-150_days":"","151-180_days":"",">180_days":"","total":"rb_total_stocks_total"}]
       
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info(f"before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except:
                    pass
        logging.info(f"before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info(f"after table data dumps into table")
        query1=f"UPDATE ocr SET `rb_stock_summary1`= '{table_obj}' where case_id='{case_id}'"
        
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in rb_stock_summaryTable1 function")
        logging.error(e)
        return False



@register_method
def cons_stockTable(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info(f"data for extraction")
    
    try:
        logging.info(f"try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"<30 Days","stack_id":"sub_stack_1"},{"field":"6-60 Days","stack_id":"sub_stack_2"},{"field":"30-90 Days","stack_id":"sub_stack_3"},{"field":"<60 Days","stack_id":"sub_stack_4"},{"field":"60-90 Days","stack_id":"sub_stack_5"},{"field":"<90 Days","stack_id":"sub_stack_6"},{"field":"90-120 Days","stack_id":"sub_stack_7"},{"field":"90-180 Days","stack_id":"sub_stack_8"},{"field":"<120 Days","stack_id":"sub_stack_9"},{"field":"120-150 Days","stack_id":"sub_stack_10"},{"field":"120-180 Days","stack_id":"sub_stack_11"},{"field":">180 Days","stack_id":"sub_stack_12"},{"field":"Unit/Quantity","stack_id":"sub_stack_13"}]
        columns=[]
        logging.info(f" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info(f"after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"co_raw_materials","<30_days":"co_rm_lt30","6-60_days":"co_rm_6to60","30-90_days":"co_rm_30to90","<60_days":"co_rm_lt60","60-90_days":"co_rm_60to90","<90_days":"co_rm_90","90-120_days":"co_rm_90to120","90-180_days":"co_rm_90to180","<120_days":"co_rm_lt120","120-150_days":"co_rm_120to150","120-180_days":"co_rm_120to180",">180_days":"co_rm_gt180","unit/quantity":"co_rm_unit_quan"},{"particulars":"co_wip","<30_days":"co__wip_lt30","6-60_days":"co_wip_6to60","30-90_days":"co_wip_30to90","<60_days":"co_wip_lt60","60-90_days":"co_wip_60to90","<90_days":"co_wip_lt90","90-120_days":"co_wip_90to120","90-180_days":"co_wip_90to180","<120_days":"co_wip_lt120","120-150_days":"co_wip_120to150","120-180_days":"co_wip_120to180",">180_days":"co_wip_gt180","unit/quantity":"co_wip_unit_quan"},{"particulars":"consumables_packing_material","<30_days":"co_con&pm_lt30","6-60_days":"co_con&pm_6to60","30-90_days":"co_con&pm_30to90","<60_days":"co_con&pm_lt60","60-90_days":"co_con&pm_60to90","<90_days":"co_con&pm_lt90","90-120_days":"co_con&pm_90to120","90-180_days":"co_con&pm_90to180","<120_days":"co_con&pm_lt120","120-150_days":"co_con&pm_120to150","120-180_days":"co_con&pm_120to180",">180_days":"co_con&pm_gt180","unit/quantity":"co_con&pm_unit_quan"},{"particulars":"co_fg","<30_days":"co_fg_lt30","6-60_days":"co_fg_6to60","30-90_days":"co_fg_30to90","<60_days":"co_fg_lt60","60-90_days":"co_fg_60to90","<90_days":"co_fg_lt90","90-120_days":"co_fg_90to120","90-180_days":"co_fg_90to180","<120_days":"co_fg_lt120","120-150_days":"co_fg_120to150","120-180_days":"co_fg_120to180",">180_days":"co_fg_gt180","unit/quantity":"co_fg_unit_quan"},{"particulars":"vehicle_stock","<30_days":"co_vs_lt30","6-60_days":"co_vs_6to60","30-90_days":"co_vs_30to90","<60_days":"co_vs_lt60","60-90_days":"co_vs_60to90","<90_days":"co_vs_lt90","90-120_days":"co_vs_90to120","90-180_days":"co_vs_90to180","<120_days":"co_vs_lt120","120-150_days":"co_vs_120to150","120-180_days":"co_vs_120to180",">180_days":"co_vs_gt180","unit/quantity":"co_vs_unit_quan"},{"particulars":"spares_stock","<30_days":"co_ss_lt30","6-60_days":"co_ss_6to60","30-90_days":"co_ss_30to90","<60_days":"co_ss_lt60","60-90_days":"co_ss_60to90","<90_days":"co_ss_lt90","90-120_days":"co_ss_90to120","90-180_days":"co_ss_90to180","<120_days":"co_ss_lt120","120-150_days":"co_ss_120to150","120-180_days":"co_ss_120to180",">180_days":"co_ss_gt180","unit/quantity":"co_ss_unit_quan"},{"particulars":"total_stock","<30_days":"co_total_stock_lt30","6-60_days":"co_total_stock_30to90","30-90_days":"co_total_stock_30to90","<60_days":"co_total_stock_lt60","60-90_days":"co_ss_60to90","<90_days":"co_ss_lt90","90-120_days":"co_total_stock_90to120","90-180_days":"co_total_stock_90to180","<120_days":"co_total_stock_lt120","120-150_days":"co_total_stock_120to150","120-180_days":"co_total_stock_120to180",">180_days":"co_total_stock_gt180","unit/quantity":"co_total_stock_unit_quan"}]
        
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info(f"before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except:
                    pass
        logging.info(f"before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info(f"after table data dumps into table")
        query1=f"UPDATE ocr SET `co_stock_table`= '{table_obj}' where case_id='{case_id}'"
        
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in cons_stockTable function")
        logging.error(e)
        return False


@register_method
def cons_crediTable(self,parameters):
    logging.info(f"parameters got are {parameters}")
    logging.info(f"Consumers creditors table started")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    logging.info(f"data for extraction")
    
    try:
        logging.info(f"try block started")
        header_name=[{"field":"Particulars","stack_id":"sub_stack_0"},{"field":"Amount","stack_id":"sub_stack_12"},{"field":"No. of Creditors","stack_id":"sub_stack_13"}]        
        columns=[]
        logging.info(f" before header for loop ")
        for i in header_name:
            columns.append(i['field'])
        logging.info(f"after for loop")
        table_obj = {}
        table_obj["table"] = {}
        table_obj["table"]["header"] = columns
        row_data=[{"particulars":"co_lcbc","amount":"co_lcbc_amount","no._of_creditors":"co_lcbc_noof_creditors"},{"particulars":"co_otherthan_lcbc","amount":"co_otherthan_lcbc_amount","no._of_creditors":"co_otherthan_lcbc_nof_cred"},{"particulars":"co_spares_creditors","amount":"co_spares_creditors_amount","no._of_creditors":"co_spares_nof_cred"},{"particulars":"co_total_creditors","amount":"co_total_creditors_amount","no._of_creditors":"co_total_credi_nof_credit"}]
        query = f'SELECT * FROM `ocr` where case_id="{case_id}"'
        df = ocr_db.execute_(query)
        df.fillna('',inplace=True)
        logging.info(f"before row data for loop")
        for i in row_data:
            for key,value in i.items():
                try:
                    i[key] = df[value][0]
                except:
                    pass
        logging.info(f"before table obj data dumps into table")
        table_obj["table"]["rowData"] = row_data
        logging.info(f"creditors table obj")
        logging.info(f"table_obj is {table_obj}")
        table_obj = json.dumps(table_obj)
        logging.info(f"after table data dumps into table")
        
        query1=f"UPDATE ocr SET `co_creditors_table`= '{table_obj}' where case_id='{case_id}'"
        ocr_db.execute_(query1)
        return True
    
    except Exception as e:
        logging.error("Error in cons_crediTable function")
        logging.error(e)
        return False

@register_method
def month_and_year(self,parameters):


    logging.info(f"parameters got are {parameters}")
    input=parameters['input']
    logging.info(f"input  is {input}")
    try:
        match = re.search(r'\w+\s\d{4}', input)
        logging.info(f"match date is {match}")
        if match:
            extracted_date = match.group()
            logging.info(f"extracted date is {extracted_date}")
            result = extracted_date
            return result
        else:
            pass
            
    except ValueError:
        result=input
        return result
        
@register_method
def doValidation_params(self, parameters):
    logging.info(f"parameters got are {parameters}")

    validation_params_ = {}
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction",**db_config)
    try:
        colour = parameters['colour']
        error_message = parameters['error_message']
        source = parameters['source']
        
        validation_params_[source] = {'color':colour, 'error_message':error_message}
        logging.info(f"validation_params: {validation_params_}")
        final_out = json.dumps(validation_params_)
        
    except Exception as e:
        logging.error("cannot add validation comments")
        logging.error(e)
        final_out = json.dumps(validation_params_)

    query1 = f"select `case_id` from rule_data where case_id='{case_id}'"
    query1_data = ocr_db.execute_(query1)

    if query1_data.empty:
        insert_data = {
                        'case_id': case_id,
                        'validation_params': validation_params_
                    }
        ocr_db.insert_dict(insert_data, 'rule_data')
    else:
        update = {
            'validation_params': validation_params_
        }
        where = {
            'case_id': case_id
        }
        ocr_db.update('process_queue', update=update, where=where)
    return final_out

@register_method
def get_month_last_date(self, parameters):
    logging.info(f"Parameters received: {parameters}")
    input_date = parameters.get('input')
    logging.info(f"Input date: {input_date}")

    try:
        # Attempt to parse the input date in expected format
        parsed_date = datetime.datetime.strptime(input_date, "%d-%m-%Y")
        logging.info(f"Parsed date: {parsed_date}")

        year = parsed_date.year
        month = parsed_date.month
        logging.info(f"Year: {year}, Month: {month}")

        last_day = calendar.monthrange(year, month)[1]
        logging.info(f"Last day of the month: {last_day}")

        result_date = datetime.datetime(year, month, last_day)
        logging.info(f"Resulting date: {result_date}")

        return result_date.strftime("%Y-%m-%d")  # Return in a standard string format
    except Exception as e:
        logging.warning("An error occurred while parsing the date. Returning original input.")
        logging.error(e)
        return input_date 
    
# def get_month_last_date(self,parameters):
#     logging.info(f"parameters got are {parameters}")
#     input=parameters['input']
#     logging.info(f"input  is {input}")
#     final_result = ""
#     try:
#         parsed_date = datetime.datetime.strptime(input, "%d-%m-%Y")
#         logging.info(f"parsed date is {parsed_date}")
#         year = parsed_date.year
#         logging.info(f"year is {year}")
#         month = parsed_date.month
#         logging.info(f"month is {month}")
#         last_day = calendar.monthrange(year,month)[1]
#         logging.info(f"last day  is {last_day}")
#         result = datetime.datetime(year,month,last_day)
#         logging.info(f"result is {result}")
        
#         return result
#     except Exception as e:
#         logging.info(f"Error occured during get__month_last_date")
#         logging.error(e)
        
#         return result



@register_method
def  get_month_agri_fifteenth(self,parameters):
    logging.info(f"parameters got are {parameters}")
    input=parameters['input']
    logging.info(f"input  is {input}")
    try:
        parsed_date = parser.parse(input)
        logging.info(f"parsed date is fifteenth {parsed_date}")
        year = parsed_date.year
        logging.info(f"year is for fifteenth {year}")
        month = parsed_date.month
        logging.info(f"month is fifteenth is {month}")
        first_day_next_month = datetime(year, month, 1) + timedelta(days=calendar.monthrange(year, month)[1])
        result = first_day_next_month.replace(day=15)
        logging.info(f"result is fifteenth is {result}")
        return result
    except Exception as e:
        logging.info(f"Error occured during get__month_agri_fifteenth")
        logging.error(e)
        
        return result






@register_method
def merge_dict(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    a = parameters['a']
    b = parameters['b']
    
    merged_dict = {}
    try:
        
        l=[]
        l.append(a)
        l.append(b)
        
        for i in l:
            if i!=None and i!='NULL' and i!='null' and i!='None' and i!='none' and i!='':
                i = json.loads(i)
                merged_dict.update(i)
        merged_dict = json.dumps(merged_dict)
        return merged_dict
    except Exception as e:
        logging.error(f"Error occured in merge_dict function{e}")
        
        return merged_dict        





############ commention to handle stores and spares#########
# @register_method
# def get_data_dict(self,parameters):
#     logging.info(f"Parameters got are {parameters}")
#     dic = parameters['input']
#     col = parameters['col_name']
#     #logging.info(f"Input Dictionary is {dic}")
#     logging.info(f"Type of the Input is {type(dic)}")
#     logging.info(f"Key from which we need value is {col}")
#     value = '0'
    
#     try:
#         col = re.sub('[^a-zA-Z\d]', '', col.lower())
#         logging.info(f"col_cleaned is {col}")

#         if isinstance(dic, str):
#             dic = json.loads(dic)

#         found_key = None
#         value = 0
#         for key,_ in dic.items():
#             check_cleaned = re.sub('[^a-zA-Z\d]', '', key.lower())
#             if col == check_cleaned:
#                 found_key = key
#                 print(f"found key is {found_key} and key is {key}")
#                 break
#         if found_key is not None:
#             if 'a.v' in dic[found_key]:
#                 value = list(dic[found_key]['a.v'].values())[0] 
#                 value = float(value.replace(',', ''))
#                 print(f"value is {value}")
#             else:
#                 value = dic[found_key]
#                 value = float(value.replace(',', ''))
#                 print(f"value is {value}")
#         else:
#             print("Column not found")
#             value = 0

#         return float(value) if value else 0
#     except Exception as e:
#         logging.info(f"Error Occured in the get_data_dict Function")
#         logging.error(e)
#         return 0


@register_method
# def get_data_dict(self,parameters):
#     logging.info(f"Parameters got are {parameters}")
#     dic = parameters['input']
#     col = parameters['col_name']
#     #logging.info(f"Input Dictionary is {dic}")
#     logging.info(f"Type of the Input is {type(dic)}")
#     logging.info(f"Key from which we need value is {col}")
#     value = '0'
    
#     try:
#         if col.lower().replace(' ', '') == 'stores&spares': 
#             col = col.replace('&', 'and')
#         col = col.lower().replace('<', 'lt').replace('>', 'gt')
#         col = re.sub('[^a-zA-Z\d]', '', col.lower())
#         logging.info(f"col_cleaned is {col}")

#         if isinstance(dic, str):
#             dic = json.loads(dic)

#         found_key = None
#         value = 0
#         for key,_ in dic.items():
#             check_cleaned = key.lower().replace('<', 'lt').replace('>', 'gt')
#             check_cleaned = re.sub('[^a-zA-Z\d]', '', check_cleaned)
#             #check_cleaned = re.sub('[^a-zA-Z\d]', '', key.lower())
#             if col == check_cleaned:
#                 found_key = key
#                 #print(f"found key is {found_key} and key is {key}")
#                 break
#         if found_key is not None:
#             if 'a.v' in dic[found_key]:
#                 value = list(dic[found_key]['a.v'].values())[0] 
#                 value = float(value.replace(',', ''))
#                 #print(f"value is {value}")
#             else:
#                 value = dic[found_key]
#                 value = float(value.replace(',', ''))
#                 #print(f"value is {value}")
#         else:
#             #print("Column not found")
#             value = 0

#         return float(value) if value else 0
#     except Exception as e:
#         logging.info(f"Error Occured in the get_data_dict Function")
#         logging.error(e)
#         return 0


# def get_data_dict(self,parameters):
#     logging.info(f"Parameters got are {parameters}")
#     dic = parameters['input']
#     col = parameters['col_name']
#     #logging.info(f"Input Dictionary is {dic}")
#     logging.info(f"Type of the Input is {type(dic)}")
#     logging.info(f"Key from which we need value is {col}")
#     value = '0'
    
#     try:
#         if col.lower().replace(' ', '') == 'stores&spares':
#             col = col.replace('&', 'and')
#         col = col.lower().replace('<', 'lt').replace('>', 'gt')
#         col = re.sub('[^a-zA-Z\d]', '', col.lower())
#         logging.info(f"col_cleaned is {col}")

#         if isinstance(dic, str):
#             dic = json.loads(dic)

#         found_key = None
#         value = 0
#         for key,_ in dic.items():
#             check_cleaned = key.lower().replace('<', 'lt').replace('>', 'gt')
#             check_cleaned = re.sub('[^a-zA-Z\d]', '', check_cleaned)
#             #check_cleaned = re.sub('[^a-zA-Z\d]', '', key.lower())
#             #if col == check_cleaned:
#             #    found_key = key
#                 #print(f"found key is {found_key} and key is {key}")
#             #    break
#             if col == check_cleaned:
#                 if dic[key] not in ("", None):
#                     found_key = key
#                     break
#             else:
#                   # fallback if no better key appears
#                 found_key = key
#         if found_key is not None:
#             if 'a.v' in dic[found_key]:
#                 value = list(dic[found_key]['a.v'].values())[0]
#                 #value = float(value.replace(',', ''))
#                 value_str = str(value).replace(',', '').strip()
#                 value = float(value_str) if value_str else 0

#                 #print(f"value is {value}")
#             else:
#                 value = dic[found_key]
#                 #value = float(value.replace(',', ''))
#                 value_str = str(value).replace(',', '').strip()
#                 value = float(value_str) if value_str else 0
#                 #print(f"value is {value}")
#         else:
#             #print("Column not found")
#             value = 0

#         return float(value) if value else 0
#     except Exception as e:
#         logging.info(f"Error Occured in the get_data_dict Function")
#         logging.error(e)
#         return 0

def get_data_dict(self, parameters):
    logging.info(f"Parameters got are {parameters}")

    dic = parameters['input']
    col = parameters['col_name']
    print(f"col is......:{col}")

    try:
        # Basic cleaning: ONLY lowercase + strip
        col_clean = col.strip()

        if isinstance(dic, str):
            dic = json.loads(dic)

        found_key = None
        value = 0

        # search exact match ignoring case
        for key in dic.keys():
            key_clean = key.strip()

            if col_clean == key_clean:
                found_key = key
                break

        # if not found → return 0 safely
        if found_key is None:
            return 0

        # extract value
        data = dic[found_key]

        if isinstance(data, dict) and 'a.v' in data:
            val = list(data['a.v'].values())[0]
        else:
            val = data

        val_str = str(val).replace(',', '').strip()
        return float(val_str) if val_str else 0

    except Exception as e:
        logging.error(e)
        return 0



@register_method
def dosummary(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    field_changes=self.field_changes
    tables=["STOCK STATEMENT","DEBITORS STATEMENT","CREDITORS"]
    for table_ in field_changes:
        if table_ in tables:
            table= table_
    try:
        query =f"SELECT `{table}` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df[table][0]
        df=json.loads(df)
        
        header1=df[0]["header"]
        logging.info(f"Headers is for df of one {header1}")
        headers=df[0]["header"]
        logging.info(f"Headers is for df of zero {headers}")
        headers = [header for header in headers if header not in ['Total', 'Unit/Quantity','No. of debtors','No. of creditors']]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:-1]
        logging.info(f"Row data is {row_data_}")
        if row_data_:
            l=[]
            for j in headers:
                logging.info(f"j value is  {j}")
                sum=0
                for i in row_data_:
                    logging.info(f"i value is {i}")
                    try:
                        sum+=float(i[j])
                    except:
                        pass
                l.append(str(sum))
                logging.info(f"L value is {l}")
            last_dic=df[0]["rowData"][-1]
            logging.info(f"last_dic {last_dic}")
            for i in range(len(headers)):
                last_dic[headers[i]]=l[i]
            row_data_.append(last_dic)
            res={}
            res["header"]=header1
            res["rowData"]=row_data_
            logging.info(f"res values is {res}")
            li=[]
            li.append(res)
            c_dict=json.dumps(li)
            logging.info(f"c dictionary dumps check {c_dict}")
            query1 = f"UPDATE `custom_table` SET `{table}` = %s WHERE case_id = %s"
            params1 = [c_dict, case_id]
            self.cus_table=table
            ocr_db.execute_(query1, params=params1)
            return table
        else:
            return True
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False
    
@register_method
def dosummary_1(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    field_changes=self.field_changes
    tables=["STOCK STATEMENT","DEBITORS STATEMENT","CREDITORS"]
    for table_ in field_changes:
        if table_ in tables:
            table= table_
    try:
        query =f"SELECT `{table}` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df[table][0]
        df=json.loads(df)

        header2=df[0]["header"]
        logging.info(f"Headers is for df of two {header2}")
        headers=df[0]["header"]
        logging.info(f"Headers is for df of zero {headers}")
        
        headers = [header for header in headers if header not in ['Total', 'Unit/Quantity','No. of debtors','No. of creditors']]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:]
        logging.info(f"Row data is {row_data_}")
        for x in row_data_:
            logging.info(f"x value is  {x}")
            sum=0
            for y in headers:
                logging.info(f"y value is {y}")
                try:
                    sum+=float(x[y])
                except:
                    pass
            t=[str(sum)]
            if 'Total' in x:
                x['Total'] =str(sum) 
            else:
                x.update({'Total': str(sum)}) 
            logging.info(f"T value is {t}")        
        res={}
        res["header"]=header2
        res["rowData"]=row_data_
        logging.info(f"res values is {res}")
        li=[]
        li.append(res)
        c_dict_=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict_}")
        query1 = f"UPDATE `custom_table` SET `{table}` = %s WHERE case_id = %s"
        params1 = [c_dict_, case_id]
        self.cus_table=table
        ocr_db.execute_(query1, params=params1)
        return table
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False







@register_method
def dosummary_debtors(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    try:
        query = "SELECT `DEBITORS STATEMENT` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df['DEBITORS STATEMENT'][0]
        df=json.loads(df)
        
        header1=df[0]["header"]
        headers=df[0]["header"]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:-1]
        l=[]
        for j in headers:
            sum=0
            for i in row_data_:
                try:
                    sum+=float(i[j])
                except:
                    pass
            l.append(str(sum))
        last_dic=df[0]["rowData"][-1]
        for i in range(len(headers)):
            last_dic[headers[i]]=l[i]
        row_data_.append(last_dic)
        res={}
        res["header"]=header1
        res["rowData"]=row_data_
        li=[]
        li.append(res)
        c_dict=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict}")
        query1 = "UPDATE `custom_table` SET `DEBITORS STATEMENT` = %s WHERE case_id = %s"
        params1 = [c_dict, case_id]
        ocr_db.execute_(query1, params=params1)
        self.cus_table_deb=True
        return True
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False






@register_method
def dosummary_creditors(self,parameters):
    logging.info(f"parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    ocr_db=DB("extraction",**db_config)
    try:
        query = "SELECT `CREDITORS` FROM `custom_table` WHERE case_id = %s"
        params = [case_id]
        df = ocr_db.execute_(query, params=params)
        df=df['CREDITORS'][0]
        df=json.loads(df)
        
        header1=df[0]["header"]
        headers=df[0]["header"]
        headers=headers[1:]
        row_data_=df[0]["rowData"]
        row_data_=row_data_[0:-1]
        l=[]
        for j in headers:
            sum=0
            for i in row_data_:
                try:
                    sum+=float(i[j])
                except:
                    pass
            l.append(str(sum))
        last_dic=df[0]["rowData"][-1]
        for i in range(len(headers)):
            last_dic[headers[i]]=l[i]
        row_data_.append(last_dic)
        res={}
        res["header"]=header1
        res["rowData"]=row_data_
        li=[]
        li.append(res)
        c_dict=json.dumps(li)
        logging.info(f"c dictionary dumps check {c_dict}")
        query1 = "UPDATE `custom_table` SET `CREDITORS` = %s WHERE case_id = %s"
        params1 = [c_dict, case_id]
        ocr_db.execute_(query1, params=params1)
        self.cus_table_crd=True
        return True
    
    except Exception as e:
        logging.error("Error in do_summary function")
        logging.error(e)
        return False                 











@register_method
def add_columns_values(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    value = parameters['input']
    col = parameters['col_name']
    col1 = parameters['col_name1']
    logging.info(f"Input Dictionary is {value}")
    logging.info(f"Key from which we need value is {col}")
    logging.info(f"Type of the Input is {type(col)}")
    logging.info(f"Key from which we need value is {col1}")
    try:
        c = json.loads(col)
        for key in c:
            if key == col1:
                c[col1]=value
                
        logging.warning(f"Overall dictionary {c}")
        logging.warning(f"Overall dictionary {type(c)}")
        c = json.dumps(c)
        
        return c     

    except Exception as e:
        logging.info(f"Error Occured in the get_data_dict Function")
        logging.error(e)
        return c

@register_method
def month_in_words(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    month_number=parameters['month_number']
    #logging.info(f"month_number  is {month_number}")
    try:
        month_number = int(month_number)

        if 1 <= month_number <= 12:
            month_name = calendar.month_name[month_number]
            return month_name 
        else:
            return ''

    except Exception as e:
        logging.info(f"invalid month number")
        logging.error(e)

"""@register_method
def month_in_words(self, parameters):
    logging.info(f"Parameters got are {parameters}")
    
    month_input = str(parameters.get('month_number')).strip()
    
    try:
        # Handle numeric month input (e.g. '3' or 3)
        if month_input.isdigit():
            month_number = int(month_input)
            if 1 <= month_number <= 12:
                prev_month = 12 if month_number == 1 else month_number - 1
                return calendar.month_name[prev_month]
            else:
                return month_input  # invalid number, return input as-is
        
        # Normalize text input (e.g. 'mar', 'MARCH')
        month_input_lower = month_input.lower()
        
        # Create lookup dictionaries once
        month_name_to_num = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}
        month_abbr_to_num = {m.lower(): i for i, m in enumerate(calendar.month_abbr) if m}
        
        # Determine month number
        month_number = month_name_to_num.get(month_input_lower) or month_abbr_to_num.get(month_input_lower)
        if not month_number:
            logging.info(f"Invalid month input: {month_input}")
            return month_input  # invalid text input, return input as-is
        
        # Compute previous month (wrap around)
        prev_month = 12 if month_number == 1 else month_number - 1
        return calendar.month_name[prev_month]
    
    except Exception as e:
        logging.error(f"Error while processing month input '{month_input}': {e}")
        return month_input"""

@register_method
def assign_value_json(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['data']
    key_data=parameters['key_data']
    value_data=parameters['value_data']
    
    #logging.info(f"key data is {key_data}")
    #logging.info(f"value data is {value_data}")
    try:
        data = json.loads(data)
    
        value_data_str = str(value_data)
        if value_data_str!='0' and value_data_str!='0.0':
            data[key_data] = value_data_str
        elif float(value_data_str.strip() or 0) == 0:
            data[key_data] =  "0.0"
        else:
            data[key_data] = ''
        updated_json_data = json.dumps(data)
        logging.info(f"updated json data is  {updated_json_data}")
        return updated_json_data
    except Exception as e:
        logging.info(f"invalid json")
        logging.error(e)



@register_method
def margin_data(self, parameters):
    logging.info(f"Parameters got are {parameters}")
    data = parameters['party_id']
    add = parameters['conv']
    logging.info(f"data is {data}")
    
    db_config['tenant_id'] = 'hdfc'
    ocr_db = DB("extraction", **db_config)
    logging.info(f"data for extraction")
    
    try:
        query = f"SELECT COMPONENT_NAME, MARGIN from AGE_MARGIN_WORKING_UAT where PARTY_ID = '{data}'"
        df = ocr_db.execute_(query)
        logging.info(f"df is  {df}")

        if not isinstance(df, bool) and not df.empty:
            logging.info(f"Again df is {df}")
            rm_margin = df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].empty else None
            logging.info(f"rm margin is {rm_margin}")
            wip_margin = df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].empty else None
            logging.info(f"wip_margin is {wip_margin}")
            fg_margin = df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].empty else None
            logging.info(f"fg margin is {fg_margin}")
            stores_and_spares_margin = df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].empty else None
            logging.info(f"stores and spares {stores_and_spares_margin}")
            if rm_margin is None or wip_margin is None or fg_margin is None or stores_and_spares_margin is None:
                logging.info("One or more margin values are missing, returning 0")
                return 0  
            if rm_margin == wip_margin == fg_margin == stores_and_spares_margin:
                logging.info(f"All margins are equal, performing calculation")
                try:
                    margings = float(add) * (float(rm_margin) % 100)
                    logging.info(f"margings is {margings}")
                    return margings
                except Exception as e:
                    logging.error(f"Error occurred while calculating margings: {e}")
                    return 0
            else:
                logging.info("Margins are not equal, returning 0")
                return 0
        else:
            logging.info("No data found for the provided PARTY_ID")
            return 0  

    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return 0

# def margin_data(self,parameters):
#     logging.info(f"Parameters got are {parameters}")
#     data=parameters['party_id']
    
#     add=parameters['conv']
#     logging.info(f"data is {data}")
    
#     db_config['tenant_id'] = 'hdfc'
#     ocr_db=DB("extraction",**db_config)
#     logging.info(f"data for extraction")
#     try:
#         query = f"SELECT COMPONENT_NAME, MARGIN from AGE_MARGIN_WORKING_UAT where PARTY_ID = '{data}'"
#         df = ocr_db.execute_(query)
#         logging.info(f"df is  {df}")

#         if not isinstance(df, bool) and not df.empty:
#             logging.info(f"Again df is {df}")
#             rm_margin = df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].empty else None
#             logging.info(f"rm margin is {rm_margin}")
#             wip_margin = df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].empty else None
#             logging.info(f"wip_margin is{wip_margin}")
#             fg_margin = df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].empty else None
#             logging.info(f"fg margin is {fg_margin}")
#             stores_and_spares_margin = df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].empty else None
#             logging.info(f"stores and spares {stores_and_spares_margin}")
#             if rm_margin==wip_margin and rm_margin==fg_margin and rm_margin==stores_and_spares_margin and wip_margin==fg_margin and wip_margin==stores_and_spares_margin and fg_margin==stores_and_spares_margin:
#                 print(f"!!!!!!!!1 comes to if cond")
#                 margings = float(add) * (float(rm_margin)% 100)
#                 print(f"!!!!!!!!! margings is {margings}")
#                 return margings
#     except Exception as e:
#         logging.error(f"Error occurred: {e}")








@register_method
def margin_data_different(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['party_id']
    data1=parameters['data_type_1']
    data2=parameters['data_type_2']
    data3=parameters['data_type_3']
    data4=parameters['data_type_4']
    
    
    logging.info(f"data is {data}")
    
    db_config['tenant_id'] = 'hdfc'
    ocr_db=DB("extraction",**db_config)
    logging.info(f"data for extraction")
    try:
        query = f"SELECT COMPONENT_NAME, MARGIN from MARGIN_MASTER where PARTY_ID = '{data}'"
        df = ocr_db.execute_(query)
        logging.info(f"df is  {df}")

        if not isinstance(df, bool) and not df.empty:
            logging.info(f"Again df is {df}")
            rm_margin = df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'RAW MATERIALS INSURED', 'MARGIN'].empty else None
            logging.info(f"rm margin is {rm_margin}")
            wip_margin = df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'WORK IN PROGRESS INSURED', 'MARGIN'].empty else None
            logging.info(f"wip_margin is{wip_margin}")
            fg_margin = df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'FINISHED GOODS INSURED', 'MARGIN'].empty else None
            logging.info(f"fg margin is {fg_margin}")
            stores_and_spares_margin = df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].values[0] if not df.loc[df['COMPONENT_NAME'] == 'STOCK & STORES INSURED', 'MARGIN'].empty else None
            logging.info(f"stores and spares {stores_and_spares_margin}")
            if rm_margin!=wip_margin and rm_margin!=fg_margin and rm_margin!=stores_and_spares_margin and wip_margin!=fg_margin and wip_margin!=stores_and_spares_margin and fg_margin!=stores_and_spares_margin:
                margings = float(data1) * (float(rm_margin)% 100)
                margings_1 = float(data2) * (float(wip_margin)% 100)
                margings_2 = float(data3) * (float(fg_margin)% 100)
                margings_3= float(data4) * (float(stores_and_spares_margin)% 100)
                total=margings+margings_1+margings_2+margings_3
                return total

    except Exception as e:
        logging.error(f"Error occurred: {e}")        

    

@register_method
def add_key_value(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    data=parameters['data']
    key_data=parameters['key_data']
    value_data=parameters['value_data']
    
    logging.info(f"key data is {key_data}")
    logging.info(f"value data is {value_data}")
    try:
        data = json.loads(data)
    except json.JSONDecodeError:
        #print("Invalid JSON data provided.")
        return None
    data[key_data] = value_data
    updated_json_data = json.dumps(data)
    logging.info(f"updated json data is  {updated_json_data}")
    return updated_json_data


















@register_method
def checking_files(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    db_config["tenant_id"]=self.tenant_id
    case_id=self.case_id
    queues_db=DB("queues",**db_config)
    try:
        query = "SELECT `file_name` FROM `process_queue` WHERE case_id = %s"
        params = [case_id]
        df = queues_db.execute_(query, params=params)
        file_name=df['file_name'][0]
        file_name = file_name.split("_")
        party_id = file_name[0]
        query2 = f"SELECT COUNT(*) as count from `process_queue` where file_name LIKE '%%{party_id}%%'"
        count = queues_db.execute_(query2)
        count = count['count'][0]
        if count>1:
            return "Yes"
        else:
            return "No"
    except Exception as e:
        logging.error(e)
        return "No"

def normalize_component_name(name):
    
    normalized_name = re.sub(r'\s+', '', name).lower()
    return normalized_name


def extract_date(text):
    try:
        return pd.to_datetime(text, dayfirst=True, errors='raise')
    except:
        match = re.search(r'(\d{4})[./-](\d{2})[./-](\d{2})', text)
        if match:
            try:
                date_str = match.group(0).replace('.', '/').replace('-', '/')
                return pd.to_datetime(date_str, dayfirst=True, errors='coerce')
            except:
                return pd.NaT
    return pd.NaT

def case_creation_cnt(case_id,tenant_id):
    db_config['tenant_id']=tenant_id
    extraction_db=DB('extraction',**db_config)
    
    qry=f"select party_id,created_date from ocr where case_id='{case_id}'"
    party_id=extraction_db.execute_(qry)['party_id'].tolist()

    if len(party_id)>=1:
        party_id=party_id[0]
        # print(f"party id's are {party_ids}")
        qry=f"select o.case_id,o.party_id,o.created_date from hdfc_extraction.ocr o join hdfc_queues.queue_list ql on ql.case_id=o.case_id where party_id in '{party_id}' order by created_date"
        party_ids_df=extraction_db.execute_(qry)
        if not party_ids_df.empty:
            party_ids_df.columns = [col.lower() for col in party_ids_df.columns]
            party_ids_df = party_ids_df.loc[:, ~party_ids_df.columns.duplicated()]
            party_ids_df=party_ids_df.dropna()

            # Parse and clean dates
            party_ids_df["parsed_date"] = party_ids_df["created_date"].apply(extract_date)
            # Drop NaT rows for ranking
            # mask = party_ids_df["parsed_date"].notna()
            # party_ids_df.loc[mask, "case_count"] = pd.factorize(party_ids_df.loc[mask, "parsed_date"])[0] + 1
            
            # rank the dates
            #Sort by datetime
            party_ids_df = party_ids_df.sort_values("parsed_date").reset_index(drop=True)

            # # Assign increasing count
            party_ids_df["case_count"] = range(1, len(party_ids_df) + 1)
            
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
        print(f"there is no party id")

    return {"data":"Success","flag":True}
@register_method
# def margin_for_extracted_fields(self, parameters):
#     logging.info(f"Parameters got are {parameters}")
#     db_config["tenant_id"] = self.tenant_id
#     case_id = self.case_id
#     ocr_db = DB("extraction", **db_config)
#     query = f"SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
#     params = [case_id]
#     result_ = ocr_db.execute_(query, params=params)
#     party_id = result_['PARTY_ID'][0]
#     qry=f"""select is_active,component_name from age_margin_working_uat WHERE CAST(party_id AS VARCHAR2(4000))='{party_id}'"""
#     result=ocr_db.execute_(qry).to_dict(orient="records")
#     component_is_active_map = {row['COMPONENT_NAME']: row['IS_ACTIVE'] for row in result}
#     #is_active=result_['IS_ACTIVE'][0]
#     try:
#         columns = ["STOCKS", "DEBTORS", "CREDITORS","ADVANCES"]
#         for column in columns:
#             query = f"SELECT `{column}` FROM `OCR` WHERE case_id = %s"
#             params = [case_id]
#             result = ocr_db.execute_(query, params=params).to_dict(orient='records')
#             query = f"SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
#             params = [case_id]
#             result_ = ocr_db.execute_(query, params=params)
#             party_id = result_['PARTY_ID'][0]
#             logging.info(f"result for {column}: {result}")

#             if result:  
#                 df = result[0][column]  
#                 logging.info(f"df for {column}: {df}")
#                 data_dict = json.loads(df)
#             else:
#                 data_dict={}
                
#             key_component_map = {"Raw Materials":["RAW MATERIALS INSURED"],"Finished Goods":["FINISHED GOODS INSURED"],"Total Stock":["TOTAL STOCKS INSURED"],"Work in Process":["WORK IN PROGRESS INSURED"],"Stores and Spares":["STORES & SPARES INSURED"],"Stock in Transit":["STOCK IN TRANSIT"],"Consumable and Spares":["CONSUMABLE SPARES INSURED"],"goods in transist":["GOODS IN TRANSIT INSURED"],"Domestic Stock":["DOMESTIC STOCKS INSURED"],"Export Stock":["EXPORT STOCKS INSURED"],"Sales":["SALES"],"Total Debtors":["DEBTORS","RECEIVABLES","BOOK DEBTS"],"Debtors <30 days":["Debtors <30 days","BOOK DEBTS UPTO 30 DAYS"],"Debtors <60 days":["Debtors <60 days","BOOK DEBTS UPTO 60 DAYS"],"Debtors <90 days":["Debtors <90 days","BOOK DEBTS UPTO 90 DAYS"],"Debtors <120 days":["Debtors <120 days","BOOK DEBTS UPTO 120 DAYS"],"Debtors <150 days":["Debtors <150 days","BOOK DEBTS UPTO 150 DAYS"],"Debtors <180 days":["Debtors <180 days","BOOK DEBTS UPTO 180 DAYS","EXPORT DEBTORS<180 DAYS INSURED"],"Debtors - Exports":["BOOK DEBTS -EXPORTS"],"Debtors - Domestic":["BOOK DEBTS -DOMESTIC"],"Debtiors of Group Companies":["DEBTORS OF GROUP COMPANIES"],"Receivables":["RECEIVABLES"],"Domestic receivables":["DOMESTIC RECEIVABLES"],"Export receivables":["EXPORT RECEIVABLES"],"Total Creditors":["CREDITORS","Trade Creditors"],"Creditors PC":["CREDITORS (PC)"],"Unpaid Stocks":["UNPAID STOCKS"],"DALC":["LESS : DALC"],"Advances paid to suppliers":["ADD : ADVANCE TO SUPPLIER"],"Debtors PC":["DEBTORS (PC)"]}
            
#             data_dict_ = {}

#             for key, value in data_dict.items():       
#                 if not value:
#                     if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                         #for row in data_dict['tab_view']['rowData']:
#                         #    if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
#                         #        row['margin'] = ""
#                         #        row['aging'] = ""
#                         for row in data_dict["tab_view"]["rowData"]:
#                             field = row.get("fieldName")  # safely get fieldName
#                             #print(f"[DEBUG] Checking row fieldName={field!r} against key={key!r}")

#                             if not field:
#                                 #print("[DEBUG] Skipping row because fieldName is missing/empty")
#                                 continue  # skip rows without fieldName

#                                     # normalize both sides for comparison
#                             field_norm = field.replace(" &", "").replace(" ", "").lower()
#                             key_norm = key.replace(" ", "").lower()

#                             #print(f"[DEBUG] Normalized field={field_norm!r}, key={key_norm!r}")

#                             if field_norm == key_norm:
#                                 #print(f"[MATCH] Updating row for fieldName={field!r}")
#                                 row["margin"] = ""
#                                 row["aging"] = ""
#                             else:
#                                 print(f"[NO MATCH] field={field_norm!r} != key={key_norm!r}")

#             # Iterate over each key-value pair in the data dictionary
#                 else:
#                     # Get the component name and margin from the mapping and database
#                     component_names = key_component_map.get(key, [])
#                     component_names = [component.strip() for component in component_names]
#                     normalized_component_names = [normalize_component_name(name) for name in component_names]
#                     margin_row = None
#                     #print(f"component map is:{component_is_active_map}")
#                     for component_name in normalized_component_names:   
#                         query = f"SELECT `AGE`,`MARGIN` FROM AGE_MARGIN_WORKING_UAT WHERE  REPLACE(TRIM(UPPER(COMPONENT_NAME)), ' ', '') = '{component_name.upper()}' and PARTY_ID='{party_id}' "
#                         margin_row = ocr_db.execute_(query).to_dict(orient='records')
#                         logging.info(f"margin_row: {margin_row}")
#                         if margin_row:
#                             break
#                     for component_name in component_names: 
#                         #print(f"component name is:{component_name}")
#                         is_active=component_is_active_map.get(component_name,None)
#                         #print(f"is active is:{is_active}")
#                         if is_active=='1' or is_active=='None' or is_active=='' or is_active=='null' or is_active is None:
#                             #print(f"entered as active is 1..")
#                             if margin_row and value:
#                                 margin = margin_row[0]['MARGIN']
#                                 age = margin_row[0]['AGE']
                                
#                                 logging.info(f"age: {age}")
                                
#                                 if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                                     for row in data_dict['tab_view']['rowData']:
#                                         #logging.info(f"field_name: {row['fieldName']}")
#                                         #logging.info(f"key is: {key}")
#                                         if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
#                                             row['margin'] = margin
#                                             row['aging'] = age
#                             else:
#                                 pass
#                         # else:
#                         #     if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                         #         for row in data_dict['tab_view']['rowData']:
#                         #             if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
#                         #                 row['margin'] = ""
#                         #                 row['aging'] = ""
#                         else:
#                             #print(f"entered not active")
#                             if "tab_view" in data_dict and "rowData" in data_dict["tab_view"]:
#                                # print("[DEBUG] Found tab_view with rowData")
#                                 for row in data_dict["tab_view"]["rowData"]:
#                                 #    print(f"row in the in active is 0:{row}")
#                                     field = row.get("fieldName")  # safely get fieldName
#                                  #   print(f"[DEBUG] Checking row fieldName={field!r} against key={key!r}")
                                    
#                                     if not field:
#                                   #      print("[DEBUG] Skipping row because fieldName is missing/empty")
#                                         continue  # skip rows without fieldName

#                                     # normalize both sides for comparison
#                                     field_norm = field.replace(" &", "").replace(" ", "").lower()
#                                     key_norm = key.replace(" ", "").lower()

#                                    # print(f"[DEBUG] Normalized field={field_norm!r}, key={key_norm!r}")

#                                     if field_norm == key_norm:
#                                     #    print(f"[MATCH] Updating row for fieldName={field!r}")
#                                         row["margin"] = ""
#                                         row["aging"] = ""
#                                     else:
#                                         print(f"[NO MATCH] field={field_norm!r} != key={key_norm!r}")


                            
#             final_data_dict = {**data_dict_,**data_dict}
#             logging.info(f"final data dict: {final_data_dict}")
            
            
#             chunk_size = 4000 
#             value=json.dumps(final_data_dict)
#             logging.info(f"Updated JSON data: {final_data_dict}")

#             chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


#             sql = f"UPDATE ocr SET {column} = "

#             # Append each chunk to the SQL query
#             for chunk in chunks:
#                 sql += "TO_CLOB('" + chunk + "') || "

#             # Remove the last ' || ' and add the WHERE clause to specify the case_id
#             sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
#             print(f"The SQL is.................................:{sql}")  
#             ocr_db.execute_(sql)

#         try:
#             data = case_creation_cnt(case_id,tenant_id='hdfc')
#         except Exception as e:
#             logging.exception(f"Exception occured while updating the case ingestion count")  
#             pass  
#     except Exception as e:
#         logging.error("Unable to execute the python code:", e)

@register_method
def margin_for_extracted_fields(self, parameters):

    logging.info(f"Parameters got are {parameters}")

    # DB Setup
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction", **db_config)

    try:
        # ─────────────────────────────────────
        # STEP 1: Fetch PARTY_ID
        # ─────────────────────────────────────
        party_query = "SELECT PARTY_ID FROM OCR WHERE case_id = %s"
        party_res = ocr_db.execute_(party_query, params=[case_id])

        if (
            party_res is None 
            or party_res.empty
            or "PARTY_ID" not in party_res.columns
            or party_res["PARTY_ID"].isna().all()
        ):
            logging.error("PARTY_ID not found. Stopping execution.")
            return

        party_id = party_res["PARTY_ID"].iloc[0]
        logging.info(f"PARTY_ID fetched: {party_id}")

        # ─────────────────────────────────────
        # STEP 2: Load ACTIVE Component → Age & Margin
        # ─────────────────────────────────────
        margin_query = f"""
            SELECT 
    w.COMPONENT_NAME,
    w.AGE,
    w.MARGIN
FROM age_margin_working_uat w
JOIN component_master c
    ON TRIM(UPPER(CAST(w.COMPONENT_NAME AS VARCHAR2(4000))))
       =
       TRIM(UPPER(CAST(c.COMPONENT_NAME AS VARCHAR2(4000))))
WHERE 
    w.IS_ACTIVE = 1
    AND UPPER(TRIM(CAST(c.STATUS AS VARCHAR2(4000)))) = 'ACTIVE' AND w.PARTY_ID = '{party_id}'

        """
        margin_raw = ocr_db.execute_(margin_query).to_dict(orient="records")

        # Convert to easier dict → { COMPONENT_NAME: {"AGE": x, "MARGIN": y} }
        margin_data = {
            row["COMPONENT_NAME"]: {"AGE": row["AGE"], "MARGIN": row["MARGIN"]}
            for row in margin_raw
        }

        logging.info(f"Loaded Active Components: {list(margin_data.keys())}")

        # ─────────────────────────────────────
        # STEP 3: Process Columns
        # ─────────────────────────────────────
        columns = ["STOCKS", "DEBTORS", "CREDITORS", "ADVANCES"]

        for column in columns:
            try:
                query = f"SELECT `{column}` FROM OCR WHERE case_id = %s"
                result = ocr_db.execute_(query, params=[case_id]).to_dict(orient="records")

                if not result or not result[0][column]:
                    continue

                data_dict = json.loads(result[0][column])
                if not data_dict:
                    continue

                # ─────────────────────────────────────
                # STEP 4: Clear aging/margin for all rows
                # ─────────────────────────────────────
                if "tab_view" in data_dict and "rowData" in data_dict["tab_view"]:
                    for row in data_dict["tab_view"]["rowData"]:
                        if isinstance(row, dict):
                            row["aging"] = ""
                            row["margin"] = ""

                # ─────────────────────────────────────
                # STEP 5: Assign new values
                # ─────────────────────────────────────
                for row in data_dict.get("tab_view", {}).get("rowData", []):
                    if not isinstance(row, dict):
                        continue

                    field_name = row.get("fieldName")
                    if not field_name:
                        continue

                    # If matching component exists
                    if field_name in margin_data:
                        row["aging"] = margin_data[field_name]["AGE"]
                        row["margin"] = margin_data[field_name]["MARGIN"]

                # ─────────────────────────────────────
                # STEP 6: Save JSON back to OCR (CLOB)
                # ─────────────────────────────────────
                json_value = json.dumps(data_dict)
                chunks = [json_value[i:i + 4000] for i in range(0, len(json_value), 4000)]

                update_sql = (
                    f"UPDATE OCR SET {column} = " +
                    " || ".join([f"TO_CLOB('{chunk}')" for chunk in chunks]) +
                    f" WHERE case_id = '{case_id}'"
                )

                ocr_db.execute_(update_sql)
                logging.info(f"{column} updated successfully for case {case_id}")

            except Exception:
                logging.exception(f"Error while processing column: {column}")
                continue

        # ─────────────────────────────────────
        # STEP 7: Update ingestion count
        # ─────────────────────────────────────
        try:
            case_creation_cnt(case_id, tenant_id="hdfc")
        except:
            logging.exception("Failed to update case ingestion count")

    except Exception as e:
        logging.exception(f"Error in margin_for_extracted_fields: {e}")

        




# def margin_for_extracted_fields(self, parameters):
#     logging.info(f"Parameters got are {parameters}")
#     db_config["tenant_id"] = self.tenant_id
#     case_id = self.case_id
#     ocr_db = DB("extraction", **db_config)
#     query = f"SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
#     params = [case_id]
#     result_ = ocr_db.execute_(query, params=params)
#     party_id = result_['PARTY_ID'][0]
#     qry=f"""select is_active,component_name from age_margin_working_uat WHERE CAST(party_id AS VARCHAR2(4000))='{party_id}'"""
#     result=ocr_db.execute_(qry).to_dict(orient="records")
#     component_is_active_map = {row['COMPONENT_NAME']: row['IS_ACTIVE'] for row in result}
#     #is_active=result_['IS_ACTIVE'][0]
#     try:
#         columns = ["STOCKS", "DEBTORS", "CREDITORS","ADVANCES"]
#         for column in columns:
#             query = f"SELECT `{column}` FROM `OCR` WHERE case_id = %s"
#             params = [case_id]
#             result = ocr_db.execute_(query, params=params).to_dict(orient='records')
#             query = f"SELECT `PARTY_ID` FROM `OCR` WHERE case_id = %s"
#             params = [case_id]
#             result_ = ocr_db.execute_(query, params=params)
#             party_id = result_['PARTY_ID'][0]
#             logging.info(f"result for {column}: {result}")

#             if result:  
#                 df = result[0][column]  
#                 logging.info(f"df for {column}: {df}")
#                 data_dict = json.loads(df)
#             else:
#                 data_dict={}
                
#             key_component_map = {"Raw Materials":["RAW MATERIALS INSURED"],"Finished Goods":["FINISHED GOODS INSURED"],"Total Stock":["TOTAL STOCKS INSURED"],"Work in Process":["WORK IN PROGRESS INSURED"],"Stores and Spares":["STORES & SPARES INSURED"],"Stock in Transit":["STOCK IN TRANSIT"],"Consumable and Spares":["CONSUMABLE SPARES INSURED"],"goods in transist":["GOODS IN TRANSIT INSURED"],"Domestic Stock":["DOMESTIC STOCKS INSURED"],"Export Stock":["EXPORT STOCKS INSURED"],"Sales":["SALES"],"Total Debtors":["DEBTORS","RECEIVABLES","BOOK DEBTS"],"Debtors <30 days":["Debtors <30 days","BOOK DEBTS UPTO 30 DAYS"],"Debtors <60 days":["Debtors <60 days","BOOK DEBTS UPTO 60 DAYS"],"Debtors <90 days":["Debtors <90 days","BOOK DEBTS UPTO 90 DAYS"],"Debtors <120 days":["Debtors <120 days","BOOK DEBTS UPTO 120 DAYS"],"Debtors <150 days":["Debtors <150 days","BOOK DEBTS UPTO 150 DAYS"],"Debtors <180 days":["Debtors <180 days","BOOK DEBTS UPTO 180 DAYS","EXPORT DEBTORS<180 DAYS INSURED"],"Debtors - Exports":["BOOK DEBTS -EXPORTS"],"Debtors - Domestic":["BOOK DEBTS -DOMESTIC"],"Debtiors of Group Companies":["DEBTORS OF GROUP COMPANIES"],"Receivables":["RECEIVABLES"],"Domestic receivables":["DOMESTIC RECEIVABLES"],"Export receivables":["EXPORT RECEIVABLES"],"Total Creditors":["CREDITORS","Trade Creditors"],"Creditors PC":["CREDITORS (PC)"],"Unpaid Stocks":["UNPAID STOCKS"],"DALC":["LESS : DALC"],"Advances paid to suppliers":["ADD : ADVANCE TO SUPPLIER"],"Debtors PC":["DEBTORS (PC)"]}
            
#             data_dict_ = {}

#             for key, value in data_dict.items():       
#                 if not value:
#                     if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                         for row in data_dict["tab_view"]["rowData"]:
#                             field = row.get("fieldName")  # safely get fieldName
#                             #print(f"[DEBUG] Checking row fieldName={field!r} against key={key!r}")

#                             if not field:
#                                 #print("[DEBUG] Skipping row because fieldName is missing/empty")
#                                 continue  

#                                     # normalize both sides for comparison
#                             field_norm = field.replace(" &", "").replace(" ", "").lower()
#                             key_norm = key.replace(" ", "").lower()

#                             #print(f"[DEBUG] Normalized field={field_norm!r}, key={key_norm!r}")

#                             if field_norm == key_norm:
#                                 #print(f"[MATCH] Updating row for fieldName={field!r}")
#                                 row["margin"] = ""
#                                 row["aging"] = ""
#                             else:
#                                 print(f"[NO MATCH] field={field_norm!r} != key={key_norm!r}")

#             # Iterate over each key-value pair in the data dictionary
#                 else:
#                     # Get the component name and margin from the mapping and database
#                     component_names = key_component_map.get(key, [])
#                     component_names = [component.strip() for component in component_names]
#                     normalized_component_names = [normalize_component_name(name) for name in component_names]
#                     margin_row = None
#                     #print(f"component map is:{component_is_active_map}")
#                     for component_name in normalized_component_names:   
#                         query = f"SELECT `AGE`,`MARGIN` FROM AGE_MARGIN_WORKING_UAT WHERE  REPLACE(TRIM(UPPER(COMPONENT_NAME)), ' ', '') = '{component_name.upper()}' and PARTY_ID='{party_id}' "
#                         margin_row = ocr_db.execute_(query).to_dict(orient='records')
#                         logging.info(f"margin_row: {margin_row}")
#                         if margin_row:
#                             break
#                     for component_name in component_names: 
#                         #print(f"component name is:{component_name}")
#                         is_active=component_is_active_map.get(component_name,None)
#                         #print(f"is active is:{is_active}")
#                         if is_active=='1' or is_active=='None' or is_active=='' or is_active=='null' or is_active is None:
#                             #print(f"entered as active is 1..")
#                             if margin_row and value:
#                                 margin = margin_row[0]['MARGIN']
#                                 age = margin_row[0]['AGE']
                                
#                                 logging.info(f"age: {age}")
                                
#                                 if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                                     for row in data_dict['tab_view']['rowData']:
#                                         #logging.info(f"field_name: {row['fieldName']}")
#                                         #logging.info(f"key is: {key}")
#                                         if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
#                                             row['margin'] = margin
#                                             row['aging'] = age
#                             else:
#                                 pass
#                         # else:
#                         #     if 'tab_view' in data_dict and 'rowData' in data_dict['tab_view']:
#                         #         for row in data_dict['tab_view']['rowData']:
#                         #             if row['fieldName'].replace(' &', '').replace(' ', '').lower() == key.replace(' ', '').lower():
#                         #                 row['margin'] = ""
#                         #                 row['aging'] = ""
#                         else:

#                             clear_aging_margin_fields(data_dict,component_name)
#                             #print(f"entered not active")
#                             # if "tab_view" in data_dict and "rowData" in data_dict["tab_view"]:
#                             #    # print("[DEBUG] Found tab_view with rowData")
#                             #     for row in data_dict["tab_view"]["rowData"]:
#                             #     #    print(f"row in the in active is 0:{row}")
#                             #         field = row.get("fieldName")  # safely get fieldName
#                             #         if field.lower() == "tab_view":
#                             #             continue
#                             #      #   print(f"[DEBUG] Checking row fieldName={field!r} against key={key!r}")
                                    
#                             #         if not field:
#                             #       #      print("[DEBUG] Skipping row because fieldName is missing/empty")
#                             #             continue  # skip rows without fieldName

#                             #         # normalize both sides for comparison
#                             #         field_norm = field.replace(" &", "").replace(" ", "").lower()
#                             #         key_norm = key.replace(" ", "").lower()

#                             #        # print(f"[DEBUG] Normalized field={field_norm!r}, key={key_norm!r}")

#                             #         if field_norm == key_norm:
#                             #             print(f"[MATCH] Updating  brefore row for fieldName={field!r}")
#                             #             print(f"brfore row updatings is:{row}")
#                             #             row["margin"] = ""
#                             #             row["aging"] = ""
#                             #             print(f"after row updatings is:{row}")
#                             #         else:
#                             #             print(f"[NO MATCH] field={field_norm!r} != key={key_norm!r}")


                            
#             final_data_dict = {**data_dict_,**data_dict}
#             logging.info(f"final data dict: {final_data_dict}")
            
            
#             chunk_size = 4000 
#             value=json.dumps(final_data_dict)
#             logging.info(f"Updated JSON data: {final_data_dict}")

#             chunks = [value[i:i+chunk_size] for i in range(0, len(value), chunk_size)]


#             sql = f"UPDATE ocr SET {column} = "

#             # Append each chunk to the SQL query
#             for chunk in chunks:
#                 sql += "TO_CLOB('" + chunk + "') || "

#             # Remove the last ' || ' and add the WHERE clause to specify the case_id
#             sql = sql[:-4] + f"WHERE case_id = '{case_id}'"
#             print(f"The SQL is.................................:{sql}")  
#             ocr_db.execute_(sql)

#         try:
#             data = case_creation_cnt(case_id,tenant_id='hdfc')
#         except Exception as e:
#             logging.exception(f"Exception occured while updating the case ingestion count")  
#             pass  
#     except Exception as e:
#         logging.error("Unable to execute the python code:", e)



# def clear_aging_margin_fields(data, fields_to_clear):
#     """
#     Recursively clear 'aging' and 'margin' fields for specific stock field names.
    
#     Args:
#         data: JSON data (dict, list, or primitive)
#         fields_to_clear: List of field names to clear aging/margin for
    
#     Returns:
#         Modified data with cleared fields
#     """
#      # Default fields that should have aging and margin cleared
#     # Handle different data types
#     if isinstance(data, dict):
#         result = {}
#         for key, value in data.items():
#             if key == "rowData" and isinstance(value, list):
#                 # Process rowData array
#                 result[key] = []
#                 for row in value:
#                     if isinstance(row, dict):
#                         new_row = copy.deepcopy(row)
                        
#                         # Check if this row's fieldName matches our criteria
#                         field_name = row.get("fieldName", "")
#                         if field_name in fields_to_clear:
#                             # Clear aging and margin fields
#                             if "aging" in new_row:
#                                 new_row["aging"] = ""
#                             if "margin" in new_row:
#                                 new_row["margin"] = ""
                        
#                         # Recursively process nested structures
#                         for row_key, row_value in new_row.items():
#                             if row_key not in ["aging", "margin"]:
#                                 new_row[row_key] = clear_aging_margin_fields(row_value, fields_to_clear)
                        
#                         result[key].append(new_row)
#                     else:
#                         result[key].append(clear_aging_margin_fields(row, fields_to_clear))
#             else:
#                 # Recursively process other values
#                 result[key] = clear_aging_margin_fields(value, fields_to_clear)
#         return result
    
#     elif isinstance(data, list):
#         return [clear_aging_margin_fields(item, fields_to_clear) for item in data]
    
#     else:
#         # Return primitive values as-is
#         return data
 


def clear_aging_margin_fields_code(data, fields_to_clear):
    fields_to_clear_lower = [f.lower().strip() for f in fields_to_clear]
    print(f"fields_to_clear_lower is :{fields_to_clear_lower}")
    if isinstance(data, dict):
        # Check if this dict has a 'fieldName' to match
        field_name = data.get("fieldName")
        if isinstance(field_name, str) and field_name.strip().lower() in fields_to_clear_lower:
            data["aging"] = ""
            data["margin"] = ""

        # Recursively update all dict values
        for k, v in data.items():
            data[k] = clear_aging_margin_fields_code(v, fields_to_clear)
        return data

    elif isinstance(data, list):
        return [clear_aging_margin_fields_code(v, fields_to_clear) for v in data]

    else:
        # Normalize None / NaN / inf
        if data is None:
            return ""
        if isinstance(data, float) and (math.isnan(data) or math.isinf(data)):
            return ""
        return data

@register_method
def array_data_append(self,parameters):
    logging.info(f"Parameters got are {parameters}")
    input_column=parameters['input_column']
    output_column=parameters['output_column']
    #logging.info(f'input_column = {input_column}')
    #logging.info(f'output_column = {output_column}')

    try:
        input_column = json.loads(input_column)
        output_column = json.loads(output_column)if output_column is not None else []
        result = input_column + output_column if output_column is not None else input_column
        logging.info(f'result is  = {result}')
        result = json.dumps(result)
        return result
        
        

    except Exception as e:
            logging.error("Invalid data provided.",exc_info=True)
            return None
    

@register_method
def do_allow_4_char(self, parameters):
    logging.info(f"Parameters received: {parameters}")
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction", **db_config)
    try:
        query = "SELECT `STOCK_DOC_YEAR` FROM `OCR` WHERE case_id = %s"
        params = [case_id]
        result_ = ocr_db.execute_(query, params=params)
        
        STOCK_DOC_YEAR = result_['STOCK_DOC_YEAR'][0]  
        logging.info(f"Fetched STOCK_DOC_YEAR: {STOCK_DOC_YEAR}" and type is {type (STOCK_DOC_YEAR)})
        STOCK_DOC_YEAR = str(STOCK_DOC_YEAR)
        
        if len(STOCK_DOC_YEAR) > 4:
            logging.warning("Value exceeds 4 characters, returning ''")
            return False
        else:
            logging.info("Value is 4 characters or less, returning the value")
            return True
    
    except Exception as e:
        logging.error("An error occurred while processing do_allow_4_char")
        logging.error(e)
        return ''
    
@register_method 
def update_columns_data(self, parameters):
    """
    Updates the data in specified columns after applying recursive cleaning.
    """
    logging.info(f"Parameters received: {parameters}")
    db_config["tenant_id"] = self.tenant_id
    case_id = self.case_id
    ocr_db = DB("extraction", **db_config)

    # Columns to process
    columns = ["STOCKS", "DEBTORS", "CREDITORS", "ADVANCES", "TERM", "WORKING_CAPITAL_OCR"]

    def recursive_clean(data):
        """
        Recursively cleans the data. Applies regex cleaning only to the values, not to the keys.
        """
        if isinstance(data, dict):
            # Recursively clean values in the dictionary, keys remain untouched
            return {key: recursive_clean(value) for key, value in data.items()}
        elif isinstance(data, list):
            # Recursively clean items in the list
            return [recursive_clean(item) for item in data]
        elif isinstance(data, str):
            # Apply regex only to clean the value (not keys)
            return re.sub(r"[^a-zA-Z0-9., ]", "", data)  # Adjust regex as necessary
        return data  # Return original data if not a string, dict, or list

    for column in columns:
        try:
            # Fetch the data from the OCR table
            query = f"SELECT `{column}` FROM `OCR` WHERE case_id = %s"
            params = [self.case_id]
            result = ocr_db.execute_(query, params=params).to_dict(orient='records')

            # Extract the column data
            column_data = result[0].get(column, None) if result else None

            if column_data:
                # Parse JSON if column_data is a string
                if isinstance(column_data, str):
                    try:
                        column_data = json.loads(column_data)
                    except json.JSONDecodeError:
                        logging.warning(f"Invalid JSON format for column {column} in case_id {self.case_id}")
                        continue

                # Apply cleaning to the column data
                cleaned_column_data = recursive_clean(column_data)
                logging.info(f"Cleaned data for {column}: {cleaned_column_data}")

                # Update the OCR table
                update_query = f"UPDATE `OCR` SET `{column}` = %s WHERE case_id = %s"
                update_params = [json.dumps(cleaned_column_data), self.case_id]
                ocr_db.execute_(update_query, params=update_params)
                logging.info(f"Updated {column} in OCR table for case_id {self.case_id}")
            else:
                logging.warning(f"No data found for column {column} in case_id {self.case_id}")
        except Exception as e:
            logging.error(f"Error processing column {column} for case_id {self.case_id}: {e}")


@register_method 
def biz_rules_change_data(self,parameters):
    try:
        #print("paraams",parameters)
        username=parameters.get("username",None)

        #print("**username",username)
        case_id = self.case_id

        db_config["tenant_id"] = self.tenant_id
        current_datetime=datetime.now(pytz.timezone(tmzone))
        formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')


        ocr_db = DB("extraction", **db_config)
        queues_db= DB("queues", **db_config)
        group_access_db= DB("group_access", **db_config)
        query = f"SELECT `BIZ_RULES_LIST` FROM `OCR` WHERE case_id = %s"
        params = [case_id]
        result = ocr_db.execute_(query, params=params).to_dict(orient='records')
        column_data = result[0].get('BIZ_RULES_LIST', None) if result else None
        reapply_columns=["customer_name", "stock_doc_month", "stock_doc_year","customer_category","bank_share","due_date"]
        reapply_fields = ', '.join(f'`{field}`' for field in reapply_columns)
        logging.info(f"reapply_fileds {reapply_fields}")
        

        # selecting fields that may changes after reapply
        query =f"select {reapply_fields}  from ocr where `case_id`= %s"
        params = [case_id]
        query_result = ocr_db.execute_(query, params=params).to_dict(orient='records')
        logging.info(f"query result {query_result}")

        query_result2={key.lower(): value for key, value in query_result[0].items()}

        changed_fields_list=[]
        old_values_dict={}
        new_values_dict={}
        if column_data:
            column_data=json.loads(column_data)
            for field in reapply_columns:
                previous_data=column_data.get(field,None)
                current_data=query_result2.get(field,None)
                logging.info(f"previous_data  {previous_data}-----{current_data}")
                if previous_data and previous_data!=current_data:
                    changed_fields_list.append(field)
                    old_values_dict[field]=previous_data
                    new_values_dict[field]=current_data
                
        
        new_values_str = ", ".join(
                    [
                        f"{field.replace('_', ' ').title()}: {value}"  
                        for field, value in new_values_dict.items()
                    ]
                    )
        old_values_str=", ".join(
                    [
                        f"{field.replace('_', ' ').title()}: {value}"  
                        for field, value in old_values_dict.items()
                        ]
                    )
        changed_fields_str = ", ".join(
                    [
                        f"{value.replace('_', ' ').title()}"  
                        for value in changed_fields_list
                    ]
                    )
        
        logging.info(f"changed_fields_str- {changed_fields_str}")

        
                    

        
        
        if changed_fields_str:

            # Getting user role from active_directory
            query =f"select `role` from active_directory where `username`= %s"
            params = [username]
            query_result = group_access_db.execute_(query, params=params).to_dict(orient='records')
            #print("***query result",query_result)
            role=query_result[0].get("role",None) if query_result else None
            
            if role.lower()=='maker':
                update_by_maker='maker_id'
                datatime_for='maker_datetime'
            else:
                update_by_maker='checker_id'
                datatime_for='checker_datetime'

            # inserting record in audit table 
            query=f"""
            INSERT INTO audit_report_case (changed_fields, old_values, new_values,case_id,{update_by_maker},{datatime_for},category)
            VALUES ('{changed_fields_str}','{old_values_str}','{new_values_str}','{case_id}','{username}',TO_TIMESTAMP('{formatted_datetime}', 'YYYY-MM-DD HH24:MI:SS'),'Change in Transaction')"""
            ocr_db.execute_(query)

            return {"flag":True,"message":"Changed updated in audit"}
        else:
            return {"flag":True,"message":"No changes to update in audit"}


    except Exception as e:
        return {"flag":False,"message":f"Error updating audit {e}"}
        




