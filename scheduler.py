"""
@author: Gopi Teja
Created Date: 2022-11-23
"""

import json
import schedule
import requests
import time
import os
from datetime import datetime, timedelta
import logging
import multiprocessing
import concurrent.futures
from db_utils import DB
from datetime import datetime
import pytz

from datetime import datetime, timedelta,date
from time import time as tt
from dateutil import parser

from pathlib import Path
from cryptography.fernet import Fernet
from zoneinfo import ZoneInfo

from ace_logger import Logging
from cryptography.fernet import Fernet
FERNET_KEY="cvo_Q-OsOntFT0DKLKVWFXGmooal7NDakdgeVRsBUDE="
logging = Logging()

# Database configuration
db_config = {
    'host': os.environ['HOST_IP'],
    'user': os.environ['LOCAL_DB_USER'],
    'password': os.environ['LOCAL_DB_PASSWORD'],
    'port': os.environ['LOCAL_DB_PORT']
}
# Write an function to hit the folder monitor flow
def start_folder_monitor(tenant_id, port, workflow_name):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"Current Time:{current_time}")
    print(f"#### Started FM of {tenant_id}")
    host = os.environ.get('SERVER_IP')
    api_params = {}
    request_api = f'http://{host}:{port}/rest/engine/default/process-definition/key/{workflow_name}/start'
    headers = {'Content-type': 'application/json; charset=utf-8'}
    print(f"#### Hitting the camunda api of {request_api}")
    response = requests.post(request_api, json=api_params, headers=headers,verify=False)
    response_dict = json.loads(response.text)
    print(f"#### {tenant_id} FM Response Dict", response_dict)

def load_license():
    fernet = Fernet(FERNET_KEY)
    with open("/var/www/scheduler/app/license.txt", "rb") as f:
        encrypted_data = f.read()

    decrypted = fernet.decrypt(encrypted_data)
    return json.loads(decrypted.decode())



def hit_get_files_from_sftp():
        print("Hitting get_files_from_sftp route...")
        host = 'foldermonitor'
        port = 443
        request_api = f'https://{host}:{port}/get_files_from_sftp'

        # Headers and payload
        headers = {'Content-type': 'application/json; charset=utf-8'}
        payload = {}  # Add any necessary data for the route if required

        try:
            response = requests.post(request_api, json=payload, headers=headers, verify=False)
            response_data = response.json()
            print(f"Response from get_files_from_sftp: {response_data}")
        except Exception as e:
            print(f"Error hitting get_files_from_sftp: {e}")

### For Multi Processing of a file 
def move_predo_do(tenant_id):
    # LICENSE_INFO = {
    #     "TENANT": "HDFC",
    #     "LICENSE_NUMBER": "HDFC-3886-6403-0934",
    #     "START_DATE": "06-12-2024",   # DD-MM-YYYY
    #     "END_DATE": "05-12-2025"      # DD-MM-YYYY
    # }

    # # Parse end date
    # end_date_str = LICENSE_INFO["END_DATE"]
    # end_date = datetime.strptime(end_date_str, "%d-%m-%Y")

    # # Current IST
    # ist = pytz.timezone('Asia/Kolkata')
    # now_ist = datetime.now(ist).replace(tzinfo=None)

    # # Stop workflow movement if expired
    # if now_ist > end_date:
    #     print(" License expired — workflow processing disabled. No cases will be moved.")
    #     return
    try:
        print(f"move_predo_do: #### Started Moving from case creation to Seg of {tenant_id}")
        start = tt()
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        # query = "select case_id, task_id, last_updated from queue_list where queue='case_creation' and case_creation_status IS NULL order by last_updated"
        query = "select case_id, task_id, last_updated from queue_list where queue='case_creation' and case_creation_status IS NULL FETCH FIRST 4 ROWS ONLY"
        predo_data = queue_db.execute_(query)

        if not predo_data.empty:
            # marking all the cases which picked here in this bucket as Picked to avoid duplicate picking
            cases = predo_data['case_id'].to_list()
            print(f"## Cases are {cases}")
            if len(cases) == 1:
                case=cases[0]
                query = f"UPDATE `queue_list` SET `case_creation_status` = CASE WHEN count_of_tries >= 3 THEN 'Failed' ELSE 'Picked' END WHERE `case_id` = '{case}'"
                #query=f"UPDATE `queue_list` SET `case_creation_status` ='Picked' WHERE `case_id` ='{case}' "
            elif len(cases) > 1:
                cases=tuple(cases)
                query = f""" UPDATE `queue_list` SET `case_creation_status` = CASE WHEN count_of_tries >= 3 THEN 'Failed' ELSE 'Picked' END ,count_of_tries = count_of_tries + 1 WHERE case_id IN {cases}"""
            queue_db.execute_(query)
            status_check_query = f"""
                                    SELECT case_id, case_creation_status
                                    FROM queue_list
                                    WHERE case_id IN {tuple(cases) if len(cases) > 1 else f"('{case}')"}
                                """
            updated_statuses = queue_db.execute_(status_check_query)
    
            # Filter cases where the status is not 'Failed'
            valid_cases = updated_statuses[
                updated_statuses['case_creation_status'] != 'Failed'
            ]

            if not valid_cases.empty:
                print(f"####: Valid cases for processing - {valid_cases['case_id'].to_list()}")
                valid_predo_data = predo_data[
                    predo_data['case_id'].isin(valid_cases['case_id'].to_list())
                ]
                move_predo_do_(tenant_id, valid_predo_data)
            else:
                print(f"####: No valid cases to process for tenant_id {tenant_id}")
        else:
            print(f"#### No cases for moving from {tenant_id}")
    except Exception as e:
        print("something went wrong while processing the cases", e)    


def move_predo_do_chunk(tenant_id, chunk):
    try:
        print(f"Started Moving Chunks {len(chunk)} records of tenant_id: {tenant_id}")
        start = tt()
        db_config['tenant_id'] = tenant_id
        queue_db = DB('queues', **db_config)
        headers = {'Content-type': 'application/json; charset=utf-8'}
        processed_cases = []
        failed_cases = []
        for index, row in chunk.iterrows():
            case_id = row['case_id']
            task_id = row['task_id']
            query = f"UPDATE `process_queue` set `accept_flag`= 1 where case_id='{case_id}'"
            queue_db.execute_(query)

            api_params = {"variables": {"button": {"value": 'Accept'}},'case_id': case_id, 'tenant_id': tenant_id}
            ## here camundaworkflow is the container name
            url = f'http://camundaworkflow:8080/rest/engine/default/task/{task_id}/complete'       
            print(f"move_predo_do_chunk: {datetime.now()} : hitting url for case_id: {case_id} to complete task: {url}")
            response = requests.post(url, json=api_params, headers=headers)
            response_json = response.json
            print(f"move_predo_do_chunk: response of {case_id} complete task: {response_json}")
            if response.status_code == 500:
                print(f'in if condition of 500 response')
                message = json.loads(response.content)
                message = message['message'].split(':')[0]
                #queue_db.execute_(f"update process_queue set accept_flag=0,error_logs='Failed in Predo-do: {message}' where case_id='{case_id}'")
                query_1=f"UPDATE `process_queue` set accept_flag=0, error_logs='{message}' where case_id='{case_id}'"
                # params_1 = [0,f'Failed in case_creation: {message}', case_id]
                # params_1 = [0,message, case_id]
                queue_db.execute(query_1)
                
                query_error = f" UPDATE `queue_list` SET `case_creation_status` = CASE WHEN count_of_tries >= 3 THEN 'Failed' ELSE NULL END, count_of_tries = count_of_tries + 1, error_logs = '{message}' WHERE case_id = '{case_id}'"
                queue_db.execute_(query_error)
                #query_2=f"UPDATE `queue_list` set case_creation_status=NULL where case_id='{case_id}'"
                count_query = f"SELECT count_of_tries FROM `queue_list` WHERE case_id = '{case_id}'"
                count_of_tries = queue_db.execute_(count_query)
                print(f'count_of_tries-----------{count_of_tries}')
                count_of_tries=count_of_tries['count_of_tries'].iloc[0]
                print(f'count_of_tries-----------{count_of_tries}')
                # if count_of_tries >= 3:
                #     return_data= {
                #         "case_id": case_id,
                #         "template": "case_processing_failure",
                #         "tenant_id": tenant_id
                #         }
                    
                #     host = 'emailtriggerapi'
                #     port = 80
                #     route = 'send_email_auto'
                #     print(f'Hitting URL: http://{host}:{port}/{route} for email_triggering')
                #     # logging.debug(f'Sending Data: {value_predict_params}')
                #     headers = {'Content-type': 'application/json; charset=utf-8',
                #             'Accept': 'text/json'}
                #     response = requests.post(
                #         f'http://{host}:{port}/{route}', json= return_data, headers=headers)
                    
                #     response=response.json()
                #     print(f" ####### Response Received from email_trigger is {response}")
                #     print(f"move_predo_do_chunk: File processing {case_id} is failed")


                # failed_cases.append(case_id)
                # print(f"move_predo_do_chunk: Camunda sent 500 the message is: {message}")
            elif response.status_code == 200:
                processed_cases.append(case_id)
                print(f"File processed {case_id} successfully")
                # queue_db.execute(f"update process_queue set accept_flag=1 where case_id='{case_id}'")
            elif response.status_code == 204:
                processed_cases.append(case_id)
                print(f"File processed {case_id} successfully")
            else:
                print(f"Some other status code is returning other than expected, status code: {response.status_code}")
                query_2=f"UPDATE `queue_list` set case_creation_status=NULL where case_id='{case_id}'"
                queue_db.execute_(query_2)
                query_reset = f"UPDATE `queue_list` SET `count_of_tries` = 0 WHERE `case_id` = '{case_id}'"
                queue_db.execute_(query_reset)
                failed_cases.append(case_id)
                print(f"move_predo_do_chunk: File processing {case_id} is failed")
        return processed_cases, failed_cases
    except Exception as e:
        print(f"Something went wrong while processing the chunk .. {e}.. {case_id}")

def move_predo_do_(tenant_id, predo_data):
    try:
        chunk_size = 1  # Define chunk size here
        chunks = [predo_data[i:i+chunk_size] for i in range(0, len(predo_data), chunk_size)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for chunk in chunks:
                print(f"## Chunk got is {chunk}")
                future = executor.submit(move_predo_do_chunk, tenant_id, chunk)
                futures.append(future)
                
            # Wait for any of the futures to complete and start new tasks if any are pending
            # while True:
            completed = concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_COMPLETED)
            for future in completed.done:
                futures.remove(future)
        print("All chunks processed successfully.")
            
    except Exception as e:
        print("something went wrong while processing the cases", e)

def process_pending_cases(tenant_id):
    try:
        db_config['tenant_id']= tenant_id
        queue_db = DB('queues',**db_config)
        today = datetime.now().strftime("%Y-%m-%d 00:00:00")
        query = f"select ql.case_id,ql.task_id,ql.last_updated,ql.count_of_tries from queue_list ql join process_queue pq on ql.case_id=pq.case_id where ql.queue='case_creation' and ql.case_creation_status='Failed' and ql.count_of_tries>=3 and ql.count_of_tries<4 and (pq.created_date<TO_TIMESTAMP('{today}', 'YYYY-MM-DD HH24:MI:SS') or pq.created_date>=TO_TIMESTAMP('{today}', 'YYYY-MM-DD HH24:MI:SS')) order by pq.created_date asc FETCH FIRST 1 ROWS ONLY"
        res_df = queue_db.execute_(query)
        if not res_df.empty:
            case_id, tries = res_df.iloc[0]['case_id'], res_df.iloc[0]['count_of_tries']  # pick one case only
            query = f"update queue_list set case_creation_status='Picked' where case_id='{case_id}'"
            queue_db.execute_(query)
            if tries < 4:
                move_predo_do_(tenant_id, res_df)
            else:
                print(f"Skipping case {case_id}, reached max tries = {tries}")
        else:
            print("No cases to process")
            pass
    except Exception as e:
        logging.info(f"Error in processing pending cases: {e}")

def run_pending_cases(tenant_id):
    try:
        now = datetime.now()
        if now.hour >= 19:
            process_pending_cases(tenant_id)
    except Exception as e:
        logging.info(f"Error in run_pending_cases: {e}")

def uam_dormancy_scheduler(tenant_id):

    db_config['tenant_id'] = tenant_id
    db = DB('group_access', **db_config)
    try:
        dormant_rules_query = f"SELECT first_login_day, login_day,dormant_status from dormant_rules"
        dormant_rules = db.execute_(dormant_rules_query)

        first_login_day_limit = dormant_rules['first_login_day'].iloc[0]
        login_day_limit = dormant_rules['login_day'].iloc[0]
        dormant_status_users=json.loads(dormant_rules['dormant_status'].iloc[0])
        logging.info(f"first_login_day_limit :{first_login_day_limit},{login_day_limit},{dormant_status_users}")
        # Fetch data from active_directory
        act_dir_query = f"SELECT username, created_date, last_updated, status, previous_status FROM active_directory"
        act_dir = list(db.execute_(act_dir_query).to_dict(orient='records'))

        # Fetch data from live_sessions
        live_sess_query = f"SELECT last_request_new, user_ FROM live_sessions"
        live_sess = list(db.execute_(live_sess_query).to_dict(orient='records'))

        userss = [entry['USER_'] for entry in live_sess]

        logging.info("Data fetched from database.")
        live_sess_query = f"SELECT status, user_ FROM live_sessions"
        live_sess_active = list(db.execute_(live_sess_query).to_dict(orient='records'))
    except Exception as e:
        logging.info("Error fetching data from the database:", e)

    try:
        # Format live session last_request dates
        for entry in live_sess:
            if entry['last_request_new']:
                entry['last_request_new'] = entry['last_request_new'].strftime('%Y-%m-%d %H:%M:%S')
        logging.info("Live sessions formatted:", live_sess)

        # Prepare a dictionary for fast lookup
        live_sess_dict = {entry['user_']: entry['last_request_new'] for entry in live_sess}
        
        live_sess_active_dict = {entry['user_']: entry['status'] for entry in live_sess_active}

        # Get system date
        sys_date = (datetime.now()+timedelta(hours=5 , minutes=30)).date()

        # Initialize results
        dormant_users = []
        def parse_date(date_str):
            """Parses a string into a date object (without time)."""
            if isinstance(date_str, datetime):
                return date_str.date()  # Convert to date only
            try:
                return parser.parse(date_str).date() if date_str else None  # Extract date only
            except Exception as e:
                logging.info(f"Error parsing date {date_str}: {e}")
                return None

        # Process each user in the active directory
        for user in act_dir:
            username = user.get('USERNAME')
            created_date = parse_date(user.get('CREATED_DATE'))
            updated_date = parse_date(user.get('LAST_UPDATED'))
            status = user.get('STATUS')
            previous_status = user.get('PREVIOUS_STATUS')
            # if live_sess_active_dict.get(username)=='active':
            #     continue
            if status=='delete' or status=='disable':
                continue
            # Parse previous_status JSON

            try:
                parsed_previous_status = json.loads(previous_status) if previous_status else []
            except Exception as e:
                logging.info(f"Error parsing previous_status for {username}: {e}")
                parsed_previous_status = []
            try:
                # Get last login date from live sessions
                last_login_date = parse_date(live_sess_dict.get(username))

                # Determine the most recent activity date
                # valid_dates = [date for date in [created_date, updated_date, last_login_date] if date]
                # recent_activity_date = max(valid_dates) if valid_dates else None

                # Calculate inactivity period
                # if recent_activity_date:
                #     inactive_days = (sys_date - recent_activity_date).days
                # else:
                #     inactive_days = (sys_date - created_date).days if created_date else float('inf')

                # logging.info(f"User: {username}, Inactive Days: {inactive_days}")

                # Check dormancy conditions

                # Condition 1: User is created and first_login_day_limit days have passed since creation
                if created_date and (sys_date - created_date).days >= first_login_day_limit and username not in userss:
                    dormant_users.append(username)
                    logging.info(f"User '{username}' is now dormant as he was not login within dormancy period from created date.")

                # Condition 2: Last login was more than login_day_limit days 
                elif last_login_date and (sys_date - last_login_date).days >= login_day_limit:
                    dormant_users.append(username)
                    logging.info(f"User '{username}' is now in dormant as he was not login within dormancy period from last login date.")

                # Condition 3: User is created and locked on the same day, has not logged in
                elif status == "lock" and created_date == updated_date and ((sys_date - updated_date).days >= login_day_limit or (sys_date - updated_date).days >= first_login_day_limit):
                    dormant_users.append(username)
                    logging.info(f"User '{username}' created and locked on the same day is now dormant.")

                # Condition 4: Reactivated dormant user without login on the activation day
                elif parsed_previous_status[-1] == 'dormant' and (sys_date - updated_date).days <= 1:
                    dormant_users.append(username)
                    logging.info(f"User '{username}' is now in dormant as he was not login on the day of reactivation")
                if username in dormant_status_users and status == "lock" and (sys_date - last_login_date).days <= 1:
                    dormant_users.append(username)
                if status=='dormant' and username not in dormant_users:
                    dormant_users.append(username)
            except Exception as e:
                logging.info(f"Error : {e}")
        dormant_users_update=f"update dormant_rules set dormant_status='{json.dumps(dormant_users)}' where id=1"
        db.execute_(dormant_users_update)
        if dormant_users:
            username_str = "', '".join(dormant_users)
            query = f"""
                UPDATE active_directory
                SET status = 'dormant'
                WHERE username IN ('{username_str}')
            """
            db.execute_(query)
            logging.info(f"Users marked as dormant: {dormant_users}")
        else:
            logging.info("No users to update.")
    except Exception as e:
        logging.info("Error processing dormancy logic:", e)



def hit_get_files_from_sftp_masters():
    print("Hitting get_files_from_sftp_masters...")
    host = 'masterupload'
    port = 443
    request_api = f'https://{host}:{port}/get_files_from_sftp_masters'

    # Headers and payload
    headers = {'Content-type': 'application/json; charset=utf-8'}
    payload = {}  # Add any necessary data for the route if required

    try:
        response = requests.post(request_api, json=payload, headers=headers, verify=False)
        response_data = response.json()
        print(f"Response from get_files_from_sftp_masters: {response_data}")
    except Exception as e:
        print(f"Error hitting get_files_from_sftp_masters: {e}")

def hit_run_business_rules(tenant_id):
        print("Hitting hit_run_business_rules route...")

        host = 'businessrules'
        port = 443
        route = 'run_business_rule'
        request_api=f'https://{host}:{port}/{route}'
        print(f'Hitting URL: https://{host}:{port}/{route} for rules execution')

        ## fetch the NA case_id's 
        db_config['tenant_id']=tenant_id
        queues_db=DB('queues',**db_config)
        qry=f"""SELECT pq.case_id, pq.region  FROM QUEUE_LIST ql JOIN PROCESS_QUEUE pq ON ql.case_id = pq.case_id WHERE pq.REGION = 'NA' or pq.REGION is Null"""
        df=queues_db.execute_(qry)

        if not df.empty:
            case_ids=df['case_id'].tolist()
        else:
            return {"Flag":True,'Message':"No cases to Process"}

        headers = {
            'Content-type': 'application/json; charset=utf-8',
            'Accept': 'text/json'
        }
        for case_id in case_ids:
            parameters = {
                "tenant_id": tenant_id,
                "rule_id":"['OBC0-MV5P-KK4M']",
                "case_id": case_id,
                "master_data_require": "True",
                "master_data_columns": "{'party_master': ['RELATION_MGR_EMP_CODE','PARTY_NAME','RELATION_MGR', 'RM_REGION','PARTY_ID'],'rm_master': ['RM_STATE','RM_MGR_CODE','RM_CITY'],'age_margin_working_uat': ['AGE','BANKING_SHARE','MARGIN','PARTY_ID','COMPONENT_NAME'],'component_master':['COMPONENT_NAME'],'wbo_region':['WBO_REGION','RM_STATE']}"
            }
            
            try:
                response= requests.post(request_api,json=parameters,stream=True,verify=False)
                response_data = response.json()
            except Exception as e:
                print(f"Error hitting get_files_from_sftp: {e}")
                continue
        return {"Flag":True,'Message':"Cases Processed Successfully"}


def run_schedule(job_func,interval=None, at_time=None, *args):
    if at_time:
        # Schedule to run daily at a specific time
        print("at_timeeee--------------")
        schedule.every().day.at(at_time).do(job_func, *args)
    elif interval:
        # Schedule to run periodically at a fixed interval in seconds
        print("intervel-------")
        schedule.every(interval).seconds.do(job_func, *args)
    else:
        raise ValueError("You must specify either an interval or at_time.")
    while True:
        schedule.run_pending()
        time.sleep(1)

def run_clims_request(tenant_id):
    try:
        host = 'camunda_api'
        port = 443
        route = 'clims_process'
        request_api=f'https://{host}:{port}/{route}'
        print(f'Hitting URL: https://{host}:{port}/{route} for clims request formation')

        db_config['tenant_id']=tenant_id
        queues_db=DB('queues',**db_config)
        ext_db=DB('extraction',**db_config)
        group_db=DB('group_access',**db_config)
        query = """select case_id, clims_status, approved_date, no_of_retries, api_trigger_date
from (
    select q.case_id,
           c.clims_status,
           c.approved_date,
           o.no_of_retries,
           c.api_trigger_date,
           row_number() over (
               partition by q.case_id 
               order by c.api_trigger_date desc
           ) as rn
    from queue_list q
    join clims_audit c on q.case_id = c.case_id
    join hdfc_extraction.ocr o on q.case_id = o.case_id
    where q.queue = 'waiting_queue'
      and o.no_of_retries < 3
) t
where rn = 1"""
        query_res = queues_db.execute_(query)
        logging.info(f"query_res is {query_res}")
        query_res = query_res.to_dict(orient='records')
        clean_records = []
        for record in query_res:
            clean_record = {}
            for k, v in record.items():
                key = k.lower()
                if key not in clean_record:  # keep first occurrence
                    clean_record[key] = v
            clean_records.append(clean_record)

        headers = {
        'Content-type': 'application/json; charset=utf-8',
                            'Accept': 'application/json'
        }

        for record in clean_records:
            logging.info(f"record is {record}")
            clims_status = record.get('clims_status')
            case_id = record.get('case_id')
            last_updated = record.get('approved_date')
            retries = record.get('no_of_retries')
            api_trigger_date = record.get('api_trigger_date')
            if api_trigger_date.tzinfo is None:
                api_trigger_date = api_trigger_date.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            logging.info(f"clims_status: {clims_status}, last_updated: {last_updated}, retries: {retries}")
            if retries > 3:
                logging.info(f"Skipping case {case_id}, reached max retries = {retries}")
                continue
            if api_trigger_date and (datetime.now(ZoneInfo("Asia/Kolkata")) - api_trigger_date) < timedelta(minutes=7):
                logging.info(f"Skipping case {case_id}, API was triggered less than 7 minutes ago at {api_trigger_date}")
                continue
            last_user_query = f"select last_updated_by from process_queue where case_id='{case_id}'"
            last_user = queues_db.execute_(last_user_query).iloc[0].get('last_updated_by')
            clims_request_params = {
            "case_id":case_id,
            "tenant_id":tenant_id,
            "user":last_user
            }
            logging.info(f"clims_params: {clims_request_params}")
            now = datetime.now(ZoneInfo("Asia/Kolkata"))
            if last_updated.tzinfo is None:
                last_updated = last_updated.replace(tzinfo=ZoneInfo("Asia/Kolkata"))
            logging.info(f"now is {now.strftime('%d-%m-%y %I:%M:%S.%f %p %z')}, "
             f"last_updated is {last_updated.strftime('%d-%m-%y %I:%M:%S.%f %p %z')}")
            if clims_status and clims_status.lower() == 'nstp' and last_updated:
                logging.info("entered if block")
                # if isinstance(last_updated, str):
                #     logging.info("last_updated is str")
                #     from dateutil import parser
                #     last_updated = parser.parse(last_updated)
                if (now - last_updated) <= timedelta(minutes=30) and retries <= 3:
                    logging.info(f"entered condition block of 30 minutes and retries {retries}")
                    clims_response = requests.post(request_api,json=clims_request_params,headers=headers,stream= True,verify=False)
                    if clims_response.status_code == 200:
                        clims_response = clims_response.json()
                    else:
                        logging.info('Got non 200 code for clims failed case')
            #elif clims_status and clims_status.lower() == 'stp' and (now - last_updated_dt) > timedelta(minutes=10):
            elif clims_status and clims_status.lower() == 'stp' and last_updated:
                logging.info("entered elif block")
                if isinstance(last_updated, str):
                    logging.info("last_updated is str")
                    from dateutil import parser
                    last_updated = parser.parse(last_updated)
    
                if (now - last_updated) >= timedelta(days=5) and retries <= 3:
                    logging.info('more than 5 days')
                    host = 'button_functions'
                    port = 443
                    route = 'execute_button_function'
                    hit_request_api=f'https://{host}:{port}/{route}'
                    print(f'Hitting URL: https://{host}:{port}/{route} for execute button functions request formation for moving to accept on STP')
                    move_to = 'Accept'
                    #query_queue_list = f"select task_id from queue_list where case_id = '{case_id}' and queue='waiting_queue'"
                    #task_id = queues_db.execute_(query_queue_list)
                    #task_id=task_id.iloc[0].get('task_id')
                    #logging.info(f"move to Accepted queue task_id is {task_id}")
                    api_params = {"group":move_to, "case_id": case_id, "tenant_id": tenant_id,"user": last_user,"session_presence": False, "queue_id":9}
                    accept_response = requests.post(hit_request_api,json=api_params,headers=headers,stream= True,verify=False)
                    #request_post_ = requests.post(
                    #f'http://camundaworkflow:8080/rest/engine/default/task/{task_id}/complete', json=api_params, headers=headers , timeout=1500)
                    if accept_response.status_code == 200:
                        request_post = accept_response.json()
                        logging.info(f"##########request_post is {request_post} ")
                    else:
                        logging.info('Got non 200 code for button_functions')
                else:
                    logging.info('still less than 10 miutes')
            else:
                logging.info("Status from clims is neither failure nor success")
    except Exception as e: 
        logging.info("Exception in run_clims_request")          


# Call that function
"""
schedule.every(60).seconds.do(start_folder_monitor,'hdfc','8080', 'hdfc_folder_monitor')
schedule.every(60).seconds.do(hit_get_files_from_sftp)
# schedule.every(1).hour.do(start_folder_monitor, 'hdfc', '8080', 'hdfc_folder_monitor')

# schedule.every(45).minutes.do(hit_get_files_from_sftp)

schedule.every().day.at("03:30").do(hit_get_files_from_sftp_masters)

# This below line code is useful for hitting the camunda work flow for master data upload 5h:30 min below because of server time
schedule.every().day.at("03:32").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')
schedule.every().day.at("03:35").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')
schedule.every().day.at("03:38").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')


schedule.every().day.at("01:30").do(hit_get_files_from_sftp_masters)

# This below line code is useful for hitting the camunda work flow for master data upload 5h:30 min below because of server time
schedule.every().day.at("01:32").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')
## This call is responsible to filter the NA Cases and run the region rule for those case at 9 PM IST
schedule.every().day.at("15:30").do(hit_run_business_rules,'hdfc')

# This call is responsible for hitting clims_request
schedule.every(5).minutes.do(run_clims_request,'hdfc')
#schedule.every(60).seconds.do(run_pending_cases,'hdfc')
"""


# ================= LICENSE CHECK BEFORE SCHEDULING ==================
# LICENSE_INFO = {
#     "TENANT": "HDFC",
#     "LICENSE_NUMBER": "HDFC-3886-6403-0934",
#     "START_DATE": "06-12-2024",   # DD-MM-YYYY
#     "END_DATE": "28-11-2026"      # DD-MM-YYYY
# }

LICENSE_INFO = load_license()
# Parse end date
end_date = datetime.strptime(LICENSE_INFO["END_DATE"], "%d-%m-%Y")

# Current IST
ist = pytz.timezone('Asia/Kolkata')
now_ist = datetime.now(ist).replace(tzinfo=None)

# If license expired → do NOT schedule anything
if now_ist > end_date:
    logging.warning("\n LICENSE EXPIRED — ALL SCHEDULED JOBS DISABLED.")
    logging.warning("TENANT:", LICENSE_INFO["TENANT"])
    logging.warning("LICENSE:", LICENSE_INFO["LICENSE_NUMBER"])
    logging.warning("END DATE:", LICENSE_INFO["END_DATE"])
    logging.warning("---------------------------------------------------\n")
else:
    # ==========================================================
    # ONLY IF LICENSE VALID → ENABLE SCHEDULING
    # ==========================================================

    schedule.every(60).seconds.do(start_folder_monitor,'hdfc','8080', 'hdfc_folder_monitor')
    schedule.every(60).seconds.do(hit_get_files_from_sftp)

    schedule.every().day.at("03:30").do(hit_get_files_from_sftp_masters)

    schedule.every().day.at("03:32").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')
    schedule.every().day.at("03:35").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')
    schedule.every().day.at("03:38").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')

    schedule.every().day.at("01:30").do(hit_get_files_from_sftp_masters)
    schedule.every().day.at("01:32").do(start_folder_monitor, 'hdfc', '8080', 'folder_monitor_sftp')

    schedule.every().day.at("15:30").do(hit_run_business_rules,'hdfc')

    schedule.every(5).minutes.do(run_clims_request,'hdfc')


if __name__ == '__main__':
    while True:

        mode = os.environ.get('MODE')
        auto_dormancy_process = multiprocessing.Process(target=run_schedule, args=(uam_dormancy_scheduler,None,"18:33", 'hdfc'))
        move_predo_process = multiprocessing.Process(target=run_schedule, args=(move_predo_do, 20,None, 'hdfc'))
        move_predo_process.start()
        auto_dormancy_process.start()

        move_predo_process.join()
        auto_dormancy_process.join()
        print("##########Mode is ",mode)
        if mode == "UAT":
            print("####In UAT ---> Hitting servers in the UAT only###")
        else:
            print("######In DEV mode")

        schedule.run_pending()
        time.sleep(1)
