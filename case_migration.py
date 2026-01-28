import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random

from ace_logger import Logging

logging = Logging()


__all__ = ["call_camunda_api", "get_bpmn_list", "get_bpmn_process_versions",
           "get_tokens_xml", "validate_migration_plan", "get_process_instances", "migrate_cases"]


def call_camunda_api(variable_route, request_params=None, params_dict=None):

    if request_params is None:
        request_params = {}
    host = os.environ['SERVER_IP']
    port = "8080"
    base_camunda_url = f'http://{host}:{port}/rest/engine/default/{variable_route}'

    if params_dict is not None:
        query_params = "?"
        for key, value in params_dict.items():
            query_params += f"{key}={value}"

        base_camunda_url += query_params
    try:
        response = None

        logging.info(
            f"########## Final Camunda calling url: {base_camunda_url}")
        
        try:
            logging.info("####### Trying OPTIONS method with Camunda")
            options_response = requests.options(base_camunda_url, verify=False)
            allowed_methods = options_response.headers.get('Allow', '')
            logging.info(f"Allowed methods: {allowed_methods}")
        except Exception as e:
            logging.error(f"OPTIONS request failed: {e}")
            return None

        logging.info("####### Trying Post method with Camunda")
        response = requests.post(base_camunda_url, json=request_params,verify=False)
        logging.info(f"########## {response}")
        logging.info(f"########## {response.status_code}")

        if response.status_code in range(400, 500):

            logging.info(
                "####### Failed Post method Falling back to GET method with camunda")

            response = requests.get(base_camunda_url, json=request_params,verify=False)

        try:

            return True, response.json()
        except Exception as e:
            logging.info(
                "###### Error in getting json, so converting to utf-8")

            try:
                return True, response.content.decode('utf-8')

            except Exception as e:
                logging.info(
                    "### Error in decoding response content, probably empty content")

                return (True, "") if response.status_code in range(200, 300) else (True, e)
    except Exception as e:

        logging.info(f"######### Error in fetching data from Camunda {e}")
        return False, e


def get_bpmn_list():

    camunda_route = "process-definition"

    flag, response = call_camunda_api(camunda_route)

    if not flag:
        return flag, response
    logging.info("###### Parsing the camunda response for bpmn lists")

    logging.info(f"Response Type: {type(response)}")

    logging.info(f"Response: {response}")

    bpmn_names = set()

    for ele in response:

        key = ele.get("key", "")

        if key != "":

            bpmn_names.add(key)

    return flag, list(bpmn_names)


# sourcery skip: avoid-builtin-shadow
def get_bpmn_process_versions(bpmn_name):  # sourcery skip: avoid-builtin-shadow

    camunda_route = "process-definition"

    flag, response = call_camunda_api(
        camunda_route, params_dict={"key": bpmn_name})

    if not flag:
        return flag, response
    logging.info("###### Parsing the camunda response for bpmn lists")

    logging.info(f"Response Type: {type(response)}")

    logging.info(f"Response: {response}")

    bpmn_versions = set()

    for ele in response:

        id = ele.get("id", "")

        if id != "":

            bpmn_versions.add(id)

    return flag, list(bpmn_versions)


def get_tokens(bpmn_version):  # sourcery skip: avoid-builtin-shadow

    camunda_route = f"process-definition/{bpmn_version}/statistics"

    flag, response = call_camunda_api(
        camunda_route, params_dict={"failedJobs": False})

    if not flag:
        return flag, response
    logging.info("###### Parsing the camunda response for bpmn tokens")

    logging.info(f"Response Type: {type(response)}")

    logging.info(f"Response: {response}")

    bpmn_stats = []

    for ele in response:
        temp_dict = {}

        id = ele.get("id", "")
        count = ele.get("instances", 0)

        if id != "":

            temp_dict["id"] = id
            temp_dict["instances"] = count

            bpmn_stats.append(temp_dict)
    return flag, list(bpmn_stats)


def get_xml(bpmn_version):

    camunda_route = f"process-definition/{bpmn_version}/xml"

    flag, response = call_camunda_api(camunda_route)

    if not flag:
        return flag, response
    logging.info("###### Parsing the camunda response for bpmn tokens")

    logging.info("Response Type: {type(response)}")

    logging.info("Response: {response}")

    return flag, response.get("bpmn20Xml", "Empty XML")


def get_tokens_xml(bpmn_version):

    tokens_flag, tokens_response = get_tokens(bpmn_version)

    return_dict = {"stats": tokens_response if tokens_flag else []}
    xml_flag, xml_response = get_xml(bpmn_version)

    return_dict["xml"] = xml_response if xml_flag else ""
    return True, return_dict


def get_migration_plan(source_version, dest_version):

    request_params = {"updateEventTriggers": True,
                      "sourceProcessDefinitionId": source_version, "targetProcessDefinitionId": dest_version}

    route = "migration/generate"

    plan_flag, plan_response = call_camunda_api(
        route, request_params=request_params)

    if plan_flag:

        logging.info(f"##### Generating Migration is Success: {plan_response}")

        return True, plan_response

    else:
        logging.info(f"##### Generating Migration Failed: {plan_response}")
        return False, {}


def validate_migration_plan(source_version, dest_version):

    plan_flag, plan_response = get_migration_plan(source_version, dest_version)

    if not plan_flag:
        return False, {}
    route = "migration/validate"

    valid_flag, valid_response = call_camunda_api(
        route, request_params=plan_response)

    if valid_flag:

        logging.info(
            f"##### Validating Migration is Success: {valid_response}")

        return (False, valid_response) if valid_response["instructionReports"] else (True, plan_response)

    else:

        logging.info(f"##### Validating Migration Failed: {valid_response}")

        return False, valid_response


def get_process_instances(bpmn_version):

    route = "process-instance"

    request_params = {"processDefinitionId": bpmn_version}

    instance_flag, instance_response = call_camunda_api(
        route, request_params=request_params)

    if instance_flag:
        return [ele.get("id", "") for ele in instance_response]

    logging.info(f"######## Error in fetching instances: {instance_response}")
    return []


# def migrate_cases(source_version, dest_version):

#     valid_flag, valid_response = validate_migration_plan(
#         source_version, dest_version)

#     route = "migration/execute"

#     if not valid_flag:
#         return False, valid_response
#     process_instances = get_process_instances(source_version)

#     if not process_instances:
#         return False, "Couldnt get source process instances"

#     migrate_dict = {"processInstanceIds": process_instances, "migrationPlan": valid_response,
#                     "processInstanceQuery": {"processDefinitionId": source_version}}

#     logging.info(f"############ Migration request params: {migrate_dict}")

#     migrate_flag, migrate_response = call_camunda_api(
#         route, request_params=migrate_dict)

#     return (True, "Migration Successful") if migrate_flag else (False, "Mirgation Failed")



# def migrate_cases(source_version, dest_version):
#     # Define batch size and number of parallel workers inside the function
#     batch_size = 1000      # process 100 instances per batch
#     max_workers = 3       # run 3 batches in parallel

#     # Inner function to migrate a single batch
#     def migrate_batch(batch_number, chunk, migration_plan, source_version):
#         route = "migration/execute"
#         migrate_dict = {
#             "migrationPlan": migration_plan,
#             "processInstanceIds": chunk,
#             "processInstanceQuery": {"processDefinitionId": source_version}
#         }

#         logging.info(f"Starting migration for batch {batch_number} with {len(chunk)} instances")
#         migrate_flag, migrate_response = call_camunda_api(route, request_params=migrate_dict)

#         if migrate_flag:
#             logging.info(f"Batch {batch_number} migrated successfully")
#         else:
#             logging.error(f"Batch {batch_number} failed: {migrate_response}")

#         return migrate_flag, batch_number

#     # Validate migration plan
#     valid_flag, valid_response = validate_migration_plan(source_version, dest_version)
#     if not valid_flag:
#         return False, valid_response

#     # Get all process instances
#     process_instances = get_process_instances(source_version)
#     if not process_instances:
#         return False, "Could not get source process instances"

#     total_instances = len(process_instances)
#     num_batches = (total_instances + batch_size - 1) // batch_size
#     logging.info(f"Total instances to migrate: {total_instances}")
#     logging.info(f"Migrating in {num_batches} batches of {batch_size} instances each using {max_workers} parallel workers")

#     # Prepare batches
#     batches = []
#     for i in range(0, total_instances, batch_size):
#         chunk = process_instances[i:i+batch_size]
#         batch_number = i // batch_size + 1
#         batches.append((batch_number, chunk))

#     # Execute batches in parallel
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         future_to_batch = {executor.submit(migrate_batch, batch_num, chunk, valid_response, source_version): batch_num
#                            for batch_num, chunk in batches}

#         all_success = True
#         for future in as_completed(future_to_batch):
#             success, batch_num = future.result()
#             if not success:
#                 logging.error(f"Batch {batch_num} failed")
#                 all_success = False

#     return (all_success, "All batches processed successfully" if all_success else "Some batches failed")

def migrate_cases(source_version, dest_version):
    """
    Perform asynchronous case migration from source_version to dest_version
    using Camunda's async batch migration API.
    Retries failed batches automatically until success.
    """

    # import time
    # import random

    # Validate migration plan
    valid_flag, valid_response = validate_migration_plan(source_version, dest_version)
    if not valid_flag:
        return False, valid_response

    # Get all process instances
    process_instances = get_process_instances(source_version)
    if not process_instances:
        return False, "Could not get source process instances"

    logging.info(f"Total instances to migrate: {len(process_instances)}")
    logging.info(f" Found {len(process_instances)} process instances for {source_version}")
    logging.debug(f"Instance IDs: {process_instances[:50]}")

    # Create async migration request
    route = "migration/executeAsync"
    migrate_dict = {
        "migrationPlan": valid_response,
        "processInstanceIds": process_instances,
        "processInstanceQuery": {"processDefinitionId": source_version}
    }

    logging.info(f"Submitting async migration request for {len(process_instances)} instances")
    migrate_flag, migrate_response = call_camunda_api(route, request_params=migrate_dict)

    if not migrate_flag:
        logging.error(f"Failed to start async migration: {migrate_response}")
        return False, migrate_response

    batch_id = migrate_response.get("id")
    logging.info(f"Migration batch created with id: {batch_id}")

    # Poll batch status until complete
    batch_status_route = f"batch/{batch_id}"

    retries = 0
    max_retries = 5
    backoff = 5  # seconds

    while True:
        time.sleep(10)  # wait between polls
        status_flag, status_response = call_camunda_api(batch_status_route)

        if not status_flag:
            logging.error(f"Error fetching batch status: {status_response}")
            retries += 1
            if retries > max_retries:
                return False, f"Batch {batch_id} failed after {max_retries} retries"
            sleep_time = backoff * (2 ** retries) + random.uniform(0, 3)
            logging.info(f"Retrying batch status check in {sleep_time:.2f}s")
            time.sleep(sleep_time)
            continue

        remaining = status_response.get("remainingJobs", 0)
        failed = status_response.get("failedJobs", 0)
        completed = status_response.get("completedJobs", 0)

        logging.info(
            f"Batch {batch_id} status - Completed: {completed}, Failed: {failed}, Remaining: {remaining}"
        )

        if remaining == 0:
            if failed > 0:
                logging.warning(
                    f"Batch {batch_id} completed with {failed} failed jobs. Retrying failed jobs."
                )

                # Get failed job IDs
                jobs_route = f"batch/{batch_id}/jobs"
                jobs_flag, jobs_response = call_camunda_api(jobs_route)

                if jobs_flag and isinstance(jobs_response, list):
                    failed_job_ids = [job["id"] for job in jobs_response if job.get("retries", 0) == 0]
                else:
                    failed_job_ids = []

                if failed_job_ids:
                    retry_route = "job/retries"
                    retry_flag, retry_response = call_camunda_api(
                        retry_route,
                        request_params={"jobIds": failed_job_ids, "retries": 1},
                    )
                    if retry_flag:
                        logging.info(f"Retried {len(failed_job_ids)} failed jobs successfully")
                    else:
                        logging.error(f"Failed to retry jobs: {retry_response}")
                else:
                    logging.warning("No failed job IDs found to retry")

                return False, f"Batch {batch_id} completed with failures"
            else:
                logging.info(f"Batch {batch_id} completed successfully")
                dest_instances = get_process_instances(dest_version)
                logging.info(f"📌 Post-migration check: {len(dest_instances)} instances found for {dest_version}")
                return True, f"Batch {batch_id} migration successful"

