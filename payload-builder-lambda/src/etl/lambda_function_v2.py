"""
Module for building payloads for ETL processes in Lambda functions.
"""
from src.services.payload_builder_v2 import PayloadBuilderService
from src.config.logger import logger

def lambda_handler(event, context):
    """
    Lambda function handler to build ETL payloads based on the event input.

    Args:
        event (dict): The event data passed to the Lambda function.
        context (object): The runtime information of the Lambda function.

    Returns:
        dict: The result of the payload building process.
    """
    params = event.get("payloadParameters", {})
    project_name = params.get("project_name")
    job_type = params["job_type"]
    date_type = params["date_type"]
    process_type = params["process_type"]
    job_parameters = params["job_parameters"]
    db_type = params.get("db_type", "sqlserver")
    secret_db = params.get("secret_db")
    notification_emails = params.get("notification_emails")

    if project_name is None or notification_emails is None:
        logger.error("[ERROR] 'project_name' and 'notification_emails' are required in the event parameters.")
        raise ValueError("'project_name' and 'notification_emails' are required in the event parameters.")

    logger.info(f"[START] Starting process with date_type: {date_type}, process_type: {process_type}, job_type: {job_type}")

    service = PayloadBuilderService(job_type,
                                    date_type,
                                    process_type,
                                    job_parameters,
                                    db_type,
                                    secret_db)
    result = service.create_payloads()

    logger.info(f"[END] Payload building process completed successfully.")
    
    return result