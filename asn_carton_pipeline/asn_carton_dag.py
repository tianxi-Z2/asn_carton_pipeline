"""
ASN Carton Pipeline Daily DAG
Retrieves today's ASN data from BigQuery and sends to SML API
"""

import xml.etree.ElementTree as ET
from xml.dom import minidom
from google.cloud import storage
from google.cloud import bigquery
import datetime
from datetime import timedelta
import pendulum
import logging
import time
import os
import json
import sys
from airflow import models
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# Add project paths - adjusted for company structure
sys.path.append('/home/airflow/gcs/dags/asn_carton_pipeline')
sys.path.append('/opt/airflow/dags/asn_carton_pipeline')

# Import ASN Pipeline - fixed import path
try:
    from asn_carton_pipeline import ASNCartonPipeline
except ImportError as e:
    logging.error(f"Failed to import ASNCartonPipeline: {e}")
    # Fallback import attempt
    try:
        import asn_carton_pipeline

        ASNCartonPipeline = asn_carton_pipeline.ASNCartonPipeline
    except ImportError as e2:
        logging.error(f"Fallback import also failed: {e2}")

# Set timezone for DAG schedule
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'start_date': datetime.datetime(2024, 1, 1, 7, tzinfo=local_tz),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@sofg.com']  # Update with actual email
}

# Pipeline specific variables
current_day_folder = datetime.datetime.today().strftime('%Y-%m-%d')
yesterday_folder = (datetime.datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')

# Load environment variables - fixed to match docker-compose variables
project_id = Variable.get("GCP_PROJECT_PIPE", "sofg-edp-stg-pipe")
data_project_id = Variable.get("GCP_PROJECT_DATA", "sofg-edp-stg-data")

# Fallback to environment variables if Airflow variables not set
if not project_id or project_id == "sofg-edp-stg-pipe":
    project_id = os.environ.get("AIRFLOW_VAR_GCP_PROJECT_PIPE", "sofg-edp-stg-pipe")
if not data_project_id or data_project_id == "sofg-edp-stg-data":
    data_project_id = os.environ.get("AIRFLOW_VAR_GCP_PROJECT_DATA", "sofg-edp-stg-data")

# GCS configuration from docker-compose
gcs_vars = json.loads(os.environ.get("AIRFLOW_VAR_GCS_VARS", '{"landing_bucket":"sofg-stg-data-us-landing"}'))
gcs_extracts_bucket = gcs_vars.get("landing_bucket", "sofg-stg-data-us-landing")
gcs_logs_bucket = gcs_vars.get("logs_bucket", "sofg-stg-data-us-logs")

# ASN Pipeline configuration
asn_dataset = "sml"
asn_view = "vw_asn_data_v2"


def validate_asn_variables():
    """Validate that all required Airflow Variables are configured"""
    logger = logging.getLogger("validate_asn_variables")

    required_variables = [
        'sml_token_api_url',
        'sml_token_asn_scope',
        'sml_token_client_id',
        'sml_token_client_secret'
    ]

    missing_vars = []
    variable_values = {}

    for var_name in required_variables:
        try:
            value = Variable.get(var_name)
            # Mask sensitive values in logs
            if 'secret' in var_name.lower() or 'password' in var_name.lower():
                variable_values[var_name] = "***masked***"
            else:
                variable_values[var_name] = value[:50] + "..." if len(value) > 50 else value
            logger.info(f"Variable {var_name}: {variable_values[var_name]}")
        except Exception as e:
            missing_vars.append(var_name)
            logger.error(f"Missing variable: {var_name} - Error: {e}")

    if missing_vars:
        error_msg = f"Missing required Airflow Variables: {missing_vars}"
        logger.error(error_msg)
        logger.error("To set variables, use Airflow UI (Admin > Variables) or CLI:")
        for var in missing_vars:
            logger.error(f"  airflow variables set {var} 'your_value'")
        raise Exception(error_msg)

    logger.info("All required Airflow Variables are configured")
    return variable_values


def check_asn_data_availability():
    """Check if ASN data is available for today"""
    logger = logging.getLogger("check_asn_data_availability")

    # FIXED: Calculate today's date to match pipeline execution
    today = datetime.datetime.now().strftime('%Y-%m-%d')

    # Initialize BigQuery client
    client = bigquery.Client(project=data_project_id)

    # Ensure proper BigQuery table format: project.dataset.table
    table_ref = f"`{data_project_id}.{asn_dataset}.{asn_view}`"

    # Query to check data availability and quality
    query = f"""
    SELECT 
        COUNT(*) as record_count,
        COUNT(DISTINCT shipment_id) as unique_shipments,
        COUNT(DISTINCT carton_id) as unique_cartons,
        COUNT(DISTINCT purchaseOrderReference) as unique_purchase_orders,  
        COUNT(DISTINCT sku) as unique_skus,
        COUNT(DISTINCT siteID) as unique_sites,
        MIN(asn_date) as earliest_date,
        MAX(asn_date) as latest_date,
        COUNT(CASE WHEN shipment_id IS NULL THEN 1 END) as null_shipment_ids,
        COUNT(CASE WHEN carton_id IS NULL THEN 1 END) as null_carton_ids
    FROM {table_ref}
    WHERE DATE(asn_date) = '{today}'
    """

    logger.info(f"Checking data availability for: {today}")
    logger.info(f"Using table reference: {table_ref}")

    try:
        query_job = client.query(query)
        result = query_job.to_dataframe()

        if result.empty:
            raise Exception(f"Query returned no results for date {today}")

        record_count = int(result['record_count'].iloc[0])
        unique_shipments = int(result['unique_shipments'].iloc[0])
        unique_cartons = int(result['unique_cartons'].iloc[0])
        unique_purchase_orders = int(result['unique_purchase_orders'].iloc[0])
        unique_skus = int(result['unique_skus'].iloc[0])
        unique_sites = int(result['unique_sites'].iloc[0])
        null_shipment_ids = int(result['null_shipment_ids'].iloc[0])
        null_carton_ids = int(result['null_carton_ids'].iloc[0])

        logger.info(f"ASN data availability check for {today}:")
        logger.info(f"- Total records: {record_count:,}")
        logger.info(f"- Unique shipments: {unique_shipments:,}")
        logger.info(f"- Unique purchase orders: {unique_purchase_orders:,}")
        logger.info(f"- Unique cartons: {unique_cartons:,}")
        logger.info(f"- Unique SKUs: {unique_skus:,}")
        logger.info(f"- Unique sites: {unique_sites:,}")

        if null_shipment_ids > 0 or null_carton_ids > 0:
            logger.warning(f"Data quality issues detected:")
            logger.warning(f"- Null shipment IDs: {null_shipment_ids}")
            logger.warning(f"- Null carton IDs: {null_carton_ids}")

        if record_count == 0:
            logger.warning(f"No ASN data found for {today}")
            logger.warning("This may be normal for weekends or holidays")

        data_summary = {
            'date': today,
            'record_count': record_count,
            'unique_shipments': unique_shipments,
            'unique_purchase_orders': unique_purchase_orders,
            'unique_cartons': unique_cartons,
            'unique_skus': unique_skus,
            'unique_sites': unique_sites,
            'null_shipment_ids': null_shipment_ids,
            'null_carton_ids': null_carton_ids,
            'status': 'data_found' if record_count > 0 else 'no_data',
            'data_quality_issues': null_shipment_ids > 0 or null_carton_ids > 0
        }

        logger.info(f"Data check completed: {json.dumps(data_summary, indent=2)}")
        return data_summary

    except Exception as e:
        error_msg = f"Failed to check ASN data availability: {str(e)}"
        logger.error(error_msg)
        logger.error(f"Query: {query}")
        raise Exception(error_msg)


def run_asn_carton_pipeline():
    """Execute the main ASN carton pipeline"""
    logger = logging.getLogger("run_asn_carton_pipeline")
    logger.info("Starting ASN Carton Pipeline execution")

    try:
        # Initialize pipeline - using environment variables only
        pipeline = ASNCartonPipeline()

        # Run pipeline
        result = pipeline.run_pipeline()

        # Extract key metrics
        data_summary = result.get('data_summary', {})
        api_results = result.get('api_results', {})

        # Check for failed combinations
        failed_combinations = [
            combo for combo in api_results.get('combination_results', [])
            if combo.get('status') == 'failed'
        ]

        # Calculate success metrics
        total_combinations = api_results.get('total_combinations', 0)
        successful_combinations = total_combinations - len(failed_combinations)
        success_rate = (successful_combinations / total_combinations * 100) if total_combinations > 0 else 0

        # Log execution summary
        logger.info("ASN Carton Pipeline execution completed:")
        logger.info(f"- Run ID: {result.get('pipeline_run_id')}")
        logger.info(f"- Status: {result.get('status')}")
        logger.info(f"- Execution time: {result.get('execution_time')}")
        logger.info(f"- Source records: {data_summary.get('source_records', 0):,}")
        logger.info(f"- Combinations processed: {data_summary.get('combinations_processed', 0):,}")
        logger.info(f"- Cartons processed: {data_summary.get('cartons_processed', 0):,}")
        logger.info(f"- Total combinations: {total_combinations}")
        logger.info(f"- Successful combinations: {successful_combinations}")
        logger.info(f"- Failed combinations: {len(failed_combinations)}")
        logger.info(f"- Success rate: {success_rate:.1f}%")

        # Upload results summary to GCS for monitoring
        upload_results_to_gcs(result)

        # Raise exception if any combinations failed
        if failed_combinations:
            error_details = []
            for combo in failed_combinations:
                shipment_ref = combo.get('shipment_reference', 'unknown')
                site_id = combo.get('site_id', 'unknown')
                carton_count = combo.get('carton_count', 0)
                error_details.append(
                    f"Shipment {shipment_ref} + Site {site_id}: {combo.get('error')} "
                    f"({carton_count} cartons)"
                )

            error_msg = f"ASN Pipeline partially failed. Failed combinations ({len(failed_combinations)} of {total_combinations}):\n"
            error_msg += "\n".join(error_details)

            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info("ASN Carton Pipeline completed successfully")
        return result

    except Exception as e:
        error_msg = f"ASN Carton Pipeline execution failed: {str(e)}"
        logger.error(error_msg)

        # Upload error info to GCS for monitoring
        error_result = {
            'pipeline_run_id': f"asn_pipeline_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'status': 'failed',
            'error': str(e),
            'timestamp': datetime.datetime.now().isoformat()
        }
        upload_results_to_gcs(error_result)

        raise


def upload_results_to_gcs(result):
    """Upload pipeline results to GCS for monitoring and alerting"""
    logger = logging.getLogger("upload_results_to_gcs")

    try:
        # Create results summary
        results_json = json.dumps(result, indent=2, default=str)

        # Upload to GCS
        storage_client = storage.Client(project=data_project_id)
        bucket = storage_client.get_bucket(gcs_logs_bucket)

        # Create blob path with date and run ID
        run_id = result.get('pipeline_run_id', 'unknown')
        blob_path = f'asn_pipeline/results/{current_day_folder}/{run_id}_results.json'

        blob = bucket.blob(blob_path)
        blob.upload_from_string(results_json, content_type='application/json')

        logger.info(f"Pipeline results uploaded to gs://{gcs_logs_bucket}/{blob_path}")

    except Exception as e:
        logger.warning(f"Failed to upload results to GCS: {e}")


def send_success_notification():
    """Send success notification"""
    logger = logging.getLogger("send_success_notification")

    try:
        message = f"""
ASN Carton Pipeline - Execution Successful

Summary:
• Date: {current_day_folder}
• Status: SUCCESS
• Timestamp: {datetime.datetime.now().isoformat()}

Results:
• Pipeline completed without errors
• All shipment+site combinations successfully sent to SML API
• Results available in GCS logs

The ASN data has been successfully processed and sent to the SML Shipment API.
        """

        logger.info("Success notification:")
        logger.info(message)

        # Here you can add email/Slack notification logic if configured

        return True

    except Exception as e:
        logger.error(f"Failed to send success notification: {str(e)}")
        return False


def cleanup_asn_pipeline():
    """Perform cleanup and archival tasks"""
    logger = logging.getLogger("cleanup_asn_pipeline")

    try:
        logger.info("Performing ASN pipeline cleanup tasks")

        # Archive old log files (older than 30 days)
        cutoff_date = datetime.datetime.now() - timedelta(days=30)
        cutoff_folder = cutoff_date.strftime('%Y-%m-%d')

        storage_client = storage.Client(project=data_project_id)

        try:
            bucket = storage_client.get_bucket(gcs_logs_bucket)

            # List blobs older than 30 days
            prefix = 'asn_pipeline/results/'
            blobs_to_archive = []

            for blob in bucket.list_blobs(prefix=prefix):
                if blob.name.count('/') >= 3:  # asn_pipeline/results/YYYY-MM-DD/filename
                    date_part = blob.name.split('/')[2]
                    try:
                        blob_date = datetime.datetime.strptime(date_part, '%Y-%m-%d')
                        if blob_date < cutoff_date:
                            blobs_to_archive.append(blob.name)
                    except ValueError:
                        continue

            # Archive old files (in practice, you might move to different bucket or delete)
            if blobs_to_archive:
                logger.info(f"Found {len(blobs_to_archive)} old log files to archive")
                # For now, just log the files that would be archived
                # In production, implement actual archival logic here
            else:
                logger.info("No old log files found for archival")

        except Exception as e:
            logger.warning(f"GCS cleanup failed: {e}")

        logger.info(f"Cleanup completed for ASN pipeline")

        cleanup_summary = {
            'status': 'completed',
            'cleanup_date': current_day_folder,
            'cutoff_date': cutoff_folder,
            'files_to_archive': len(blobs_to_archive) if 'blobs_to_archive' in locals() else 0,
            'timestamp': datetime.datetime.now().isoformat()
        }

        logger.info(f"Cleanup summary: {json.dumps(cleanup_summary, indent=2)}")
        return cleanup_summary

    except Exception as e:
        logger.error(f"ASN pipeline cleanup failed: {str(e)}")
        return {'status': 'failed', 'error': str(e)}


# Define DAG
with models.DAG(
        dag_id="asn_carton_pipeline_daily",
        schedule_interval="0 7 * * *",  # Run daily at 7 AM EST
        description="DAG to process and send ASN carton data to SML API",
        max_active_runs=1,
        default_args=default_args,
        catchup=False) as dag:
    # Start task
    t_start = DummyOperator(task_id="start")

    # Wait for upstream dependencies (if any)
    # Uncomment and modify as needed based on your data dependencies
    # t_waitfor_asn_data_load = ExternalTaskSensor(
    #     task_id="t_waitfor_asn_data_load",
    #     external_dag_id="asn_data_load_daily",
    #     allowed_states=['success'],
    #     check_existence=True,
    #     poke_interval=60,
    #     execution_delta=timedelta(hours=1)
    # )

    # Validate Airflow Variables
    t_validate_variables = PythonOperator(
        task_id='t_validate_asn_variables',
        python_callable=validate_asn_variables,
        provide_context=True,
        dag=dag
    )

    # Check data availability
    t_check_data_availability = PythonOperator(
        task_id='t_check_asn_data_availability',
        python_callable=check_asn_data_availability,
        provide_context=True,
        dag=dag
    )

    # Run ASN carton pipeline
    t_run_asn_pipeline = PythonOperator(
        task_id='t_run_asn_carton_pipeline',
        python_callable=run_asn_carton_pipeline,
        provide_context=True,
        dag=dag
    )

    # Send success notification
    t_success_notification = PythonOperator(
        task_id='t_send_success_notification',
        python_callable=send_success_notification,
        provide_context=True,
        dag=dag
    )

    # Cleanup tasks
    t_cleanup = PythonOperator(
        task_id='t_cleanup_asn_pipeline',
        python_callable=cleanup_asn_pipeline,
        provide_context=True,
        dag=dag
    )

    # End task
    t_end = DummyOperator(task_id="end")

    # Define task dependencies
    t_start >> t_validate_variables >> t_check_data_availability >> t_run_asn_pipeline

    # Success path
    t_run_asn_pipeline >> t_success_notification >> t_cleanup >> t_end