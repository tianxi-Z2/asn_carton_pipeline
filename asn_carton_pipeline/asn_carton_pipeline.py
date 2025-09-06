import os
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import storage
from airflow.models import Variable
from typing import Dict, List, Optional
import json
import yaml


class ASNCartonPipeline:
    def __init__(self, config_path: str = None):
        """
        Initialize ASN Carton Pipeline

        Args:
            config_path: Not used - kept for backward compatibility
        """
        # Setup logging first
        self.logger = logging.getLogger("asn_carton_pipeline")

        # Get environment variables directly - no config file needed
        self.project_id = self._get_project_env_var("DATA")
        self.pipe_project_id = self._get_project_env_var("PIPE")
        self.location = 'US'

        # Dataset configuration
        self.sml_dataset = "sml"
        self.asn_view = "vw_asn_data_v2"

        # Initialize clients
        self.bq_client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client(project=self.project_id)

        # Get SML API configuration from Airflow Variables
        self.sml_config = self._get_sml_config_from_variables()

        # API token cache
        self.api_token = None
        self.token_expiry = None

    def _get_project_env_var(self, project_type: str) -> str:
        """Get project ID from environment variables with proper fallbacks"""
        # Try Airflow Variable first
        try:
            var_name = f"GCP_PROJECT_{project_type}"
            project_id = Variable.get(var_name)
            if project_id:
                return project_id
        except:
            pass

        # Try environment variable (from docker-compose)
        env_var_name = f"AIRFLOW_VAR_GCP_PROJECT_{project_type}"
        project_id = os.environ.get(env_var_name)
        if project_id:
            return project_id

        # Legacy environment variable
        legacy_env_name = f"GCP_PROJECT_{project_type}"
        project_id = os.environ.get(legacy_env_name)
        if project_id:
            return project_id

        # Default values based on project type
        if project_type == "DATA":
            return "sofg-edp-stg-data"
        else:  # PIPE
            return "sofg-edp-stg-pipe"

    def _load_config(self, config_path: str) -> Dict:
        """Load YAML configuration file"""
        if not config_path:
            self.logger.info("No config path provided, using environment variables only")
            return self._get_default_config()

        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as file:
                    config = yaml.safe_load(file)
                    self.logger.info(f"Successfully loaded config from {config_path}")
                    return config
            else:
                self.logger.warning(f"Config file {config_path} not found, using environment variables")
        except Exception as e:
            self.logger.warning(f"Failed to load config file {config_path}: {str(e)}")

        # Return default configuration
        return self._get_default_config()

    def _get_default_config(self) -> Dict:
        """Get default configuration based on environment variables"""
        return {
            'bq': {
                'project-id': self._get_project_env_var("DATA"),
                'location': 'US',
                'datasets': {
                    'stg_sml': {'name': 'stg_sml'}
                }
            }
        }

    def _get_sml_config_from_variables(self) -> Dict:
        """Get SML API configuration from Airflow Variables"""
        try:
            # Debug logging for configuration
            self.logger.info("Retrieving SML configuration from Airflow Variables...")

            sml_config = {
                'token_endpoint': Variable.get("sml_token_api_url"),
                'scope': Variable.get("sml_token_asn_scope"),
                'client_id': Variable.get("sml_token_client_id"),
                'client_secret': Variable.get("sml_token_client_secret")
            }

            # Log the token endpoint to verify base URL
            self.logger.info(f"Token endpoint: {sml_config['token_endpoint']}")
            self.logger.info(f"Scope: {sml_config['scope']}")
            self.logger.info(
                f"Client ID: {sml_config['client_id'][:10]}..." if sml_config['client_id'] else "Client ID: None")

            # Get optional carton endpoint
            try:
                sml_config['carton_endpoint'] = Variable.get("sml_carton_api_url")
                self.logger.info(f"Carton endpoint from variable: {sml_config['carton_endpoint']}")
            except:
                # Derive from token endpoint if not configured
                base_url = sml_config['token_endpoint'].replace('/oauth/token', '')
                sml_config['carton_endpoint'] = f"{base_url}/v1/cartons"
                self.logger.warning(
                    f"sml_carton_api_url variable not found, derived endpoint: {sml_config['carton_endpoint']}")

            # Get optional configuration
            try:
                sml_config['batch_size'] = int(Variable.get("sml_api_batch_size", "100"))
            except:
                sml_config['batch_size'] = 100

            self.logger.info(f"Batch size: {sml_config['batch_size']}")

            try:
                sml_config['timeout'] = int(Variable.get("sml_api_timeout", "300"))
            except:
                sml_config['timeout'] = 300

            self.logger.info(f"API timeout: {sml_config['timeout']} seconds")

            self.logger.info("Successfully retrieved SML config from Airflow Variables")
            return sml_config

        except Exception as e:
            self.logger.error(f"Failed to retrieve SML config from Airflow Variables: {str(e)}")
            raise

    def get_today_date(self) -> str:
        """Get today's date as string"""
        today = datetime.now()
        return today.strftime('%Y-%m-%d')

    def retrieve_asn_data(self) -> pd.DataFrame:
        """
        Retrieve today's ASN data from BigQuery vw_asn_data_v2 view

        Returns:
            DataFrame containing ASN data
        """
        today = self.get_today_date()

        # Ensure proper BigQuery table format: project.dataset.table
        table_ref = f"`{self.project_id}.{self.sml_dataset}.{self.asn_view}`"

        query = f"""
        SELECT 
            insert_date,
            asn_date,
            shipment_id,
            purchaseOrderReference,
            siteID,
            carton_id,
            sku,
            quantity
        FROM {table_ref}
        WHERE DATE(asn_date) = '{today}'
        """

        self.logger.info(f"Retrieving ASN data for date: {today}")
        self.logger.info(f"Using table reference: {table_ref}")

        try:
            query_job = self.bq_client.query(query)
            df = query_job.to_dataframe()
            self.logger.info(f"Successfully retrieved {len(df)} records")

            # Log sample data for debugging
            if not df.empty:
                self.logger.debug("Sample of retrieved data (first 3 rows):")
                self.logger.debug(df.head(3).to_string())

            return df
        except Exception as e:
            self.logger.error(f"BigQuery query failed: {str(e)}")
            self.logger.error(f"Query: {query}")
            raise

    def _clean_sku(self, sku: str) -> str:
        """
        Clean SKU format - remove decimals and leading zeros

        Args:
            sku: Raw SKU string

        Returns:
            Cleaned SKU string
        """
        if pd.isna(sku):
            return None

        sku_str = str(sku)

        # Remove decimal portion if exists
        if '.' in sku_str:
            sku_str = sku_str.split('.')[0]

        # Remove leading zeros but keep at least one digit
        sku_str = sku_str.lstrip('0') or '0'

        return sku_str

    def format_data_for_api(self, df: pd.DataFrame) -> List[Dict]:
        """
        Format BigQuery data into API expected format
        Groups by shipment AND siteID to handle each combination separately

        Args:
            df: DataFrame containing ASN data

        Returns:
            List of formatted shipment objects (one per shipment + siteID combination)
        """
        self.logger.info("Starting data formatting for API")

        if df.empty:
            self.logger.warning("No data to format - empty DataFrame")
            return []

        formatted_data = []

        # CRITICAL FIX: Group by BOTH shipment_id AND siteID
        # This ensures separate API calls for each shipment + siteID combination
        shipment_site_groups = df.groupby(['shipment_id', 'siteID'])

        self.logger.info(f"Found {len(shipment_site_groups)} unique shipment + siteID combinations")

        for (shipment_id, site_id), shipment_site_data in shipment_site_groups:
            self.logger.info(f"Processing shipment {shipment_id} for siteID {site_id}")

            # Get shipment-level info from first row
            first_row = shipment_site_data.iloc[0]

            # Dictionary to collect cartons for this shipment + site combination
            cartons_dict = {}

            # Group by carton within this shipment + site
            carton_groups = shipment_site_data.groupby('carton_id')

            for carton_id, carton_data in carton_groups:
                # Collect items for this carton
                line_items = []

                # Group by SKU to consolidate quantities (within same carton)
                sku_groups = carton_data.groupby('sku')
                for sku, sku_data in sku_groups:
                    # Sum quantities for same SKU in same carton
                    total_quantity = sku_data['quantity'].sum()

                    # Clean the SKU
                    cleaned_sku = self._clean_sku(sku)

                    if cleaned_sku:
                        line_items.append({
                            "quantity": int(total_quantity) if pd.notna(total_quantity) else 0,
                            "sku": cleaned_sku
                        })

                # Add carton to dictionary if it has items
                if line_items:
                    cartons_dict[carton_id] = {
                        "cartonReference": str(carton_id) if pd.notna(carton_id) else None,
                        "customProperties": "",  # Empty string as per expected format
                        "lineItems": line_items
                    }

            # Build shipment object with all its cartons for this siteID (only if there are cartons)
            if cartons_dict:
                shipment_obj = {
                    "cartons": list(cartons_dict.values()),
                    "purchaseOrderReference": str(first_row['purchaseOrderReference']) if pd.notna(
                        first_row['purchaseOrderReference']) else None,
                    "shipmentReference": str(shipment_id) if pd.notna(shipment_id) else None,
                    "siteId": str(site_id) if pd.notna(site_id) else None
                }

                formatted_data.append(shipment_obj)

                # Log details for this shipment + site combination
                carton_count = len(shipment_obj['cartons'])
                self.logger.info(f"  - Shipment {shipment_id} + Site {site_id}: {carton_count} cartons")

        self.logger.info(f"Data formatting completed: {len(formatted_data)} shipment+site combinations")

        # Log statistics
        total_cartons = sum(len(shipment['cartons']) for shipment in formatted_data)
        self.logger.info(f"Total cartons across all combinations: {total_cartons}")

        # Log shipment summary
        unique_shipments = set(shipment['shipmentReference'] for shipment in formatted_data)
        unique_sites = set(shipment['siteId'] for shipment in formatted_data)
        self.logger.info(f"Unique shipments: {len(unique_shipments)} ({list(unique_shipments)})")
        self.logger.info(f"Unique sites: {len(unique_sites)} ({list(unique_sites)})")

        # Log sample formatted data for debugging
        if formatted_data:
            self.logger.debug("Sample formatted shipment (first combination):")
            self.logger.debug(json.dumps(formatted_data[0], indent=2, default=str))

        return formatted_data

    def get_api_token(self) -> str:
        """
        Get API token from SML Token Endpoint

        Returns:
            API access token
        """
        # Check if cached token is still valid
        if self.api_token and self.token_expiry and datetime.now() < self.token_expiry:
            self.logger.debug("Using cached API token")
            return self.api_token

        self.logger.info("Getting new API token from SML endpoint")

        try:
            # Build authentication request
            auth_data = {
                "grant_type": "client_credentials",
                "client_id": self.sml_config['client_id'],
                "client_secret": self.sml_config['client_secret'],
                "scope": self.sml_config['scope']
            }

            headers = {
                "Content-Type": "application/x-www-form-urlencoded"
            }

            self.logger.debug(f"Requesting token from: {self.sml_config['token_endpoint']}")
            self.logger.debug(f"Auth scope: {auth_data['scope']}")

            response = requests.post(
                self.sml_config['token_endpoint'],
                data=auth_data,
                headers=headers,
                timeout=self.sml_config.get('timeout', 30)
            )

            self.logger.debug(f"Token response status: {response.status_code}")

            if response.status_code != 200:
                self.logger.error(f"Token request failed with status {response.status_code}")
                self.logger.error(f"Response: {response.text}")

            response.raise_for_status()

            token_data = response.json()
            self.api_token = token_data.get('access_token')

            if not self.api_token:
                raise Exception("No access_token in response")

            # Calculate token expiry time (refresh 5 minutes before actual expiry)
            expires_in = token_data.get('expires_in', 3600)
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)

            self.logger.info(f"Successfully obtained API token, expires in {expires_in} seconds")
            return self.api_token

        except Exception as e:
            self.logger.error(f"Failed to get API token: {str(e)}")
            raise

    def call_sml_carton_endpoint(self, shipment_data: List[Dict]) -> Dict:
        """
        Call SML Shipment Endpoint to send data
        Now sends one API call per shipment+siteID combination

        Args:
            shipment_data: List of formatted shipment objects

        Returns:
            API response results
        """
        if not shipment_data:
            self.logger.info("No shipment data to send")
            return {
                "total_shipments": 0,
                "total_batches": 0,
                "batch_results": [],
                "overall_status": "completed"
            }

        # Get API token
        token = self.get_api_token()

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # CRITICAL FIX: Send one call per shipment+siteID combination
        # Each shipment object already represents one shipment+siteID combo
        results = []

        total_combinations = len(shipment_data)
        total_cartons = sum(len(shipment['cartons']) for shipment in shipment_data)

        self.logger.info(f"Sending {total_combinations} shipment+site combinations ({total_cartons} total cartons)")
        self.logger.info(f"Target endpoint: {self.sml_config['carton_endpoint']}")

        # Send each shipment+siteID combination individually
        for i, shipment_combo in enumerate(shipment_data, 1):
            shipment_ref = shipment_combo.get('shipmentReference', 'unknown')
            site_id = shipment_combo.get('siteId', 'unknown')
            carton_count = len(shipment_combo.get('cartons', []))

            self.logger.info(
                f"Sending combination {i}/{total_combinations}: Shipment {shipment_ref} + Site {site_id} ({carton_count} cartons)")

            try:
                # Send single shipment as array (API expects array format)
                payload = [shipment_combo]

                # Log the exact endpoint being called
                self.logger.debug(f"Sending POST request to: {self.sml_config['carton_endpoint']}")

                response = requests.post(
                    self.sml_config['carton_endpoint'],
                    json=payload,
                    headers=headers,
                    timeout=self.sml_config.get('timeout', 300)
                )

                # Log response status and content for debugging
                self.logger.info(f"Response status code: {response.status_code}")

                # CRITICAL FIX: Handle empty 200 responses properly
                if response.status_code == 200:
                    # Check if response has content before trying to parse JSON
                    response_text = response.text.strip()
                    if response_text:
                        try:
                            result = response.json()
                            self.logger.debug(f"Success response: {json.dumps(result, indent=2, default=str)}")
                        except json.JSONDecodeError as e:
                            # Even if JSON parsing fails, a 200 means success
                            self.logger.warning(f"200 response but invalid JSON: {e}")
                            self.logger.warning(f"Response content: {response_text[:200]}...")
                            result = {"status": "success", "message": "API returned 200 but no/invalid JSON"}
                    else:
                        # Empty 200 response - still success
                        self.logger.info("200 response with empty body - treating as success")
                        result = {"status": "success", "message": "API returned 200 with empty response"}

                    # Record success
                    results.append({
                        "combination_id": f"shipment_{shipment_ref}_site_{site_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        "status": "success",
                        "shipment_reference": shipment_ref,
                        "site_id": site_id,
                        "carton_count": carton_count,
                        "response": result,
                        "timestamp": datetime.now().isoformat()
                    })

                    self.logger.info(f"Combination {i} sent successfully: Shipment {shipment_ref} + Site {site_id}")

                else:
                    # Non-200 status codes
                    self.logger.error(f"Failed with status {response.status_code}")
                    self.logger.error(f"Response content: {response.text}")
                    response.raise_for_status()

            except Exception as e:
                error_result = {
                    "combination_id": f"shipment_{shipment_ref}_site_{site_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    "status": "failed",
                    "shipment_reference": shipment_ref,
                    "site_id": site_id,
                    "carton_count": carton_count,
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
                results.append(error_result)
                self.logger.error(f"Combination {i} failed: Shipment {shipment_ref} + Site {site_id} - {str(e)}")

                # Log the type of exception for debugging
                self.logger.error(f"Exception type: {type(e).__name__}")

        # Calculate final results
        total_combinations_sent = len(results)
        successful_combinations = len([r for r in results if r['status'] == 'success'])

        self.logger.info(
            f"API calls completed: {successful_combinations}/{total_combinations_sent} combinations successful")

        if successful_combinations < total_combinations_sent:
            self.logger.warning(f"Failed combinations: {total_combinations_sent - successful_combinations}")

        return {
            "total_shipments": total_combinations,
            "total_cartons": total_cartons,
            "total_combinations": total_combinations_sent,
            "successful_combinations": successful_combinations,
            "combination_results": results,
            "overall_status": "completed" if successful_combinations == total_combinations_sent else "partial_failure"
        }

    def run_pipeline(self) -> Dict:
        """
        Run complete ASN Carton Pipeline

        Returns:
            Pipeline execution results
        """
        start_time = datetime.now()
        pipeline_run_id = f"asn_pipeline_{start_time.strftime('%Y%m%d_%H%M%S')}"

        self.logger.info(f"Starting ASN Carton Pipeline execution - Run ID: {pipeline_run_id}")

        try:
            # Step 1: Retrieve today's ASN data from BigQuery
            self.logger.info("Step 1: Retrieving ASN data from BigQuery")
            asn_data = self.retrieve_asn_data()

            if asn_data.empty:
                self.logger.warning("No ASN data found for today - completing with no processing")
                result = {
                    "pipeline_run_id": pipeline_run_id,
                    "status": "completed",
                    "message": "No data to process",
                    "execution_time": str(datetime.now() - start_time),
                    "data_summary": {
                        "source_records": 0,
                        "combinations_processed": 0,
                        "cartons_processed": 0
                    },
                    "api_results": {
                        "total_shipments": 0,
                        "total_cartons": 0,
                        "total_combinations": 0,
                        "combination_results": [],
                        "overall_status": "completed"
                    }
                }
                return result

            # Step 2: Format data for API
            self.logger.info("Step 2: Formatting data for SML API")
            formatted_data = self.format_data_for_api(asn_data)

            # Step 3: Call SML Carton Endpoint
            self.logger.info("Step 3: Sending data to SML Carton API")
            api_results = self.call_sml_carton_endpoint(formatted_data)

            # Step 4: Build final execution results
            execution_time = datetime.now() - start_time
            result = {
                "pipeline_run_id": pipeline_run_id,
                "status": "completed" if api_results['overall_status'] == 'completed' else "partial_failure",
                "execution_time": str(execution_time),
                "data_summary": {
                    "source_records": len(asn_data),
                    "combinations_processed": len(formatted_data),
                    "cartons_processed": api_results.get('total_cartons', 0)
                },
                "api_results": api_results
            }

            self.logger.info(f"ASN Carton Pipeline execution completed")
            self.logger.info(f"Execution time: {execution_time}")
            self.logger.info(
                f"Processed {len(asn_data)} source records into {len(formatted_data)} shipment+site combinations")
            self.logger.info(f"Total cartons sent: {api_results.get('total_cartons', 0)}")

            return result

        except Exception as e:
            execution_time = datetime.now() - start_time
            self.logger.error(f"ASN Carton Pipeline execution failed: {str(e)}")

            result = {
                "pipeline_run_id": pipeline_run_id,
                "status": "failed",
                "error": str(e),
                "execution_time": str(execution_time)
            }

            raise


# Airflow task function
def run_asn_pipeline():
    """Airflow task function for running ASN pipeline"""
    pipeline = ASNCartonPipeline()
    result = pipeline.run_pipeline()

    # Check for failed combinations
    failed_combinations = [combo for combo in result.get('api_results', {}).get('combination_results', [])
                           if combo.get('status') == 'failed']

    if failed_combinations:
        raise Exception(f"ASN Pipeline execution failed - {len(failed_combinations)} combinations failed")

    return result


# Direct execution example (for testing)
if __name__ == "__main__":
    # Set logging level for testing
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Run directly for testing
    pipeline = ASNCartonPipeline()
    result = pipeline.run_pipeline()
    print(json.dumps(result, indent=2, default=str))