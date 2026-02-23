"""
Module for building payloads for ETL processes.
"""
from src.utils.date_utils import get_incremental_dates, get_full_dates
from typing import Any, Dict, List, Optional
from src.config.logger import logger
import json

class PayloadBuilderService:
    """
    Class to build payloads for ETL processes based on provided parameters.
    """
    def __init__(self,
                 job_type: str,
                 date_type: str,
                 process_type: str,
                 job_parameters: List[Dict[str, Any]],
                 db_type: Optional[str] = None,
                 secret_db: Optional[str] = None):
        self.job_type = job_type
        self.date_type = date_type
        self.process_type = process_type.upper()
        self.job_parameters = job_parameters or []
        self.db_type = db_type
        self.secret_db = secret_db


    def create_payloads(self) -> Dict[str, Any]:
        """
        Process ETL job based on process_type.

        Returns:
            Dict[str, Any]: Payloads for the ETL job.
        """
        if self.process_type not in ("FULL", "INC"):
            logger.error(f"[ERROR] Invalid process_type: {self.process_type}. Use 'FULL' or 'INC'.")
            raise ValueError("Invalid 'process_type'. Must be 'FULL' or 'INC'.")
        
        payloads_raw = []
        payloads_staging = []

        if self.job_type in ('raw', 'raw-to-staging'):
            logger.info(f"[INFO] Building {self.process_type} payloads for raw job...")
            if self.db_type is None or self.secret_db is None:
                logger.error(f"[ERROR] Missing required parameters for raw job payload creation (db_type or secret_db).")
                raise ValueError("Missing required parameters for raw job payload creation")
            payloads_raw = self.create_payloads_raw()
            
        if self.job_type in ('staging', 'raw-to-staging'):
            logger.info(f"[INFO] Building {self.process_type} payloads for staging job...")
            payloads_staging = self.create_payloads_staging()

        return {"status": "payload generation complete",
                "payloads_raw": payloads_raw,
                "payloads_staging": payloads_staging}
    

    def create_payloads_raw(self) -> List[Dict[str, Any]]:
        """
        Create payloads for Raw job.

        Returns:
            List[Dict[str, Any]]: List of payloads for Raw job.
        """
        payloads = []

        if self.process_type == "FULL":
            for config in self.job_parameters:
                if 'fecha_inicial' in config['params'].keys() and 'fecha_final' in config['params'].keys():
                    logger.info(f"[INFO] Using 'fecha_inicial' and 'fecha_final' for FULL process in Raw job.")
                    curr_start_date, curr_end_date = get_full_dates(config['params']['fecha_inicial']['value'], config['params']['fecha_final']['value'], self.date_type)
                elif 'fecha_corte' in config['params'].keys():
                    logger.info(f"[INFO] Using 'fecha_corte' for FULL process in Raw job.")
                    curr_start_date, curr_end_date = get_full_dates(config['params']['fecha_corte']['value'], config['params']['fecha_corte']['value'], self.date_type)
                else:
                    logger.error(f"[ERROR] Missing date parameters for FULL process in Raw job.")
                    raise ValueError("Missing date parameters for FULL process in Raw job.")
                payloads.append(self._build_raw_payload(config, curr_start_date, curr_end_date))
        else:
            curr_start_date, curr_end_date = get_incremental_dates(self.date_type)
            for config in self.job_parameters:
                payloads.append(self._build_raw_payload(config, curr_start_date, curr_end_date))

        return payloads
    

    def _build_raw_payload(self, config: Dict[str, Any], curr_start_date: str, curr_end_date: str) -> Dict[str, Any]:
        """
        Build payload for a Raw job based on configuration.

        Args:
            config (Dict[str, Any]): Configuration for the job.
            curr_start_date (str): Initial date for the job.
            curr_end_date (str): Final date for the job.

        Returns:
                Dict[str, Any]: Payload for the Raw job.
        """
        params = []
        for param_name, param_detail in config['params'].items():
            param_type = param_detail['type']
            param_value = param_detail['value']
            if param_name == 'fecha_inicial':
                param_value = curr_start_date
            elif param_name in ('fecha_final', 'fecha_corte'):
                param_value = curr_end_date
            params.append({"name": param_name,
                           "type": param_type,
                           "value": param_value})

        return {"name": config['name'],
                "path_name_input": config["path_name_input"].rstrip('/'),
                "file_name": config["file_name"],
                "partition_key_raw": config["partition_key_raw"],
                "params": json.dumps(params, separators = (',', ':'))}
    

    def create_payloads_staging(self) -> List[Dict[str, Any]]:
        """
        Create payloads for staging jobs.

        Returns:
            List[Dict[str, Any]]: List of payloads for staging jobs.
        """
        payloads = []

        if self.process_type == "FULL":
            for config in self.job_parameters:
                if 'fecha_inicial' in config['params'].keys() and 'fecha_final' in config['params'].keys():
                    logger.info(f"[INFO] Using 'fecha_inicial' and 'fecha_final' for FULL process in Staging jobs.")
                    curr_start_date, curr_end_date = get_full_dates(config['params']['fecha_inicial']['value'], config['params']['fecha_final']['value'], self.date_type)
                elif 'fecha_corte' in config['params'].keys():
                    logger.info(f"[INFO] Using 'fecha_corte' for FULL process in Staging jobs.")
                    curr_start_date, curr_end_date = get_full_dates(config['params']['fecha_corte']['value'], config['params']['fecha_corte']['value'], self.date_type)
                else:
                    logger.error(f"[ERROR] Missing date parameters for FULL process in Staging jobs.")
                    raise ValueError("Missing date parameters for FULL process in Staging jobs.")
                payloads.append(self._build_staging_payload(config, curr_start_date, curr_end_date))
        else:
            curr_start_date, curr_end_date = get_incremental_dates(self.date_type)
            for config in self.job_parameters:
                payloads.append(self._build_staging_payload(config, curr_start_date, curr_end_date))

        return payloads


    def _build_staging_payload(self, config: Dict[str, Any], curr_start_date: str, curr_end_date: str) -> Dict[str, Any]:
        """
        Build payload for a staging job based on configuration.

        Args:
            config (Dict[str, Any]): Configuration for the job.
            curr_start_date (str): Initial date for the job.
            curr_end_date (str): Final date for the job.

        Returns:
            Dict[str, Any]: Payload for the staging job.
        """
        return {"path_name_raw": config["path_name_input"],
                "path_name_staging": config["path_name_output"],
                "partition_key_raw": config["partition_key_raw"],
                "partition_key_staging": config["partition_key_staging"],
                "catalog_database": config["catalog_database"],
                "catalog_table": config["catalog_table"],
                "merge_keys": config["merge_keys"],
                "excel_sheet": config.get("excel_sheet", ""),
                "partition_key_raw_path": config.get("partition_key_raw_path", ""),
                "file_name_excel": config.get("file_name_excel", ""),
                "start_date": curr_start_date,
                "end_date": curr_end_date}