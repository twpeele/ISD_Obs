import os
import types
from dotenv import load_dotenv

class Configuration:

    def __init__(self):
        load_dotenv()

        self.db = {
            'host':     os.getenv('DB_HOST', 'localhost'),
            'user':     os.getenv('DB_USER', ''),
            'password': os.getenv('DB_PASSWORD', ''),
            'database': os.getenv('DB_NAME', 'hindsight')
        }

        if os.getenv('DEV_DB_HOST', False):
            self.dev = types.SimpleNamespace()
            self.dev.db = {
                'host':     os.getenv('DEV_DB_HOST'),
                'user':     os.getenv('DEV_DB_USER', ''),
                'password': os.getenv('DEV_DB_PASSWORD', ''),
                'database': os.getenv('DEV_DB_NAME', '')
            }

        self.aws = {
            'access_key':   os.getenv('AWS_ACCESS_KEY_ID', ''),
            'secret_key':   os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'instance':     os.getenv('AWS_INSTANCE_TYPE', ''),
        }

        self.paths = {
            'wgrib':            os.getenv('WGRIB2_PATH',''),
            'local_backup':     os.getenv('LOCAL_BACKUP_PATH', os.getenv('LOCAL_PROCESSING_PATH','')),
            'local_processing': os.getenv('LOCAL_PROCESSING_PATH',''),
            'local_asos_hrly_processing': os.getenv('LOCAL_ASOS_HRLY_PROCESSING',''),
            'local_asos_5min_processing': os.getenv('LOCAL_ASOS_5MIN_PROCESSING',''),
            'local_asos_buoy_processing': os.getenv('LOCAL_ASOS_BUOY_PROCESSING',''),
            'local_asos_buoy_backfill': os.getenv('LOCAL_ASOS_BUOY_BACKFILL', ''),
            'local_asos_can_backfill': os.getenv('LOCAL_ASOS_CAN_BACKFILL', '')
        }

    
