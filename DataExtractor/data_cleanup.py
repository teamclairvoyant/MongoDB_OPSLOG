import optparse
import sys

from config import config_parser as config_parser
from logger import extraction_logger
from datetime import datetime
from  SqliteQueue1 import SqliteQueue
log = list()
err_log = list()

if __name__ == "__main__":
    p = optparse.OptionParser()
    p.add_option('--config_path')
    p.add_option('--notification', default='False')

    arg_options, arguments = p.parse_args()
    config_path = arg_options.config_path
    notification = arg_options.notification in ['True', 'true', 'TRUE']

    if config_path is None:
        print("Error: 1 or more parameters are missing")
        print("Usage: script --config_path <config_path>")
        sys.exit(1)

    # Read config file
    config_parser.config_file_path = config_path
    config_parser.set_config()
    log = extraction_logger.getLogger(__name__)

    try:
        print("Starting data extraction")
        log.info("Starting data extraction")
        log.info("Reading environment configuration from {}".format(config_path))

        # Read config file
        config_parser.config_file_path = config_path
        config_parser.set_config()
        log = extraction_logger.getLogger(__name__)
        log.info(extraction_logger)
        metadata_db_process_config = config_parser.config_section_map("metadata_db_connection")
        #source_db_config = config_parser.config_section_map("source_db_connection")
        sqlite_db = metadata_db_process_config["meta_file_path"]
        cleaup_param=config_parser.config_section_map("cleanup_process")
        watermark_cleanup=cleaup_param["watermark_queue"]
        collection_objectId_cleanup=cleaup_param["collection_objectid"]

        sqlite_db_connection = SqliteQueue(sqlite_db)
        del_watermark_rows=sqlite_db_connection.delete_3daysold_data_watermark(watermark_cleanup)
        log.info("Deleted rows from opslog_ts_watermark_queue" + ','+str(del_watermark_rows))
        del_objectid_rows=sqlite_db_connection.delete_7daysold_data_collection_objectid(collection_objectId_cleanup)
        log.info("Deleted rows from opslog_ts_collection_objectid" + ','+str(del_objectid_rows))

    except Exception as e:
        log.error("Extraction process failed. {}".format(str(e)))
        print("Extraction process failed. {}".format(str(e)))
        sys.exit(1)
    finally:
        print("Extraction process logs: {}".format(log.handlers[0].baseFilename))