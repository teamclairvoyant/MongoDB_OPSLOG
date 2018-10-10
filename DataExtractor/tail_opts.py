import optparse
import ast
import sys
import time
from pymongo import CursorType
from bson import Timestamp
from  SqliteQueue1 import SqliteQueue
from config import config_parser as config_parser
from logger import extraction_logger
from connector.mongodb_connector import MongoDatabase
# connect to the target mongo server


def __get_id(doc):
    id = None
    o2 = doc.get('o2')
    if o2 is not None:
        id = o2.get('_id')

    if id is None:
        id = doc['o'].get('_id')

    return id

def oplog_watcher(mongo_db,sqlite_db,collection_list,last_ts_high_watermark):
 while True:
    # prepare the tail query and kick it off
    #query = {'ts': {'$gt': last_ts}}
    #tail_opts = {'tailable': True, 'await_data': True}
    # query = {'$and': [{'ts': {'$gt': last_ts}},{'op': {'$in': ['i', 'u']}}]}

    query= {'$and': [{'ts': {'$gt': last_ts_high_watermark}},{'ns':{'$in':collection_list}},{ 'op': { '$in': ['i','u'] } } ]}
    cursor = mongo_db.oplog.rs.find(query, cursor_type = CursorType.TAILABLE_AWAIT)
    cursor.add_option(8)
    '''Tailable Cusrosr Which fetches New Records from opslog Collection in Infinite Loop'''
    try:
        while cursor.alive:
            try:
                collection_objectid_list = []
                opslog_ts_watermark_list = []
                # grab a document if available
                doc = cursor.next()

                '''Below get_id will get IDS for Insert or Update document since for update we get id value in  o2 Field and for insert in o Field'''
                id = __get_id(doc)
                #print(doc)

                oplog_ts=str(doc.get('ts')).split('(')[1].split(',')[0]
                oplog_ts_datetime=str(doc.get('ts').as_datetime().replace(tzinfo=None))
                ts_inc=doc.get('ts').inc
                mongo_object_id=str(id)
                operation_type=doc.get('op')
                name_space=doc.get('ns')
                status='PENDING'

                '''Will make entry in Two Tables one for High Water Mark
                and One table will contain all the object ID and status
                collection_objectid_queue(Table)======> 3       test_database.test_collection_with_timestamp      5b2e451be0410d13cc042721 1529759035       1       2018-06-23 13:03:55     i       PENDING
                opslog_ts_watermark_queue (Table)===>  61       1529758703      1       2018-06-23 12:58:23
                '''
                collection_objectid_list.append(name_space)
                collection_objectid_list.append(mongo_object_id)
                collection_objectid_list.append(oplog_ts)
                collection_objectid_list.append(ts_inc)
                collection_objectid_list.append(oplog_ts_datetime)
                collection_objectid_list.append(operation_type)
                collection_objectid_list.append(status)

                collection_objectid_tuple=tuple(collection_objectid_list)

                opslog_ts_watermark_list.append(oplog_ts)
                opslog_ts_watermark_list.append(ts_inc)
                opslog_ts_watermark_list.append(oplog_ts_datetime)
                opslog_ts_watermark_tuple = tuple(opslog_ts_watermark_list)

                #print(doc.get('ts'),id,doc.get('ts').as_datetime(),doc.get('ts').inc,doc.get('op'),doc.get('ns'))
                #now = Timestamp(doc.get('ts').as_datetime(), doc.get('ts').inc)
                sqlite_db.append(collection_objectid_tuple,opslog_ts_watermark_tuple)

            except StopIteration:
                # thrown when the cursor is out of data, so wait
                # for a period for some more data
                time.sleep(10)
    finally:
        cursor.close()

if __name__ == "__main__":
    extraction_jobs = dict()
    extraction_logs = dict()
    p = optparse.OptionParser()
    p.add_option('--config_path')
    p.add_option('--notification', default='false')

    arg_options, arguments = p.parse_args()
    config_path = arg_options.config_path
    notification = arg_options.notification in ['True', 'true', 'TRUE']

    if config_path is None :
        print("Error: 1 or more parameters are missing")
        print("Usage: script --config_path <config_path>")
        sys.exit(1)

    # Read config file
    config_parser.config_file_path = config_path
    config_parser.set_config()
    log = extraction_logger.getLogger(__name__)
    log.info(extraction_logger)
    metadata_db_process_config = config_parser.config_section_map("metadata_db_connection")
    source_db_config = config_parser.config_section_map("source_db_connection")
    sqlite_db = metadata_db_process_config["meta_file_path"]

    collection_white_list = ast.literal_eval(config_parser.config_section_map("source_db_connection")['collection_list'])
    log.info("<=====Collection List Processes by Opslog ======>"+', '.join(collection_white_list))
    source_db = MongoDatabase(dbtype=source_db_config.get("dbtype"), host=source_db_config.get("host"),
                              port=int(source_db_config.get("port")),
                              user=source_db_config.get("user"), password=source_db_config.get("pass"),
                              database=source_db_config.get("database"))
    mongo_db_connection=source_db.connect()

    # get the latest timestamp in the database
    # last_ts = db.oplog.rs.find().sort('$natural', -1)[0]['ts'];
    # print(last_ts.as_datetime())
    sqlite_db_connection = SqliteQueue(sqlite_db)
    ts_value = sqlite_db_connection.select_max()

    if not ts_value:
        last_ts=Timestamp(int(time.time()), 1)
        #last_ts = Timestamp(1533198932, 1)
    else:
        (key, value) = ts_value
        last_ts = Timestamp(key, value)

    print(last_ts.as_datetime())

    oplog_watcher(mongo_db_connection,sqlite_db_connection,collection_white_list,last_ts)