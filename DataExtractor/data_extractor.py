import errno
import os,re
import sys
from datetime import datetime
from pathlib import Path

from dateutil import parser as date_parser
from dateutil.relativedelta import *

log = list()
err_log = list()
global last_key_value_max;

def generate_replace_column_select_list(column_list,database_name,table_name,source_db, metadata_db):
    try:
        column_with_datatype = source_db.execute_custom_query("show fields from {}.{}".format(database_name,table_name))
        # Will use this List to Generate REPLACE( for all the text/varchar Column
        select_varchar_replace_list = list()
        # Will use this List for all integer or date column,will not run replace \\n for this
        select_non_varchar_list= list()
        #Will hold all column in case * is passed in SELECT
        select_all_column_list=list()
        select_str = ""
        all_column_list=column_list

        get_columnname_datatype=[[x[0],x[1]] for x in column_with_datatype]

        for item in get_columnname_datatype:
            select_all_column_list.append(item[0])
            if len(re.findall(r'varchar|char|text',item[1])) >=1:
                select_varchar_replace_list.append(item[0])
            else:
                select_non_varchar_list.append(item[0])

        if str(column_list).find("*") != -1:
            all_column_list=" "
            all_column_str= ','.join(str(e) for e in select_all_column_list)
            all_column_list=all_column_str


        for item in all_column_list.split(','):
         if item.upper() in (name.upper() for name in select_varchar_replace_list):

            select_str += "REPLACE(REPLACE(REPLACE({}, '\\r', ' '), '\\n', '\\\\n'), '\\r\\n', ' ') ,".format(item)
         else:
            select_str += item +","

        final_string = select_str.rstrip(',')
        log.append("Final Select String For Extraction {}".format(final_string))



    except Exception as e:
        err_log.append("Failed to execute show fields  query {}".format(str(e)))
        sys.exit(1)
    return final_string


def generate_data_query(base_config, table_config,source_db, metadata_db):
    try:
        log.append("Extracting data based on configuration {}".format(table_config))
        property_name = base_config.get('property')
        domain = base_config.get('domain')
        table_name = table_config.get('table')
        where_clause = ''
        batch_date = base_config.get('batch_date')
        data_date = base_config.get('data_date')
        database_name = base_config.get('database')
        column_list = ",".join(table_config.get('columns')) if table_config.get('columns') else '*'
        log.append("Fetch column list: {}".format(column_list))
        #Method For Replcaing new Line Character from MYSQL Source
        column_list=generate_replace_column_select_list(column_list,database_name,table_name,source_db, metadata_db)
        #print(column_list)

        # Add data filter condition
        if table_config.get('extraction_type') == "FULL" and table_config.get('where_clause'):
            log.append("Executing as FULL load for data util {}".format(data_date))
            log.append('Generate where clause based on data date value')
            next_data_date = (date_parser.parse(data_date) + relativedelta(days=1)).strftime("%Y-%m-%d")
            where_clause = table_config.get('where_clause').format(next_data_date)

        elif table_config.get('extraction_type') == "MYSQL_WHERE" and table_config.get('where_clause'):
            log.append('Generate where clause based on data date value')
            next_data_date = (date_parser.parse(data_date) + relativedelta(days=1)).strftime("%Y-%m-%d")

            log.append("Query for data_date: {} and batch_date: {}".format(data_date, batch_date))
            where_clause = table_config.get('where_clause').format(next_data_date, data_date)

        elif table_config.get('extraction_type') == "MYSQL_KEY" and table_config.get('key_column'):
            log.append('Generate where clause based on key_column latest value')
            table_record = metadata_db.get_table_record(table_name)
            last_key_value = table_record.get('last_key_value') if table_record else "0"
            last_key_query = "select max({}) from {}".format(table_config.get('key_column'), table_name) if table_config.get('key_column') else None
            global last_key_value_max
            last_key_value_max = source_db.execute_custom_query(last_key_query)[0] if last_key_query else None
            where_clause = table_config.get('key_column') + " > " + str(int(last_key_value)) + " and " + table_config.get('key_column')  +" <= " +str(int(last_key_value_max))

        elif table_config.get('extraction_type') == "MYSQL_RANGE" and table_config.get('where_clause'):
            where_clause = table_config.get('where_clause')

        log.append("Filter condition: {}".format(where_clause))

        s3_target_config = base_config.get('s3_target_config')
        s3_prefix_config = s3_target_config.get('s3_file_prefix')
        if s3_prefix_config and s3_prefix_config.strip() and len(s3_prefix_config.strip())>1:
            s3_data_file = s3_prefix_config+"_"+"".join(table_name.title().split('_')) +  "MySQLToS3Task_" + data_date
        else:
            s3_data_file = "".join(table_name.title().split('_')) + "MySQLToS3Task_" + data_date

        # s3_data_file = "{}_{}_{}_{}_{}.txt".format(property_name, domain, table_name, batch_date, data_date)
        s3_data_location = s3_target_config.get("s3_location") + "/" + s3_data_file
        log.append("Data file for table {} : {}".format(table_name, s3_data_location))
        data_query = "SELECT {} ".format(column_list)
        data_query += "FROM {}.{} ".format(database_name,table_name)
        if where_clause:
            data_query += "WHERE {} ".format(where_clause)

        data_query += "INTO OUTFILE S3 '{}' ".format(s3_data_location)
        data_query += "FIELDS TERMINATED BY '{}' ".format(s3_target_config.get("field_delim"))
        data_query += "LINES TERMINATED BY '{}'".format(s3_target_config.get("line_delim"))
        data_query+="OVERWRITE ON;"

        log.append("Fetch data as: " + data_query)
        log.append("Fetch data for table {} as : {}".format(table_name, data_query))

    except Exception as e:
        err_log.append("Failed to generate extraction query query {}".format(str(e)))
        sys.exit(1)

    return data_query


def execute_data_query(source_db, extraction_query):
    try:
        stats = source_db.execute_query(extraction_query)
    except Exception as e:
        err_log.append("Failed to execute query {}".format(str(e)))
        sys.exit(1)
    return stats


def generate_touch_file(base_config, table_config):
    try:
        property_name = base_config.get('property')
        domain = base_config.get('domain')
        table_name = table_config.get('table')
        batch_date = base_config.get('batch_date')
        touch_file_path = base_config.get('touch_file_base_dir') + "/" + base_config.get('batch_date')
        if not os.path.exists(touch_file_path):
            os.makedirs(touch_file_path)
        touch_file = touch_file_path + "/" + "{}_{}_{}_{}_0000.done".format(property_name, domain, table_name, batch_date)

        log.append("Creating touch file {}".format(touch_file))
        Path(touch_file).touch()
    except OSError as e:
        if e.errno != errno.EEXIST:
            err_log.append("Failed to create touch batch date location {} Error: {}".format(touch_file_path, str(e)))
    except Exception as e:
        err_log.append("Failed to create touch file {} Error: {}".format(touch_file, str(e)))
        sys.exit(1)


def extract_process(queue, base_config, table_config, source_db, metadata_db):
    try:
        # Generate extraction query
        log.append("Generating extraction query based on table configuration")
        extraction_query = generate_data_query(base_config, table_config,source_db, metadata_db)

        # Execute extraction query
        log.append("Executing extraction query for table {}".format(table_config.get('table')))
        stats = execute_data_query(source_db, extraction_query)
        log.append("Fetched {} records from table {}".format(stats, table_config.get('table')))

        # Update metadata record for the table
        global last_key_value_max;
        if table_config.get('extraction_type') == "MYSQL_KEY" and table_config.get('key_column') and stats>0:
            update_table_metadata(base_config, table_config, source_db, metadata_db,last_key_value_max)

        # Generate touch file for the following process to pick from
        # generate_touch_file(base_config, table_config) --- not generating touchfiles as not required.
    finally:
        queue.put(log)
        queue.put(err_log)


def update_table_metadata(base_config, table_config, source_db, metadata_db,last_key_value_max):
    try:
        property_name = base_config.get('property')
        domain = base_config.get('domain')
        database = table_config.get('database')
        table = table_config.get('table')
        key_column = table_config.get('key_column')
        last_key_value = last_key_value_max
        last_updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record_update = {'property': property_name, 'domain': domain, 'database': database, 'table': table, 'key_column': key_column, 'last_key_value': last_key_value, 'last_updated': last_updated}

        log.append("Updating record for table {} as {}".format(table, record_update))
        metadata_db.upsert(record_update)
    except Exception as e:
        err_log.append("Failed to update table record for {} Error: {}".format(table, str(e)))
        sys.exit(1)


def generate_qc_metrics():
    pass


def generate_qc_file():
    pass