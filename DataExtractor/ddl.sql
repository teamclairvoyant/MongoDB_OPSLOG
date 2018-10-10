CREATE TABLE IF NOT EXISTS collection_objectid_queue 
            (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
              name_space TEXT ,
             mongo_object_id TEXT ,
             oplog_ts INTEGER ,
             ts_inc  INTEGER ,
             oplog_ts_datetime TEXT ,
              operation_type TEXT ,
             extraction_status TEXT

            );

CREATE TABLE IF NOT EXISTS opslog_ts_watermark_queue 
            (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
             oplog_ts INTEGER ,
            ts_inc  INTEGER ,
             oplog_ts_datetime TEXT 

            );