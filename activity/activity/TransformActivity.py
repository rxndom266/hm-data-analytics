from pyspark.sql.functions import col, lit, trim, current_timestamp, date_format, md5, concat
from delta.tables import *
import urllib

spark = SparkSession \
    .builder \
    .appName("Transform") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .getOrCreate()

emptyRDD = spark.sparkContext.emptyRDD()

transconfig = {
    "base_activity_types":[
        "email_sent",
        "email_open",
        "email_clickthrough",
        "incoming_call",
        "webclick"
    ],
    "fields":{
        "email_event_id": "email_event_id",
        "mastered_person_id": "mastered_person_id",
        "source_person_id": "source_person_id",
        "source_activity_id":"source_activity_id",
        "data_source":"data_source",
        "subject_line": "subject_line",
        "asset_id": "asset_id",
        "asset_name": "asset_name",
        "email_sent_datetime": "email_sent_datetime",
        "email_opened_datetime": "email_opened_datetime",
        "email_clickthrough_datetime": "email_clickthrough_datetime",
        "email_web_url": "email_web_url",
        "email_clicked_through_url": "email_clicked_through_url",
        "campaign_id": "campaign_id",
        "phone_number_to":"phone_number_to",
        "phone_number_from":"phone_number_from",
        "call_tracking_number":"call_tracking_number",
        "direction":"direction",
        "call_status_code":"call_status_code",
        "call_start_datetime":"call_start_datetime",
        "call_end_datetime":"call_end_datetime",
        "call_duration":"call_duration",
        "activity_type":"activity_type",
        "activity_datetime":"activity_datetime",
        "referrer_url":"referrer_url",
        "landing_page_url":"landing_page_url",
        "page_visited":"page_visited",
        "number_of_pages":"number_of_pages",
        "start_datetime":"start_datetime",
        "end_datetime":"end_datetime",
        "first_name":"first_name",
        "last_name":"last_name",
        "street_address_1":"street_address_1",
        "city":"city",
        "state_province":"state_province",
        "postal_code":"postal_code",
        "gender_code":"gender_code",
        "birth_date":"birth_date",
        "home_phone":"home_phone",
        "primary_email":"primary_email"
    },
    "email_sent": {
        "hash":"email_event_id",
        "mandatory": [
            "email_event_id",
            "source_person_id",
            "data_source",
            "subject_line",
            "asset_id",
            "asset_name",
            "email_sent_datetime",
            "email_web_url",
            "campaign_id"
        ]
    },
    "email_open": {
        "hash":"email_event_id",
        "mandatory": [
            "email_event_id",
            "source_person_id",
            "data_source",
            "subject_line",
            "asset_id",
            "asset_name",
            "email_opened_datetime",
            "email_web_url",
            "campaign_id"
        ]
    },
    "email_clickthrough": {
        "hash":"email_event_id",
        "mandatory": [
            "email_event_id",
            "source_person_id",
            "data_source",
            "subject_line",
            "asset_id",
            "asset_name",
            "email_clickthrough_datetime",
            "email_web_url",
            "email_clicked_through_url",
            "campaign_id"
        ]
    },
    "incoming_call":{
        "hash":"source_activity_id",
        "mandatory":[
            "source_person_id",
            "source_activity_id",
            "data_source",
            "phone_number_to",
            "phone_number_from",
            "call_tracking_number",
            "direction",
            "call_status_code",
            "call_start_datetime",
            "call_end_datetime",
            "campaign_id",
            "first_name",
            "last_name",
            "street_address_1",
            "city",
            "state_province",
            "postal_code",
            "gender_code",
            "birth_date",
            "home_phone",
            "primary_email"
        ]
        },
        "webclick":{
        "hash":"source_activity_id",
        "mandatory":[
            "source_person_id",
            "source_activity_id",
            "data_source",
            "activity_type",
            "activity_datetime",
            "referrer_url",
            "landing_page_url",
            "page_visited",
            "number_of_pages",
            "start_datetime",
            "end_datetime",
            "first_name",
            "last_name",
            "street_address_1",
            "city",
            "state_province",
            "postal_code",
            "gender_code",
            "birth_date",
            "home_phone",
            "primary_email"
        ]
    }
}

def purge(path):
    dbutils.fs.rm(path, recurse=True)

def transform(load_id, client_file_path, destpath, rejectpath):

    df = spark.read.csv(client_file_path)
    config = transconfig                    
    null = '--'
    
    # renaming columns
    for k, v in config['fields'].items():
        if v in df.columns:
            df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
        else:
            df = df.withColumn(k, lit(null))
    df = df.withColumn('base_activity_type', lit(null))\
    .withColumn('base_activity_id', lit(null)) # 42 columns

    print("exiting with", len(df.columns), "columns")

    # adding metadata
    df = df.withColumn('create_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('delete_flag', lit(0))\
    .withColumn('load_id', lit(load_id)) # 46 columns
                        
    emptyCDM = spark.createDataFrame(emptyRDD, df.schema) # 46 columns
    errorlog = emptyCDM.withColumn('error_type', lit(null)) # 47 columns

    try:
        errorlog.write.format('delta').save(rejectpath)
    except:
        print('reject cdm already exists')
    rejectDelta = DeltaTable.forPath(spark, rejectpath)

        # classifying activities
    res = emptyCDM
    for typ in config['base_activity_types']:
        temp = df
        for x in config[typ]['mandatory']:
            temp = temp.where((col(x)!=null) & (col(k).isNotNull()) & (col(k)!=''))
        df = df.subtract(temp)
        temp = temp.withColumn('base_activity_type', lit(typ))
        print(temp.count(), typ)
        res = res.union(temp)
    err = df.withColumn('error_type', lit("UNCLASSIFIABLE_ACTIVITY"))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id')\
        .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    trans = emptyCDM
    for typ in config['base_activity_types']:
        temp = res.where(col('base_activity_type')==typ).withColumn('base_activity_id',lit(md5(concat(col('source_person_id'),\
                                                                                                    col('base_activity_type'),\
                                                                                                    col(config[typ]['hash']\
                                                                                                        )))))
        trans = trans.union(temp)
        
        # persist to Delta
        try:
            trans.write.format('delta').save(destpath)
        except:
            transDelta = DeltaTable.forPath(spark, destpath)
            transDelta.alias('del').merge(trans.alias('dat'), 'del.base_activity_id = dat.base_activity_id')\
                .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

    print('transform done')
    return
