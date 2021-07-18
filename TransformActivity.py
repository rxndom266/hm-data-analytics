act_trans_conf = {
    "aws":{
      "bucket_name":"hmtrialbucket",
      "access_key":"********",
      "secret_key":"********"
    },
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
      "primary_email":"primary_email",
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
      ],
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

from pyspark.sql.functions import col, lit, trim, current_timestamp, date_format, md5, concat
from delta.tables import *
import urllib

spark.conf.set("spark.sql.legacy.timeParserPolicy","CORRECTED")

class TransformActivity:
  
  emptyRDD = spark.sparkContext.emptyRDD()
    null = '--'
  
  def __init__(self, config, load_type, cust_code):
    self.config = config
    self.load_type = load_type
    self.cust_code = cust_code
    self.deltapath = "/delta/{}/{}/transform".format(cust_code, load_type)
    self.rejectpath = "/delta/{}/{}/reject".format(cust_code, load_type)
    
  def mountfile(self):
    access_key = self.config['aws']['access_key']
    secret_key = self.config['aws']['secret_key'].replace("/","%2F")
    encoded_secret_key = urllib.parse.quote(secret_key,"")
    AWS_s3_bucket = "{}/{}".format(self.config['aws']['bucket_name'], self.cust_code)
    mount_name = "{}/{}".format(self.cust_code, self.load_type)
    sourceurl="s3a://{0}:{1}@{2}".format(access_key,encoded_secret_key,AWS_s3_bucket)
    dbutils.fs.mount(sourceurl,"/mnt/%s" %mount_name)
    self.mountpath = "/mnt/{}".format(self.mount_name)
    # dbutils.fs.ls("/mnt/%s" %mount_name)
    self.inputdf = spark.read.csv(mountpath)
    
  def purge(self):
    dbutils.fs.rm(self.deltapath, recurse=True)
    dbutils.fs.rm(self.rejectpath, recurse=True)
  
  def transform(self, df):
    # renaming columns
    
    for k, v in self.config['fields'].items():
      if v in df.columns:
        df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
      else:
        df = df.withColumn(k, lit(self.null))
    df = df.withColumn('base_activity_type', lit(self.null))\
        .withColumn('base_activity_id', lit(self.null)) # 42 columns
    
    print("exiting with", len(df.columns), "columns")
    
    # adding metadata
    df = df.withColumn('create_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
        .withColumn('delete_flag', lit(0))# 45 columns
    self.emptyCDM = spark.createDataFrame(self.emptyRDD, df.schema) # 45 columns
    errorlog = self.emptyCDM.withColumn('error_type', lit(self.null)) # 46 columns
    
    try:
      errorlog.write.format('delta').save(self.rejectpath)
    except:
      print('reject cdm already exists')
    rejectDelta = DeltaTable.forPath(spark, self.rejectpath)
    
     # classifying activities
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df
      for x in self.config[typ]['mandatory']:
          temp = temp.where(col(x)!=self.null & col(k).isNotNull() & col(k)!='')
      df = df.subtract(temp)
      temp = temp.withColumn('base_activity_type', lit(typ))
      print(temp.count(), typ)
      res = res.union(temp)
    err = df.withColumn('error_type', lit("UNCLASSIFIABLE_ACTIVITY"))
    # err = err.withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll().execute()
    
    trans = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = res.where(col('base_activity_type')==typ)\
        .withColumn('base_activity_id',lit(md5(concat(col('source_person_id'),\
                                                                                                   col('base_activity_type'),\
                                                                                                   col(self.config[typ]['hash']\
                                                                                                      )))))
      trans = trans.union(temp)
      
      # persist to Delta
      try:
        trans.write.format('delta').save(self.deltapath)
      except:
        transDelta = DeltaTable.forPath(spark, self.deltapath)
        transDelta.alias('del').merge(trans.alias('dat'), 'del.base_activity_id = dat.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    return self.deltapath, self.rejectpath
  
ob = TransformActivity(act_trans_conf, 'incoming_call', 'ANK266')
traspath, rejectpath = ob.transform(df)
