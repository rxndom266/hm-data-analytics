from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, to_date, trim, regexp_replace, md5, concat, date_format, coalesce, current_date, current_timestamp
import re

class TransformActivity:
  
  emptyRDD = spark.sparkContext.emptyRDD()
  
  def __init__(self, config, load_type, inputdf, cust_code):
    self.config = config
    self.load_type = load_type
    self.inputdf = inputdf
    self.cust_code = cust_code
    
  def field_map(self, df):
    for k, v in self.config['fields'].items():
      if v in df.columns:
        df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
      else:
        df = df.withColumn(k, lit(None))
    df = df.withColumn('base_activity_type', lit(None)).withColumn('base_activity_id', lit(None)) # 42 columns
    print("exiting with", len(df.columns), "columns")
    return df
  
  def add_metadata(self, df):
    df = df.withColumn('create_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')) # 44 columns
    self.emptyCDM = spark.createDataFrame(self.emptyRDD, df.schema) # 44 columns
    self.errorlog = self.emptyCDM.withColumn('error_type', lit(None)) # 45 columns
    return df
  
  def group(self, df):
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df
      for x in self.config[typ]['mandatory']:
          temp = temp.where(col(x).isNotNull())
      df = df.subtract(temp)
      temp = temp.withColumn('base_activity_type', lit(typ))
      print(temp.count(), typ)
      res = res.union(temp)
    err = df.withColumn('error_type', lit("UNCLASSIFIABLE_ACTIVITY"))
    self.errorlog = self.errorlog.union(err)
    return res
  
  def generate_unique_id(self, df):
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ).withColumn('base_activity_id',lit(md5(concat(col('source_person_id'),\
                                                                                                   col('base_activity_type'),\
                                                                                                   col(self.config[typ]['hash']\
                                                                                                      )))))
      res = res.union(temp)
    return res
  
  def transform(self):
    res = self.field_map(self.inputdf)
    res = self.add_metadata(res)
    res = self.group(res)
    res = self.generate_unique_id(res)
    
    return res, self.errorlog
  
  
activity_transform_config = {
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

# driver code

inputdf = # test dataset
ob = TransformActivity(activity_transform_config, inputdf, 'test_dataset', 'RandomString')
ob.transform()
