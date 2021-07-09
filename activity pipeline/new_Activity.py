from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import *
# from pyspark import SparkSession
from pyspark.sql.functions import col, lit, to_date, trim, regexp_replace, md5, concat
import re

# spark = SparkSession.builder.getOrCreate()
class Activity:
  emptyRDD = spark.sparkContext.emptyRDD()
  schema = StructType([
      StructField('email_event_id', StringType(), True),
      StructField('mastered_person_id', StringType(), True),
      StructField('source_person_id', StringType(), True),
      StructField('source_activity_id', StringType(), True),
      StructField('subject_line', StringType(), True),
      StructField('asset_id', StringType(), True),
      StructField('asset_name', StringType(), True),
      StructField('email_sent_datetime', StringType(), True),
      StructField('email_opened_datetime', StringType(), True),
      StructField('email_clickthrough_datetime', StringType(), True),
      StructField('email_web_url', StringType(), True),
      StructField('email_clicked_through_url', StringType(), True),
      StructField('campaign_id', StringType(), True),
      StructField('phone_number_to', StringType(), True),
      StructField('phone_number_from', StringType(), True),
      StructField('call_tracking_number', StringType(), True),
      StructField('direction', StringType(), True),
      StructField('call_status_code', StringType(), True),
      StructField('call_start_datetime', StringType(), True),
      StructField('call_end_datetime', StringType(), True),
      StructField('call_duration', StringType(), True),
      StructField('activity_type', StringType(), True),
      StructField('activity_datetime', StringType(), True),
      StructField('referrer_url', StringType(), True),
      StructField('landing_page_url', StringType(), True),
      StructField('page_visited', StringType(), True),
      StructField('number_of_pages', StringType(), True),
      StructField('first_name', StringType(), True),
      StructField('last_name', StringType(), True),
      StructField('street_address_1', StringType(), True),
      StructField('city', StringType(), True),
      StructField('state_province', StringType(), True),
      StructField('postal_code', StringType(), True),
      StructField('gender_code', StringType(), True),
      StructField('birth_date', StringType(), True),
      StructField('home_phone', StringType(), True),
      StructField('primary_email', StringType(), True),
      StructField('start_datetime', StringType(), True),
      StructField('end_datetime', StringType(), True),
      StructField('base_activity_type', StringType(), True),
      StructField('base_activity_id', IntegerType(), True)
  ])
  errschema = StructType([
      StructField('email_event_id', StringType(), True),
      StructField('mastered_person_id', StringType(), True),
      StructField('source_person_id', StringType(), True),
      StructField('source_activity_id', StringType(), True),
      StructField('subject_line', StringType(), True),
      StructField('asset_id', StringType(), True),
      StructField('asset_name', StringType(), True),
      StructField('email_sent_datetime', StringType(), True),
      StructField('email_opened_datetime', StringType(), True),
      StructField('email_clickthrough_datetime', StringType(), True),
      StructField('email_web_url', StringType(), True),
      StructField('email_clicked_through_url', StringType(), True),
      StructField('campaign_id', StringType(), True),
      StructField('phone_number_to', StringType(), True),
      StructField('phone_number_from', StringType(), True),
      StructField('call_tracking_number', StringType(), True),
      StructField('direction', StringType(), True),
      StructField('call_status_code', StringType(), True),
      StructField('call_start_datetime', StringType(), True),
      StructField('call_end_datetime', StringType(), True),
      StructField('call_duration', StringType(), True),
      StructField('activity_type', StringType(), True),
      StructField('activity_datetime', StringType(), True),
      StructField('referrer_url', StringType(), True),
      StructField('landing_page_url', StringType(), True),
      StructField('page_visited', StringType(), True),
      StructField('number_of_pages', StringType(), True),
      StructField('first_name', StringType(), True),
      StructField('last_name', StringType(), True),
      StructField('street_address_1', StringType(), True),
      StructField('city', StringType(), True),
      StructField('state_province', StringType(), True),
      StructField('postal_code', StringType(), True),
      StructField('gender_code', StringType(), True),
      StructField('birth_date', StringType(), True),
      StructField('home_phone', StringType(), True),
      StructField('primary_email', StringType(), True),
      StructField('start_datetime', StringType(), True),
      StructField('end_datetime', StringType(), True),
      StructField('base_activity_type', StringType(), True),
      StructField('base_activity_id', StringType(), True),
      StructField('error_type', StringType(), False)
  ])
  emptyCDM = spark.createDataFrame(emptyRDD, schema)
  config = {   
    "args":{
      "header":"true",
      "inferSchema":"false"
    },
    "base_activity_types":[
      "email_sent",
      "email_open",
      "email_clickthrough",
      "incoming_call",
      "webclick"
    ],
    "field_mapping":{
      "email_event_id": "email_event_id",
      "mastered_person_id": "mastered_person_id",
      "source_person_id": "source_person_id",
      "source_activity_id":"source_activity_id",
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
    "reorder":[
      "email_event_id",
      "mastered_person_id",
      "source_person_id",
      "source_activity_id",
      "subject_line",
      "asset_id",
      "asset_name",
      "email_sent_datetime",
      "email_opened_datetime",
      "email_clickthrough_datetime",
      "email_web_url",
      "email_clicked_through_url",
      "campaign_id",
      "phone_number_to",
      "phone_number_from",
      "call_tracking_number",
      "direction",
      "call_status_code",
      "call_start_datetime",
      "call_end_datetime",
      "call_duration",
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
    ],
    "email_sent": {
      "hash":"email_event_id",
      "mandatory": [
        "email_event_id",
        "source_person_id",
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
    },
    "validate_date":{
      "pattern":'yyyy-MM-dd',
      "fields":[
        "birth_date"
      ]
    },
    "validate_datetime":{
      "pattern":"yyyy-MM-dd HH:mm:ss",
      "fields":[
        "email_sent_datetime",
        "email_opened_datetime",
        "email_clickthrough_datetime",
        "call_start_datetime",
        "call_end_datetime",
        "start_datetime",
        "end_datetime"
      ]
    },
    "validate_url":{
      "pattern":"((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)",
      "fields":[
        "email_web_url",
        "email_clicked_through_url",
        "referrer_url",
        "landing_page_url"
      ]
    },
    "validate_email":{
      "pattern":'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)',
      "fields":[
        'primary_email'
      ]
    },
    "validate_phone":{
      "pattern":'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
      "fields":[
        'phone_number_from',
        'phone_number_to',
        'home_phone'
      ]
    }
  }
  def __init__(self, cust_code, lookup):
    self.cust_code = cust_code
    self.lookup = lookup
    self.CDMPATH = "/delta/{}/CDM".format(cust_code)
    self.error_df = spark.createDataFrame(self.emptyRDD, self.errschema)
    print('created Activity object with cust_code', cust_code)
    
  def error_text(self, s):
    return "_".join(["WRONG", s.upper(), "FORMAT"])
  
  def __field_map(self, df):
    for k, v in self.config['field_mapping'].items():
      if v in df.columns:
        df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
      else:
        df = df.withColumn(k, lit(None))
    df = df.select(self.config['reorder']).withColumn('base_activity_type', lit(None)).withColumn('base_activity_id', lit(None)) # 41 columns
    print("exiting with", len(df.columns), "columns")
    return df
  
  def __group(self, df):
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
    self.error_df = self.error_df.union(err)
    return res
  
  def __generate_unique_id(self, df):
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ).withColumn('base_activity_id', lit(md5(concat(col('source_person_id'), col(self.config[typ]['hash'])))))
      res = res.union(temp)
    return res                               
    
  def transform(self, df):
    res = self.__field_map(df)
    res = self.__group(res)
    res = self.__generate_unique_id(res)
    return res
  
  def __validate_date(self, df):
    res = self.emptyCDM
    pattern = self.config['validate_date']['pattern']
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_date']['fields']:
        if k in self.config[typ]['mandatory']:
          temp = temp.withColumn(k, to_date(k, pattern))
          valid = temp.where(col(k).isNotNUll())
          err = temp.where(col(k).isNull()).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      print(temp.count(), typ)
      res = res.union(temp)
    return res
  
  def __validate_datetime(self, df):
    res = self.emptyCDM
    pattern = self.config['validate_datetime']['pattern']
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_datetime']['fields']:
        if k in self.config[typ]['mandatory']:
          temp = temp.withColumn(k, to_date(k, pattern))
          valid = temp.where(col(k).isNotNull())
          err = temp.where(col(k).isNull()).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      print(temp.count(), typ)
      res = res.union(temp)
    return res

  def __validate_url(self, df):
    pattern=r'{}'.format(self.config['validate_url']['pattern'])
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_url']['fields']:
        print("field", k)
        if k in self.config[typ]['mandatory']:
          print("field in mandatory", k)
          valid = temp.where(col(k).rlike(pattern))
          print("valid", valid.count(), typ, k)
          err = temp.subtract(valid)
          print("error", err.count(), typ, k)
          err = err.withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      res = res.union(temp)
      print(temp.count(), typ)
    return res

  def __validate_email(self, df):
    pattern = r'{}'.format(self.config['validate_email']['pattern'])
    res = self.emptyCDM
    for typ in self.config['base_activity_type']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_email']['fields']:
        if k in self.config[typ]['mandatory']:
          valid = temp.where(col(k).rlike(pattern))
          err = temp.where(~col(k).rlike(pattern)).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      res = res.union(temp)
      print(temp.count(), typ)
    return res
  
  def __validate_phone(self, df):
    res = self.emptyCDM
    pattern = r'{}'.format(self.config['validate_phone']['pattern'])
    for typ in self.config['base_activity_type']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      # temp = spark.sql("")
      for k in self.config['validate_date']['fields']:
        if k in self.config[typ]['mandatory']:
          temp = temp.withColumn(k, regexp_replace(col(k), r'-', ''))
          valid = temp.where(col(k).rlike(pattern))
          err = temp.where(~col(k).rlike(pattern)).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      res = res.union(temp)
      print(temp.count(), typ)
    return res
    
  def cleanse(self, df):
    newdf = self.__validate_date(df)
    newdf = self.__validate_datetime(newdf)
    newdf = self.__validate_url(newdf)
    newdf = self.__validate_email(newdf)
    newdf = self.__validate_phone(newdf)
    return newdf
  def __create_base_CDM(self):
    self.emptyCDM.write.format('delta').save(self.CDMPATH)

  def __clear_delta(self, path):
    dbutils.fs.rm(path, recurse=True)
    
  def add_to_CDM(self, df):
    
    # check if CDM already exists at location
    if(not DeltaTable.isDeltaTable(spark, self.CDMPATH)):
      self.__create_base_CDM()
    else:
      print("CDM already exists at", self.CDMPATH)
      
    # get valid and invalid datasets
    valid = df.where(col('mastered_person_id').isNotNull())
    invalid = df.where(col('mastered_person_id').isNull())
    
    # update mastered_person_id from lookup table
    remasterPATH = '/delta/{}/remaster'.format(self.cust_code)
    self.__clear_delta(remasterPATH)
    invalid.write.format('delta').save(remasterPATH)
    remasterDelta = DeltaTable.forPath(spark, remasterPATH)
    remasterDelta.alias("rm").merge(
      self.lookup.alias("lp"),
      "rm.source_person_id = lp.source_person_id")\
      .whenMatchedUpdate(set = {'mastered_person_id':'lp.mastered_person_id'})\
      .execute()
    #read DeltaTable into DataFrame
    invalid = spark.read.format('delta').load(remasterPATH)
    # merge valid dataset into CDM DeltaTable
    CDMDelta = DeltaTable.forPath(spark, self.CDMPATH)
    CDMDelta.alias("cdm").merge(
      valid.alias("new"),
      "cdm.base_activity_id=new.base_activity_id")\
      .whenNotMatchedInsertAll().execute()
    
    # merge remastered dataset into CDM DeltaTable
    CDMDelta.alias("cdm").merge(
      invalid.alias("new"),
      "cdm.base_activity_id=new.base_activity_id")\
      .whenNotMatchedInsertAll().execute()
    
  def show_errors(self):
    display(self.error_df)
    
  def get_CDM(self):
    CDM = spark.read.format('delta').load(self.CDMPATH)
    return CDM
