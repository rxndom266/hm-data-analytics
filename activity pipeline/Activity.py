from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from delta.tables import *
from daettime import dattime
# from pyspark import SparkSession
from pyspark.sql.functions import col, lit, to_date, trim, regexp_replace, md5, concat, date_format, coalesce, current_datetime
import re

# spark = SparkSession.builder.getOrCreate()
class Activity:
  emptyRDD = spark.sparkContext.emptyRDD()
  schema = StructType([
      StructField('email_event_id', StringType(), True),
      StructField('mastered_person_id', StringType(), True),
      StructField('source_person_id', StringType(), False),
      StructField('source_activity_id', StringType(), True),
      StructField('data_source', StringType(), False),
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
      StructField('start_datetime', StringType(), True),
      StructField('end_datetime', StringType(), True),
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
      StructField('base_activity_type', StringType(), True),
      StructField('base_activity_id', StringType(), True),
      StructField('create_datetime', StringType(), True),
      StructField('update_datetime', StringType(), True)
  ])
  errschema = StructType([
      StructField('email_event_id', StringType(), True),
      StructField('mastered_person_id', StringType(), True),
      StructField('source_person_id', StringType(), False),
      StructField('source_activity_id', StringType(), True),
      StructField('data_source', StringType(), False),
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
      StructField('start_datetime', StringType(), True),
      StructField('end_datetime', StringType(), True),
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
      StructField('base_activity_type', StringType(), True),
      StructField('base_activity_id', StringType(), True),
      StructField('create_datetime', StringType(), True),
      StructField('update_datetime', StringType(), True),
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
    "reorder":[
      "email_event_id",
      "mastered_person_id",
      "source_person_id",
      "source_activity_id",
      "data_source",
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
      "primary_email",
      "base_activity_type",
      "base_activity_id",
      "create_datetime",
      "update_datetime"
    ],
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
    },
    "validate_date":{
      "formats":["yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy", "dd/MM/yyyy", "dd-MM-yyyy"],
      "pattern":"yyyy-MM-dd",
      "fields":[
        "birth_date"
      ]
    },
    "validate_datetime":{
      "formats":["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss yyyy-MM-dd", "HH:mm:ss yyyy/MM/dd", "HH:mm:ss MM/dd/yyyy", "HH:mm:ss dd/MM/yyyy", "HH:mm:ss dd-MM-yyyy"],
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
    self.errorPATH = "/delta/{}/error".format(cust_code)
    self.resPATH = "/delta/{}/res".format(cust_code)
    self.dfPATH = "/delta/{}/df".format(cust_code)
    self.error_df = spark.createDataFrame(self.emptyRDD, self.errschema)
    try:
      self.error_df.write.format('delta').save(self.errorPATH)
    except:
      print('Error DeltaTAble already exists')
    print('created Activity object with cust_code', cust_code)
  
  def __field_map(self, df):
    for k, v in self.config['field_mapping'].items():
      if v in df.columns:
        df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
      else:
        df = df.withColumn(k, lit(None))
    df = df.withColumn('base_activity_type', lit(None)).withColumn('base_activity_id', lit(None)) # 41 columns
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
      temp = df.where(col('base_activity_type')==typ)/
      .withColumn('base_activity_id', /
                  lit(md5(concat(/
                                 col('source_person_id'), /
                                 col('base_activity_type'), /
                                 col(self.config[typ]['hash'])/
                                ))))
      res = res.union(temp)
    return res
  
  def __add_metadata(self, df):
    df = df.withColumn('create_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))/
    .withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))
    return df
  
  def __reorder(self, df):
    df = df.select(self.config['reorder'])
    return df
    
  def transform(self, df):
    res = self.__field_map(df)
    res = self.__group(res)
    res = self.__generate_unique_id(res)
    res = self.__add_metadata(res)
    res = self.__reorder(res)
    return res
  
  def clear_delta(self, path):
    dbutils.fs.rm(path, recurse=True)
    
  def drop_table(self, name):
    spark.sql("drop table if exists {}".format(name))
    
  def create_table(self, name, path):
    self.drop_table(name)
    spark.sql('create table {} using delta location "{}"'.format(name, path))
  
  def __validate_date(self, df):
    print('date')
    res = self.emptyCDM
    formats = self.config['validate_date']['formats']
    pattern = self.config['validate_date']['pattern']
    print(pattern)
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_date']['fields']:
        if k in self.config[typ]['mandatory']:
          print(k, "in mandatory")
          # temp = temp.where(col(k).rlike(r'\d{1}\D*\d{1}\D*\d{1}\D*\d{1}\D*\d{1}\D*\d{1}')).withColumn(k, date_format(coalesce(*[to_date(k, f) for f in formats]), pattern))
          temp = temp.withColumn(k, date_format(coalesce(*[to_date(k, f) for f in formats]), pattern))
          valid = temp.where(col(k).isNotNull())
          err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          self.error_df = self.error_df.union(err)
          temp = valid
      print(temp.count(), typ)
      res = res.union(temp)
    return res
  
  def __validate_datetime(self, df):
    print('datetime')
    res = self.emptyCDM
    formats = self.config['validate_datetime']['formats']
    pattern = self.config['validate_datetime']['pattern']
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_datetime']['fields']:
        if k in self.config[typ]['mandatory']:
          print(k, "in mandatory")
          temp = temp.withColumn(k, date_format(coalesce(*[to_date(k, f) for f in formats]), pattern))
          display(temp)
          valid = temp.where(col(k).isNotNull())
          err = temp.where(col(k).isNull()).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          print(err.count(), "invalid")
          self.error_df = self.error_df.union(err)
          temp = valid
      print(temp.count(), typ)
      res = res.union(temp)
    return res

  def __validate_url(self, df):
    print('url')
    pattern=r'{}'.format(self.config['validate_url']['pattern'])
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_url']['fields']:
        if k in self.config[typ]['mandatory']:
          print("field in mandatory", k)
          valid = temp.where(col(k).rlike(pattern))
          err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          print(err.count(), "invalid")
          self.error_df = self.error_df.union(err)
          temp = valid
      res = res.union(temp)
      print(temp.count(), typ)
    return res
  
  def __validate_email(self, df):
    print('email')
    pattern = r'{}'.format(self.config['validate_email']['pattern'])
    res = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      for k in self.config['validate_email']['fields']:
        if k in self.config[typ]['mandatory']:
          print(k, "in mandatory")
          valid = temp.where(col(k).rlike(pattern))
          err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          print(err.count(), "invalid")
          self.error_df = self.error_df.union(err)
          temp = valid
      res = res.union(temp)
      print(temp.count(), typ)
    return res
  
  def __validate_phone(self, df):
    print('phone')
    res = self.emptyCDM
    pattern = r'{}'.format(self.config['validate_phone']['pattern'])
    for typ in self.config['base_activity_types']:
      temp = df.where(col('base_activity_type')==typ)
      if(temp.count()==0):
        continue
      # temp = spark.sql("")
      for k in self.config['validate_phone']['fields']:
        if k in self.config[typ]['mandatory']:
          print(k, "in mandatory")
          # temp = temp.withColumn(k, regexp_replace(col(k), r'-', ''))
          valid = temp.where(col(k).rlike(pattern))
          err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
          print(err.count(), "invalid")
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

  def __remaster(self, df):
    # get valid and invalid datasets
    valid = df.where(col('mastered_person_id').isNotNull())
    invalid = df.where(col('mastered_person_id').isNull())
    
    # clear any possible old data
    remasterPATH = '/delta/{}/remaster'.format(self.cust_code)
    self.__clear_delta(remasterPATH)
    
    # update mastered_person_id from lookup table
    invalid.write.format('delta').save(remasterPATH)
    remasterDelta = DeltaTable.forPath(spark, remasterPATH)
    remasterDelta.alias("rm").merge(
      self.lookup.alias("lp"),
      "rm.source_person_id = lp.source_person_id")\
      .whenMatchedUpdate(set = {'mastered_person_id':'lp.mastered_person_id'})\
      .execute()
    # combine valid and invalid datasets
    remasterDelta.alias("rm").merge(
      valid.alias("v"),
      "rm.base_activity_id = v.base_activity_id")\
      .whenNotMatchedInsertAll()\
      .execute()
   
    # return location of new dataset
    return remasterPATH
  
  def __merge_into_CDM(self, remasterPATH):
    # check if CDM already exists at location
    if(not DeltaTable.isDeltaTable(spark, self.CDMPATH)):
      self.__create_base_CDM()
    else:
      print("CDM already exists at", self.CDMPATH)
      
    #read data at DeltaTable location into DataFrame
    newdt = spark.read.format('delta').load(remasterPATH)
    
    # merge new dataset into CDM DeltaTable
    CDMDelta = DeltaTable.forPath(spark, self.CDMPATH)
    CDMDelta.alias("cdm").merge(
      newdt.alias("new"),
      "new.base_activity_id=cdm.base_activity_id")\
      .whenNotMatchedInsertAll().execute()
    
  def add_to_CDM(self, df):
    remasterPATH = self.__remaster(df)
    self.__merge_into_CDM(remasterPATH)
    
  def show_errors(self):
    display(self.error_df)
    
  def get_CDM(self):
    CDM = spark.read.format('delta').load(self.CDMPATH)
    return CDM
