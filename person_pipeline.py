# Databricks notebook source
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.storeAssignmentPolicy", "LEGACY")
spark.conf.set("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
spark.conf.set("spark.sql.legacy.json.allowEmptyString.enabled", "TRUE")
spark.conf.set("spark.sql.legacy.createEmptyCollectionUsingStringType", "TRUE")
spark.conf.set("spark.sql.legacy.allowHashOnMapType", "TRUE")
spark.conf.set("spark.sql.adaptive.enabled", "TRUE")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "TRUE")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "TRUE")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "TRUE")
spark.conf.set("spark.sql.legacy.followThreeValuedLogicInArrayExists", "TRUE")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "TRUE")
spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "TRUE")
spark.conf.set("spark.databricks.io.cache.enabled", "TRUE")

# COMMAND ----------

import urllib
access_key="" 
secret_key="".replace("/","%2F") 
encoded_secret_key=urllib.parse.quote(secret_key,"")
AWS_s3_bucket="sample-aws-bucket-njp"
mount_name="sample_upload3"
sourceurl="s3a://{0}:{1}@{2}".format(access_key,encoded_secret_key,AWS_s3_bucket)
dbutils.fs.mount(sourceurl,"/mnt/%s" % mount_name)
dbutils.fs.ls("/mnt/%s" %mount_name)

# COMMAND ----------

from datetime import datetime, date
t = datetime.now()
frm = "%Y-%m-%d %H:%M:%S"

transform_config = {
    "instagram_format": {
        "header": "true",
        "inferSchema": "true",
        "field_mapping": {
            "source_person_id": "insta_id",
            "first_name": "first_name",
            "middle_name": "middle_name",
            "last_name": "last_name",
            "street_address": "street",
            "city": "city",
            "state": "state",
            "zipcode": "zipcode",
            "gender": "gender",
            "birthdate": "birthdate",
            "phone": "mobile",
            "home_phone": "",
            "email": "email",
            "secondary_email": "",
            "marital_status_code": "",
            "race_code": "",
            "race_description": "",
            "religion_code": "",
            "religion_description": "",
            "age": "",
            "mastered_person_id": ""
        },
      "meta_data": {
            "data_source": "instagram",
            "file_name": "instagram.csv",
            "created_datetime":t.strftime(frm),
            "updated_datetime":t.strftime(frm),
        },
    }   ,
    "facebook_format": {
        "header": "true",
        "inferSchema": "true",
        "field_mapping": {
            "source_person_id": "FacebookId",
            "first_name": "FirstName",
          
            "last_name": "LastName",
            "street_address": "MailingStreet",
            "city": "MailingCity",
            "state": "MailingState",
            "zipcode": "MailingZipCode",
            "gender": "Gender",
            "birthdate": "Birthdate",
            "phone": "Mobile",
            "email": "PrimaryEmail",
            "middle_name": "",
            "home_phone": "",
            "secondary_email": "",
            "marital_status_code": "",
            "race_code": "",
            "race_description": "",
            "religion_code": "",
            "religion_description": "",
            "age": "",
            "mastered_person_id": ""
        },
        "meta_data": {
            "data_source": "facebook",
            "file_name": "facebook.csv",
            "created_datetime":t.strftime(frm),
            "updated_datetime":t.strftime(frm),
        },
       
    },
    "crm_format": {
        "header": "true",
        "inferSchema": "true",
        "field_mapping": {
            "source_person_id": "crm_iD",
            "first_name": "first_name",
            "middle_name": "middle_name",
            "last_name": "last_name",
            "street_address": "street",
            "city": "city",
            "state": "state",
            "zipcode": "postalCode",
            "gender": "gender",
            "birthdate": "birthdate",
            "marital_status_code": "marital_status_code",
            "race_code": "race_code",
            "race_description": "race_description",
            "religion_code": "religion_code",
            "religion_description": "religion_description",
            "home_phone": "home_phone",
            "phone": "mobile_phone",
            "email": "primary_email",
            "secondary_email": "secondary_email",
            "age": "",
            "mastered_person_id": ""
        },
        "meta_data": {
            "data_source": "crm",
            "file_name": "crm.csv",
            "created_datetime":t.strftime(frm),
            "updated_datetime":t.strftime(frm),
        }
    },
  "meta_data": {
            "data_source": "instagram",
            "file_name": "instagram.csv",
            "created_datetime":t.strftime(frm),
            "updated_datetime":t.strftime(frm),
        },
        "required": [
            "source_person_id",
            "first_name",
            "last_name",
            "birthdate",
            "email",
            "street_address",
            "zipcode"
        ],
        "master_data": [
            "first_name",
            "last_name",
            "birthdate",
            "street_address",
            "zipcode"
        ],
        "clean_data": {
            "email":"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)",
            "phone":"^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"
        },
        "integer_type_columns": [
            "zipcode",
            "age"
        ],
        "date_type_columns": [
            "birthdate"
        ]
}



# COMMAND ----------

instaRAW = spark.read.csv('/mnt/sample_upload3/insta_data.csv', header='true')
fbRAW = spark.read.csv('/mnt/sample_upload3/facebook_data.csv', header='true')
crmRAW = spark.read.csv('/mnt/sample_upload3/crm_data.csv', header='true')
instaRAW.display()
print("*******************************************************************************************************")
fbRAW.display()
print("*******************************************************************************************************")
crmRAW.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import SparkSession
import functools
  
# explicit function
def unionAll(dfs):
    return functools.reduce(lambda df1, df2: df1.union(df2.select(df1.columns)), dfs)
  
def format_data(inp, config):
  from pyspark.sql.functions import lit, to_date, col, date_format
  for k, v in config['field_mapping'].items():
    if v!="":
      inp = inp.withColumnRenamed(v, k)
    else:
      inp = inp.withColumn(k, lit(None))
      print(inp)
      inp = inp.withColumn('birthdate', to_date(inp.birthdate, 'yyyy-MM-dd'))
      inp = inp.withColumn('age', date.today().year - date_format(col("birthdate"), "Y"))
  return inp


  
# function to insert metadata
def insert_meta_data(inp, config):
  from pyspark.sql.functions import lit
  for k, v in config['meta_data'].items():
    inp = inp.withColumn(k, lit(v))
  return inp



# function to cleanse data
# please add a way to insert error records into a separate DataFrame with error_type field
def clean_data(df, config):
  import re
  from pyspark.sql.functions import col
  df = df.withColumn('phone',regexp_replace('phone','-', ''))
  # dropping data with null values in required fields
  for k in config['required']:
    df = df.where(df[k].isNotNull())
  # check for validity of email
  for k, v in config['clean_data'].items():
    ptr = r'{}'.format(v)
    df = df.filter(df[k].rlike(ptr))
  # data_type_conversions
  from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
  for k  in config["integer_type_columns"]:
    df = df.withColumn(k,col(k).cast(IntegerType()))
  return df

  
def master_data(df):
  import pyspark
  from pyspark.sql import SparkSession
  from pyspark.sql.functions import concat, sha1
  mdf = df.withColumn("mastered_person_id", sha1(concat(df['first_name'],df['last_name'],df['zipcode'],df['street_address'])))
  return mdf
  
#final transform and cleanse layer
def transform_layer(inp, config):
  inp = format_data(inp, config)
  inp = insert_meta_data(inp, config)
  #inp.display()
  
  inp = master_data(inp)
  #inp = clean_data(inp, config)
  

  #inp.display()
  return inp

def merge_dataframes(df1,df2,df3):
  spark = SparkSession.builder.getOrCreate()
  unioned_df = unionAll([df1, df2, df3])
  return unioned_df  

    
  

insta_trans = transform_layer(instaRAW, transform_config['instagram_format'])
fb_trans =  transform_layer(fbRAW, transform_config['facebook_format'])
crm_trans = transform_layer(crmRAW, transform_config['crm_format'])
merged_df = merge_dataframes(insta_trans, fb_trans ,crm_trans)
cleaned_df = clean_data(merged_df, transform_config)
cleaned_df.display()








# COMMAND ----------

@udf(returnType=StringType()) 
def calculate_age_group(age):
    if (age>=70):
       return "senior citizen"
    elif(age>=20 and age<70):
      return "adult"
    else:
      return "child"
    
#enrichment
cleaned_df.withColumn("age_group", calculate_age_group(col("age"))).display()

# COMMAND ----------

#enrichment 

#cleaned_df=cleaned_df.withColumn('age_group', calculate_age_group(col(age)))

# COMMAND ----------



# COMMAND ----------

#events = spark.read.json("/databricks-datasets/structured-streaming/events/")
#cleaned_df.write.format("delta").save("/mnt/delta/events")
#spark.sql("CREATE TABLE cdm_table USING DELTA LOCATION '/mnt/delta/events/'")

# COMMAND ----------

#cleaned_df.registerTempTable("my_table")

