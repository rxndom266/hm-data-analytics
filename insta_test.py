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
    rejected_df= df.where(df[k].isNull())
  # check for validity of email
  for k, v in config['clean_data'].items():
    ptr = r'{}'.format(v)
    df = df.filter(df[k].rlike(ptr))
  # data_type_conversions
  from pyspark.sql.types import StringType,BooleanType,DateType,IntegerType
  for k  in config["integer_type_columns"]:
    df = df.withColumn(k,col(k).cast(IntegerType()))
  return df ,rejected_df

  
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
# transformed_df.write.format("delta").mode("append").save('/mnt/delta/transform/DEMO/person') 
accepted_df , rejected_df = clean_data(merged_df, transform_config)
# accepted_df.write.format("delta").mode("append").save('/mnt/delta/clean/DEMO/person') 
accepted_df.display()
rejected_df.display()








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
cleaned_df = accepted_df.withColumn("age_group", calculate_age_group(col("age")))
cleaned_df.display()

# COMMAND ----------


cleaned_df.write.format("delta").mode("append").save('/mnt/delta/clean/DEMO/person') 

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
      StructField('source_person_id', StringType(), True),
      StructField('first_name', StringType(), True),
      StructField('middle_name', StringType(), False),
      StructField('last_name', StringType(), True),
      StructField('street_address', StringType(), False),
      StructField('city', StringType(), True),
      StructField('state', StringType(), True),
      StructField('zipcode', StringType(), True),
      StructField('gender', StringType(), True),
      StructField('birthdate', StringType(), True),
      StructField('phone', StringType(), True),
      StructField('email', StringType(), True),
      StructField('home_phone', StringType(), True),
      StructField('age', StringType(), True),
      StructField('secondary_email', StringType(), True),
      StructField('marital_status_code', StringType(), True),
      StructField('race_code', StringType(), True),
      StructField('race_description', StringType(), True),
      StructField('religion_code', StringType(), True),
      StructField('religion_description', StringType(), True),
      StructField('mastered_person_id', StringType(), True),
      StructField('data_source', StringType(), True),
      StructField('file_name', StringType(), True),
      StructField('created_datetime', StringType(), True),
      StructField('updated_datetime', StringType(), True),
      StructField('age_group', StringType(), True)
  
  ])

# COMMAND ----------

emptyRDD = spark.sparkContext.emptyRDD()
emptyCDM = spark.createDataFrame(emptyRDD, schema)
  

# COMMAND ----------

emptyCDM.write.format("delta").mode("append").save('/mnt/delta/CDM/DEMO/person') 

# COMMAND ----------

from delta.tables import *
CDMDelta = DeltaTable.forPath(spark,'/mnt/delta/CDM/DEMO/person')

# COMMAND ----------

CDMDelta.alias("cdm").merge(
      cleaned_df.alias("cd"),
      "cd.mastered_person_id=cdm.mastered_person_id")\
      .whenNotMatchedInsertAll().execute()

# COMMAND ----------

#spark.sql("CREATE TABLE cdm_table_ USING DELTA LOCATION '/mnt/delta/CDM/DEMO/person'")
spark.sql("select * from cdm_table_").display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC Show create table('/mnt/delta/CDM/DEMO/person')

# COMMAND ----------

merge_sql = f"""
    merge into {target_temp_table} as target
    using {changed_records_temp_table} as updates
    on target.{id_column} = updates.{id_column} {merge_delete_flag_clause}
    when matched then update set {updates_list}
    when not matched then insert ({insert_source_list}) values ({insert_target_list})
    """

# COMMAND ----------

from delta.tables import *
cleaned_df.createOrReplaceTempView('temp_table')



# COMMAND ----------

def save_persons(
    spark: SparkSession,
    cfg: dict,
    input_df: DataFrame,
    id_column: str = 'mastered_person_id'
):
  
  source_temp_table = f"input_df_{utils.random_string()}"
  source_temp_table = cleaned_df.createorReplacetable(source_temp_table)
  target_df = spark.read.format('delta').load('/mnt/delta/cdm/DEMO/person')
  target_temp_table = target_df.createorReplacetable(target_temp_table)
  

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

