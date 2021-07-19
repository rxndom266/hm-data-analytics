from pyspark.sql.functions import col, lit, to_date
from delta.tables import *
import re

spark = SparkSession \
    .builder \
    .appName("Cleanse") \
    .config("spark.sql.legacy.timeParserPolicy", "CORRECTED") \
    .getOrCreate()

emptyRDD = spark.sparkContext.emptyRDD()
cleanseconfig = {
  'activity': {
    "mandatory":[
      "base_activity_id"
    ],
    "date":{
      "formats":["yyyy-MM-dd", "dd-MM-yyyy"],
      "pattern":"yyyy-MM-dd",
      "fields":[
        "birth_date"
      ]
    },
    "datetime":{
      "formats":["yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss"],
      "format":["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss yyyy-MM-dd", "HH:mm:ss yyyy/MM/dd", "HH:mm:ss MM/dd/yyyy", "HH:mm:ss dd/MM/yyyy", "HH:mm:ss dd-MM-yyyy"],
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
    "url":{
      "pattern" : "((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)",
      "fields":[
        "email_web_url",
        "email_clicked_through_url",
        "referrer_url",
        "landing_page_url"
      ]
    },
    "email":{
      "pattern":'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)',
      "fields":[
        "primary_email"
      ]
    },
    "phone":{
      "pattern":'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
        "fields":[
          "phone_number_from",
          "phone_number_to",
          "home_phone"
        ]
    }
  },
  'person': {
    "mandatory":[
      "source_person_id",
      "mastered_person_id",
      "age"
    ],
    "date":{
      "formats":["yyyy-MM-dd", "dd-MM-yyyy"],
      "pattern":"yyyy-MM-dd",
      "fields":[
        "birth_date"
      ]
    },
    "datetime":{
      "formats":["yyyy-MM-dd HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss yyyy-MM-dd", "HH:mm:ss dd-MM-yyyy"],
      "pattern":"yyyy-MM-dd HH:mm:ss",
      "fields":[
      ]
    },
    "url":{
      "pattern" : "((http|https)://)(www.)?[a-zA-Z0-9@:%._\\+~#?&//=]{2,256}\\.[a-z]{2,6}\\b([-a-zA-Z0-9@:%._\\+~#?&//=]*)",
      "fields":[
      ]
    },
    "email":{
      "pattern":'(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)',
      "fields":[
        "email",
        "secondary_email"
      ]
    },
    "phone":{
      "pattern":'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}$',
        "fields":[
          "phone",
          "home_phone"
        ]
    }
  }
} 

def purge(path):
  dbutils.fs.rm(path)
  
def cleanse(load_type, srcpath, destpath, rejectpath):
  temp = spark.read.format('delta').load(srcpath)
  rejectDelta = DeltaTable.forPath(spark, rejectpath)
  null='--'
  config = cleanseconfig[load_type]
  
  for k in config['mandatory']:
    err = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)==''))
    temp = temp.subtract(err)
    err = err.withColumn('error_type', lit("_".join(["MISSING", k.upper()])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  
  print('date validation')
  # formats = config['date']['formats']
  pattern = config['date']['pattern']
  for k in config['date']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (to_date(k, pattern).isNotNull())) # not mandatory and null
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    temp = valid
    
  print('datetime validation')    
  # formats = config['datetime']['formats']
  pattern = config['datetime']['pattern']
  for k in config['datetime']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (to_date(k, pattern).isNotNull())) # not mandatory and null
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    temp = valid
    
  print('url validation')
  pattern = r'{}'.format(config['url']['pattern'])
  for k in config['url']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    temp = valid
    
  print('email validation')
  pattern = r'{}'.format(config['email']['pattern'])
  for k in config['email']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    temp = valid
    
  print('phone validation')
  pattern = r'{}'.format(config['phone']['pattern'])
  for k in config['phone']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    temp = valid
  
  # persist to delta
  try:
    temp.write.format('delta').save(destpath)
  except:
    cleanDelta = DeltaTable.forPath(destpath)
    cleanDelta.alias('del').merge(temp.alias('dat'), 'del.base_activity_id = dat.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
  
  print('cleanse done')
  return