from pyspark.sql.functions import col, current_timestamp, date_sub, lit, date_format, coalesce, to_timestamp, to_date, when
from delta.tables import *
import re

spark = SparkSession \
  .builder \
  .appName("Cleanse") \
  .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
  .getOrCreate()

emptyRDD = spark.sparkContext.emptyRDD()
cleanseconfig = {
  'activity': {
    "mandatory":[
      "base_activity_id",
      "base_activity_type",
      "base_activity_datetime",
      "data_source",
      "load_id"
    ],
    "date":{
      "formats":["yyyy-MM-dd", 'MM-dd-yyyy', "dd-MM-yyyy", 'yyyy/MM/dd', 'MM/dd/yyyy', 'dd/MM/yyyy'],
      "pattern":"yyyy-MM-dd",
      "fields":[
        "birth_date"
      ]
    },
    "datetime":{
      "formats":["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss yyyy-MM-dd", "HH:mm:ss yyyy/MM/dd", "HH:mm:ss MM/dd/yyyy", "HH:mm:ss dd/MM/yyyy", "HH:mm:ss dd-MM-yyyy"],
      "pattern":"yyyy-MM-dd HH:mm:ss",
      "fields":[
        "email_sent_datetime",
        "email_opened_datetime",
        "email_clickthrough_datetime",
        "call_start_datetime",
        "call_end_datetime",
        "start_datetime",
        "end_datetime",
        "base_activity_datetime"
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
      "first_name",
      "last_name",
      "data_source",
      "age",
      "load_id"
    ],
    "date":{
      "formats":["yyyy-MM-dd", 'MM-dd-yyyy', "dd-MM-yyyy", 'yyyy/MM/dd', 'MM/dd/yyyy', 'dd/MM/yyyy'],
      "pattern":"yyyy-MM-dd",
      "fields":[
        "birth_date"
      ]
    },
    "datetime":{
      "formats":["yyyy-MM-dd HH:mm:ss", "yyyy/MM/dd HH:mm:ss", "MM/dd/yyyy HH:mm:ss", "dd/MM/yyyy HH:mm:ss", "dd-MM-yyyy HH:mm:ss", "HH:mm:ss yyyy-MM-dd", "HH:mm:ss yyyy/MM/dd", "HH:mm:ss MM/dd/yyyy", "HH:mm:ss dd/MM/yyyy", "HH:mm:ss dd-MM-yyyy"],
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
        "primary_email",
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
  
def cleanse(load_id, load_type, srcpath, destpath, rejectpath):
  null='--'
  temp = spark.read.format('delta').load(srcpath).where(col('load_id')==load_id)
  
  if not DeltaTable.isDeltaTable(spark, rejectpath):
    spark.createDataFrame(emptyRDD, temp.schema).withColumn('error_type', lit(null)).write.format('delta').save(rejectpath)
  # rejectDelta = DeltaTable.forPath(spark, rejectpath)
  
  if load_type=='activity':
    config = cleanseconfig['activity']
  else:
    config = cleanseconfig['person']

  # remove null-valued rows
  for k in config['mandatory']:
    err = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)==''))
    temp = temp.subtract(err)
    err = err.withColumn('error_type', lit("_".join(["MISSING", k.upper()])))
    err.write.format('delta').mode('append').save(rejectpath)
    
  
  print('date validation')
  formats = config['date']['formats']
  pattern = config['date']['pattern']
  for k in config['date']['fields']:
    temp = temp.withColumn(k, when((col(k)!=null) & (col(k).isNotNull()) & (col(k)!=''), date_format(coalesce(*[to_date(k, f) for f in formats]), pattern)).otherwise(lit(null)))
    valid = temp.where((col(k)==null) | ((col(k).isNotNull()) & (col(k)<current_timestamp())))
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    err.write.format('delta').mode('append').save(rejectpath)
    temp = valid
    
  print('datetime validation')    
  formats = config['datetime']['formats']
  pattern = config['datetime']['pattern']
  for k in config['datetime']['fields']:
    temp = temp.withColumn(k, when((col(k)!=null) & (col(k).isNotNull()) & (col(k)!=''), date_format(coalesce(*[to_timestamp(k, f) for f in formats]), pattern)).otherwise(lit(null)))
    valid = temp.where((col(k)==null) | ((col(k).isNotNull()) & (col(k)<current_timestamp())))
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    err.write.format('delta').mode('append').save(rejectpath)
    temp = valid
    
  print('url validation')
  pattern = config['url']['pattern']
  for k in config['url']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    err.write.format('delta').mode('append').save(rejectpath)
    temp = valid
    
  print('email validation')
  pattern = r'{}'.format(config['email']['pattern'])
  for k in config['email']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    err.write.format('delta').mode('append').save(rejectpath)
    temp = valid
    
  print('phone validation')
  pattern = r'{}'.format(config['phone']['pattern'])
  for k in config['phone']['fields']:
    valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
    err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
    err.write.format('delta').mode('append').save(rejectpath)
    temp = valid
  
  # persist to delta
  print(temp.count(), 'valid entries')
  try:
    temp.write.format('delta').save(destpath)
  except:
    temp.write.format('delta').mode('append').save(destpath)
  
  print('cleanse done')
  return