from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, lit, to_date, trim, md5, concat, date_format, coalesce, current_date, date_sub, current_timestamp
import re

class Cleanse:
  
  emptyRDD = spark.sparkContext.emptyRDD()
  def __init__(self, config, load_type, inputdf, cust_code, errorlog = 'null'):
    self.config = config
    self.load_type = load_type
    self.schema = inputdf.schema
    self.emptyCDM = spark.createDataFrame(self.emptyRDD, self.schema)
    if(errorlog=='null'):
      self.errorlog = self.emptyCDM.withColumn('error_type', lit(None))
    else:
      self.errorlog = errorlog
    
  def remove_null(self, df):
    res = self.emptyCDM
    temp = df
    for k in self.config['mandatory']:
      err = temp.where(col(k).isNull())
      temp = temp.subtract(err)
      err = err.withColumn('error_type', lit("_".join(["MISSING", k.upper()])))
      self.errorlog = self.errorlog.union(err)
    res = res.union(temp)
    return res
  
  def validate_date(self, df):
    print('date validation')
    res = self.emptyCDM
    formats = self.config['date']['formats']
    pattern = self.config['date']['pattern']
    temp = df
    for k in self.config['date']['fields']:
      valid = temp.where(col(k).isNull()) # not mandatory and null
      check = temp.subtract(valid).withColumn(k, date_format(coalesce(*[to_date(k, f) for f in formats]), pattern)) # change format
      err = check.where(col(k).isNull().withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))) # remove those with wrong format                 
      self.errorlog = self.errorlog.union(err)
      check = check.where(col(k).isNotNull())
      err = check.where(col(k) < date_sub(current_date(), 3650) | col(k) > current_date()) # remove invalid values
      check = check.subtract(err)
      err = err.withColumn('error_type', lit("_".join(["INVALID", k.upper()])))
      self.errorlog = self.errorlog.union(err)
      temp = check.union(valid)
      res = res.union(temp)
    return res
  
  def validate_datetime(self, df):
    print('date validation')
    res = self.emptyCDM
    formats = self.config['datetime']['formats']
    pattern = self.config['datetime']['pattern']
    temp = df
    for k in self.config['datetime']['fields']:
      valid = temp.where(col(k).isNull()) # not mandatory and null
      check = temp.subtract(valid).withColumn(k, date_format(coalesce(*[to_date(k, f) for f in formats]), pattern)) # change format
      err = check.where(col(k).isNull()).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"]))) # remove those with wrong format
      self.errorlog = self.errorlog.union(err)
      check = check.where(col(k).isNotNull())
      err = check.where(col(k) < date_sub(current_date(), 3650) | col(k) > current_timestamp()) # remove invalid values
      check = check.subtract(err)
      err = err.withColumn('error_type', lit("_".join(["INVALID", k.upper()])))
      self.errorlog = self.errorlog.union(err)
      temp = check.union(valid)
      res = res.union(temp)
    return res
  
  def validate_url(self, df):
    print('url validation')
    res = self.emptyCDM
    temp=df
    pattern = r'{}'.format(self.config['url']['pattern'])
    for k in self.config['url']['fields']:
      valid = temp.where(col(k).isNull() | (col(k).isNotNull() & col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"]))) # remove those with wrong format
      self.error_df = self.error_df.union(err)
      temp = valid
    res = res.union(temp)
    return res
  
  def validate_email(self, df):
    print('email validation')
    res = self.emptyCDM
    temp=df
    pattern = r'{}'.format(self.config['email']['pattern'])
    for k in self.config['email']['fields']:
      valid = temp.where(col(k).isNull() | (col(k).isNotNull() & col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"]))) # remove those with wrong format
      self.error_df = self.error_df.union(err)
      temp = valid
    res = res.union(temp)
    return res
  
  def validate_phone(self, df):
    print('phone validation')
    res = self.emptyCDM
    temp=df
    pattern = r'{}'.format(self.config['email']['pattern'])
    for k in self.config['phone']['fields']:
      valid = temp.where(col(k).isNull() | (col(k).isNotNull() & col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"]))) # remove those with wrong format
      self.error_df = self.error_df.union(err)
      temp = valid
    res = res.union(temp)
    return res
  
  
  
activity_cleanse_config = {
  "mandatory":[
    "base_activity_id"
  ],
  "date":{
    "formats":["yyyy-MM-dd", "yyyy/MM/dd", "MM/dd/yyyy", "dd/MM/yyyy", "dd-MM-yyyy"],
    "pattern":"yyyy-MM-dd",
    "fields":[
      
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
}

# driver code

inputdf = transdf
errorlog = error_log

ob = Cleanse(activity_cleanse_config,'test_dataset',inputdf,'random',errorlog)
display(ob.validate_datetime(inputdf))
