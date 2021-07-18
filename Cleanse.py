# cleanse operation is after transform operation

act_cleanse_config = {
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
}

from pyspark.sql.functions import col, lit, to_date, trim, md5, concat, date_format, coalesce, current_date, date_sub, current_timestamp
from delta.tables import *
import re

null='--'
spark.conf.set("spark.sql.legacy.timeParserPolicy","CORRECTED")

class Cleanse:
  
  emptyRDD = spark.sparkContext.emptyRDD()
  
  def __init__(self, config, load_type, cust_code, rejectpath = 'null'):
    self.config = config
    self.load_type = load_type
    self.cust_code = cust_code
    self.deltapath = '/delta/{}/{}/clean'.format(self.cust_code, self.load_type)
    self.rejectpath = rejectpath
  
  def purge(self):
    dbutils.fs.rm(self.deltapath)
    
  def cleanse(self, dfpath):
    temp = spark.read.format('delta').load(dfpath)
    
    if(self.rejectpath=='null'):
      path = "/delta/{}/{}/reject".format(cust_code, load_type)
      if(not DeltaTable.isDeltaTable(spark, path)):
        spark.createDataFrame(self.emptyRDD, df.schema).withColumn('error_type', lit(null)).write.format('delta').save(path)
      self.rejectpath = path
    rejectDelta = DeltaTable.forPath(spark, self.rejectpath)
    
    for k in self.config['mandatory']:
      err = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)==''))
      temp = temp.subtract(err)
      err = err.withColumn('error_type', lit("_".join(["MISSING", k.upper()])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    print('date validation')
    # formats = self.config['date']['formats']
    pattern = self.config['date']['pattern']
    for k in self.config['date']['fields']:
      valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='')) # not mandatory and null
      check = temp.subtract(valid).withColumn(k, to_date(k, pattern)) # change format
      err = check.where(col(k).isNull())
      check = check.subtract(err)
      err = err.withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      temp = check.union(valid)
      
    print('datetime validation')    
    # formats = self.config['datetime']['formats']
    pattern = self.config['datetime']['pattern']
    for k in self.config['datetime']['fields']:
      print(k)
      valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (to_date(k, pattern).isNotNull())) # not mandatory and null
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      print('added to errorlog')
      
    print('url validation')
    pattern = r'{}'.format(self.config['url']['pattern'])
    for k in self.config['url']['fields']:
      print(k)
      valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      temp = valid
      
    print('email validation')
    pattern = r'{}'.format(self.config['email']['pattern'])
    for k in self.config['email']['fields']:
      print(k)
      valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      temp = valid
      
    print('phone validation')
    pattern = r'{}'.format(self.config['phone']['pattern'])
    for k in self.config['phone']['fields']:
      print(k)
      valid = temp.where((col(k)==null) | (col(k).isNull()) | (col(k)=='') | (col(k).rlike(pattern))) # not mandatory and null OR not null and valid
      err = temp.subtract(valid).withColumn('error_type', lit("_".join(["WRONG", k.upper(), "FORMAT"])))
      rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
      temp = valid
    
    try:
      temp.write.format('delta').save(self.deltapath)
    except:
      cleanDelta = DeltaTable.forPath(self.deltapath)
      cleanDelta.alias('del').merge(temp.alias('dat'), 'del.base_activity_id = dat.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    return self.deltapath, self.rejectpath
  

# driver code

inputdf = # output res from transform
errorlog = # output errorlog from transform

cob = Cleanse(act_cleanse_config, 'incoming_call', 'ANK266', rejectpath) # output rejectpath from transform operation
cleanpath, rejectpath = cob.cleanse(transpath) # output transpath from transform operation
