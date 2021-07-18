from pyspark.sql.functions import col, lit, trim, current_timestamp, date_format, md5, concat
from delta.tables import *
import urllib

null = '--'
spark.conf.set("spark.sql.legacy.timeParserPolicy","CORRECTED")

class TransformActivity:
  
  emptyRDD = spark.sparkContext.emptyRDD()
  
  def __init__(self, config, load_type, cust_code):
    self.config = config
    self.load_type = load_type
    self.cust_code = cust_code
    self.deltapath = "/delta/{}/{}/transform".format(cust_code, load_type)
    self.rejectpath = "/delta/{}/{}/reject".format(cust_code, load_type)
    
  def mountbkt(self):
    access_key = self.config['aws']['access_key']
    secret_key = self.config['aws']['secret_key'].replace("/","%2F")
    encoded_secret_key = urllib.parse.quote(secret_key,"")
    AWS_s3_bucket=self.config['aws']['bucket_name']
    folder = self.cust_code
    mount_name = "{}/{}".format(folder, self.load_type)
    sourceurl="s3a://{0}:{1}@{2}".format(access_key,encoded_secret_key,AWS_s3_bucket)
    dbutils.fs.mount(sourceurl,"/mnt/%s" %mount_name)
    self.mountpath = "/mnt/{}".format(mount_name)
    dbutils.fs.ls("/mnt/%s" %mount_name)
    self.inputdf = spark.read.csv(self.mountpath)
    
  def purge(self):
    dbutils.fs.rm(self.deltapath, recurse=True)
    dbutils.fs.rm(self.rejectpath, recurse=True)
  
  def transform(self, df):
    # renaming columns
    
    for k, v in self.config['fields'].items():
      if v in df.columns:
        df = df.withColumnRenamed(v, k).withColumn(k, trim(col(k)))
      else:
        df = df.withColumn(k, lit(null))
    df = df.withColumn('base_activity_type', lit(null)).withColumn('base_activity_id', lit(null)) # 42 columns
    
    print("exiting with", len(df.columns), "columns")
    
    # adding metadata
    df = df.withColumn('create_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))\
    .withColumn('delete_flag', lit(0))# 45 columns
    self.emptyCDM = spark.createDataFrame(self.emptyRDD, df.schema) # 45 columns
    errorlog = self.emptyCDM.withColumn('error_type', lit(null)) # 46 columns
    
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
          temp = temp.where(col(x)!=null)
      df = df.subtract(temp)
      temp = temp.withColumn('base_activity_type', lit(typ))
      print(temp.count(), typ)
      res = res.union(temp)
    err = df.withColumn('error_type', lit("UNCLASSIFIABLE_ACTIVITY"))
    # err = err.withColumn('update_datetime', date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss'))
    rejectDelta.alias('rej').merge(err.alias('err'), 'rej.base_activity_id = err.base_activity_id').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    
    trans = self.emptyCDM
    for typ in self.config['base_activity_types']:
      temp = res.where(col('base_activity_type')==typ).withColumn('base_activity_id',lit(md5(concat(col('source_person_id'),\
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
