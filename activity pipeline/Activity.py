# class to handle tranform layer

import pyspark.sql
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

class Activity:
  #Creates Empty RDD
  emptyRDD = spark.sparkContext.emptyRDD()
  schema = StructType([
    StructField('email_event_id', IntegerType(), True),
    StructField('mastered_person_id', StringType(), True),
    StructField('source_person_id', IntegerType(), True),
    StructField('subject_line', StringType(), True),
    StructField('asset_id', IntegerType(), True),
    StructField('asset_name', StringType(), True),
    StructField('email_sent_datetime', StringType(), True),
    StructField('email_opened_datetime', StringType(), True),
    StructField('email_clickthrough_datetime', StringType(), True),
    StructField('email_web_url', StringType(), True),
    StructField('email_clicked_through_url', StringType(), True),
    StructField('campaign_id', IntegerType(), True),
    StructField('error_type', StringType(), True)
  ])
  error_df = spark.createDataFrame(emptyRDD,schema)
  # config file, changes need to be made
  config = {
      "email_sent":{
          "args":{
              "header":"true",
              "inderSchema":"true"
          },
          "format":{
              "email_event_id":"email_event_id", 
              "mastered_person_id": "mastered_person_id", 
              "source_person_id": "source_person_id", 
              "subject_line": "subject_line", 
              "asset_id": "asset_id", 
              "asset_name": "asset_name", 
              "email_sent_datetime": "email_sent_datetime",
              "email_opened_datetime": "email_opened_datetime",
              "email_clickthrough_datetime": "email_clickthrough_datetime",
              "email_web_url": "email_web_url",
              "email_clicked_through_url": "email_clicked_through_url",
              "campaign_id": "campaign_id" 
          },
          "mandatory":[
              "email_event_id",
              "mastered_person_id",
              "source_person_id",
              "subject_line",
              "asset_id",
              "asset_name",
              "email_sent_datetime",
              "email_web_url",
              "campaign_id" 
          ]
      },
      "email_open":{
          "args":{
              "header":"true",
              "inderSchema":"true"
          },
          "format":{
              "email_event_id":"email_event_id", 
              "mastered_person_id": "mastered_person_id", 
              "source_person_id": "source_person_id", 
              "subject_line": "subject_line", 
              "asset_id": "asset_id", 
              "asset_name": "asset_name", 
              "email_sent_datetime": "email_sent_datetime",
              "email_opened_datetime": "email_opened_datetime",
              "email_clickthrough_datetime": "email_clickthrough_datetime",
              "email_web_url": "email_web_url",
              "email_clicked_through_url": "email_clicked_through_url",
              "campaign_id": "campaign_id" 
          },
          "mandatory":[
              "email_event_id",
              "mastered_person_id",
              "source_person_id",
              "subject_line",
              "asset_id",
              "asset_name",
              "email_opened_datetime",
              "email_web_url",
              "campaign_id" 
          ]
      },
      "email_clickthrough":{
          "args":{
              "header":"true",
              "inderSchema":"true"
          },
          "format":{
              "email_event_id":"email_event_id", 
              "mastered_person_id": "mastered_person_id", 
              "source_person_id": "source_person_id", 
              "subject_line": "subject_line", 
              "asset_id": "asset_id", 
              "asset_name": "asset_name", 
              "email_sent_datetime": "email_sent_datetime",
              "email_opened_datetime": "email_opened_datetime",
              "email_clickthrough_datetime": "email_clickthrough_datetime",
              "email_web_url": "email_web_url",
              "email_clicked_through_url": "email_clicked_through_url",
              "campaign_id": "campaign_id" 
          },
          "mandatory":[
              "email_event_id",
              "mastered_person_id",
              "source_person_id",
              "subject_line",
              "asset_id",
              "asset_name",
              "email_clickthrough_datetime",
              "email_web_url",
              "email_clicked_through_url",
              "campaign_id" 
          ]
      },
    "url_validation":[
            "email_web_url",
            "email_clicked_through_url"
        ]
  }
  # act_type is either 'email_sent', 'email_open' or 'email_clickthrough' for now
  def __init__(self, act_type):
    self.act_type = act_type
  
  # function to format data
  def format_data(self, df):
    from pyspark.sql.functions import lit, to_date
    for k, v in self.config[self.act_type]['format'].items():
      if v!="":
        df = df.withColumnRenamed(v, k)
      else:
        df = df.withColumn(k, lit(None))
    return df

  # function to clean data
  def clean_data(self, df):
    import re
    from pyspark.sql.functions import lit, col, upper
    # dropping data with null values in required fields
    for k in self.config[self.act_type]['mandatory']:
      err = df.where(col(k).isNull()).withColumn('error_type', lit("NULL_"+k.upper()))
      df = df.where(col(k).isNotNull())
      self.error_df = self.error_df.union(err)
    display(self.error_df)
    return df
  
   #validating url
    def validate_url(self,df):
        import re
        from pyspark.sql.functions import *
        first = "^(?:http|ftp)s?://"
        domain = "(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|"
        local_h = "localhost|"
        ip = "\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})"
        port = "(?::\d+)?"
        end = "(?:/?|[/?]\S+)$"
        pattern = r'{}'.format(first, domain, local_h, ip, port, end, re.IGNORECASE)
        #df1 = df1.filter(df1['email_web_url'].rlike(pattern))
        for k in self.config['url_validation']:
            err = df.where(~df[k].rlike(pattern)).withColumn('error',lit("wrong url format"))
            df = df.where(df[k].rlike(pattern))
            self.error_df=self.error_df.union(err)
        return df
      
  def persist(self, df, name):
    # function to convert to delta table
  def show_errors(self):
    self.error_df.show()
  

