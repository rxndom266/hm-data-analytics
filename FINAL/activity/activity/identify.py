from pyspark.sql.functions import col, lit, concat, md5
from pyspark.sql.types import NullType
from delta.tables import *
import re

spark = SparkSession \
    .builder \
    .appName("Identify") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

updatesets = {
  "activity":{
    "email_event_id": "dat.email_event_id",
    "mastered_person_id": "dat.mastered_person_id",
    "source_person_id": "dat.source_person_id",
    "source_activity_id":"dat.source_activity_id",
    "subject_line": "dat.subject_line",
    "asset_id": "dat.asset_id",
    "asset_name": "dat.asset_name",
    "email_sent_datetime": "dat.email_sent_datetime",
    "email_opened_datetime": "dat.email_opened_datetime",
    "email_clickthrough_datetime": "dat.email_clickthrough_datetime",
    "email_web_url": "dat.email_web_url",
    "email_clicked_through_url": "dat.email_clicked_through_url",
    "campaign_id": "dat.campaign_id",
    "phone_number_to":"dat.phone_number_to",
    "phone_number_from":"dat.phone_number_from",
    "call_tracking_number":"dat.call_tracking_number",
    "direction":"dat.direction",
    "call_status_code":"dat.call_status_code",
    "call_start_datetime":"dat.call_start_datetime",
    "call_end_datetime":"dat.call_end_datetime",
    "call_duration":"dat.call_duration",
    "activity_type":"dat.activity_type",
    "activity_datetime":"dat.activity_datetime",
    "referrer_url":"dat.referrer_url",
    "landing_page_url":"dat.landing_page_url",
    "page_visited":"dat.page_visited",
    "number_of_pages":"dat.number_of_pages",
    "start_datetime":"dat.start_datetime",
    "end_datetime":"dat.end_datetime",
    "first_name":"dat.first_name",
    "last_name":"dat.last_name",
    "street_address":"dat.street_address",
    "city":"dat.city",
    "state":"dat.state",
    "postal_code":"dat.postal_code",
    "gender_code":"dat.gender_code",
    "birth_date":"dat.birth_date",
    "home_phone":"dat.home_phone",
    "primary_email":"dat.primary_email",
    "base_activity_type":"dat.base_activity_type",
    "base_activity_id":"dat.base_activity_id",
    "base_activity_datetime":"dat.base_activity_datetime",
    "update_datetime":"dat.update_datetime",
    "load_id":"dat.load_id",
    "delete_flag":"dat.delete_flag"
  }
}

emptyRDD = spark.sparkContext.emptyRDD()

def identify(load_id, srcpath, destpath, cdmpath, lookuppath):
  sets = updatesets['activity']
  # read data
  data = spark.read.format('delta').load(srcpath).where(col('load_id')==load_id)
  data = data.dropDuplicates(['base_activity_id', 'data_source'])
  
  try:
    data.write.format('delta').save(destpath)
  except:
    destDelta = DeltaTable.forPath(spark, destpath)
    destDelta.alias('dest').merge(data.alias('dat'), 'dest.base_activity_id = dat.base_activity_id and dest.data_source = dat.data_source')\
      .whenMatchedUpdate(set = sets)\
      .whenNotMatchedInsertAll()\
      .execute()

    destDelta.update('load_id!={}'.format(load_id), {"delete_flag":"'1'"})
  
  # lookup mastered_person_id
  predelta = DeltaTable.forPath(spark, destpath)
  lookup = spark.read.format('delta').load(lookuppath)\
    .select('source_person_id', 'mastered_person_id', 'data_source')

  predelta.alias('pre').merge(lookup.alias('src'), 'pre.load_id = {} and pre.source_person_id = src.source_person_id and pre.data_source = src.data_source'.format(load_id))\
    .whenMatchedUpdate(set = {"mastered_person_id":"src.mastered_person_id"})\
    .execute()
  

  updates = spark.read.format('delta').load(destpath).where(col('load_id')==load_id)
  deletes = spark.read.format('delta').load(destpath).where(col('load_id')!=load_id)
  print(updates.count(), "updates")
  print(deletes.count(), "deletes")
  # save as CDM if no CDM exists
  try:
    updates.write.format('delta').save(cdmpath)
    deletes.write.format('delta').mode('append').save(cdmpath)
    print('identify done')
    return
  except:
    cdmdelta = DeltaTable.forPath(spark, cdmpath)
    cdmdelta.alias('cdm').merge(updates.alias('dat'), 'cdm.base_activity_id = dat.base_activity_id and cdm.data_source = dat.data_source')\
      .whenMatchedUpdate(set = sets)\
      .whenNotMatchedInsertAll()\
      .execute()
    cdmdelta.alias('cdm').merge(deletes.alias('del'), 'cdm.base_activity_id = del.base_activity_id and cdm.data_source = del.data_source')\
      .whenMatchedUpdate(set = {"delete_flag":"'1'"})\
      .execute()
  
  print('identify done')