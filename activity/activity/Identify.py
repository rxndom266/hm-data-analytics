from pyspark.sql.functions import col, lit, date_format, coalesce, to_timestamp, to_date
from delta.tables import *
import re

spark = SparkSession \
    .builder \
    .appName("Identify") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

emptyRDD = spark.sparkContext.emptyRDD()

def identify(load_id, srcpath, destpath, cdmpath, personcdmpath):
  data = spark.read.format('delta').load(srcpath).where(col('load_id')==load_id)
  if not DeltaTable.isDeltaTable(spark, cdmpath):
    spark.createDataFrame(emptyRDD, data.schema).write.format('delta').save(cdmpath)
  cdmdelta = DeltaTable.forPath(spark, cdmpath)
  data.write.format('delta').mode('append').save(destpath)
  predelta = DeltaTable.forPath(spark, destpath)
  lookup = spark.read.format('delta').load(personcdmpath)\
    .select('source_person_id', 'mastered_person_id', 'data_source')

  predelta.alias('pre').merge(lookup.alias('src'), 'pre.source_person_id = src.source_person_id and pre.data_source = src.data_source and pre.load_id = "{}"'.format(load_id))\
    .whenMatchedUpdate(set = {"mastered_person_id":"src.mastered_person_id"}).execute()

  updates = spark.read.format('delta').load(destpath).where(col('load_id')==load_id)
  print(updates.count())
  cdmdelta.alias('cdm').merge(updates.alias('dat'), 'cdm.base_activity_id = dat.base_activity_id and cdm.data_source = dat.data_source')\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()
  print('identify done')