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
  "person":{
    "source_person_id":"dat.source_person_id",
    "first_name": "dat.first_name",
    "middle_name": "dat.middle_name",
    "last_name": "dat.last_name",
    "street_address": "dat.street_address",
    "city": "dat.city",
    "state": "dat.state",
    "zipcode": "dat.zipcode",
    "gender": "dat.gender",
    "birth_date": "dat.birth_date",
    "phone": "dat.phone",
    "home_phone": "dat.home_phone",
    "primary_email": "dat.primary_email",
    "secondary_email": "dat.secondary_email",
    "marital_status_code": "dat.marital_status_code",
    "race_code": "dat.race_code",
    "race_description": "dat.race_description",
    "religion_code": "dat.religion_code",
    "religion_description": "dat.religion_description",
    "age": "dat.age",
    "update_datetime":"dat.update_datetime",
    "load_id":"dat.load_id",
    "delete_flag":"dat.delete_flag"
  }
}

emptyRDD = spark.sparkContext.emptyRDD()

def identify(load_id, srcpath, destpath, cdmpath):
    sets = updatesets['person']
    # read data
    data = spark.read.format('delta').load(srcpath).where(col('load_id')==load_id)
    data = data.withColumn('mastered_person_id', md5(concat(col('first_name'), col('last_name'), col('zipcode'))))\
        .dropDuplicates(['mastered_person_id'])
    try:
        data.write.format('delta').save(destpath)
    except:
        destDelta = DeltaTable.forPath(spark, destpath)
        destDelta.alias('dest').merge(data.alias('dat'), 'dest.mastered_person_id = dat.mastered_person_id')\
            .whenMatchedUpdate(set = sets)\
            .whenNotMatchedInsertAll().execute()

        destDelta.update("load_id!={}".format(load_id), {"delete_flag":"'1'"})
    
    updates = spark.read.format('delta').load(destpath).where(col('load_id')==load_id)
    deletes = spark.read.format('delta').load(destpath).where(col('load_id')!=load_id)
    # save as CDM if no CDM exists
    try:
        updates.write.format('delta').save(cdmpath)
        deletes.write.format('delta').mode('append').save(cdmpath)
        return
    except:
        cdmdelta = DeltaTable.forPath(spark, cdmpath)
        print(updates.count(), "updates")
        cdmdelta.alias('cdm').merge(updates.alias('dat'), 'cdm.mastered_person_id = dat.mastered_person_id')\
            .whenMatchedUpdate(set = sets)\
            .whenNotMatchedInsertAll()\
            .execute()
        print(deletes.count(), "deletes")
        cdmdelta.alias('cdm').merge(deletes.alias('del'), 'cdm.mastered_person_id = del.mastered_person_id')\
            .whenMatchedUpdate(set = {"delete_flag":"'1'"})\
            .execute()
      
    print('identify done')
    return