from pyspark.sql.functions import col, lit, when
from delta.tables import *

spark = SparkSession \
    .builder \
    .appName("Enrich") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()

def enrich(srcpath, destpath):
    null='--'
    absent = 'absent'
    dbutils.fs.rm(destpath, recurse=True)
    
    act = spark.read.format('delta').load(srcpath).select('mastered_person_id', 'base_activity_type', 'base_activity_datetime')
    data = spark.read.format('delta').load(srcpath)\
        .select('mastered_person_id')\
        .dropDuplicates(['mastered_person_id'])\
        .where(col('mastered_person_id').isNotNull() & (col('mastered_person_id')!=null) & (col('mastered_person_id')!=''))\
        .withColumn('email_affinity', lit(null))\
        .withColumn('call_affinity', lit(null))\
        .withColumn('webclick_affinity', lit(null))
    data.write.format('delta').save(destpath)
    enriched = DeltaTable.forPath(spark, destpath)
    
    act.createOrReplaceTempView('actcdm')
    data.createOrReplaceTempView('temp')
    
    spark.sql('create or replace table act as (select * from actcdm a where a.base_activity_type in ("email_sent", "email_open", "email_clickthrough"))')
    data = spark.sql('select t.mastered_person_id,(select count(*) from act a where a.mastered_person_id = t.mastered_person_id and datediff(current_date, a.base_activity_datetime)<=180) as last_6, (select count(*) from act a where a.mastered_person_id = t.mastered_person_id and datediff(current_date, a.base_activity_datetime)>180 and datediff(current_date, a.base_activity_datetime)<=720) as 6_to_24,(select count(*) from act a where a.mastered_person_id = t.mastered_person_id and datediff(current_date, a.base_activity_datetime)<=1080) as last_36 from temp t')
    data = data.withColumn('affinity', when(lit((col('last_6')>=1) & (col('last_36')>2)), 'recurring')\
                                            .when(lit((col('last_6')>=1) & (col('last_36')<3)), 'recent')\
                                            .when(lit((col('6_to_24')>=1) & (col('last_36')>2)), 'periodic')\
                                            .when(lit((col('6_to_24')>=1) & (col('last_36')<3)), 'sporadic')\
                                            .when(lit((col('last_6')==0) & (col('6_to_24')==0) & (col('last_36')==1)), 'dormant').otherwise(lit(absent)))
    enriched.alias('enr').merge(data.alias('dat'), 'enr.mastered_person_id = dat.mastered_person_id').whenMatchedUpdate(set = {'email_affinity': 'dat.affinity'}).execute()
    print('email')
    
    spark.sql('create or replace table act as (select * from actcdm a where a.base_activity_type="incoming_call")')
    enriched.alias('enr').merge(data.alias('dat'), 'enr.mastered_person_id = dat.mastered_person_id').whenMatchedUpdate(set = {'call_affinity': 'dat.affinity'}).execute()
    print('call')
    
    spark.sql('create or replace table act as (select * from actcdm a where a.base_activity_type="webclick")')
    enriched.alias('enr').merge(data.alias('dat'), 'enr.mastered_person_id = dat.mastered_person_id').whenMatchedUpdate(set = {'webclick_affinity': 'dat.affinity'}).execute()
    print('webclick')
    print('enrich done')
    return