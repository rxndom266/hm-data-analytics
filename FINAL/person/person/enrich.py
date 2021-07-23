from pyspark.sql.functions import col, lit, when
from delta.tables import *

spark = SparkSession \
    .builder \
    .appName("Enrich") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .getOrCreate()
def enrich(load_id, srcpath, destpath):
    null='--'
    sets = updatesets['person']
    dbutils.fs.rm(destpath, recurse=True)
    data = spark.read.format('delta').load(srcpath).where(col('delete_flag')==0)
    data = data.withColumn('age_group', when(lit((col('age')>=0) & (col('age')<=14)), 'child')\
                                        .when(lit((col('age')>14) & (col('age')<=24)), 'youth')\
                                        .when(lit((col('age')>24) & (col('age')<=64)), 'adult')\
                                        .when(lit(col('age')>64), 'senior'))
    
    data.write.format('delta').save(destpath)
    print('enrich done')
    return