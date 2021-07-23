# run this code in Databricks notebook
import random

dbutils.widgets.text('client_code', 'null', 'Client Code') # any random string works
dbutils.widgets.dropdown('load_type', 'activity', ['fb_person', 'ig_person', 'crm_person', 'activity'], 'Load Type') # one out of four types
dbutils.widgets.text('data_source_code', 'null', 'Data Source Code') # any random string works
dbutils.widgets.text('mount_root', 'null', 'AWS Source URI') # folder level URL to file in S3
dbutils.widgets.text('file_name', 'null', 'File Name') # name of the file WITH EXTENSION
dbutils.widgets.dropdown('enrich_flag', 'Yes', ['Yes', 'No'], 'Enrichment') # to be used for enrichment later

from datetime import datetime

client_code = dbutils.widgets.get('client_code')
load_type = dbutils.widgets.get('load_type')
load_id = str(random.randint(0, 99999999))
data_source_code = dbutils.widgets.get('data_source_code')
mount_root = dbutils.widgets.get('mount_root')
for mnt in dbutils.fs.mounts():
  if mnt.source == mount_root:
    client_data_path = mnt.mountPoint
file_name = dbutils.widgets.get('file_name')
enrich_flag = dbutils.widgets.get('enrich_flag')

pipeline_config = {
  'client_code': client_code,
  'load_type': load_type,
  'load_id': load_id,
  'data_source': data_source_code,
  'mount_root': mount_root,
  'client_data_path': client_data_path,
  'file_name': file_name,
  'enrich_flag': enrich_flag,
  'batch_datetime': datetime.now()
  
}
