# hm-data-analytics

# to connect to AWS
# run this code in a
# DataBricks notebook

import urllib
access_key="<your-AWS-access-key>"
secret_key="<your-AWS-secret-key>".replace("/","%2F")
encoded_secret_key=urllib.parse.quote(secret_key,"")
AWS_s3_bucket="<your-AWS-bucket-name>"
mount_name="<your-file-name-inside-bucket>"
sourceurl="s3a://{0}:{1}@{2}".format(access_key,encoded_secret_key,AWS_s3_bucket)
dbutils.fs.mount(sourceurl,"/mnt/%s" % mount_name)
dbutils.fs.ls("/mnt/%s" %mount_name)
