import os
import gzip
import pipes
import tarfile
import time
import boto3
import shutil
#from botocore.client import config

bkp_path_file = '/home/manulal/backup1/compressed_file'
bkp_path_db = '/home/manulal/backup1/dbbkp'
date = time.strftime('%Y%m%d')
backup_dir_name_file = bkp_path_file + "/" + date
backup_dir_name_db = bkp_path_db + "/" + date
try:    
    os.stat(backup_dir_name_file)       
    os.stat(backup_dir_name_db)

except:
    os.mkdir(backup_dir_name_file)
    os.mkdir(backup_dir_name_db)
    print("Backup directory Created")

listdir = os.listdir('/home/manulal/backup1/file')
print("Files to compress", listdir)
for folder in listdir:
    name = "/home/manulal/backup1/file/"+folder
    tar = tarfile.open(name+".tar.gz", "w:gz")
    tar.add(name)
    tar.close()
    print("File Compression completed")
    print("Log removing in progress")
    try:
        os.remove(name)
        print("Removed Old Logs")
    except:
        os.remove(name)
        print("Error while removing old Logs")

# Uploading to S3
AWS_ACCESS_KEY_ID = '*********************'
AWS_SECRET_ACCESS_KEY = '*****************************'
Bucket_name = 'testing-script'
#AWS_S3_REGION_NAME = 'us-east-2'
#AWS_S3_SIGNATURE_VERSION = 's3v4'
#data = open('/home/manulal/backup1/file/a.tar.gz', 'rb')
s3 = boto3.resource(
       's3',
       aws_access_key_id = AWS_ACCESS_KEY_ID,
       aws_secret_access_key = AWS_SECRET_ACCESS_KEY
#       config = Config(signature_version = 's3v4')
 )

for files in listdir:
    bucket = open("/home/manulal/backup1/file/"+files+".tar.gz", 'rb')
    s3.Bucket(Bucket_name).put_object(Key='/data/upload/'+files+'_'+date+'.tar.gz', Body=bucket)
    print("Log file Uploaded to S3")
    filename = "/home/manulal/backup1/file/"+files+".tar.gz"
    shutil.move(filename, backup_dir_name_file)

#Remove old logs having age of 7 days
print("checking for old logs")
removeold = 'find /home/manulal/backup1/compressed_file/* -mtime +7 -exec rm -rf {} \;'
#find /tmp/*/* -mtime +7 -type d -exec rmdir {} \;
try:
    os.system(removeold)
    print("Old logs has been removed")
except:
    print("Old logs not found")

DB_HOST = 'localhost' 
DB_USER = 'root'
DB_USER_PASSWORD = '12345678'

#dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD
#os.system(dumpcmd)
print("Database backup in progress")
dumpcmd = "mysqldump -h " + DB_HOST + " -u " + DB_USER + " -p" + DB_USER_PASSWORD + " > " + pipes.quote(backup_dir_name_db) + "/" + date + ".sql"
os.system(dumpcmd)
gzipcmd = "gzip " + pipes.quote(backup_dir_name_db) + "/" + date + ".sql"
os.system(gzipcmd)
print("\n\nBackup and compression completed and uploading to s3\n\n")
dbucket = open(backup_dir_name_db+"/"+date+".sql.gz", 'rb')
s3.Bucket(Bucket_name).put_object(Key='/data/upload/'+date+'.sql.gz', Body=dbucket)
print("Log file Uploaded to S3")





