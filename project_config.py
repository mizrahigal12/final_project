import os
import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

#==========pyarrow connector=========================================
import pyarrow as pa
fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

#==========mysql connector==========================================
import sqlalchemy as sa
# default
mysql_cnx = sa.create_engine("mysql+pymysql://naya:NayaPass1!@localhost/crypto_coins")

### mysql.connector has bugs with chunking, so use pymysql instead
'''import mysql.connector
cnx = mysql.\
    connector.\
    connect(user='naya',
            password='NayaPass1!1',
            host='localhost',
            database='crypto_coins')
'''

import pymysql
  
cnx = pymysql.connect(
    host='cnt7-naya-cdh63',
    user='naya', 
    password = "NayaPass1!",
    db='crypto_coins_agg',
    autocommit=True
    )


#==========hdfs connector===========================================
# HDFS details
hdfs_host = 'Cnt7-naya-cdh63'
hdfs_port = 9870

#==========impala connector=========================================
from pyhive import hive
import ibis.impala.ddl

impala_host = 'Cnt7-naya-cdh63'
impala_port = 21050
impala_database = 'audiostore'
impala_username = 'hdfs'
impala_password = 'naya'

hdfs = ibis.hdfs_connect(
    host=hdfs_host,
    port=hdfs_port,
    protocol='webhdfs',
    use_https='default',
    auth_mechanism='NOSASL',
    verify=True)


client = ibis.impala.connect(
    host=impala_host,
    port=impala_port,
    user=impala_username,
    password=impala_password,
    pool_size=8,
    hdfs_client=hdfs)

#==========hive connector=========================================
# hive
hive_port = 10000
hive_username = 'hdfs'
hive_password = 'naya'
hive_database = 'audiostore'
hive_mode = 'CUSTOM'

# Create Hive connection
hive_cnx = hive.Connection(
    host=hdfs_host,
    port=hive_port,
    username=hive_username,
    password=hive_password,
    auth=hive_mode)

#==========path====================================================
path_staging = '/tmp/staging'
stg = '/tmp/staging/'
impala_path = 'hdfs://Cnt7-naya-cdh63:8020/user/hive/warehouse/audiostore/'