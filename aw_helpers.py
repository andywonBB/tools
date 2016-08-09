#!/home/andywon/projects/pyscript_test/venv/bin/python
import tinys3
import subprocess
import psycopg2
import imp
from settings import settings
from pandas import read_sql

#helpers = imp.load_source('helpers', '/home/andywon/tools/helpers.py')
#settings = imp.load_source('settings', '/home/andywon/tools/settings.py').settings

def is_redshift_vacuum():
    """ 
    Check to see if Redshift is being vacuumed. Used in Luigi ETL pipeline.
    """
    query = "select * FROM stv_recents WHERE lower(status) = 'running' AND trim(query) LIKE 'vacuum'"
    con = create_redshift_conn()
    results = read_sql(query, con)
    con.close()
    return len(results) != 0

def pipe_to_file(folder, filepath):
    """ take folder output from hive query and pipe into single file in bash """
    if folder.endswith("/"):
        pass
    else:
        folder = folder + "/"
    cmd = "cat " + folder + "* > " + filepath
    subprocess.call(cmd, shell=True)
    

def upload_to_s3(filepath, targetpath):
    """ uploads to TMP folder on s3 bucket """
    access_key = settings['s3']['access_key']
    secret_key = settings['s3']['secret_key']
    bucket = settings['s3']['bucket']
    conn = tinys3.Connection(access_key , secret_key, tls=True) #, endpoint = 's3-external-1.amazonaws.com')
    f = open(filepath,'rb')
    conn.upload(targetpath,f, bucket)


def create_redshift_conn(*args,**kwargs):
    """ open connection to redshift """
    config = settings['redshift']
    try:
        con=psycopg2.connect(dbname=config['name'], host=config['host'], 
                              port=config['port'], user=config['user'], 
                              password=config['pass'])
        return con
    except Exception as err:
        print(err)


def copy_to_redshift_stg(filename, table, delim='\\t'):
    """ insert file to existing table in redshift """
    access_key = settings['s3']['access_key']
    secret_key = settings['s3']['secret_key']
    bucket = 's3://' + settings['s3']['bucket'] + '/'
    filepath = bucket + filename
    con = create_redshift_conn()
    cur = con.cursor()
    sql = """
    TRUNCATE %s;
    COPY %s
    FROM '%s'
    credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
    blanksasnull
    emptyasnull
    maxerror 100
    delimiter '%s'
    ;
    """ % (table, table, filepath, access_key, secret_key, delim)
    cur.execute(sql)
    con.commit()
    con.close()
    cur.close()

def sessions_exist(date):
    query = """
        select count(*) 
        from tmp.session_analytics 
        WHERE to_char(convert_timezone('America/New_York', visit_start), 'YYYY-MM-DD') = '%s'
        """
    query_with_date = query % date.strftime('%Y-%m-%d')
    con = create_redshift_conn()
    count = read_sql(query_with_date, con)
    con.close()
    return count[0,0] > 10000