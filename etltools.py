#!/home/andywon/projects/pyscript_test/venv/bin/python
# -*- coding: utf-8 -*-

import tinys3
import subprocess
import psycopg2
import imp
from settings import settings
from pandas import read_sql
from datetime import datetime, date
import os.path

"""
Updated Fri Oct 14 2016
A bunch of functions to use with ETL jobs, particularly Luigi scripts.
These are meant to supplement the helpers.py functions found in the 
warehouse crons folder

@author: awon
"""

def is_redshift_vacuum():
    """ Returns a boolean value. Check to see if Redshift is being vacuumed. 
        A Redshift task should not run simultaneously during vacuum. """
    query = "select * FROM stv_recents WHERE lower(status) = 'running' AND trim(query) LIKE 'vacuum'"
    con = create_redshift_conn()
    results = read_sql(query, con)
    con.close()
    return len(results) != 0

def pipe_to_file(folder, filepath):
    """ Pipe raw output - typically from an INSERT OVERWRITE Hive query - and
        cat them into a single file specified in filepath. """
    if folder.endswith("/"):
        pass
    else:
        folder = folder + "/"
    cmd = "cat " + folder + "* > " + filepath
    subprocess.call(cmd, shell=True)   

def upload_to_s3(filepath, targetpath):
    """ Upload file specified in filepath argument to s3 location specified
        in targetpath argument. Requires s3 creds in settings module. """
    access_key = settings['s3']['access_key']
    secret_key = settings['s3']['secret_key']
    bucket = settings['s3']['bucket']
    conn = tinys3.Connection(access_key , secret_key, tls=True) #, endpoint = 's3-external-1.amazonaws.com')
    f = open(filepath,'rb')
    conn.upload(targetpath,f, bucket)

def create_redshift_conn(*args,**kwargs):
    """ Open connection to redshift. Required for other Reshift functions. """
    config = settings['redshift']
    try:
        con=psycopg2.connect(dbname=config['name'], host=config['host'], 
                              port=config['port'], user=config['user'], 
                              password=config['pass'])
        return con
    except Exception as err:
        print(err)

def copy_to_redshift_stg(filename, table, delim='\\t'):
    """ Copy to STAGING tables. Warning: truncates table named
        in argument before copying flat file into Redshift. """
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
    """ Hacky function to check if there are greater than 10,000
        sessions for the specified date in the session_analytics
        table. Used as a Luigi complete() attribute"""
    query = """
        select count(*) 
        from tmp.session_analytics 
        WHERE to_char(convert_timezone('America/New_York', visit_start), 'YYYY-MM-DD') = '%s'
        """
    query_with_date = query % date.strftime('%Y-%m-%d')
    con = create_redshift_conn()
    count = read_sql(query_with_date, con)
    con.close()
    return count.ix[0,0] > 10000

def get_file_mod_datetime(file, date=False):
    """ Returns latest modification datetime of a file. Use date=True
        to return date only. Useful for Luigi targets. """
    if os.path.isfile(file):
        cmd = ['date', '-r', file]
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
        out, err = p.communicate('foo\nfoofoo\n')
        file_mod_dt = datetime.strptime(out[:-1], "%a %b %d %H:%M:%S %Z %Y")
        return file_mod_dt.date() if date else file_mod_dt
    else:
        raise ValueError('File not found')

def get_hadoop_file_mod_datetime(file, date=False):
    """ Returns latest modification datetime of a file in HDFS. 
        Use date=True to return date only. Useful for Luigi targets. """
    cmd = ['hdfs', 'dfs', '-stat', file]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, stdin=subprocess.PIPE)
    out, err = p.communicate('foo\nfoofoo\n')
    if len(out) > 0:
        file_mod_dt = datetime.strptime(out[:-1], "%Y-%m-%d %H:%M:%S")
        return file_mod_dt.date() if date else file_mod_dt
    else:
        raise ValueError('File not found')

def is_file_updated(file, start, end=date.today(), hadoop=False):
    """ Check to see if a file has been modified within a timerange
        returns false if no file exists because we want the script to run """
    try:
        file_mod_dt = get_hadoop_file_mod_datetime(file, date=True) if hadoop else get_file_mod_datetime(file, date=True)
        return start <= file_mod_dt <= end
    except ValueError:
        return False
