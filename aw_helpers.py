#!/home/andywon/projects/pyscript_test/venv/bin/python
import tinys3
import subprocess
import psycopg2
import imp
from settings import settings
#from pandas import read_sql

#helpers = imp.load_source('helpers', '/home/andywon/tools/helpers.py')
#settings = imp.load_source('settings', '/home/andywon/tools/settings.py').settings


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


def copy_to_redshift(filename, table, delim='\\t'):
    """ insert file to existing table in redshift """
    access_key = settings['s3']['access_key']
    secret_key = settings['s3']['secret_key']
    bucket = 's3://' + settings['s3']['bucket'] + '/'
    filepath = bucket + filename
    con = create_redshift_conn()
    cur = con.cursor()
    sql = """
    copy %s
    FROM '%s'
    credentials 'aws_access_key_id=%s;aws_secret_access_key=%s'
    delimiter '%s'
    ;
    """ % (table, filepath, access_key, secret_key, delim)
    cur.execute(sql)
    con.commit()


"""
ADD JAR /home/andywon/projects/udfs/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;
---CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
CREATE TEMPORARY FUNCTION combine AS 'brickhouse.udf.collect.CombineUDF';

select 
ev.visitor_id,
ev.visit_id,
count(distinct CASE WHEN ev.event = 'view' and ev.context = 'pdp' THEN get_json_object(ev.properties, '$.path') END) as unique_pdp_views,
count(distinct CASE WHEN ev.event = 'view' and ev.context = 'catalog-index' THEN get_json_object(ev.properties, '$.path') END) as unique_catalog_views,
count(distinct CASE WHEN ev.event = 'view' and ev.context = 'serp' THEN get_json_object(ev.properties, '$.path') END) as unique_searches,
count(distinct CASE WHEN ev.event = 'view' and ev.context = 'bdp' THEN get_json_object(ev.properties, '$.path') END) as unique_brand_views,
MAX(CASE WHEN ev.event = 'view' AND ev.context = 'shop-home' THEN '1-true' ELSE '0-false' END) as show_home,
MAX(CASE WHEN ev.event = 'view' AND ev.context = 'brand-index' THEN '1-true' ELSE '0-false' END) as brand_index,
MAX(CASE WHEN ev.event = 'view' AND ev.context = 'cart' THEN '1-true' ELSE '0-false' END) as cart_viewed,
MAX(CASE WHEN ev.event = 'view' AND ev.context = 'checkout' THEN '1-true' ELSE '0-false' END) as checkout_view,
exp.experiments
from event_raw ev
JOIN(
SELECT visitor_id, visit_id, collect(event, get_json_object(properties, '$.variant')) as experiments
from event_raw
where context = 'experiment'
and dt = '2015-10-20'
group by visitor_id, visit_id
) exp
ON ev.visitor_id = exp.visitor_id and ev.visit_id = exp.visit_id
where ev.dt = '2015-10-20'
---and context = 'experiment'
group by ev.visitor_id, ev.visit_id, exp.experiments
;

select visitor_id, visit_id, collect(if(context='experiment', explode(event, get_json_object(properties, '$.variant')), NULL))
from event_raw
where visitor_id = 'fffc825d-84f9-4113-a8ae-299a10095b1a'
and visit_id = 8
and dt = '2015-10-20'
group by visitor_id, visit_id
;
"""