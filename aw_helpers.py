#!/home/andywon/projects/pyscript_test/venv/bin/python
import tinys3
import subprocess
from settings import settings


cmd = "cat /home/andywon/projects/session_wide02/* > test1.tsv"
subprocess.call(cmd, shell=True)

def pipe_to_file(folder, filename):
	""" take folder output from hive query and pipe into single file in bash """
	cmd = "cat " + folder + "*"


def upload_to_s3(filepath, targetpath):
	""" uploads to TMP folder """
	access_key = settings['s3']['access_key']
	secret_key = settings['s3']['secret_key']
	bucket = settings['s3']['bucket']
	conn = tinys3.Connection(access_key , secret_key, tls=True) #, endpoint = 's3-external-1.amazonaws.com')
	f = open(filepath,'rb')
	conn.upload(targetpath,f, bucket)




###
select visitor_id, visit_id, get_json_object(properties, '$.')


select visitor_id, visit_id, count(*)
from event_raw
where context='experiment' and dt = '2015-10-20'
group by visitor_id, visit_id
having count(*) > 1
;

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
