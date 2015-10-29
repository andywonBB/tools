hive_session_analytics = """

    ADD JAR /home/klai/projects/warehouse_and_analytics/warehouse_crons/lib/HiveSwarm-1.0-SNAPSHOT.jar;
    create temporary function user_agent_parser as 'com.livingsocial.hive.udf.UserAgentParser';
    ADD JAR /home/andywon/projects/udfs/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
    CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
    INSERT OVERWRITE LOCAL DIRECTORY '/home/andywon/projects/session_analytics_test/%s'
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\\t'
    LINES TERMINATED BY '\\n'

    ### event_raw backdates to 2014-02-01
    SELECT ws.visitor_id, ws.visit_id, to_date(to_utc_timestamp(from_unixtime(ws.visit_start), 'America/New_York')) as dt, ws.visit_start, ws.visit_end, ws.num_pages, ws.customer_ids[0], ws.landing_url, ws.exit_url,
    ws.ecom_session_revenue, ws.rebillable_sub_session_revenue, ws.gift_sub_session_revenue, ws.giftcard_session_revenue, ws.ecom_session_units, ws.rebillable_sub_session_units, ws.gift_sub_session_units, ws.giftcard_session_units,
    CASE
        WHEN source = 'paid' THEN
        (CASE 
            WHEN lower(parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'utm_medium')) RLIKE '(ppc|cpc)' OR parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'gclid') IS NOT NULL THEN 'paid_search'
            WHEN lower(parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'utm_medium')) RLIKE '(affiliate)' THEN 'affiliate'
            WHEN lower(parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'utm_medium')) RLIKE '(display)' THEN 'display'
            WHEN lower(parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'utm_medium')) = 'statusupdate' AND lower(parse_url(concat('http://a.com', ws.landing_url), 'QUERY', 'utm_campaign')) RLIKE '.*[pP]aid.*' THEN 'paid_statusupdate'
        ELSE 'paid_other'
        END)
    ELSE source
    END as source,
    pv.ua_family,
    pv.os_family,
    pv.device,
    er.unique_pdp_views,
    er.unique_catalog_views,
    er.unique_searches,
    er.unique_brand_views,
    er.shop_home,
    er.brand_index,
    er.cart_viewed,
    er.checkout_viewed,
    er.bbplus_addon,
    exp.experiments
    FROM web_session ws
    JOIN (
    select 
    visitor_id,
    visit_id,
    count(distinct CASE WHEN event = 'view' and context = 'pdp' THEN get_json_object(properties, '$.product_id') END) as unique_pdp_views, 
    count(distinct CASE WHEN event = 'view' and context = 'catalog-index' THEN get_json_object(properties, '$.category-id') END) as unique_catalog_views, 
    count(distinct CASE WHEN event = 'view' and context = 'serp' THEN get_json_object(properties, '$.search_term') END) as unique_searches, 
    count(distinct CASE WHEN event = 'view' and context = 'bdp' THEN get_json_object(properties, '$.brand_id') END) as unique_brand_views, 
    MAX(CASE WHEN event = 'view' AND context = 'shop-home' THEN '1-true' ELSE '0-false' END) as shop_home,
    MAX(CASE WHEN event = 'view' AND context = 'brand-index' THEN '1-true' ELSE '0-false' END) as brand_index,
    MAX(CASE WHEN event = 'view' AND context = 'cart' THEN '1-true' ELSE '0-false' END) as cart_viewed,
    MAX(CASE WHEN event = 'view' AND context RLIKE 'checkout*' THEN '1-true' ELSE '0-false' END) as checkout_viewed,
    MAX(CASE WHEN context = 'pdp' AND get_json_object(properties, '$.title') = 'Reserve Now' THEN '1-true' ELSE '0-false' END) as bbplus_addon
    FROM event_raw ev
    WHERE dt >= %s and dt <= %s
    GROUP BY visitor_id, visit_id
    ) er 
    ON ws.visitor_id = er.visitor_id AND ws.visit_id = er.visit_id
    JOIN (
    SELECT 
    visitor_id,
    MAX(user_agent_parser(user_agent, 'ua_family')) AS ua_family,
    MAX(user_agent_parser(user_agent, 'os_family')) AS os_family,
    MAX(user_agent_parser(user_agent, 'device')) AS device
    FROM pageviews_raw
    WHERE dt >= %s and dt <= %s
    GROUP BY visitor_id
    ) pv ON ws.visitor_id = pv.visitor_id
    JOIN (
    SELECT visitor_id, visit_id, to_json(collect(event, get_json_object(properties, '$.variant'))) as experiments
    FROM event_raw
    WHERE context = 'experiment'
    AND dt >= %s and dt <= %s
    GROUP BY visitor_id, visit_id
    ) exp
    ON ws.visitor_id = exp.visitor_id AND ws.visit_id = exp.visit_id
    WHERE ws.year = %s
    AND ws.month = %s
    AND ws.day >= %s AND ws.day <= %s
    ;
    """ 