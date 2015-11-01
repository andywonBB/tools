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
    SELECT ws.visitor_id, ws.visit_id, to_utc_timestamp(from_unixtime(ws.visit_start), 'America/New_York') as visit_start, to_utc_timestamp(from_unixtime(ws.visit_end), 'America/New_York') as visit_end, ws.num_pages, ws.customer_ids[0], ws.landing_url, ws.exit_url,
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
    ws.extensible_param,
    ws.remote_ip,
    ws.zipcode,
    ws.city,
    ws.dmacode,
    ws.increment_ids,
    pv.browser,
    pv.client_os,
    pv.device,
    er.unique_pdp_views,
    er.unique_catalog_views,
    er.unique_searches,
    er.unique_brand_views,
    er.shop_home,
    er.brand_index,
    er.cart_viewed,
    er.box_viewed,
    er.samples_viewed,
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
    MAX(CASE WHEN event = 'view' AND context = 'bxdp' THEN '1-true' ELSE '0-false' END) as box_viewed,
    MAX(CASE WHEN event = 'view' AND context = 'samples' THEN '1-true' ELSE '0-false' END) as samples_viewed,
    MAX(CASE WHEN event = 'view' AND context in ('checkout', 'checkout-success', 'express-checkout') THEN '1-true' ELSE '0-false' END) as checkout_viewed,
    MAX(CASE WHEN context = 'pdp' AND get_json_object(properties, '$.title') = 'Reserve Now' THEN '1-true' ELSE '0-false' END) as bbplus_addon
    FROM event_raw ev
    WHERE dt >= %s and dt <= %s
    AND context in ('pdp', 'catalog-index', 'serp', 'bdp', 'shop_home', 'brand_index', 'cart', 'checkout', 'checkout-success', 'express-checkout', 'samples', 'bxdp')
    AND event in ('view', 'click')
    GROUP BY visitor_id, visit_id
    ) er 
    ON ws.visitor_id = er.visitor_id AND ws.visit_id = er.visit_id
    JOIN (
    SELECT 
    visitor_id,
    MAX(user_agent_parser(user_agent, 'ua_family')) AS browser,
    MAX(user_agent_parser(user_agent, 'os_family')) AS client_os,
    MAX(user_agent_parser(user_agent, 'device')) AS device
    FROM pageviews_raw
    WHERE dt >= %s and dt <= %s
    GROUP BY visitor_id
    ) pv ON ws.visitor_id = pv.visitor_id
    LEFT JOIN (
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

session_analytics_stg_to_prod = """
    WITH box_invervals AS
    (
      SELECT a.country,
             customer_id,
             CASE
               WHEN subscription_box_number = 1 THEN d.created_at_time
               ELSE f_start.date_value
             END AS int_start,
             f_end.date_value + 10 AS int_end,
             c.vertical_id,
             a.subscription_box_number
      FROM f_box_subscription_profile a
        JOIN d_customer b ON a.customer_key = b.customer_key
        JOIN d_vertical c ON c.vertical_key = a.vertical_key
        JOIN d_subscription d ON d.subscription_key = a.subscription_key
        JOIN d_cycle e ON e.cycle_key = a.shipping_cycle_key
        JOIN d_date f_end ON f_end.date_key = e.end_date_key
        JOIN d_date f_start ON f_start.date_key = e.start_date_key
      WHERE a.is_latest_version = 1
      AND   a.box_payment_status = 'paid'
    ),

    sub_status AS
    (
      SELECT ss.visitor_id,
             ss.visit_id,
             ss.customer_id,
             MAX(CASE WHEN ss.visit_date BETWEEN fb.int_start AND fb.int_end THEN '2-active' WHEN fb.vertical_id IS NULL THEN '0-never' ELSE '1-cancelled' END) AS womens_status,
             MAX(CASE WHEN ss.visit_date BETWEEN fb_m.int_start AND fb_m.int_end THEN '2-active' WHEN fb_m.vertical_id IS NULL THEN '0-never' ELSE '1-cancelled' END) AS mens_status
      FROM tmp.stg_session_analytics ss
        LEFT JOIN box_invervals fb ON fb.int_start < ss.visit_date AND fb.customer_id = ss.customer_id AND fb.country = 'US' AND fb.vertical_id = 1
        LEFT JOIN box_invervals fb_m ON fb_m.int_start < ss.visit_date AND fb_m.customer_id = ss.customer_id AND fb_m.country = 'US' AND fb_m.vertical_id = 2
      where trim(ss.customer_id) not like ''
      and ss.customer_id is not null
      GROUP BY ss.visitor_id,
               ss.visit_id,
               ss.customer_id
    )
    stg.visitor_id,
    stg.visit_id,
    stg.visit_date,
    stg.visit_start,
    stg.visit_end,
    stg.num_pages,
    stg.customer_id,
    stg.entry_page,
    stg.exit_page,
    stg.ecom_session_revenue,
    stg.rebillable_sub_session_revenue,
    stg.gift_sub_session_revenue,
    stg.giftcard_session_revenue,
    stg.ecom_session_units,
    stg.rebillable_sub_session_units,
    stg.gift_sub_session_units,
    stg.giftcard_session_units,
    stg.source,
    stg.ua_family,
    stg.os_family,
    stg.device,
    stg.unique_pdp_views,
    stg.unique_catalog_views,
    stg.unique_searches,
    stg.unique_brand_views,
    stg.shop_home,
    stg.brand_index,
    stg.cart_viewed,
    stg.box_viewed,
    stg.samples_viewed,
    stg.checkout_view,
    stg.experiments,
    (stg.visit_end - stg.visit_start) as time_spent_sec,
    --entry_page_type,
    --exit_page_type,
    sub.womens_status,
    sub.mens_status
    INTO tmp.session_analytics
    FROM tmp.stg_session_analytics stg
    JOIN sub_status sub
    ON stg.visitor_id = sub.visitor_id AND stg.visit_id = sub.visit_id
    ;
    """