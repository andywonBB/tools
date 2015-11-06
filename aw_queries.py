hive_session_analytics = """
    ADD JAR /home/klai/projects/warehouse_and_analytics/warehouse_crons/lib/HiveSwarm-1.0-SNAPSHOT.jar;
    create temporary function user_agent_parser as 'com.livingsocial.hive.udf.UserAgentParser';
    ADD JAR /home/andywon/projects/udfs/brickhouse/target/brickhouse-0.7.1-SNAPSHOT.jar;
    CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';
    CREATE TEMPORARY FUNCTION to_json AS 'brickhouse.udf.json.ToJsonUDF';
    INSERT OVERWRITE LOCAL DIRECTORY '/home/andywon/projects/web_analytics/%s'
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
    LEFT JOIN (
    select 
    visitor_id,
    visit_id,
    count(distinct CASE WHEN event = 'view' and context = 'pdp' THEN get_json_object(properties, '$.product_id') END) as unique_pdp_views, 
    count(distinct CASE WHEN event = 'view' and context = 'catalog-index' THEN get_json_object(properties, '$.category-id') END) as unique_catalog_views, 
    count(distinct CASE WHEN event = 'view' and context = 'serp' THEN get_json_object(properties, '$.search_term') END) as unique_searches, 
    count(distinct CASE WHEN event = 'view' and context = 'bdp' THEN get_json_object(properties, '$.brand_id') END) as unique_brand_views, 
    MAX(CASE WHEN event = 'view' AND context = 'shop-home' THEN '1-true' ELSE '0-false' END) as shop_home,
    MAX(CASE WHEN event = 'view' AND context = 'brands' OR context = 'brand-home' THEN '1-true' ELSE '0-false' END) as brand_index,
    MAX(CASE WHEN event = 'view' AND context = 'cart' THEN '1-true' ELSE '0-false' END) as cart_viewed,
    MAX(CASE WHEN event = 'view' AND context = 'bxdp' THEN '1-true' ELSE '0-false' END) as box_viewed,
    MAX(CASE WHEN event = 'view' AND context = 'samples' THEN '1-true' ELSE '0-false' END) as samples_viewed,
    MAX(CASE WHEN event = 'view' AND context in ('checkout', 'checkout-success', 'express-checkout') THEN '1-true' ELSE '0-false' END) as checkout_viewed,
    MAX(CASE WHEN context = 'pdp' AND get_json_object(properties, '$.title') = 'Reserve Now' THEN '1-true' ELSE '0-false' END) as bbplus_addon
    FROM event_raw ev
    WHERE dt >= '%s' and dt <= '%s'
    AND context in ('pdp', 'catalog-index', 'serp', 'bdp', 'shop-home', 'brands', 'brand-home', 'cart', 'checkout', 'checkout-success', 'express-checkout', 'samples', 'bxdp')
    AND event in ('view', 'click')
    GROUP BY visitor_id, visit_id
    ) er 
    ON ws.visitor_id = er.visitor_id AND ws.visit_id = er.visit_id
    LEFT JOIN (
    SELECT 
    visitor_id,
    MAX(user_agent_parser(user_agent, 'ua_family')) AS browser,
    MAX(user_agent_parser(user_agent, 'os_family')) AS client_os,
    MAX(user_agent_parser(user_agent, 'device')) AS device
    FROM pageviews_raw
    WHERE dt >= '%s' and dt <= '%s'
    GROUP BY visitor_id
    ) pv ON ws.visitor_id = pv.visitor_id
    LEFT JOIN (
    SELECT visitor_id, visit_id, to_json(collect(event, get_json_object(properties, '$.variant'))) as experiments
    FROM event_raw
    WHERE context = 'experiment'
    AND dt >= '%s' and dt <= '%s'
    GROUP BY visitor_id, visit_id
    ) exp
    ON ws.visitor_id = exp.visitor_id AND ws.visit_id = exp.visit_id
    WHERE ws.year = %s
    AND ws.month = %s
    AND ws.day >= %s AND ws.day <= %s
    ;
    """ 

session_analytics_stg_to_prod = """
    INSERT INTO tmp.session_analytics
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
             MAX(CASE WHEN ss.visit_start BETWEEN fb.int_start AND fb.int_end THEN '2-active' WHEN fb.vertical_id IS NULL THEN '0-never' ELSE '1-cancelled' END) AS womens_status,
             MAX(CASE WHEN ss.visit_start BETWEEN fb_m.int_start AND fb_m.int_end THEN '2-active' WHEN fb_m.vertical_id IS NULL THEN '0-never' ELSE '1-cancelled' END) AS mens_status
      FROM tmp.stg_web_analytics ss
        LEFT JOIN box_invervals fb ON fb.int_start < ss.visit_start AND fb.customer_id = ss.customer_id AND fb.country = 'US' AND fb.vertical_id = 1
        LEFT JOIN box_invervals fb_m ON fb_m.int_start < ss.visit_start AND fb_m.customer_id = ss.customer_id AND fb_m.country = 'US' AND fb_m.vertical_id = 2
      where trim(ss.customer_id) not like ''
      and ss.customer_id is not null
      GROUP BY ss.visitor_id,
               ss.visit_id,
               ss.customer_id
    )
    SELECT 
    stg.visitor_id,
    stg.visit_id,
    stg.visit_start,
    stg.visit_end,
    stg.num_pages,
    stg.customer_id,
    cast(split_part(split_part(stg.entry_page, '?', 1), 'referer', 1) AS VARCHAR(255)) AS entry_page,
    cast(split_part(split_part(stg.exit_page, '?', 1), 'referer', 1) AS VARCHAR(255)) AS exit_page,
    stg.ecom_session_revenue,
    stg.rebillable_sub_session_revenue,
    stg.gift_sub_session_revenue,
    stg.giftcard_session_revenue,
    stg.ecom_session_units,
    stg.rebillable_sub_session_units,
    stg.gift_sub_session_units,
    stg.giftcard_session_units,
    stg.source,
    stg.extensible_param,
    stg.remote_ip,
    stg.zipcode,
    stg.city,
    stg.dmacode,
    stg.increment_ids,
    stg.browser,
    stg.client_os,
    CASE WHEN stg.device in ('null', 'Other') AND stg.client_os not in ('iOS', 'Android', 'Windows Phone', 'Windows Phone OS', 'Symbian OS') THEN 'Desktop' ELSE stg.device END as device,
    CASE WHEN stg.unique_pdp_views is null THEN 0 ELSE stg.unique_pdp_views END as unique_pdp_views,
    CASE WHEN stg.unique_catalog_views is null THEN 0 ELSE stg.unique_catalog_views END as unique_catalog_views,
    CASE WHEN stg.unique_searches is null THEN 0 ELSE stg.unique_searches END as unique_searches,
    CASE WHEN stg.unique_brand_views is null THEN 0 ELSE stg.unique_brand_views END as unique_brand_views,
    CASE WHEN stg.shop_home is null THEN '0-false' ELSE stg.shop_home END as shop_home,
    CASE WHEN stg.brand_index is null THEN '0-false' ELSE stg.brand_index END as brand_index,
    CASE WHEN stg.cart_viewed is null THEN '0-false' ELSE stg.cart_viewed END as cart_viewed,
    CASE WHEN stg.box_viewed is null THEN '0-false' ELSE stg.box_viewed END as box_viewed,
    CASE WHEN stg.samples_viewed is null THEN '0-false' ELSE stg.samples_viewed END as samples_viewed,
    CASE WHEN stg.checkout_viewed is null THEN '0-false' ELSE stg.checkout_viewed END as checkout_viewed,
    CASE WHEN stg.bbplus_addon is null THEN '0-false' ELSE stg.bbplus_addon END as bbplus_addon,
    stg.experiments,
    datediff('seconds', visit_start, visit_end) as time_spent_sec,
    --entry_page_type,
    --exit_page_type,
    sub.womens_status,
    sub.mens_status
    FROM tmp.stg_web_analytics stg
    LEFT JOIN sub_status sub
    ON stg.visitor_id = sub.visitor_id AND stg.visit_id = sub.visit_id
    ;
    """
