hive_session_analytics = """

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