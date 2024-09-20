
   WITH combined_data AS (
    SELECT
        ad_date,
        COALESCE(fa.spend, 0) AS spend,
        COALESCE(fa.impressions, 0) AS impressions,
        COALESCE(fa.reach, 0) AS reach,
        COALESCE(fa.clicks, 0) AS clicks,
        COALESCE(fa.leads, 0) AS leads,
        COALESCE(fa.value, 0) AS value,
        fa.url_parameters,
        fc.campaign_name AS campaign_name,
        fas.adset_name AS adset_name
    FROM
        facebook_ads_basic_daily fa
    LEFT JOIN
        facebook_adset fas ON fa.adset_id = fas.adset_id
    LEFT JOIN
        facebook_campaign fc ON fa.campaign_id = fc.campaign_id
    UNION ALL
    SELECT
        ad_date,
        COALESCE(ga.spend, 0) AS spend,
        COALESCE(ga.impressions, 0) AS impressions,
        COALESCE(ga.reach, 0) AS reach,
        COALESCE(ga.clicks, 0) AS clicks,
        COALESCE(ga.leads, 0) AS leads,
        COALESCE(ga.value, 0) AS value,
        ga.url_parameters,
        ga.campaign_name AS campaign_name,
        ga.adset_name AS adset_name
    FROM
        google_ads_basic_daily ga
),
utm_data AS (
    SELECT
        ad_date,
        spend,
        impressions,
        clicks,
        value,
        campaign_name,
        adset_name,
        LOWER(
            COALESCE(
                substring(url_parameters FROM 'utm_campaign=([^&]+)'),
                'nan'
            )
        ) AS utm_campaign
    FROM
        combined_data
)

SELECT
    ad_date,
    CASE 
        WHEN utm_campaign = 'nan' THEN NULL
        ELSE utm_campaign
    END AS utm_campaign,
    SUM(spend) AS total_spend,
    SUM(impressions) AS total_impressions,
    SUM(clicks) AS total_clicks,
    SUM(value) AS total_value,
    CASE 
        WHEN SUM(impressions) = 0 THEN 0 
        ELSE SUM(clicks)::numeric / SUM(impressions) * 100
    END AS ctr,
    CASE 
        WHEN SUM(clicks) = 0 THEN 0 
        ELSE SUM(spend)::numeric / SUM(clicks)
    END AS cpc,
    CASE 
        WHEN SUM(impressions) = 0 THEN 0 
        ELSE SUM(spend)::numeric / SUM(impressions) * 1000
    END AS cpm,
    CASE 
        WHEN SUM(spend) = 0 THEN 0 
        ELSE SUM(value)::numeric / SUM(spend)
    END AS romi
FROM
    utm_data
GROUP BY
    ad_date, utm_campaign;
    
   
   
   ----
   
 WITH combined_data AS (
    SELECT
        ad_date,
        COALESCE(fa.spend, 0) AS spend,
        COALESCE(fa.impressions, 0) AS impressions,
        COALESCE(fa.reach, 0) AS reach,
        COALESCE(fa.clicks, 0) AS clicks,
        COALESCE(fa.leads, 0) AS leads,
        COALESCE(fa.value, 0) AS value,
        fa.url_parameters,
        fc.campaign_name AS campaign_name,
        fas.adset_name AS adset_name
    FROM
        facebook_ads_basic_daily fa
    LEFT JOIN
        facebook_adset fas ON fa.adset_id = fas.adset_id
    LEFT JOIN
        facebook_campaign fc ON fa.campaign_id = fc.campaign_id
    UNION ALL
    SELECT
        ad_date,
        COALESCE(ga.spend, 0) AS spend,
        COALESCE(ga.impressions, 0) AS impressions,
        COALESCE(ga.reach, 0) AS reach,
        COALESCE(ga.clicks, 0) AS clicks,
        COALESCE(ga.leads, 0) AS leads,
        COALESCE(ga.value, 0) AS value,
        ga.url_parameters,
        ga.campaign_name AS campaign_name,
        ga.adset_name AS adset_name
    FROM
        google_ads_basic_daily ga
),
utm_data AS (
    SELECT
        ad_date,
        spend,
        impressions,
        clicks,
        value,
        campaign_name,
        adset_name,
        LOWER(
            COALESCE(
                substring(url_parameters FROM 'utm_campaign=([^&]+)'),
                'nan'
            )
        ) AS utm_campaign
    FROM
        combined_data
),
monthly_data AS (
    SELECT
        date_trunc('month', ad_date) AS ad_month,
        CASE 
            WHEN utm_campaign = 'nan' THEN NULL
            ELSE utm_campaign
        END AS utm_campaign,
        SUM(spend) AS total_spend,
        SUM(impressions) AS total_impressions,
        SUM(clicks) AS total_clicks,
        SUM(value) AS total_value,
        CASE 
            WHEN SUM(impressions) = 0 THEN 0 
            ELSE SUM(clicks)::numeric / SUM(impressions) * 100
        END AS ctr,
        CASE 
            WHEN SUM(clicks) = 0 THEN 0 
            ELSE SUM(spend)::numeric / SUM(clicks)
        END AS cpc,
        CASE 
            WHEN SUM(impressions) = 0 THEN 0 
            ELSE SUM(spend)::numeric / SUM(impressions) * 1000
        END AS cpm,
        CASE 
            WHEN SUM(spend) = 0 THEN 0 
            ELSE SUM(value)::numeric / SUM(spend)
        END AS romi
    FROM
        utm_data
    GROUP BY
        date_trunc('month', ad_date), utm_campaign
),
monthly_differences AS (
    SELECT
        md.ad_month,
        md.utm_campaign,
        md.total_spend,
        md.total_impressions,
        md.total_clicks,
        md.total_value,
        md.ctr,
        md.cpc,
        md.cpm,
        md.romi,
        LAG(md.cpm) OVER (PARTITION BY md.utm_campaign ORDER BY md.ad_month) AS prev_cpm,
        LAG(md.ctr) OVER (PARTITION BY md.utm_campaign ORDER BY md.ad_month) AS prev_ctr,
        LAG(md.romi) OVER (PARTITION BY md.utm_campaign ORDER BY md.ad_month) AS prev_romi
    FROM
        monthly_data md
)
SELECT
    ad_month,
    utm_campaign,
    total_spend,
    total_impressions,
    total_clicks,
    total_value,
    ctr,
    cpc,
    cpm,
    romi,
    CASE
        WHEN prev_cpm IS NULL THEN NULL
        ELSE ((cpm - prev_cpm) / prev_cpm) * 100
    END AS cpm_change,
    CASE
        WHEN prev_ctr IS NULL THEN NULL
        ELSE ((ctr - prev_ctr) / prev_ctr) * 100
    END AS ctr_change,
    CASE
        WHEN prev_romi IS NULL THEN NULL
        ELSE ((romi - prev_romi) / prev_romi) * 100
    END AS romi_change
FROM
    monthly_differences;  
   
   
   