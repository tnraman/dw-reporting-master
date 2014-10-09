set echo on;
set timing on;
spool FTD_STRTYGY_CAMPGN_SUM_N.lst
DROP MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV;
CREATE MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV
BUILD IMMEDIATE
REFRESH FORCE ON DEMAND
AS
SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
         A.CAMPAIGN_ID,
         A.START_DATE as AGG_START_DATE_GMT,
         (SUM (IMPRESSIONS)) AS IMPRESSIONS,
         (SUM (CLICKS)) AS CLICKS,
         (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
         (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
         (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
         SYSDATE AS AGGREGATE_DATE
    FROM (
SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
            b.CAMPAIGN_ID,ca.START_DATE, 
           --as AGG_START_DATE_GMT,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
FROM (SELECT MM_DATE, CAMPAIGN_ID,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
     FROM MT_RPT01.T1_RPT_PERFORMANCE_2010
   --  WHERE  campaign_id='147383'
     GROUP BY CAMPAIGN_ID,MM_DATE ) b,
     (SELECT CAMPAIGN_ID,c.START_DATE,
     (  TRUNC (
                          c.START_DATE
                        + (  SIGN (
                                TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 1, 3)))
                           * (  TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 2, 2))
                              +   TO_NUMBER (
                                     SUBSTR (TZ_OFFSET (c.zone_name), 5, 2))
                                / 60)
                           / 24)))  CAMP_DT
     FROM MT_RPT01.T1_META_CAMPAIGN C) ca
WHERE  b.MM_DATE >= ca.CAMP_DT  AND ca.campaign_id = b.campaign_id
       --    and b.campaign_id='147383'
GROUP BY b.CAMPAIGN_ID,ca.START_DATE
UNION
SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
            b.CAMPAIGN_ID,
            ca.START_DATE,
            -- as AGG_START_DATE_GMT,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
FROM (SELECT MM_DATE, CAMPAIGN_ID,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
        FROM MT_RPT01.T1_RPT_PERFORMANCE_2011
       -- WHERE  campaign_id='147383'
        GROUP BY CAMPAIGN_ID,MM_DATE ) b,
        (SELECT CAMPAIGN_ID,c.START_DATE,
         (  TRUNC (
                          c.START_DATE
                        + (  SIGN (
                                TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 1, 3)))
                           * (  TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 2, 2))
                              +   TO_NUMBER (
                                     SUBSTR (TZ_OFFSET (c.zone_name), 5, 2))
                                / 60)
                           / 24)))  CAMP_DT
         FROM MT_RPT01.T1_META_CAMPAIGN C) ca
    WHERE  b.MM_DATE >= ca.CAMP_DT  AND ca.campaign_id = b.campaign_id
       --    and b.campaign_id='147383'
    GROUP BY b.CAMPAIGN_ID,ca.START_DATE ) A
GROUP BY A.CAMPAIGN_ID,A.START_DATE;
CREATE OR REPLACE SYNONYM MM_API_DO.T1_RPT_FTD_PERF_CAMPGN_SUM_MV FOR MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV;
CREATE OR REPLACE PUBLIC SYNONYM T1_RPT_FTD_PERF_CAMPGN_SUM_MV FOR MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV;
GRANT SELECT ON MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV TO MM_API_DO;
----
DROP MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV;
CREATE MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV
BUILD IMMEDIATE
REFRESH FORCE ON DEMAND
AS
SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
         A.CAMPAIGN_ID,
         A.STRATEGY_ID,
         A.START_DATE as AGG_START_DATE_GMT,
         (SUM (IMPRESSIONS)) AS IMPRESSIONS,
         (SUM (CLICKS)) AS CLICKS,
         (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
         (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
         (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
         SYSDATE AS AGGREGATE_DATE
    FROM (
    SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
           b.CAMPAIGN_ID,
           b.STRATEGY_ID,
           ca.START_DATE,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
    FROM (SELECT MM_DATE, CAMPAIGN_ID,STRATEGY_ID,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
         FROM MT_RPT01.T1_RPT_PERFORMANCE_2010
        -- WHERE  campaign_id='147383'
        GROUP BY CAMPAIGN_ID,STRATEGY_ID,MM_DATE ) b,
    (SELECT CAMPAIGN_ID,c.START_DATE,
         (  TRUNC (
                          C.START_DATE
                        + (  SIGN (
                                TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 1, 3)))
                           * (  TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 2, 2))
                              +   TO_NUMBER (
                                     SUBSTR (TZ_OFFSET (c.zone_name), 5, 2))
                                / 60)
                           / 24)))  CAMP_DT
         FROM MT_RPT01.T1_META_CAMPAIGN C) ca
    WHERE  b.MM_DATE >= ca.CAMP_DT  AND ca.campaign_id = b.campaign_id
          ---and b.campaign_id='147383'
    GROUP BY b.CAMPAIGN_ID,b.STRATEGY_ID,ca.START_DATE
UNION
    SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
           b.CAMPAIGN_ID,
           b.STRATEGY_ID,
       ca.START_DATE,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
    FROM (SELECT MM_DATE, CAMPAIGN_ID,STRATEGY_ID,
           (SUM (IMPRESSIONS)) AS IMPRESSIONS,
           (SUM (CLICKS)) AS CLICKS,
           (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
           (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
           --(SUM (DIS_PV_ACTIVITIES)) AS DIS_PV_ACTIVITIES,
           (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
           SYSDATE AS AGGREGATE_DATE
         FROM MT_RPT01.T1_RPT_PERFORMANCE_2011
        -- WHERE  campaign_id='147383'
        GROUP BY CAMPAIGN_ID,STRATEGY_ID,MM_DATE ) b,
    (SELECT CAMPAIGN_ID,c.START_DATE,
         (  TRUNC (
                         C.START_DATE
                        + (  SIGN (
                                TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 1, 3)))
                           * (  TO_NUMBER (
                                   SUBSTR (TZ_OFFSET (c.zone_name), 2, 2))
                              +   TO_NUMBER (
                                     SUBSTR (TZ_OFFSET (c.zone_name), 5, 2))
                                / 60)
                           / 24)))  CAMP_DT
         FROM MT_RPT01.T1_META_CAMPAIGN C) ca
    WHERE  b.MM_DATE >= ca.CAMP_DT  AND ca.campaign_id = b.campaign_id
          ---and b.campaign_id='147383'
    GROUP BY b.CAMPAIGN_ID,b.STRATEGY_ID,ca.START_DATE ) A
GROUP BY A.CAMPAIGN_ID,A.STRATEGY_ID,A.START_DATE;
CREATE OR REPLACE SYNONYM MM_API_DO.T1_RPT_FTD_PERF_STRATGY_SUM_MV FOR MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV;
CREATE OR REPLACE PUBLIC SYNONYM T1_RPT_FTD_PERF_STRATGY_SUM_MV FOR MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV;
GRANT SELECT ON MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV TO MM_API_DO;
