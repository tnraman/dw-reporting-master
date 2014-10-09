set echo on;
set timing on;
spool FTD_STRTYGY_CAMPGN_SO_MV_N.lst
DROP MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_CAMPAIGN_MV;
CREATE MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_CAMPAIGN_MV
BUILD IMMEDIATE
REFRESH FORCE ON DEMAND
WITH PRIMARY KEY
AS 
 SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
         A.CAMPAIGN_ID,
         A.AGG_START_DATE_GMT,
         (SUM (IMPRESSIONS)) AS IMPRESSIONS,
         (SUM (CLICKS)) AS CLICKS,
         (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
         (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
         (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
         SYSDATE AS AGGREGATE_DATE
    FROM (  SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
                   CAMPAIGN_ID,
                   AGG_START_DATE_GMT,
                   (SUM (IMPRESSIONS)) AS IMPRESSIONS,
                   (SUM (CLICKS)) AS CLICKS,
                   (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
                   (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
                   (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
                   SYSDATE AS AGGREGATE_DATE
              FROM T1_RPT_FTD_PERF_CAMPGN_CUR_MV 
          ---WHERE CAMPAIGN_ID='100512'
          GROUP BY CAMPAIGN_ID,AGG_START_DATE_GMT
          UNION
              SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
                   CAMPAIGN_ID,
                   AGG_START_DATE_GMT,
                   (SUM (IMPRESSIONS)) AS IMPRESSIONS,
                   (SUM (CLICKS)) AS CLICKS,
                   (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
                   (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
                   (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
                   SYSDATE AS AGGREGATE_DATE
              FROM MT_RPT01.T1_RPT_FTD_PERF_CAMPGN_SUM_MV
           ---WHERE  CAMPAIGN_ID='100512'
             GROUP BY CAMPAIGN_ID,AGG_START_DATE_GMT) A
  GROUP BY A.CAMPAIGN_ID,A.AGG_START_DATE_GMT;
CREATE OR REPLACE SYNONYM MM_API_DO.T1_RPT_FTD_PERF_CAMPAIGN_MV FOR MT_RPT01.T1_RPT_FTD_PERF_CAMPAIGN_MV;
CREATE OR REPLACE PUBLIC SYNONYM T1_RPT_FTD_PERF_CAMPAIGN_MV FOR MT_RPT01.T1_RPT_FTD_PERF_CAMPAIGN_MV;
GRANT SELECT ON MT_RPT01.T1_RPT_FTD_PERF_CAMPAIGN_MV TO MM_API_DO;


DROP MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_STRATEGY_MV;
CREATE MATERIALIZED VIEW MT_RPT01.T1_RPT_FTD_PERF_STRATEGY_MV
BUILD IMMEDIATE
REFRESH FORCE ON DEMAND
WITH PRIMARY KEY
AS 
  SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
         A.CAMPAIGN_ID,A.STRATEGY_ID,
         A.AGG_START_DATE_GMT,
         (SUM (IMPRESSIONS)) AS IMPRESSIONS,
         (SUM (CLICKS)) AS CLICKS,
         (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
         (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
         (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
         SYSDATE AS AGGREGATE_DATE
    FROM (  SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
                   CAMPAIGN_ID,STRATEGY_ID,
                   AGG_START_DATE_GMT,
                   (SUM (IMPRESSIONS)) AS IMPRESSIONS,
                   (SUM (CLICKS)) AS CLICKS,
                   (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
                   (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
                   (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
                   SYSDATE AS AGGREGATE_DATE
              FROM T1_RPT_FTD_PERF_STRATGY_CUR_MV 
          ---WHERE CAMPAIGN_ID='100512'
          GROUP BY CAMPAIGN_ID,STRATEGY_ID,AGG_START_DATE_GMT
          UNION
              SELECT CAST ('FTD' AS VARCHAR (3)) AS MM_INTERVAL,
                   CAMPAIGN_ID,STRATEGY_ID,
                   AGG_START_DATE_GMT,
                   (SUM (IMPRESSIONS)) AS IMPRESSIONS,
                   (SUM (CLICKS)) AS CLICKS,
                   (SUM (PC_ACTIVITIES)) AS PC_ACTIVITIES,
                   (SUM (PV_ACTIVITIES)) AS PV_ACTIVITIES,
                   (SUM (TOTAL_SPEND)) AS TOTAL_SPEND,
                   SYSDATE AS AGGREGATE_DATE
              FROM MT_RPT01.T1_RPT_FTD_PERF_STRATGY_SUM_MV
          ---WHERE  CAMPAIGN_ID='100512'
             GROUP BY CAMPAIGN_ID,STRATEGY_ID,AGG_START_DATE_GMT) A
  GROUP BY A.CAMPAIGN_ID,A.STRATEGY_ID,A.AGG_START_DATE_GMT;

CREATE OR REPLACE SYNONYM MM_API_DO.T1_RPT_FTD_PERF_STRATEGY_MV FOR MT_RPT01.T1_RPT_FTD_PERF_STRATEGY_MV;
CREATE OR REPLACE PUBLIC SYNONYM T1_RPT_FTD_PERF_STRATEGY_MV FOR MT_RPT01.T1_RPT_FTD_PERF_STRATEGY_MV;
GRANT SELECT ON MT_RPT01.T1_RPT_FTD_PERF_STRATEGY_MV TO MM_API_DO;