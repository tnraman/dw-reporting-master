set echo on;
set timing on;
spool /d1/nfs/oracle_backup/JOHN/FTD/FINAL/ROLLUP_TZ_N.log
@/d1/nfs/oracle_backup/JOHN/FTD/FINAL/CAMP_STRART_FTD_SUM_N.sql
@/d1/nfs/oracle_backup/JOHN/FTD/FINAL/FTD_STRATGY_CAMPGN_CUR_MV_N.sql
@/d1/nfs/oracle_backup/JOHN/FTD/FINAL/FTD_STRTYGY_CAMPGN_TD_SO_N.sql
spool off;
