DECLARE
        CTR_STATUS_VALUE SMALLINT;
	TBL_COUNT INTEGER;
	V_COMMENT VARCHAR(1000);
	V_ERR_MSG VARCHAR(1000);
BEGIN
	CTR_STATUS_VALUE:= -1;
	SELECT count(*) INTO TBL_COUNT FROM TBL_CTR_PROCESS_STATUS WHERE LOADER_ID = p_loader_id ;

	IF TBL_COUNT < 1 THEN
		RETURN 0;
	END IF;
	
-- S - Success, L - Latency, N - Normal, F - Failure, P - Progress/Pending, M - Maintenance, R - Recovery, Z - SNOOZE
       SELECT case when STATUS_FLAG = 'S' then  0
                    when STATUS_FLAG = 'L' then  0
                    when STATUS_FLAG = 'N' then  0
                    when STATUS_FLAG = 'F' then  1
                    when STATUS_FLAG = 'P' then  1
                    when STATUS_FLAG = 'M' then  1
                    when STATUS_FLAG = 'R' then  2
                    when STATUS_FLAG = 'Z' then  3
                    else -1
                end INTO CTR_STATUS_VALUE
	FROM TBL_CTR_PROCESS_STATUS 
	where date_modified = (select max(date_modified) from tbl_ctr_process_status where status_flag not in ('I') and LOADER_ID = p_loader_id )  
	and LOADER_ID = p_loader_id 
	and status_flag not in  ('I');
	--RAISE NOTICE 'CTR_STATUS_VALUE = %', CTR_STATUS_VALUE;

	RETURN CTR_STATUS_VALUE;

EXCEPTION WHEN OTHERS THEN
	V_ERR_MSG:=SQLERRM;
        INSERT INTO TBL_CTR_PROCESS_STATUS
        SELECT p_loader_id, 'stp_ctr_process_check_status', 'F', current_user, V_ERR_MSG, now();

        RETURN -1;
END;

