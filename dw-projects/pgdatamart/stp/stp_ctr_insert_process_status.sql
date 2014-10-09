CREATE OR REPLACE FUNCTION stp_ctr_insert_process_status(p_loader_id character, p_status_flag character) RETURNS integer AS
$BODY$
DECLARE
        STATUS_VALUE SMALLINT;
	TBL_COUNT SMALLINT;
	V_ERR_MSG varchar(1000);
	V_STATUS_FLAG CHAR(1);
BEGIN
	insert into tbl_ctr_process_status select p_loader_id, 'stp_ctr_insert_process_status', p_status_flag, current_user,'', (now() + interval '1 sec');
        RETURN 0;

EXCEPTION WHEN OTHERS THEN
	V_ERR_MSG:=SQLERRM;
	insert into tbl_ctr_process_status select p_loader_id, 'stp_ctr_insert_process_status', 'F', current_user,V_ERR_MSG, now();

        RETURN -1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE
COST 100;
