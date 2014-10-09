select distinct tbl_name, max_errors from tbl_ap_object_lookup_picard where Refresh_Period_Flag=%s and status='TRUE' and tbl_name not in ('timezone') order by tbl_name
