"""
This module provides tests for postgre class

Requires:
* pg.ini containing:
[POSTGRESQL]
PG_USER=
PGPASSWORD=
PG_DATABASE=
PG_PORT=
PG_HOST=

"""

# Standard library import statements
import os
import unittest
import logging

# dw-lib library import statement
from postgre import *
from logger import Logger

class Test(unittest.TestCase):
    """Unit tests for postgre module"""

    def test_sql(self):
        """postgre: Test that sql statements run successfully"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        pg = Postgre("/opt/mm/testing/conf/pg.ini", log)
        pg.initDatabase(os.environ['PG_USER'],os.environ['PGPASSWORD'],os.environ['PG_HOST'],os.environ['PG_DATABASE'],os.environ['PG_PORT'])
        options = "-q -c"

        sql = "\"create table TEMP_CTD_BS_STG  (MM_DATE  DATE, EXCH_ID BIGINT, DEAL_ID BIGINT, MM_ADV_ID INTEGER)\""
        pg.sql(options , sql)
        stdoutdata, stderrdata, returncode = pg.execute()
        self.assertEqual(0, returncode, "Return code does not match from Postgresql statement")

        sql = "\"drop table TEMP_CTD_BS_STG\""
        pg.sql(options, sql)
        stdoutdata, stderrdata, returncode = pg.execute()
        self.assertEqual(0, returncode, "Return code does not match from Postgresql statement")

    def test_sqlError(self):
        """postgre: Test that sql statement returns error code 1"""
        log = Logger(logging.ERROR, "/opt/mm/testing/conf/logging.ini")
        log.addFileHandler(logging.DEBUG)

        pg = Postgre("/opt/mm/testing/conf/pg.ini", log)
        pg.initDatabase(os.environ['PG_USER'],os.environ['PGPASSWORD'],os.environ['PG_HOST'],os.environ['PG_DATABASE'],os.environ['PG_PORT'])
        options = "-q -c"

        sql = "\"select * from TEMP_TABLE_DOESNT_EXIST \""
        pg.sql(options , sql)
        stdoutdata, stderrdata, returncode = pg.execute()
        self.assertEqual(1, returncode, "Return code does not match from Postgresql statement")

    def test_sqlLoad(self):
        """postgre: Test that load using postgre class is working okay"""
        #pg = Postgre("pg.conf")

if __name__ == "__main__":
    unittest.main()