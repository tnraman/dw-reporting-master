Postgres Picard Datamart
========================

Postgres database will be used for Picard API. This process will:
- Use MD Loader to pull data from Postgres Adama replica DB. A lookup table (*name goes here*) will be used to determine which tables to extract.
- Load the extracted data to stage tables using Python PG loader.
- Read the staging data, join these tables (defined in sql directory) & prepare final dataset for meta tables.
- Delete and insert the final dataset to table using SQL prepared from stage tables. 


Usage
-----
TBD

```python
python pgpicard.py 

```


Environment
-----------
TBD


Possible issues
---------------
TBD

