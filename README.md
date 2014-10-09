dw-reporting
=============

dw-reporting encapsulates the generic and project-specific libraries developed by dw-reporting team.


Generic Libraries
-----------------

dw-reporting hosts a range of generic libraries that be found under dw-lib directory. Some of them include:

- Logging
- Configuration
- Netezza
- PostgreSQL
- AWS S3
- AWS DynamoDB
- Hadoop HDFS


Testing
--------

- All the tests cases are located under tests directory
- Requires database and aws specific configurations in particular directories
- To run the tests, run the following command in the dw-reporting directory:

```python
python runtests.py

```

