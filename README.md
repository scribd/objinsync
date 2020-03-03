ObjInSync
=========

![CI/CD](https://github.com/scribd/objinsync/workflows/CI/CD/badge.svg)

Daemon to continuously synchronize a directory from remote object store to a local directory.

NOTE: it automatically ignores `__pycache__` directory.


Usage
-----

```bash
objinsync pull s3://bucket/objectpath ./localdir
```


Development
------------

Run tests

```bash
make test
```

Run from source

```bash
AWS_REGION=us-east-2 go run main.go pull s3://qph-test-airflow-airflow-code/airflow_home/dags ./dags
```
