ObjInSync
=========

![CI/CD](https://github.com/scribd/objinsync/workflows/CI/CD/badge.svg)

Daemon to continuously synchronize a directory from remote object store to a local directory.

NOTE: it automatically ignores `__pycache__` directory.


Usage
-----

```bash
objinsync pull s3://bucket/keyprefix ./localdir
```

When running in daemon mode (without `--once` flag), a healthcheck endpoint is
served at `:8087/health` and a prometheus metrics endponit is served at
`:8087/metrics`. You can use `--status-addr` to override the binding address.

Objinsync also comes with builtin Sentry integration. To enable it, set the
`SENTRY_DSN` environment variable.


Installation
------------

Simply download the prebuilt binary from [release page](https://github.com/scribd/objinsync/releases) or use `go get` command:

```bash
go get github.com/scribd/objinsync
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

To cut a release, push tag to remote with format of `vx.x.x`.
