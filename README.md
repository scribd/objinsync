ObjInSync
=========

![CI/CD](https://github.com/scribd/objinsync/workflows/CI/CD/badge.svg)

Daemon to continuously and incrementally synchronize a directory from remote
object store to a local directory.


Usage
-----

```bash
objinsync pull --exclude '**/__pycache__/**' s3://bucket/keyprefix ./localdir
```

When running in daemon mode (without `--once` flag), a health check endpoint is
served at `:8087/health` and a prometheus metrics endponit is served at
`:8087/metrics`. You can use `--status-addr` to override the binding address.

Objinsync also comes with builtin Sentry integration. To enable it, set the
`SENTRY_DSN` environment variable.

You can also run objinsync in pull once mode, which behaves just like `aws s3 sync`:

```bash
objinsync pull --once s3://bucket/keyprefix ./localdir
```

To use with [Minio](https://docs.min.io/) instead of S3, you can set
`--s3-endpoint` and `--disable-ssl` flags for `pull` command as you see fit.

The `-i` or `--interval` flags allows to configure the pull time interval, which is 5 seconds by default:

```bash
objinsync pull --interval 20s s3://bucket/keyprefix ./localdir
```

---

Enable debug logs by setting the `DEBUG` environment variable `DEBUG=1 objinsync pull ...`


Installation
------------

Simply download the prebuilt single binary from [release page](https://github.com/scribd/objinsync/releases) or use `go get` command:

```bash
go get github.com/scribd/objinsync
```

An officially maintained public docker image is available at https://hub.docker.com/repository/docker/scribdinc/objinsync.


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

To cut a release, push tag to remote in the format of `vx.x.x`.
