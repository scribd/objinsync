test:
	go test -timeout 5s -race ./...

build-img:
	docker build --rm -t objinsync:latest .

run:
	DEBUG=1 AWS_REGION=us-east-2 go run main.go pull s3://airflow_bucket/airflow_home/dags ./dags
