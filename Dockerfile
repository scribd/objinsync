FROM golang:1.13.1 as build
ADD . /app
WORKDIR /app

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"'

FROM alpine:3.10.1

COPY --from=build /app/objinsync /bin/objinsync
