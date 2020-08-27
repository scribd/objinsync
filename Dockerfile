FROM golang:1.13.1 as build
RUN groupadd -g 1000 1000 && \
    useradd -r -u 1000 -g 1000 1000
ADD . /app
RUN chown 1000 app
WORKDIR /app

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"'

FROM alpine:3.10.1

COPY --from=build /app/objinsync /bin/objinsync
USER 1000
