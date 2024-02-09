FROM golang:1.20 as build
LABEL org.opencontainers.image.source https://github.com/scribd/objinsync
ADD . /app
WORKDIR /app

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags '-extldflags "-static"'

FROM alpine:3.18.6

RUN addgroup --gid 1000 app && \
    adduser --system --uid 1000 --ingroup app app
USER app

COPY --from=build /app/objinsync /bin/objinsync
