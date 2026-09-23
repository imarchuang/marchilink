# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY api ./api
COPY runtime ./runtime
COPY cmd ./cmd
RUN go build ./... && go build -o /out/marchilink ./cmd/marchilink

FROM alpine:3.20
WORKDIR /app
COPY --from=build /out/marchilink /usr/local/bin/marchilink
ENTRYPOINT ["marchilink"]
