FROM golang:1.25-alpine AS builder

WORKDIR /usr/src/go

COPY go.mod ./
RUN go mod download

COPY cmd/proxy ./cmd/proxy
RUN go build -v -o /usr/local/bin/proxy ./cmd/proxy

FROM alpine:3.22.2 AS runner

COPY --from=builder /usr/local/bin/proxy /usr/local/bin/proxy

EXPOSE 8081

ENTRYPOINT ["proxy"]

CMD ["8081"]
