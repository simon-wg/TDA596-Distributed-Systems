FROM golang:1.25-alpine AS builder

WORKDIR /usr/src/go

COPY go.mod ./
RUN go mod download

COPY cmd/http ./cmd/http
RUN go build -v -o /usr/local/bin/http ./cmd/http

FROM alpine:3.22.2 AS runner

COPY --from=builder /usr/local/bin/http /usr/local/bin/http

EXPOSE 8080

ENTRYPOINT ["http"]

CMD ["8080"]
