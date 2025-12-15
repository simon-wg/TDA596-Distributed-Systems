FROM golang:1.25-alpine AS builder

WORKDIR /usr/src/go

COPY go.mod ./
RUN go mod download

COPY cmd ./cmd
COPY pkg ./pkg
RUN go build -v -o /usr/local/bin/chord ./cmd/chord

FROM alpine:3.22.2 AS runner

COPY --from=builder /usr/local/bin/chord /usr/local/bin/chord

EXPOSE 8080

ENTRYPOINT ["chord"]