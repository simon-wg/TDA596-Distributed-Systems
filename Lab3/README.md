# How to run

## Native go

1. Start a ring

```console
go run ./cmd/chord -p <port> -a <ip> --ts <stabilization time> --tff <fix fingers time> --tcp <check predecessor time> --r <successors to track>
```

2. Join the ring

```console
go run ./cmd/chord -p <port> -a <ip> --ts <stabilization time> --tff <fix fingers time> --tcp <check predecessor time> --r <successors to track> --ja <some address in ring> --jp <port on that address>
```

## Docker

1. Build

```console
docker build -t chord .
```

2. Start ring

```console
docker run -it chord -p <port> -a <ip> --ts <stabilization time> --tff <fix fingers time> --tcp <check predecessor time> --r <successors to track>
```

3. Join the ring

```console
docker run -it chord -p <port> -a <ip> --ts <stabilization time> --tff <fix fingers time> --tcp <check predecessor time> --r <successors to track> --ja <some address in ring> --jp <port on that address>
```

# For demo

## Start

```console
docker run -it -v $(pwd):/go -w /go --network host ghcr.io/simon-wg/tda596-distributed-systems/chord:main -a 98.92.84.106 -p 8080 --ts 1000 --tff 1000 --tcp 1000 --r 4
```

## Join

```console
docker run -it -v $(pwd):/go -w /go --network host ghcr.io/simon-wg/tda596-distributed-systems/chord:main -a 98.92.84.106 -p 8080 --ts 1000 --tff 1000 --tcp 1000 --r 4 --ja 3.235.185.140 --jp 8080
```
