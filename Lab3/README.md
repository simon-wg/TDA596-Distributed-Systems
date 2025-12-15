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
