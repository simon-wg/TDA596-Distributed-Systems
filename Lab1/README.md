# How to run

## Option 1: Using go

1. In one terminal run `go run ./cmd/http 8080` to run the http fileserver.
2. In another terminal `go run ./cmd/proxy 8081` to run the proxy fileserver.

## Option 2: Using docker

0. (optional for same machine communication) Start a local docker network `docker network create my_network`. For each command in the following steps, add `--network=my_network` as a parameter before the name parameter.

1. In one terminal run `docker run --name=http -p 8080:8080 ghcr.io/simon-wg/tda596-distributed-systems/http:main`
2. In another terminal run `docker run --name=proxy -p 8081:8081 ghcr.io/simon-wg/tda596-distributed-systems/proxy:main`

# Usage examples

- Uploading an image (note that the image needs to be in your working directory):

```console
curl -i -X POST --data-binary @myimage.jpg 0:8080/myimage.jpg
```

- Uploading text:

```console
curl -i -X POST -d "Hello world!" 0:8080/hello.txt
```

- Requesting data through the proxy (can not communicate with fileserver when ran through docker unless a network is created and connected to for both):

```console
curl -i -X GET example.com -x 0:8081
```

- Requesting data through the proxy (local fileserver through docker)

```console
curl -i -X GET http:8080:hello.txt -x 0:8081
```
