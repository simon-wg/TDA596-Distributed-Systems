.PHONY: worker wc
worker: wc
	go run -race mrworker.go wc.so
wc:
	go build -race -buildmode=plugin ../mrapps/wc.go
clean:
	rm -rf mr-tmp
	rm -f mr-*