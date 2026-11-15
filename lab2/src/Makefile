.PHONY: kvsrv1 lock

kvsrv1:
	go build -o main/kvsrv1d main/kvsrv1d.go
	cd kvsrv1 && go test -v -race

lock:
	go build -o main/kvsrv1d main/kvsrv1d.go
	cd kvsrv1/lock && go test -v -race
