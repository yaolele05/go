.PHONY: build mr wc

build:
	bash build.sh

mr:
	cd src/mr && go test -v -race $(RUN)

wc:
	cd src/mr && go test -v -run TestWc
