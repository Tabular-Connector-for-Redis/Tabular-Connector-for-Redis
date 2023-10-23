build:
	CGO_ENABLED=0 go build -o rdb
test:
	go test db/*.go -v -skip TestIndex
test-search:
	go test db/*.go -v -run TestIndex
package:


.PHONY:
	build test test-search package