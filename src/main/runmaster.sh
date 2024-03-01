go build -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run mrmaster.go pg-*.txt