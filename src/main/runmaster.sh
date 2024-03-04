go build -buildmode=plugin ../mrapps/wc.go
rm -rf mr-out*
rm -rf /Users/au_miner/opt/module/Go/6.824/src/main/mr-tmp
mkdir /Users/au_miner/opt/module/Go/6.824/src/main/mr-tmp
go run mrmaster.go pg-*.txt
