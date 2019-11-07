FROM golang:1.10

WORKDIR $GOPATH/src/github.com/life360_rawdata_collector
COPY . .

ENTRYPOINT ["./life360_rawdata_collector"]

