FROM golang:1.10

WORKDIR $GOPATH/src/github.com/test_amplitude_go
COPY . .

ENTRYPOINT ["./test_amplitude_go"]

