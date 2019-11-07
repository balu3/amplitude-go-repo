FROM golang:1.10

 

WORKDIR $GOPATH/src/github.com/test_amplitude_go
COPY . .
ENV ACCOUNT_ID=374553884378
ENV QUEUE_NAME=amplitude-dlq.fifo
ENTRYPOINT ["./test_amplitude_go"]
