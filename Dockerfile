FROM golang:1.13.8

WORKDIR /go/src/github.com/gnossen/kvd/
RUN go get github.com/golang/protobuf/protoc-gen-go \
	 google.golang.org/grpc
COPY ./ .
RUN cd server/server && \
	go build

FROM phusion/baseimage:latest
WORKDIR /root/
COPY --from=0 /go/src/github.com/gnossen/kvd/server/server/server .
CMD ["./server"]
