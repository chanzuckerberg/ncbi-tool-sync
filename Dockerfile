FROM golang:1.7
RUN mkdir -p /go/src/ncbi-tool-sync
WORKDIR /go/src/ncbi-tool-sync
ADD . /go/src/ncbi-tool-sync
RUN apt-get update
RUN apt-get -y install rsync python-pip
RUN go get ./...
RUN go build
RUN pip install awscli
RUN mkdir /syncmount
VOLUME /syncmount
EXPOSE 80
CMD ["./ncbi-tool-sync"]
