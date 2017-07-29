FROM golang:1.7
RUN mkdir -p /go/src/ncbi-tool-sync
WORKDIR /go/src/ncbi-tool-sync
ADD . /go/src/ncbi-tool-sync
ENV RDS_HOSTNAME=czbiohub-ncbi.cpmmq0ugoybf.us-west-2.rds.amazonaws.com
ENV RDS_PORT=3306
ENV RDS_DB_NAME=ebdb
ENV RDS_USERNAME=czbiohub
ENV RDS_PASSWORD=qMLkJR83nyE
ENV AWS_REGION=us-west-2
ENV BUCKET=czbiohub-ncbi-store
RUN apt-get update
RUN apt-get install golang-go --yes
RUN apt-get install automake autotools-dev g++ git libcurl4-gnutls-dev libfuse-dev libssl-dev libxml2-dev make pkg-config rsync --yes
RUN git clone https://github.com/s3fs-fuse/s3fs-fuse.git
RUN cd s3fs-fuse && ./autogen.sh && ./configure && make && make install
RUN go get ./...
RUN go build
EXPOSE 80
CMD ["./ncbi-tool-sync"]
