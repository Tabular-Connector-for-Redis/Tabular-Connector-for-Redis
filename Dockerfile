# Copyright 2023 VMware, Inc.
# SPDX-License-Identifier: BSD-2-Clause

FROM golang:1.19.1

ADD . /go/src/rdb
WORKDIR /go/src/rdb

EXPOSE 8080

RUN go get rdb
RUN go install

ENTRYPOINT ["/go/bin/rdb", "local_docker"]
