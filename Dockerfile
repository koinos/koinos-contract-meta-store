FROM golang:1.18-alpine as builder

ADD . /koinos-contract-meta-store
WORKDIR /koinos-contract-meta-store

RUN apk update && \
    apk add \
        gcc \
        musl-dev \
        linux-headers

RUN go get ./... && \
    go build -ldflags="-X main.Commit=$(git rev-parse HEAD)" -o koinos_contract_meta_store cmd/koinos-contract-meta-store/main.go

FROM alpine:latest
COPY --from=builder /koinos-contract-meta-store/koinos_contract_meta_store /usr/local/bin
ENTRYPOINT [ "/usr/local/bin/koinos_contract_meta_store" ]
