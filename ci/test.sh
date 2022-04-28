#!/bin/bash

set -e
set -x

if [[ -z $BUILD_DOCKER ]]; then
   go test -v github.com/koinos/koinos-contract-meta-store/internal/metastore -coverprofile=./build/contractmetastore.out -coverpkg=./internal/metastore
   gcov2lcov -infile=./build/contractmetastore.out -outfile=./build/contractmetastore.info

   golangci-lint run ./...
else
   TAG="$TRAVIS_BRANCH"
   if [ "$TAG" = "master" ]; then
      TAG="latest"
   fi

   export CONTRACT_META_STORE_TAG=$TAG

   git clone https://github.com/koinos/koinos-integration-tests.git

   cd koinos-integration-tests
   go get ./...
   ./run.sh
fi
