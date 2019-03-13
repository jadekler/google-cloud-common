# This makefile builds the cross-language tests for Storage.
# It outputs each test as a separate .json file into the
# testdata directory of this repo.

# Prerequisites:
# - This repo is under GOPATH.
# - protoc is on the path.
# - protoc supports proto3 syntax.
# - The Go protoc plugin has been downloaded and installed.
# - The github.com/google/protobuf repo has been cloned locally (go get github.com/google/protobuf).

PROTOC = protoc

PROTOC_GO_PLUGIN_DIR = $(GOPATH)/bin

# Dependent repos.
PROTOBUF_REPO = $(GOPATH)/src/github.com/google/protobuf

.PHONY: generate-tests gen-protos

generate-tests: gen-protos
	rm testdata/*.textproto | true
	go run main.go

gen-protos:
	mkdir -p genproto
	PATH=$(PATH):$(PROTOC_GO_PLUGIN_DIR) \
		$(PROTOC) --go_out=plugins=grpc:genproto \
		-I proto -I $(PROTOBUF_REPO)/src \
		proto/*.proto

