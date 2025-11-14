IMAGE := enzo0001/dvr-upload
VERSION ?= v1.0.3

.PHONY: all build push tag help

all: build

build:
	docker build -t $(IMAGE):$(VERSION) .

push: build
	docker push $(IMAGE):$(VERSION)

tag:
	docker tag $(IMAGE):$(VERSION) $(IMAGE):latest

help:
	@echo "Usage: make [target] VERSION=1.2.3"
