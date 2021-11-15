GO=go

SRC = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

VERSION := v1.2.0
DATE=$(shell date '+%Y%m%d%H%M%S')
DESCRIBE=$(shell git describe)
TARGETS := raft-service
REPO=github.com/maodeyi/raft
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

LDFLAGS += -X "$(REPO)/version.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(REPO)/version.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(REPO)/version.Version=$(VERSION)"
LDFLAGS += -X "$(REPO)/version.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

all: build

build: $(TARGETS)

$(TARGETS): $(SRC)
	$(GO) build -ldflags '$(LDFLAGS)' $(TEST_FLAGS) $(REPO)/cmd/$@

clean:
	rm -f $(ALL_TARGETS)

.PHONY: clean all build check image

