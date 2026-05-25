UNAME_S := $(shell uname -s)
UNAME_M := $(shell uname -m)

# Race detector works on macOS (any arch) and Linux x86_64
# Disabled on Linux ARM64 due to ThreadSanitizer VMA limitation
ifeq ($(UNAME_S)-$(filter aarch64 arm%,$(UNAME_M)),Linux-$(UNAME_M))
  RACE :=
else
  RACE := -race
endif

.PHONY: default build test integration cover clean

default: build test integration

build:
	go build ./...
	go vet ./...

# Unit tests for the franzadapter module. Pure Go, no Docker.
test:
	go clean -testcache
	go test $(RACE) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# Integration tests live in their own module under ./integration/ so the
# testcontainers/Docker dependency tree doesn't bloat the franzadapter module
# that downstream consumers pull. Requires Docker on the host.
integration:
	cd integration && go test $(RACE) -timeout 15m ./...

cover: test
	go tool cover -html=coverage.out

clean:
	rm -f coverage.out integration/coverage.out
