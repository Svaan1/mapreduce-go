RACE ?=

CMDS := cmd/coordinator cmd/sequential cmd/worker
PLUGINS := plugins/wc plugins/indexer plugins/mtiming plugins/rtiming plugins/jobcount plugins/early_exit plugins/crash plugins/nocrash

.PHONY: build clean

build: clean
	@set -e; \
	for d in $(CMDS); do (cd $$d && go clean); done; \
	for d in $(CMDS); do (cd $$d && go build $(RACE) main.go); done; \
	for p in $(PLUGINS); do \
		name=$$(basename $$p); \
		src=$$name.go; \
		(cd $$p && go build $(RACE) -buildmode=plugin $$src); \
	done

clean:
	@set -e; \
	for d in $(CMDS); do \
		(cd $$d && go clean); \
		rm -f $$d/$$(basename $$d); \
		rm -f $$d/main; \
	done; \
	for p in $(PLUGINS); do \
		name=$$(basename $$p); \
		rm -f $$p/$$name.so; \
	done
