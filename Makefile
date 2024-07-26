build-redis-tester:
	cd $(shell pwd)/redis-tester
	make build

build:
	zig build

test: build
# TODO uncomment once we have actual unit tests worth running
#	zig build test || true

	CODECRAFTERS_REPOSITORY_DIR=$(shell pwd) \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"jm1\",\"tester_log_prefix\":\"stage-1\",\"title\":\"Stage #1: Bind to a port\"},{\"slug\":\"rg2\",\"tester_log_prefix\":\"stage-2\",\"title\":\"Stage #2: Respond to PING\"},{\"slug\":\"wy1\",\"tester_log_prefix\":\"stage-3\",\"title\":\"Stage #3: Respond to multiple PINGs\"},{\"slug\":\"zu2\",\"tester_log_prefix\":\"stage-4\",\"title\":\"Stage #4: Handle concurrent clients\"},{\"slug\":\"qq0\",\"tester_log_prefix\":\"stage-5\",\"title\":\"Stage #5: Implement the ECHO command\"},{\"slug\":\"la7\",\"tester_log_prefix\":\"stage-6\",\"title\":\"Stage #6: Implement the SET \u0026 GET commands\"},{\"slug\":\"yz1\",\"tester_log_prefix\":\"stage-7\",\"title\":\"Stage #7: Expiry\"}]" \
	$(shell pwd)/../redis-tester/dist/main.out