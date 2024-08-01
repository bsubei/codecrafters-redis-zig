build_redis_tester:
	cd $(shell pwd)/redis-tester
	make build

build:
	zig build

redis_test_base: build build_redis_tester
	CODECRAFTERS_REPOSITORY_DIR=$(shell pwd) \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"jm1\",\"tester_log_prefix\":\"stage-1\",\"title\":\"Stage #1: Bind to a port\"},{\"slug\":\"rg2\",\"tester_log_prefix\":\"stage-2\",\"title\":\"Stage #2: Respond to PING\"},{\"slug\":\"wy1\",\"tester_log_prefix\":\"stage-3\",\"title\":\"Stage #3: Respond to multiple PINGs\"},{\"slug\":\"zu2\",\"tester_log_prefix\":\"stage-4\",\"title\":\"Stage #4: Handle concurrent clients\"},{\"slug\":\"qq0\",\"tester_log_prefix\":\"stage-5\",\"title\":\"Stage #5: Implement the ECHO command\"},{\"slug\":\"la7\",\"tester_log_prefix\":\"stage-6\",\"title\":\"Stage #6: Implement the SET \u0026 GET commands\"},{\"slug\":\"yz1\",\"tester_log_prefix\":\"stage-7\",\"title\":\"Stage #7: Expiry\"}]" \
	$(shell pwd)/../redis-tester/dist/main.out

redis_test_replication: build build_redis_tester
	CODECRAFTERS_REPOSITORY_DIR=$(shell pwd) \
	CODECRAFTERS_TEST_CASES_JSON="[{\"slug\":\"bw1\",\"tester_log_prefix\":\"stage-101\",\"title\":\"Stage #101: Replication - Custom Port\"}, {\"slug\":\"ye5\",\"tester_log_prefix\":\"stage-102\",\"title\":\"Stage #102: Replication - Info on Master\"},{\"slug\":\"hc6\",\"tester_log_prefix\":\"stage-103\",\"title\":\"Stage #103: Replication - Info on Replica\"}, {\"slug\":\"xc1\",\"tester_log_prefix\":\"stage-104\",\"title\":\"Stage #104: Replication - Replication ID and Offset\"}, {\"slug\":\"gl7\",\"tester_log_prefix\":\"stage-105\",\"title\":\"Stage #105: Replication - Handshake 1\"},{\"slug\":\"eh4\",\"tester_log_prefix\":\"stage-106\",\"title\":\"Stage #106: Replication - Handshake 2\"},{\"slug\":\"ju6\",\"tester_log_prefix\":\"stage-107\",\"title\":\"Stage #107: Replication - Handshake 3\"},{\"slug\":\"fj0\",\"tester_log_prefix\":\"stage-108\",\"title\":\"Stage #108: Replication - REPLCONF\"},{\"slug\":\"vm3\",\"tester_log_prefix\":\"stage-109\",\"title\":\"Stage #109: Replication - PSYNC\"},{\"slug\":\"cf8\",\"tester_log_prefix\":\"stage-110\",\"title\":\"Stage #110: Replication - PSYNC w RDB file\"},{\"slug\":\"zn8\",\"tester_log_prefix\":\"stage-111\",\"title\":\"Stage #111: Command Propagation\"},{\"slug\":\"hd5\",\"tester_log_prefix\":\"stage-112\",\"title\":\"Stage #112: Command Propagation to multiple Replicas\"},{\"slug\":\"yg4\",\"tester_log_prefix\":\"stage-113\",\"title\":\"Stage #113: Command Processing\"},{\"slug\":\"xv6\",\"tester_log_prefix\":\"stage-114\",\"title\":\"Stage #114: GetAck with 0 offset\"},{\"slug\":\"yd3\",\"tester_log_prefix\":\"stage-115\",\"title\":\"Stage #115: GetAck with non-0 offset\"},{\"slug\":\"my8\",\"tester_log_prefix\":\"stage-116\",\"title\":\"Stage #116: WAIT with 0 replicas\"},{\"slug\":\"tu8\",\"tester_log_prefix\":\"stage-117\",\"title\":\"Stage #117: WAIT with 0 offset\"},{\"slug\":\"na2\",\"tester_log_prefix\":\"stage-118\",\"title\":\"Stage #118: WAIT Command\"}]" \
	$(shell pwd)/../redis-tester/dist/main.out

zig_test: build
	zig build test

test: zig_test redis_test_base redis_test_replication