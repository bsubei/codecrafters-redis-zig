build:
	zig build

zig_test: build
	zig build test

test: zig_test