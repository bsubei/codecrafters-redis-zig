# Codecrafters Redis
This is a WIP Zig solution to the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

# Requirements

- zig 0.12
- if you want to run the `redis-tester` integration tests, you need `go` and you need to grab the `redis-tester` git submodule: `git submodule update --init --recursive`.

# TODOs

## Important
- [ ] redo parser in a more general way, it's extremely hacky right now
- [x] figure out allocation for parser


## Nice-to-haves
- [ ] set up the integration tests to use the Zig build system instead of crappy Make.

# Work Log

## Base Challenge

- Finished base challenge (ECHO, GET and SET). Parsing is extremely hacky and needs redone.
- Needs a bit more unit tests, at least for the hash map.
- Figured out how to run the `redis-tester` to run integration tests locally instead of on their remote machines (which doesn't let you choose what tests to run).

## Refactor

- TODO start refactoring parser