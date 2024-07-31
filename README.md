# Codecrafters Redis
This is a WIP Zig solution to the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

# Requirements

- zig 0.12
- if you want to run the `redis-tester` integration tests, you need `go` and you need to grab the `redis-tester` git submodule: `git submodule update --init --recursive`.

# TODOs

## Important
- [x] redo parser in a more general way, it's extremely hacky right now
- [x] figure out allocation for parser


## Nice-to-haves
- [ ] set up the integration tests to use the Zig build system instead of crappy Make.
- [x] proper unit test discovery in build.zig

# Work Log

## Base Challenge

- Finished base challenge (ECHO, GET and SET). Parsing is extremely hacky and needs redone.
- Needs a bit more unit tests, at least for the hash map.
- Figured out how to run the `redis-tester` to run integration tests locally instead of on their remote machines (which doesn't let you choose what tests to run).

## Refactor

Refactored parser to be cleaner:
- break out reading the incoming text into a string,
- converting that string into a Message,
- interpreting the Message as a Request/Command,
- updating the state based on the Request,
- generating a response Message based on the Request,
- converting the Message back to a string,
- and finally sending the string back to the client stream.
- All of the above are in testable functions (most of them have tests at this point).

TODO fix timing issue with expiry. I think it might have to do with assumptions that the redis-tester makes about how long servers take to parse and record the reqeusts. Specifically, the redis-tester assumes that the timestamp of a SET request with an expiry is the instant they receive an +OK reply.

## Replication
TODO start working on replication challenge