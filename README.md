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
- [ ] a few gaps in unit tests, but they're covered by a basic integration test + the redis-tester so meh

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

[DONE] fix timing issue with expiry. I think it might have to do with assumptions that the redis-tester makes about how long servers take to parse and record the reqeusts. Specifically, the redis-tester assumes that the timestamp of a SET request with an expiry is the instant they receive an +OK reply.

## Replication
- Started working on replication challenge
- I'm doing the CLI parsing completely manually. As long as I don't get too many complex args, this should be ok-ish.
- Realized that one of my optimizations to avoid allocating when creating a Message out of a Request is giving me trouble. So my workaround is to give each message an optional allocator field, that can be used in cases where allocation is needed and is ignored when not. This makes the deinit() know when to free (we have an allocator) and when not to. This might be clever, but it's introducing a bunch of complexity and the codebase will be hard to understand and reason about.
- Ok I'm having a ton of fun with printing things out based on their type generically (for the INFO command), but it's probably too much of a distraction.
- The codebase is starting to feel unwieldy and some refactoring is in order.
- It turns out I didn't do a good job of setting up the data modeling for the server state.
- I put all the server state behind one struct. Now I need to move the mutex locking from being just in the hashmap to the rest of the server state (because that can also change).
- I think I'm getting the hang of idiomatic Zig. It's a bit different but I think it ends up being readable because it's predictable.
- D'oh! I think I made a mistake moving all the server state behind one mutex lock. I should probably have the Cache have its own lock so it's not slowed down by random unrelated requests from replicas (?).