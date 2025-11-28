# Codecrafters Redis
This is a WIP Zig solution to the ["Build Your Own Redis" Challenge](https://codecrafters.io/challenges/redis).

# Requirements

- zig 0.15.2

# TODOs

## Important
- [x] redo parser in a more general way, it's extremely hacky right now
- [x] figure out allocation for parser
- [ ] a ton of gaps in unit tests, but they're covered by a basic integration test + the codecrafters redis-tester
  - [ ] I should beef up the integration tests so we can catch any memory management problems. The redis-tester is great for testing behavior, but doesn't catch leaks.
  - [ ] eventually improve unit tests, but only where the integration test provides poor coverage (don't spend too much time on this).


## Nice-to-haves
- [x] proper unit test discovery in build.zig

# Notes on Architecture/Design

## Event-loop based
An outline of this Redis server's control flow:
- A master server starts running, and sets up an event-loop. It creates a listening socket, and registers with the `accept` event (handled in `acceptCallback`).
  - From now on, when a client or replica are ready to connect, `acceptCallback` will trigger when the event-loop picks up that event.
  - Multiple connections will be handled in series, because this is all happening on a single thread, but that's ok as long as none of these event handlers block or take too long.
  - The events described below will be picked up and run by the event-loop main thread.
- `accept` event: When `acceptCallback` triggers, we record this new client (modeled as a `Connection`) and register a `recv` event (handled in `recvCallback`). We then rearm our own event, so we continue to accept new incoming connections. End of callback.
- `recv` event: When `recvCallback` triggers:
  - if we read any bytes: parse the bytes, process the command (update state + generate a response), and register a `send` event to respond (handled in `sendCallback`). Make sure we register responses to both the client and any connected replicas if required. Return, disarming our `recv` event.
  - no bytes read or some other error: set up a `close` event on this `Connection` and return, disarming our `recv` event.
- `send` event: When `sendCallback` triggers:
  - if we wrote any bytes: set up a `recv` event again in case the client has more commands. Return, disarming our `send` event.
  - we didn't write any bytes: set up a `close` event on this `Connection` and return, disarming our `send` event.
- TODO at some point, we should support retrying reads and writes (in case of network hiccups, or messages too big to fit in our buffer).
- `close` event: When `closeCallback` triggers, the socket has already closed, so we just have to deinit the `Connection`.

### Open questions
- in the `recv` event, what if we read bytes but there were actually more on the line? Check if that happens, and handle it.
- long-lived connections?
- are the events handled in an arbitrary order?

### Is `recvCallback` doing too much? It could potentially block the event loop.
I currently do arguably a lot of work to process each incoming message when I read it. Specifically:
  1. I take the read bytes, and I convert them to a "Message" type (to distinguish between simple strings, bulk strings, and arrays)
  2. then I interpret the Message as a Command (e.g. SET, GET) and that includes validation
  3. then I process each Command to apply any state updates (e.g. update the data store if it's a SET command) and other things (depends on what features of Redis I implement).
  4. Then, based on what Command I got, I generate a response "Message"
  5. Finally, I convert this response "Message" to a string that I can send back to the client

I should profile this under heavy loads and decide whether it's worth breaking this out into "parsing" (1 and 2), "processing" (3), and "responding" (4 and 5). Or find alternative solutions. It'll really depend on how many Redis features I implement.


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
- I just realized spawning a new thread to handle each connection is a terrible idea for many reasons:
  - because we have to propagate all write commands to all connected replicas (who keep connections alive), we are basically trying to have one thread notify a bunch of other threads.
  - furthermore, all our data structures (the data store/cache, and the replica states) have to be behind mutex locks, and if we have a lot of replicas to propagate to + a lot of incoming client connections (we expect this to be the case for a master server), this'll put massive pressure on the master server and the locking will become a bottleneck.
- I have to refactor the main server logic to be event-loop based. Probably using libxev. Differences with this approach:
  - we can only process incoming client connections in series. But that's fine as long as none of our event callbacks block. We can probably still handle a ton of connections.
  - this also means that the server becomes single-threaded. So we can get rid of the mutex locks! This'll probably end up scaling better than multi-threaded w/ locks as the number of replicas increases. Plus, as long as the implementation is half-decently efficient, we should mostly be I/O-bound, and that means we're not losing too much by foregoing multi-threaded.
- Mostly finished the event-loop refactor. Everything works, except the handshake parts which still need refactoring to be event-based.
  - I had to move `libxev` out of being a git submodule (because the codecrafters tester only runs `zig build` and doesn't let me do things like pull in submodules and check out branches) and into Zig's build system. It turned out to be easy since GitHub provides tarballs for all commits, and Zig supports grabbing a dependency using a tarball.