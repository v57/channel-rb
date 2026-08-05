<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. Ruby implementation of Channel: run a websocket server, a websocket client, or communicate inside your process.

# Installation
```bash
bundle install --path vendor/bundle
```

# Usage

```ruby
# Client
require "channel_rb"

client = ChannelRB::Channel.new.connect(2049)
puts client.send('hello')
client.stop

# Server
events = { 'hello' => ChannelRB::Subscription.new }
server = ChannelRB::Channel.new
  .post('hello') { 'world' }
  .events(events)
  .listen(2050)

# In-memory process transport
sender = ChannelRB::Channel.new
  .post('hello') { 'world' }
  .process
puts sender.send('hello')
sender.stop
server.stop
```

# Implementation

- `oj` for JSON encoding/decoding
- `iodine` for WebSocket networking

# Scripts

- `./scripts/test`
  - runs unit tests
  - runs integration tests against TS server on `127.0.0.1:2049`
  - runs integration tests against a Ruby server instance
- `./scripts/benchmark`
  - follows `../channel/benchmark.md` stage protocol
  - runs 4 stages (1, 10, 100, 1000 clients; each targets 1000 req/s/client for 5000ms)
  - supports `BENCH_STAGE_MS` and `BENCH_RPS_PER_CLIENT` env overrides
  - writes results to `benchmark.json`
- `./scripts/coverage`
  - runs all tests with coverage
  - writes LCOV to `coverage/lcov.info`
