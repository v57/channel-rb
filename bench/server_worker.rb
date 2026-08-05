# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))

require 'channel_rb'
require 'json'

STDOUT.sync = true
STDERR.sync = true

port = begin
  Integer(ENV['BENCH_PORT'])
rescue StandardError
  nil
end

if port.nil? || port <= 0
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_PORT must be a valid positive integer'))
  exit(1)
end

requests = 0
started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
stopped = false
shutdown_requested = false

server = ChannelRB::Channel.new
  .post('text/count', proc { |body|
    requests += 1
    payload = body[:body]
    text = payload.is_a?(Hash) ? (payload['text'] || payload[:text]) : nil
    text.is_a?(String) ? text.length : 0
  })
  .listen(port)

puts(JSON.generate(type: 'ready', pid: Process.pid, port: port))

stop_server = proc do
  next if stopped
  stopped = true

  begin
    server.stop
  rescue StandardError
  end

  elapsed_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  puts(JSON.generate(type: 'result', pid: Process.pid, requests: requests, elapsedSeconds: elapsed_seconds))
  exit(0)
end

Signal.trap('TERM') { shutdown_requested = true }
Signal.trap('INT') { shutdown_requested = true }

loop do
  stop_server.call if shutdown_requested
  sleep 0.05
end
