# frozen_string_literal: true

$LOAD_PATH.unshift(File.expand_path('../lib', __dir__))

require 'channel_rb'
require 'json'
require 'thread'

STDOUT.sync = true
STDERR.sync = true

host = ENV['BENCH_HOST'] || '127.0.0.1'
port = begin
  Integer(ENV['BENCH_PORT'])
rescue StandardError
  nil
end
clients_count = begin
  Integer(ENV['BENCH_CLIENTS'])
rescue StandardError
  nil
end
target_rps_per_client = begin
  Float(ENV['BENCH_RPS_PER_CLIENT'])
rescue StandardError
  nil
end
total_requests = begin
  ENV.key?('BENCH_TOTAL_REQUESTS') ? Integer(ENV['BENCH_TOTAL_REQUESTS']) : nil
rescue StandardError
  nil
end
has_total_requests = ENV.key?('BENCH_TOTAL_REQUESTS')
request_mode = (ENV['BENCH_REQUEST_MODE'] || 'burst').strip.downcase
max_in_flight = begin
  Integer(ENV['BENCH_MAX_IN_FLIGHT'] || '4096')
rescue StandardError
  nil
end

if port.nil? || port <= 0
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_PORT must be a valid positive integer'))
  exit(1)
end

if clients_count.nil? || clients_count <= 0
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_CLIENTS must be a valid positive integer'))
  exit(1)
end

if has_total_requests && total_requests.nil?
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer'))
  exit(1)
end

if !total_requests.nil? && total_requests.negative?
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer'))
  exit(1)
end

if total_requests.nil? && (target_rps_per_client.nil? || !target_rps_per_client.finite? || target_rps_per_client <= 0)
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_RPS_PER_CLIENT must be a positive number'))
  exit(1)
end

if !%w[burst stream].include?(request_mode)
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_REQUEST_MODE must be either burst or stream'))
  exit(1)
end

if !total_requests.nil? && (max_in_flight.nil? || max_in_flight <= 0)
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_MAX_IN_FLIGHT must be a valid positive integer'))
  exit(1)
end

fixed_burst_mode = !total_requests.nil? && request_mode == 'burst'

address = "ws://#{host}:#{port}"
channel = ChannelRB::Channel.new
clients = []
connected = Array.new(clients_count, false)
target_rps = (target_rps_per_client || 0) * clients_count

connected_count = 0
next_client_index = 0
requests_sent = 0
responses_received = 0
errors = 0
in_flight = 0
running = false
stopped = false
shutdown_requested = false

started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
state_mutex = Mutex.new
stop_mutex = Mutex.new

requests_queue = Queue.new
response_workers = []
scheduler = nil
text_count_body = { 'text' => 'hello' }.freeze

connected_marker = proc do |index|
  state_mutex.synchronize do
    next if connected[index]

    connected[index] = true
    connected_count += 1
  end
end

send_request = proc do
  sender = nil
  state_mutex.synchronize do
    return unless running

    sender = clients[next_client_index]
    next_client_index = (next_client_index + 1) % clients.length
    requests_sent += 1
    in_flight += 1
  end

  begin
    requests_queue << sender
  rescue StandardError
    state_mutex.synchronize do
      errors += 1
      in_flight -= 1
    end
  end
end

send_request_inline = proc do
  return unless running

  sender = clients[next_client_index]
  next_client_index = (next_client_index + 1) % clients.length
  requests_sent += 1

  begin
    sender.send('text/count', text_count_body)
    responses_received += 1
  rescue StandardError
    errors += 1
  end
end

start_response_workers = proc do
  worker_count = [clients_count, 64].min
  worker_count = 1 if worker_count <= 0
  worker_count.times do
    response_workers << Thread.new do
      loop do
        sender = requests_queue.pop

        begin
          sender.send('text/count', text_count_body)
          state_mutex.synchronize { responses_received += 1 }
        rescue StandardError
          state_mutex.synchronize { errors += 1 }
        ensure
          state_mutex.synchronize { in_flight -= 1 }
        end
      end
    end
  end
end

stop_client = proc do
  should_stop = false
  stop_mutex.synchronize do
    unless stopped
      stopped = true
      should_stop = true
    end
  end
  next unless should_stop

  state_mutex.synchronize { running = false }

  if scheduler&.alive?
    scheduler.kill
    scheduler.join(0.1)
  end

  wait_until = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 2.0
  loop do
    pending = state_mutex.synchronize { in_flight }
    break if pending <= 0
    break if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= wait_until

    sleep 0.005
  end

  clients.each do |client|
    begin
      client.stop
    rescue StandardError
    end
  end

  snapshot = {
    requestsSent: requests_sent,
    responsesReceived: responses_received,
    errors: errors
  }

  elapsed_seconds = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  puts(JSON.generate(type: 'result', pid: Process.pid, elapsedSeconds: elapsed_seconds, **snapshot))
  STDOUT.flush
  exit!(0)
end

Signal.trap('TERM') { shutdown_requested = true }
Signal.trap('INT') { shutdown_requested = true }

clients_count.times do |i|
  sender = channel.connect(address, on_connect: proc { connected_marker.call(i) })
  clients << sender
end

setup_deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + 10.0
loop do
  count = state_mutex.synchronize { connected_count }
  break if count == clients_count

  if Process.clock_gettime(Process::CLOCK_MONOTONIC) >= setup_deadline
    STDERR.puts(JSON.generate(type: 'error', message: "Timed out waiting for clients to connect (#{count}/#{clients_count})"))
    exit(1)
  end
  sleep 0.01
end

state_mutex.synchronize { running = true }
puts(JSON.generate(type: 'ready', pid: Process.pid, clients: clients_count))

if fixed_burst_mode
  loop do
    break if shutdown_requested
    break if requests_sent >= total_requests

    send_request_inline.call
  end
  shutdown_requested = true
else
  start_response_workers.call

  tick_ms = 10
  tick_seconds = tick_ms / 1000.0
  scheduler = Thread.new do
    carry = 0.0
    loop do
      active = state_mutex.synchronize { running }
      break unless active

      if total_requests.nil?
        carry += (target_rps * tick_ms) / 1000.0
        sends = carry.floor
        carry -= sends
        sends.times { send_request.call }
        sleep tick_seconds
        next
      end

      if request_mode == 'stream'
        loop do
          should_send = state_mutex.synchronize do
            running && requests_sent < total_requests && in_flight < max_in_flight
          end
          break unless should_send

          send_request.call
        end
      else
        budget = 2000
        while budget.positive?
          should_send = state_mutex.synchronize do
            running && requests_sent < total_requests
          end
          break unless should_send

          send_request.call
          budget -= 1
        end
      end

      done = state_mutex.synchronize do
        requests_sent >= total_requests && (responses_received + errors) >= total_requests && in_flight <= 0
      end
      shutdown_requested = true if done
      sleep 0.001
    end
  end
end

shutdown_requested = true if !total_requests.nil? && total_requests.zero?

loop do
  stop_client.call if shutdown_requested
  sleep 0.01
end
