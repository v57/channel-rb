# frozen_string_literal: true

require 'json'
require 'channel_rb'

has_total_requests = ENV.key?('BENCH_TOTAL_REQUESTS')
total_requests = nil
if has_total_requests
  begin
    total_requests = Integer(ENV['BENCH_TOTAL_REQUESTS'])
  rescue StandardError
    STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer'))
    exit 1
  end
  if total_requests.negative?
    STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_TOTAL_REQUESTS must be a valid non-negative integer'))
    exit 1
  end
end

duration_ms = has_total_requests ? 0 : Integer(ENV['BENCH_DURATION_MS'] || '0')
if !has_total_requests && duration_ms <= 0
  STDERR.puts(JSON.generate(type: 'error', message: 'BENCH_DURATION_MS must be a valid positive integer'))
  exit 1
end

stop_requested = false
Signal.trap('TERM') { stop_requested = true }
Signal.trap('INT') { stop_requested = true }

sender = ChannelRB::Channel.new
  .post('sync') { 1 }
  .post('async') { Thread.new { 1 } }
  .process

puts(JSON.generate(type: 'ready', pid: Process.pid))

run_scenario = proc do |path|
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  completed = 0
  errors = 0
  while !stop_requested
    elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
    break if elapsed >= (duration_ms / 1000.0)

    begin
      sender.send(path)
      completed += 1
    rescue StandardError
      errors += 1
    end
  end
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  [completed, errors, elapsed]
end

run_fixed_scenario = proc do |path, requests|
  started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  completed = 0
  errors = 0
  while !stop_requested && (completed + errors) < requests
    begin
      sender.send(path)
      completed += 1
    rescue StandardError
      errors += 1
    end
  end
  elapsed = Process.clock_gettime(Process::CLOCK_MONOTONIC) - started_at
  [completed, errors, elapsed]
end

if has_total_requests
  completed, errors, elapsed_seconds = run_fixed_scenario.call('sync', total_requests)
  sender.stop
  puts(
    JSON.generate(
      type: 'result',
      pid: Process.pid,
      completed: completed,
      errors: errors,
      elapsedSeconds: elapsed_seconds
    )
  )
  exit 0
end

sync_completed, sync_errors, sync_elapsed = run_scenario.call('sync')
async_completed, async_errors, async_elapsed = run_scenario.call('async')
sender.stop

puts(
  JSON.generate(
    type: 'result',
    pid: Process.pid,
    syncCompleted: sync_completed,
    syncErrors: sync_errors,
    syncElapsedSeconds: sync_elapsed,
    asyncCompleted: async_completed,
    asyncErrors: async_errors,
    asyncElapsedSeconds: async_elapsed,
    asyncFallback: false
  )
)
