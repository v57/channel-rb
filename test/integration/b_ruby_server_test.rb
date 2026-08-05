# frozen_string_literal: true

require_relative '../test_helper'
require 'socket'

class RubyServerIntegrationTest < Minitest::Test
  @@booted = false
  @@server = nil
  @@events = nil
  @@stats = nil
  @@port = nil

  def setup
    boot_server_once
  end

  def test_post_hello
    with_simple_client do |client|
      assert_equal('world', with_timeout { client.send('hello') })
    end
  end

  def test_post_echo
    with_simple_client do |client|
      value = rand
      response = with_timeout { client.send('echo', value) }
      numeric = response.is_a?(Numeric) ? response.to_f : response.to_s.to_f
      assert_in_delta(value, numeric, 1e-12)
    end
  end

  def test_post_empty
    with_simple_client do |client|
      assert_nil(with_timeout { client.send('empty') })
    end
  end

  def test_post_not_found
    with_simple_client do |client|
      error = assert_raises(ChannelRB::ChannelError) { with_timeout { client.send('notfound') } }
      assert_equal('api not found', error.message)
    end
  end

  def test_events_receive_and_cancel
    with_simple_client do |client|
      events = []
      subscription = with_timeout do
        client.subscribe('hello', 'test') do |event|
          events << event
        end
      end

      @@events['hello'].send_event('test', 'event 1')
      @@events['hello'].send_event('test', 'event 2')
      sleep 0.05
      assert_equal(2, events.length)
      assert(events.all? { |event| event.start_with?('event ') })

      subscription.cancel
      @@events['hello'].send_event('test', 'event 3')
      sleep 0.05
      assert_equal(2, events.length)
    end
  end

  def test_stream_values
    with_simple_client do |client|
      assert_equal([0, 1, 2], with_timeout { client.values('stream/values').to_a })
    end
  end

  def test_stream_cancel_updates_server_stats
    @@stats[:stream_cancel] = 0
    with_simple_client do |client|
      values = []
      with_timeout do
        client.values('stream/cancel').each do |value|
          values << value
          break if value == 2
        end
      end
      sleep 0.05
      assert_equal([0, 1, 2], values)
      assert_equal(3, @@stats[:stream_cancel])
    end
  end

  def test_mirror_post
    with_mirror_client do |client, _names, _values_sent|
      assert_equal('client world', with_timeout { client.send('mirror') })
    end
  end

  def test_mirror_stream
    with_mirror_client do |client, _names, _values_sent|
      assert_equal([0, 1, 2], with_timeout { client.values('mirror/stream').to_a })
    end
  end

  def test_mirror_stream_cancel
    with_mirror_client do |client, _names, values_sent|
      values = []
      with_timeout do
        client.values('mirror/stream/cancel').each do |value|
          values << value
          break if value == 2
        end
      end
      sleep 0.1
      assert_equal([0, 1, 2], values)
      assert_equal(3, values_sent.call)
    end
  end

  def test_mirror_events_updates_server_stats
    @@stats[:mirror_events] = 0
    with_mirror_client do |client, names, _values_sent|
      with_timeout { client.send('mirror/events') }
      sleep 0.05
      names.send_event('2', 'po')
      names.send_event('1', 'h')
      sleep 0.1
      names.send_event('1', 'we')
      sleep 0.1
      assert_equal(1, @@stats[:mirror_events])
    end
  end

  def test_state_auth_and_name
    with_simple_client do |client|
      with_timeout { client.send('auth', { 'name' => 'tester' }) }
      assert_equal('tester', with_timeout { client.send('auth/name') })
    end
  end

  def test_state_unauthorized
    with_simple_client do |client|
      error = assert_raises(ChannelRB::ChannelError) { with_timeout { client.send('auth/name') } }
      assert_equal('unauthorized', error.message)
    end
  end

  private

  def boot_server_once
    return if @@booted

    @@events = {
      'hello' => ChannelRB::Subscription.new
    }

    @@stats = {
      mirror_events: 0,
      stream_cancel: 0
    }

    server_channel = ChannelRB::Channel.new
    server_channel.post('hello', proc { 'world' })
    server_channel.post('mirror', proc { |body| body[:sender].send('hello') })
    server_channel.post('echo', proc { |body| body[:body] })
    server_channel.post('empty', proc { nil })
    server_channel.post('disconnect', proc { |body|
      body[:sender].stop
      nil
    })
    server_channel.post('auth', proc { |body|
      name = body[:body]['name'] || body[:body][:name]
      body[:state][:name] = name
      name
    })
    server_channel.post('auth/name', proc { |body|
      name = body[:state][:name]
      raise ChannelRB::ChannelError, 'unauthorized' unless name

      name
    })
    server_channel.stream('stream/values', proc {
      Enumerator.new do |yielder|
        3.times do |i|
          yielder << i
          sleep 0.005
        end
      end
    })
    server_channel.post('mirror/events', proc { |body|
      subscription = nil
      subscription = body[:sender].subscribe('names', '1') do |_event|
        @@stats[:mirror_events] += 1
        subscription.cancel if subscription
      end
      nil
    })
    server_channel.stream('mirror/stream', proc { |body|
      Enumerator.new do |yielder|
        body[:sender].values('stream/values').each { |value| yielder << value }
      end
    })
    server_channel.stream('stream/cancel', proc {
      Enumerator.new do |yielder|
        10.times do |i|
          yielder << i
          @@stats[:stream_cancel] += 1
          sleep 0.02
        end
      end
    })
    server_channel.stream('mirror/stream/cancel', proc { |body|
      Enumerator.new do |yielder|
        body[:sender].values('stream/cancel').each { |value| yielder << value }
      end
    })
    server_channel.events(@@events)

    @@port = find_free_port
    @@server = server_channel.listen("127.0.0.1:#{@@port}")
    wait_for_port('127.0.0.1', @@port)
    @@booted = true
  end

  def wait_for_port(host, port)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    loop do
      begin
        Socket.tcp(host, port, connect_timeout: 1) { |socket| socket.close }
        return
      rescue StandardError
      end
      raise Timeout::Error, 'ruby server did not start' if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > 5

      sleep 0.05
    end
  end

  def with_timeout(seconds = 8, &block)
    Timeout.timeout(seconds, &block)
  end

  def with_simple_client
    client = ChannelRB::Channel.new.connect("ws://127.0.0.1:#{@@port}")
    wait_for_connection(client)
    yield client
  ensure
    client&.stop
  end

  def with_mirror_client
    values_sent = { count: 0 }
    names = ChannelRB::Subscription.new

    channel = ChannelRB::Channel.new
    channel.post('hello', proc { 'client world' })
    channel.stream('stream/values', proc {
      Enumerator.new do |yielder|
        3.times do |i|
          yielder << i
          sleep 0.005
        end
      end
    })
    channel.stream('stream/cancel', proc {
      Enumerator.new do |yielder|
        10.times do |i|
          yielder << i
          values_sent[:count] += 1
          sleep 0.03
        end
      end
    })
    channel.events('names' => names)

    client = channel.connect("ws://127.0.0.1:#{@@port}")
    wait_for_connection(client)
    yield client, names, proc { values_sent[:count] }
  ensure
    client&.stop
  end

  def wait_for_connection(client, seconds = 3)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    loop do
      return if client.ws.connected?
      raise Timeout::Error, 'client did not connect in time' if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > seconds

      sleep 0.02
    end
  end

  def find_free_port
    server = TCPServer.new('127.0.0.1', 0)
    port = server.addr[1]
    server.close
    port
  end
end

Minitest.after_run do
  begin
    server = RubyServerIntegrationTest.class_variable_get(:@@server)
    server&.stop
  rescue StandardError
  end
end
