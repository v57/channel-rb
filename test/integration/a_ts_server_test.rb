# frozen_string_literal: true

require_relative '../test_helper'
require 'socket'

class TsServerIntegrationTest < Minitest::Test
  def setup
    assert(ts_server_running?, 'TypeScript fixture server is not reachable on ws://127.0.0.1:2049')
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

  def test_stream_values
    with_simple_client do |client|
      assert_equal([0, 1, 2], with_timeout { client.values('stream/values').to_a })
    end
  end

  def test_stream_cancel
    with_simple_client do |client|
      values = []
      with_timeout do
        client.values('stream/cancel').each do |value|
          values << value
          break if value == 2
        end
      end
      assert_equal([0, 1, 2], values)
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
      assert_operator(values_sent.call, :>=, 3)
    end
  end

  def test_mirror_events_smoke
    with_mirror_client do |client, names, _values_sent|
      with_timeout { client.send('mirror/events') }
      sleep 0.05
      names.send_event('2', 'po')
      names.send_event('1', 'h')
      sleep 0.05
      names.send_event('1', 'we')
      sleep 0.05
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

  def test_subscribe_not_found
    with_simple_client do |client|
      error = assert_raises(ChannelRB::ChannelError) do
        with_timeout { client.subscribe('notfound', 'test') { |_event| } }
      end
      assert_equal('subscription not found', error.message)
    end
  end

  def test_disconnect_allows_reconnect
    with_simple_client do |client|
      request = client.request('disconnect')
      sleep 0.2
      request.cancel

      value = nil
      eventually(5) do
        value = client.send('hello')
        value == 'world'
      rescue ChannelRB::ChannelError
        false
      end
      assert_equal('world', value)
    end
  end

  private

  def with_timeout(seconds = 8, &block)
    Timeout.timeout(seconds, &block)
  end

  def eventually(seconds)
    started = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    loop do
      return true if yield
      raise Timeout::Error, 'condition not met in time' if Process.clock_gettime(Process::CLOCK_MONOTONIC) - started > seconds

      sleep 0.05
    end
  end

  def ts_server_running?
    Socket.tcp('127.0.0.1', 2049, connect_timeout: 1) do |socket|
      socket.close
      true
    end
  rescue StandardError
    false
  end

  def with_simple_client
    client = ChannelRB::Channel.new.connect(2049)
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

    client = channel.connect(2049)
    wait_for_connection(client)
    yield client, names, proc { values_sent[:count] }
  ensure
    client&.stop
  end

  def wait_for_connection(client, seconds = 3)
    eventually(seconds) do
      client.ws.connected?
    end
  end
end
