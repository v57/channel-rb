# frozen_string_literal: true

require_relative '../test_helper'
require 'socket'

class CoverageBranchesTest < Minitest::Test
  class DummyClient
    attr_accessor :raise_on_write, :raise_on_close
    attr_reader :writes, :closed, :subscribed, :unsubscribed

    def initialize
      @writes = []
      @closed = 0
      @subscribed = []
      @unsubscribed = []
      @raise_on_write = false
      @raise_on_close = false
    end

    def write(payload)
      raise 'write failed' if @raise_on_write

      @writes << payload
    end

    def close
      @closed += 1
      raise 'close failed' if @raise_on_close
    end

    def subscribe(topic)
      @subscribed << topic
    end

    def unsubscribe(topic)
      @unsubscribed << topic
    end
  end

  class DummyChannel
    attr_reader :received, :disconnected

    def initialize
      @received = []
      @disconnected = []
    end

    def receive(payload, controller)
      @received << [payload, controller]
    end

    def disconnect(state, sender)
      @disconnected << [state, sender]
    end
  end

  def setup
    ChannelRB::IodineRuntime.instance_variable_set(:@started, false)
    ChannelRB::IodineRuntime.instance_variable_set(:@thread, nil)
  end

  def test_runtime_stop_noop_when_not_started
    ChannelRB::IodineRuntime.instance_variable_set(:@started, false)
    ChannelRB::IodineRuntime.stop
    refute(ChannelRB::IodineRuntime.running?)
  end

  def test_runtime_handles_start_stop_and_rescue_paths
    started = 0

    Iodine.stub(:'threads=', proc { |_value| }) do
      Iodine.stub(:'workers=', proc { |_value| }) do
        Iodine.stub(:start, proc {
          started += 1
          raise 'start crash'
        }) do
          ChannelRB::IodineRuntime.ensure_started
          sleep 0.02
        end
      end
    end

    assert_equal(1, started)
    assert(ChannelRB::IodineRuntime.running?)

    bad_thread = Object.new
    def bad_thread.join(_seconds)
      raise 'join crash'
    end

    ChannelRB::IodineRuntime.instance_variable_set(:@started, true)
    ChannelRB::IodineRuntime.instance_variable_set(:@thread, bad_thread)

    Iodine.stub(:stop, proc { raise 'stop crash' }) do
      ChannelRB::IodineRuntime.stop
    end

    refute(ChannelRB::IodineRuntime.running?)
  end

  def test_server_handle_stop_is_idempotent
    calls = 0
    handle = ChannelRB::ServerHandle.new(proc { calls += 1 })

    handle.stop
    handle.stop

    assert_equal(1, calls)
  end

  def test_topic_subscriptions_remove_and_dispatch
    topics = ChannelRB::TopicSubscriptions.new
    received = []

    cancel_a = topics.add_topic('alpha') { |event| received << [:a, event] }
    cancel_b = topics.add_topic('alpha') do |event|
      received << [:b, event]
      raise 'ignored'
    end

    topics.received_event('alpha', 'payload')
    assert_equal([[:a, 'payload'], [:b, 'payload']], received)

    assert_equal(false, cancel_a.call)
    assert_equal(true, cancel_b.call)
    assert_equal(false, cancel_b.call)
  end

  def test_connection_registry_add_remove_and_each
    registry = ChannelRB::ConnectionRegistry.new
    a = Object.new
    b = Object.new

    registry.add(a)
    registry.add(b)

    entries = []
    registry.each { |item| entries << item }
    assert_equal([a, b], entries)

    registry.remove(a)
    entries = []
    registry.each { |item| entries << item }
    assert_equal([b], entries)
  end

  def test_server_connection_adapter_paths
    channel = DummyChannel.new
    client = DummyClient.new
    opened = nil
    closed = nil

    adapter = ChannelRB::ServerConnectionAdapter.new(
      channel,
      client,
      { user: 'u' },
      {
        on_connect: proc { |ctx| opened = ctx },
        on_disconnect: proc { |ctx| closed = ctx }
      }
    )

    adapter.on_open
    assert_equal('u', opened[:state][:user])

    adapter.on_message('{"id":1}')
    assert_equal(1, channel.received.length)

    adapter.on_message('{')
    assert_equal(1, client.closed)

    controller = adapter.instance_variable_get(:@controller)
    controller[:subscribe].call('news/topic')
    adapter.publish_event(topic: 'news/topic', body: 'ok')
    assert_equal(1, client.writes.length)

    client.raise_on_write = true
    adapter.publish_event(topic: 'news/topic', body: 'fail')

    controller[:unsubscribe].call('news/topic')
    adapter.publish_event(topic: 'news/topic', body: 'ignored')

    adapter.on_close
    assert_equal(1, channel.disconnected.length)
    assert_equal('u', closed[:state][:user])
  end

  def test_server_connection_transport_methods
    client = DummyClient.new
    subscriptions = ChannelRB::TopicSubscriptions.new
    connection = ChannelRB::ServerConnectionAdapter::Connection.new(client, subscriptions)

    id = connection.send(path: 'hello')
    assert_equal(0, id)
    assert_equal(1, client.writes.length)

    assert_equal(false, connection.cancel(10))
    connection.sent(10)

    connection.notify(topic: 'a')
    assert_equal(2, client.writes.length)

    sink = []
    cancel = connection.add_topic('x') { |body| sink << body }
    subscriptions.received_event('x', 1)
    assert_equal([1], sink)
    assert_equal(true, cancel.call)

    connection.stop
    assert_equal(1, client.closed)
  end

  def test_server_protocol_extract_headers_and_lifecycle
    registry = ChannelRB::ConnectionRegistry.new
    channel = DummyChannel.new
    env = {
      'HTTP_X_TOKEN' => 'abc',
      'HTTP_ACCEPT_LANGUAGE' => 'en',
      'QUERY_STRING' => 'name=tester&role=admin'
    }

    protocol = ChannelRB::ServerProtocol.new(
      channel,
      { state: proc { |headers| headers } },
      env,
      registry
    )

    headers = protocol.send(:extract_headers, env)
    assert_equal('abc', headers['X-Token'])
    assert_equal('en', headers['Accept-Language'])
    assert_equal('tester', headers['name'])
    assert_equal('admin', headers['role'])

    client = DummyClient.new
    protocol.on_open(client)
    protocol.on_message(client, '{"id":2}')
    protocol.on_close(client)

    assert_equal(1, channel.received.length)
    assert_equal(1, channel.disconnected.length)
  end

  def test_channel_listen_builds_upgrade_handler_and_cleanup
    channel = ChannelRB::Channel.new
    subscription = ChannelRB::Subscription.new
    channel.events('updates' => subscription)

    captured = {}

    Iodine.stub(:listen, proc { |**kwargs| captured[:listen] = kwargs }) do
      ChannelRB::IodineRuntime.stub(:ensure_started, proc { captured[:started] = true }) do
        ChannelRB::IodineRuntime.stub(:stop, proc { captured[:stopped] = true }) do
          handle = channel.listen(1234)

          assert_equal('127.0.0.1', captured[:listen][:address])
          assert_equal(1234, captured[:listen][:port])
          assert(captured[:started])
          assert_equal(1, subscription.publishers.length)

          app = captured[:listen][:handler]
          ws_env = { 'rack.upgrade?' => :websocket }
          ws_response = app.call(ws_env)
          assert_equal([0, {}, []], ws_response)
          assert_kind_of(ChannelRB::ServerProtocol, ws_env['rack.upgrade'])

          http_response = app.call({ 'rack.upgrade?' => nil })
          assert_equal(404, http_response[0])

          handle.stop
          assert(captured[:stopped])
          assert_equal(0, subscription.publishers.length)
        end
      end
    end
  end

  def test_channel_private_parse_address_branches
    channel = ChannelRB::Channel.new

    assert_equal(['127.0.0.1', 777], channel.__send__(:parse_address, 777))
    assert_equal(['0.0.0.0', 5050], channel.__send__(:parse_address, '0.0.0.0:5050'))
    assert_equal(['127.0.0.1', 0], channel.__send__(:parse_address, ':'))
  end

  def test_channel_disconnect_uses_return_when_stream_has_no_kill
    channel = ChannelRB::Channel.new
    sender = make_sender(channel)

    stream = Object.new
    def stream.return
      @returned = true
    end

    def stream.returned?
      !!@returned
    end

    sender.stream_set(1, stream)
    channel.disconnect({}, sender)

    assert(stream.returned?)
  end

  def test_channel_events_api_receive_array_and_async_helpers
    channel = ChannelRB::Channel.new
    assert_raises(ArgumentError) { channel.events(Object.new) }

    channel.api('x' => { 'y' => proc { 1 }, 'skip' => 123 }, '_' => proc { 2 })
    channel.api({ '_' => proc { 2 } }, 'root')
    assert(channel.post_api.key?('x/y'))
    assert(channel.post_api.key?(''))
    assert(channel.post_api.key?('root'))

    sender = make_sender(channel)
    responses = []
    controller = {
      response: proc { |response| responses << response },
      subscribe: proc { |_topic| },
      unsubscribe: proc { |_topic| },
      event: proc { |_topic, _body| },
      sender: sender,
      state: {}
    }

    channel.receive([{ id: 1, path: 'x/y' }, { id: 2, path: 'root' }], controller)
    sleep 0.05
    assert_includes(responses, { id: 1, body: 1 })
    assert_includes(responses, { id: 2, body: 2 })

    future = Object.new
    def future.wait
      @waited = true
    end

    def future.value
      42
    end

    assert_equal(42, channel.__send__(:async_result, future))
    assert_equal('plain', channel.__send__(:async_result, 'plain'))
  end

  def test_channel_stream_and_subscription_error_paths
    channel = ChannelRB::Channel.new
    sender = make_sender(channel)
    responses = []
    subscriptions = []
    controller = {
      response: proc { |response| responses << response },
      subscribe: proc { |topic| subscriptions << topic },
      unsubscribe: proc { |_topic| },
      event: proc { |_topic, _body| },
      sender: sender,
      state: {}
    }

    channel.stream('explode', proc {
      Enumerator.new do |_yielder|
        raise 'stream boom'
      end
    })
    channel.receive_one({ id: 10, stream: 'explode' }, controller)

    empty_topic = ChannelRB::Subscription.new
    empty_topic.topic { |_body| '' }
    empty_topic.set_body { |_body| 'ok' }

    bad_subscription = ChannelRB::Subscription.new
    bad_subscription.topic { |_body| 'x' }
    bad_subscription.set_body { |_body| raise 'sub boom' }

    channel.events('empty' => empty_topic, 'bad' => bad_subscription)
    channel.receive_one({ id: 20, sub: 'empty', body: 1 }, controller)
    channel.receive_one({ id: 21, sub: 'bad', body: 2 }, controller)

    sleep 0.05

    assert_includes(subscriptions, 'empty')
    assert_includes(responses, { id: 20, topic: 'empty', body: 'ok' })
    assert(responses.any? { |response| response[:id] == 21 && response[:error].include?('sub boom') })
    assert(responses.any? { |response| response[:id] == 10 && response[:error].include?('stream boom') })
  end

  def test_client_sender_accessors
    fake_sender = Struct.new(:streams, :requests) do
      def send(*); end
      def request(*); end
      def values(*); end
      def subscribe(*); end
      def stop; end
    end.new({ stream: 1 }, { request: 2 })

    sender = ChannelRB::ClientSender.new(fake_sender, Object.new)
    assert_equal({ stream: 1 }, sender.streams)
    assert_equal({ request: 2 }, sender.requests)
  end

  def test_websocket_client_private_branches
    ws = build_ws('localhost:9000')

    assert_equal('ws://127.0.0.1:9', ws.__send__(:normalize_address, 9))
    assert_equal('ws://host:1', ws.__send__(:normalize_address, 'host:1'))
    assert_equal('wss://secure', ws.__send__(:normalize_address, 'wss://secure'))

    ws.instance_variable_set(:@address, 'ws://localhost:9000/path')
    ws.instance_variable_set(:@headers_provider, proc { { token: 'abc', empty: '' } })
    decorated = ws.__send__(:decorated_url)
    assert_includes(decorated, 'token=abc')

    ws.instance_variable_set(:@headers_provider, proc { nil })
    assert_equal('ws://localhost:9000/path', ws.__send__(:decorated_url))

    ws.instance_variable_set(:@headers_provider, proc { raise 'bad headers' })
    assert_equal('ws://localhost:9000/path', ws.__send__(:decorated_url))

    bad_close = Object.new
    def bad_close.close
      raise 'close crash'
    end

    ws.instance_variable_set(:@connection, bad_close)
    ws.instance_variable_set(:@connected, false)
    ws.stop

    ws.instance_variable_set(:@connected, true)
    assert_equal(false, ws.cancel(1))

    ws.instance_variable_set(:@connected, false)
    ws.instance_variable_set(:@pending, { 7 => { a: 1 } })
    assert_equal(true, ws.cancel(7))

    ws.notify(a: 1)
    assert_equal(1, ws.instance_variable_get(:@queue).length)

    bad_write = Object.new
    def bad_write.write(_payload)
      raise 'write crash'
    end

    ws.__send__(:write_payload, bad_write, { b: 2 })
    assert_operator(ws.instance_variable_get(:@queue).length, :>=, 2)

    ws.instance_variable_set(:@connection, bad_write)
    ws.__send__(:flush_queue)
    assert_equal(0, ws.instance_variable_get(:@queue).length)

    parsed = []
    ws.on_message = proc { |message| parsed << message }
    ws.handle_message('{')
    ws.handle_message('{"ok":1}')
    assert_equal([{ 'ok' => 1 }], parsed)

    closed = 0
    ws.on_close = proc { closed += 1 }
    ws.instance_variable_set(:@running, false)
    ws.instance_variable_set(:@connected, true)
    ws.instance_variable_set(:@connection, Object.new)
    ws.handle_close
    assert_equal(1, closed)

    ws2 = build_ws('ws://localhost:9')
    ws2.instance_variable_set(:@running, false)
    connected_calls = 0
    ChannelRB::IodineRuntime.stub(:ensure_started, proc { connected_calls += 1 }) do
      Iodine.stub(:connect, proc { connected_calls += 1 }) do
        ws2.start
      end
    end
    assert_equal(0, connected_calls)

    ws3 = build_ws('ws://localhost:9')
    ws3.instance_variable_set(:@starting, true)
    ChannelRB::IodineRuntime.stub(:ensure_started, proc { connected_calls += 1 }) do
      Iodine.stub(:connect, proc { connected_calls += 1 }) do
        ws3.start
      end
    end
    assert_equal(0, connected_calls)

    ws4 = build_ws('ws://localhost:9')
    calls = []
    ChannelRB::IodineRuntime.stub(:ensure_started, proc { calls << :ensure }) do
      Iodine.stub(:connect, proc { |**kwargs| calls << kwargs[:url] }) do
        ws4.start
      end
    end
    assert_includes(calls, :ensure)
    assert_includes(calls, 'ws://localhost:9')
  end

  private

  def make_sender(channel)
    connection = Object.new
    def connection.send(_body)
      0
    end

    def connection.sent(_id); end

    def connection.cancel(_id)
      false
    end

    def connection.notify(_body); end

    def connection.add_topic(_topic)
      proc { true }
    end

    def connection.stop; end

    ChannelRB::SenderFactory.make_sender(channel, connection)
  end

  def build_ws(address)
    ws = ChannelRB::WebSocketClientConnection.allocate
    ws.instance_variable_set(:@address, address)
    ws.instance_variable_set(:@headers_provider, nil)
    ws.instance_variable_set(:@id, 0)
    ws.instance_variable_set(:@pending, {})
    ws.instance_variable_set(:@pending_mutex, Mutex.new)
    ws.instance_variable_set(:@subscribed, {})
    ws.instance_variable_set(:@subscribed_mutex, Mutex.new)
    ws.instance_variable_set(:@topic_id, 0)
    ws.instance_variable_set(:@connection, nil)
    ws.instance_variable_set(:@connected, false)
    ws.instance_variable_set(:@running, true)
    ws.instance_variable_set(:@starting, false)
    ws.instance_variable_set(:@state_mutex, Mutex.new)
    ws.instance_variable_set(:@queue, [])
    ws.instance_variable_set(:@queue_mutex, Mutex.new)
    ws.instance_variable_set(:@on_open, nil)
    ws.instance_variable_set(:@on_close, nil)
    ws.instance_variable_set(:@on_message, nil)
    ws
  end
end
