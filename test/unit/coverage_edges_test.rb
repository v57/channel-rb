# frozen_string_literal: true

require_relative '../test_helper'

class CoverageEdgesTest < Minitest::Test
  class ProcessChannelStub
    attr_reader :received

    def initialize
      @received = []
    end

    def receive(body, controller)
      @received << [body, controller]
    end

    def disconnect(_state, _sender); end
  end

  class ConnectionStub
    attr_reader :sent_bodies, :sent_ids, :notifications

    def initialize(send_return: 0, cancel_return: false)
      @send_return = send_return
      @cancel_return = cancel_return
      @sent_bodies = []
      @sent_ids = []
      @notifications = []
    end

    def send(body)
      @sent_bodies << body
      @send_return
    end

    def sent(id)
      @sent_ids << id
    end

    def cancel(_id)
      @cancel_return
    end

    def notify(body)
      @notifications << body
    end

    def add_topic(_topic)
      proc { true }
    end

    def stop; end
  end

  class ValuesChannelStub
    attr_reader :stream_callback, :cancelled

    def initialize
      @stream_callback = nil
      @cancelled = []
      @next_id = 0
    end

    def make_stream(_path, _body, _context, response, _sender = nil)
      @stream_callback = response
      id = @next_id
      @next_id += 1
      { id: id }
    end

    def cancel(id)
      @cancelled << id
    end
  end

  def test_process_interface_cancel_notify_and_topics
    channel = ProcessChannelStub.new
    process = ChannelRB::ProcessInterface.new(channel)
    process.controller = { state: {}, sender: Object.new }

    assert_equal(false, process.cancel(12))

    process.notify(path: 'x')
    assert_equal(1, channel.received.length)

    cancel = process.add_topic('topic')
    assert_equal(false, cancel.call)
  end

  def test_subscription_parse_recursion_and_send_event_default_body
    subscription = ChannelRB::Subscription.new
    subscription.topic { |_body| '' }
    subscription.set_body { |body| "computed-#{body}" }

    map = {}
    ChannelRB::Subscription.parse({ outer: { inner: subscription } }, '', map)
    assert_equal(subscription, map['outer/inner'])

    events = []
    subscription.publishers << ChannelRB::Publisher.new { |event| events << event }
    subscription.send_event('request')

    assert_equal(1, events.length)
    assert_equal(subscription.prefix, events[0][:topic])
    assert_equal('computed-request', events[0][:body])
  end

  def test_sending_request_response_paths
    ok_queue = Queue.new
    ok_queue << [:ok, 7]
    ok = ChannelRB::SendingRequest.new(ok_queue, proc {})
    assert_equal(7, ok.response)

    error_queue = Queue.new
    error_queue << [:error, 'boom']
    error_request = ChannelRB::SendingRequest.new(error_queue, proc {})
    error = assert_raises(ChannelRB::ChannelError) { error_request.response }
    assert_equal('boom', error.message)
  end

  def test_sender_request_cancel_when_transport_already_cleared
    channel = ChannelRB::Channel.new
    connection = ConnectionStub.new(send_return: 55, cancel_return: false)
    sender = ChannelRB::SenderFactory.make_sender(channel, connection)

    request = sender.request('hello')
    request_id = channel.requests.keys.first

    channel.receive({ id: request_id, body: 'world' }, {})
    assert_equal('world', request.response)

    request.cancel
    assert_includes(connection.notifications, { cancel: request_id })
  end

  def test_sender_values_cancel_path_without_transport_id
    channel = ChannelRB::Channel.new
    connection = ConnectionStub.new(send_return: nil, cancel_return: false)
    sender = ChannelRB::SenderFactory.make_sender(channel, connection)

    values = sender.values('stream/no-transport')
    values.__send__(:start)
    assert_equal(1, channel.requests.length)

    values.cancel

    assert_equal(0, channel.requests.length)
    assert(connection.sent_bodies.any? { |body| body.is_a?(Hash) && body[:cancel].is_a?(Integer) })
  end

  def test_sender_send_inline_transport_fast_path
    channel = ChannelRB::Channel.new
      .post('ok', proc { 7 })
      .post('boom', proc { raise 'boom' })
    sender = channel.process

    assert_equal(7, sender.send('ok'))

    error = assert_raises(ChannelRB::ChannelError) do
      sender.send('boom')
    end
    assert_equal('boom', error.message)
  ensure
    sender&.stop
  end

  def test_values_each_raises_channel_error_from_stream_response
    channel = ValuesChannelStub.new
    cancelled = []

    values = ChannelRB::Values.new(
      channel: channel,
      path: 'boom',
      body: nil,
      context: nil,
      on_send: proc do |_request|
        channel.stream_callback.call(error: 'stream failed')
      end,
      on_cancel: proc { |rid| cancelled << rid },
      sender: Object.new
    )

    error = assert_raises(ChannelRB::ChannelError) do
      values.each { |_value| }
    end
    assert_equal('stream failed', error.message)
    assert_equal([], cancelled)
  end

  def test_channel_merge_and_receive_edge_branches
    target = ChannelRB::Channel.new
    other = ChannelRB::Channel.new
    event_sub = ChannelRB::Subscription.new
    other.events('evt' => event_sub)
    other.post('p', proc { 'ok' })
    other.stream('s', proc { [1].each })

    target.merge(other, 'prefix')

    assert(target.post_api.key?('prefix/p'))
    assert(target.stream_api.key?('prefix/s'))
    assert(target.events_api.key?('prefixevt'))

    called = 0
    target.post('fire', proc {
      called += 1
      nil
    })

    responses = []
    controller = {
      response: proc { |response| responses << response },
      subscribe: proc { |_topic| },
      unsubscribe: proc { |_topic| },
      event: proc { |_topic, _body| },
      sender: sender_for(target),
      state: {}
    }

    target.receive_one({ path: 'fire' }, controller)
    assert_equal(1, called)
    assert_equal([], responses)

    target.receive_one({ id: 101, stream: 'missing' }, controller)
    assert(responses.any? { |response| response[:id] == 101 && response[:error].to_s.include?('api not found') })
  end

  def test_websocket_connection_throttle_branch
    ws = ChannelRB::WebSocketClientConnection.allocate
    assert_equal(true, ws.throttle)
  end

  private

  def sender_for(channel)
    ChannelRB::SenderFactory.make_sender(channel, ConnectionStub.new)
  end
end
