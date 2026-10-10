# frozen_string_literal: true

require_relative '../test_helper'

class ChannelCoreTest < Minitest::Test
  def tick
    sleep 0.02
  end

  def test_post_other_and_stream_other_route_dynamic_paths
    channel = ChannelRB::Channel.new
    channel.post_other(proc { |path| path.start_with?('dynamic/') }, proc { |body| "#{body[:path]}:#{body[:body]}" })
    channel.stream_other(proc { |path| path.end_with?('/stream') }, proc { |body|
      Enumerator.new do |yielder|
        yielder << body[:body]
      end
    })

    sender = fake_sender
    responses = []
    controller = fake_controller(sender, responses)

    channel.receive_one({ id: 1, path: 'dynamic/echo', body: 'ok' }, controller)
    channel.receive_one({ id: 2, stream: 'dynamic/stream', body: 7 }, controller)
    sleep 0.05

    assert_includes(responses, { id: 1, body: 'dynamic/echo:ok' })
    assert_includes(responses, { id: 2, body: 7 })
    assert_includes(responses, { id: 2, done: true })
  end

  def test_merge_keeps_post_other_and_stream_other_under_prefix
    match = proc { |path| path.start_with?('dyn/') }
    inner = ChannelRB::Channel.new
    inner.post_other(match, proc { |body| "post:#{body[:body]}" })
    inner.stream_other(proc { |path| path == 'feed' }, proc { [1].each })
    outer = ChannelRB::Channel.new.merge(inner, 'm')

    assert_equal(1, outer.other_post_api.length)
    assert(outer.other_post_api[0][:path].call('m/dyn/x'))
    refute(outer.other_post_api[0][:path].call('dyn/x'))
    assert(outer.other_stream_api[0][:path].call('m/feed'))

    responses = []
    controller = fake_controller(fake_sender, responses)
    outer.receive_one({ id: 1, path: 'm/dyn/x', body: 'a' }, controller)
    outer.receive_one({ id: 2, path: 'dyn/x', body: 'a' }, controller)
    sleep 0.05

    assert_includes(responses, { id: 1, body: 'post:a' })
    refute_includes(responses, { id: 2, body: 'post:a' })

    # Without a prefix the matchers are reused as-is
    flat = ChannelRB::Channel.new.merge(inner)
    assert_same(match, flat.other_post_api[0][:path])
  end

  def test_disconnect_runs_cleanup_callbacks_and_clears_pending_maps
    disconnected = 0
    channel = ChannelRB::Channel.new.on_disconnect(proc {
      disconnected += 1
      raise 'ignored'
    })

    sender = fake_sender

    stream_cancelled = 0
    sender.stream_set(1, Thread.new { sleep 10 })
    sender.request_set(5, ChannelRB::CancellableRequest.new(proc {
      stream_cancelled += 1
    }))

    channel.make_request('pending', nil, nil, proc {}, sender)
    channel.disconnect({}, sender)

    assert_equal(1, disconnected)
    assert_equal(1, stream_cancelled)
    assert_equal(0, sender.streams.size)
    assert_equal(0, sender.requests.size)
    assert_equal(0, channel.requests.size)
  ensure
    sender.stream_each { |_id, stream| stream.kill if stream.alive? }
  end

  def test_cancel_request_removes_pending_entry
    channel = ChannelRB::Channel.new
    request = channel.make_request('hello', nil, nil, proc {})
    assert_equal(1, channel.requests.size)
    channel.cancel(request[:id])
    assert_equal(0, channel.requests.size)
  end

  def test_cancel_message_invokes_task_callbacks
    calls = []
    channel = ChannelRB::Channel.new.post('wait', proc { |body|
      body[:task].on_cancel { calls << 1 }
      body[:task].on_cancel { calls << 2 }
      Thread.new { sleep 10 }
    })

    sender = fake_sender
    responses = []
    controller = fake_controller(sender, responses)

    channel.receive_one({ id: 5, path: 'wait' }, controller)
    tick
    channel.receive_one({ cancel: 5 }, controller)
    tick

    assert_equal([1, 2], calls)
    assert_includes(responses, { id: 5, error: 'cancelled' })
    assert_nil(sender.request_get(5))
  ensure
    sender.stream_each { |_id, stream| stream.kill if stream.alive? }
  end

  def test_async_cancel_with_string_key_frame
    calls = []
    channel = ChannelRB::Channel.new.post('wait', proc { |body|
      body[:task].on_cancel { calls << 1 }
      Thread.new { sleep 10 }
    })

    sender = fake_sender
    responses = []
    controller = fake_controller(sender, responses)

    channel.receive_one({ 'id' => 9, 'path' => 'wait' }, controller)
    tick
    channel.receive_one({ 'cancel' => 9 }, controller)
    tick

    assert_equal([1], calls)
    assert_includes(responses, { id: 9, error: 'cancelled' })
    assert_nil(sender.request_get(9))
  ensure
    sender.stream_each { |_id, stream| stream.kill if stream.alive? }
  end

  def test_subscription_body_sets_body_formatter_not_topic
    # TS 6d42247: body() sets the body formatter; topic() keeps formatting the topic.
    subscription = ChannelRB::Subscription.new
    events = []
    subscription.publishers << ChannelRB::Publisher.new { |event| events << event }

    subscription.topic { |body| "topic-#{body}" }
    subscription.body { |body| "body-#{body}" }
    subscription.send_event('x', 'payload')
    subscription.send_event('y')

    assert_equal([{ topic: 'topic-x', body: 'payload' }, { topic: 'topic-y', body: 'body-y' }], events)
  end

  def test_subscription_publish_drops_only_js_falsy_bodies
    # TS: `if (!body) return` drops undefined/null/false/0/NaN/""; keeps [], {}, "0".
    subscription = ChannelRB::Subscription.new
    bodies = []
    subscription.publishers << ChannelRB::Publisher.new { |event| bodies << event[:body] }

    [nil, false, 0, 0.0, Float::NAN, '', [], {}, '0', true, 1, 'a'].each do |body|
      subscription.publish('t', body)
    end

    assert_equal([[], {}, '0', true, 1, 'a'], bodies)
  end

  def test_requests_on_the_wire_do_not_carry_the_sender
    channel = ChannelRB::Channel.new
    request = channel.make_request('p', 1, nil, proc {}, Object.new)
    refute(request.key?(:sender))
    refute_includes(ChannelRB::JSONCodec.dump(request), 'sender')
  end

  def test_disconnect_fails_pending_requests_of_that_sender
    channel = ChannelRB::Channel.new
    sender = Object.new
    def sender.stream_each; end
    def sender.request_each; end
    def sender.stream_clear; end
    def sender.request_clear; end
    responses = []
    channel.make_request('p', nil, nil, proc { |response| responses << response }, sender)
    channel.make_request('q', nil, nil, proc { |response| responses << response }, Object.new)

    channel.disconnect({}, sender)

    assert_equal([{ id: 0, error: 'disconnected' }], responses)
  end

  def test_process_stop_clears_pending_requests
    channel = ChannelRB::Channel.new.post('wait', proc { |body|
      done = Queue.new
      body[:task].on_cancel { done << true }
      Thread.new do
        done.pop
        nil
      end
    })

    sender = channel.process
    5.times { sender.request('wait') }
    tick

    assert_operator(sender.requests.size, :>, 0)
    assert_operator(channel.requests.size, :>, 0)

    sender.stop
    tick

    assert_equal(0, sender.requests.size)
    assert_equal(0, channel.requests.size)
  end

  def test_receive_one_supports_string_key_protocol_frames
    channel = ChannelRB::Channel.new
    channel.post('echo', proc { |body| body[:body] })
    channel.stream('ones', proc {
      Enumerator.new do |yielder|
        yielder << 1
      end
    })

    subscription = ChannelRB::Subscription.new
    subscription.topic { |body| body.to_s.upcase }
    subscription.set_body { |body| "sub-#{body}" }
    channel.events('news' => subscription)

    sender = fake_sender
    responses = []
    events = []
    unsubscribed = []
    controller = {
      response: proc { |response| responses << response },
      subscribe: proc { |_topic| },
      unsubscribe: proc { |topic| unsubscribed << topic },
      event: proc { |topic, body| events << [topic, body] },
      sender: sender,
      state: {}
    }

    channel.receive_one({ 'id' => 1, 'path' => 'echo', 'body' => 'ok' }, controller)
    channel.receive_one({ 'id' => 2, 'stream' => 'ones' }, controller)
    channel.receive_one({ 'id' => 3, 'sub' => 'news', 'body' => 'a' }, controller)
    channel.receive_one({ 'unsub' => 'news/A' }, controller)
    channel.receive_one({ 'topic' => 'news/A', 'body' => 'event' }, controller)

    callback_frames = []
    pending = channel.make_request('pending', nil, nil, proc { |frame| callback_frames << frame })
    channel.receive_one({ 'id' => pending[:id], 'body' => 'done' }, controller)

    sleep 0.05

    assert_includes(responses, { id: 1, body: 'ok' })
    assert_includes(responses, { id: 2, body: 1 })
    assert_includes(responses, { id: 2, done: true })
    assert_includes(responses, { id: 3, topic: 'news/A', body: 'sub-a' })
    assert_equal(['news/A'], unsubscribed)
    assert_equal([['news/A', 'event']], events)
    assert_equal([{ 'id' => pending[:id], 'body' => 'done' }], callback_frames)
  end

  def test_values_return_clears_pending_stream_even_without_transport_cancel_support
    next_id = 100
    sent_bodies = []
    cancelled_ids = []

    channel = ChannelRB::Channel.new
    sender = ChannelRB::SenderFactory.make_sender(channel, Class.new {
      define_method(:initialize) do |next_id_ref, sent_ref, cancel_ref|
        @next_id_ref = next_id_ref
        @sent_ref = sent_ref
        @cancel_ref = cancel_ref
      end
      define_method(:send) do |body|
        @sent_ref << body
        id = @next_id_ref[:value]
        @next_id_ref[:value] += 1
        id
      end
      define_method(:sent) { |_id| }
      define_method(:cancel) do |id|
        @cancel_ref << id
        false
      end
      define_method(:notify) { |_body| }
      define_method(:add_topic) { |_topic, &event| proc { true } }
      define_method(:stop) {}
    }.new({ value: next_id }, sent_bodies, cancelled_ids))

    values = sender.values('stream/stuck')
    consumer = Thread.new { values.each { |_value| } }
    tick
    assert_equal(1, channel.requests.size)

    values.return
    tick

    assert_equal([100], cancelled_ids)
    assert(sent_bodies.any? { |body| body.is_a?(Hash) && body.key?(:cancel) })
    assert_equal(0, channel.requests.size)
  ensure
    consumer&.kill
  end

  private

  def fake_sender
    connection = Class.new do
      def send(_body)
        0
      end

      def sent(_id); end

      def cancel(_id)
        false
      end

      def notify(_body); end

      def add_topic(_topic)
        proc { true }
      end

      def stop; end
    end.new

    ChannelRB::SenderFactory.make_sender(ChannelRB::Channel.new, connection)
  end

  def fake_controller(sender, responses)
    {
      response: proc { |response| responses << response },
      subscribe: proc { |_topic| },
      unsubscribe: proc { |_topic| },
      event: proc { |_topic, _body| },
      sender: sender,
      state: {}
    }
  end
end
