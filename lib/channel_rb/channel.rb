# frozen_string_literal: true

require 'thread'
require_relative 'sender'
require_relative 'subscription'

module ChannelRB
  PendingRequest = Struct.new(:request, :response)

  class Channel
    attr_reader :requests, :post_api, :other_post_api, :stream_api, :other_stream_api, :events_api

    def initialize
      @id = 0
      @requests = {}
      @post_api = {}
      @other_post_api = []
      @stream_api = {}
      @other_stream_api = []
      @on_disconnect = nil
      @events_api = nil

      @id_mutex = Mutex.new
      @requests_mutex = Mutex.new
      @events_mutex = Mutex.new
    end

    def post(path, request = nil, &block)
      @post_api[path.to_s] = request || block
      self
    end

    def post_other(path_matcher, request = nil, &block)
      @other_post_api << { path: path_matcher, request: request || block }
      self
    end

    def stream(path, request = nil, &block)
      @stream_api[path.to_s] = request || block
      self
    end

    def stream_other(path_matcher, request = nil, &block)
      @other_stream_api << { path: path_matcher, request: request || block }
      self
    end

    def on_disconnect(action = nil, &block)
      @on_disconnect = action || block
      self
    end

    def disconnect(state, sender)
      begin
        @on_disconnect.call(state, sender) if @on_disconnect
      rescue StandardError
      end

      sender.stream_each do |_id, stream|
        begin
          if stream.respond_to?(:kill)
            stream.kill
          elsif stream.respond_to?(:return)
            stream.return
          end
        rescue StandardError
        end
      end

      sender.request_each do |_id, request|
        begin
          request.cancel
        rescue StandardError
        end
      end

      fail_requests(sender, 'disconnected')

      sender.stream_clear
      sender.request_clear
    end

    # Settles every pending outgoing request made through `sender` with `error`.
    # (TS silently drops them on disconnect, which strands the caller forever.)
    def fail_requests(sender, error)
      failed = []
      request_each do |id, request|
        failed << [id, request] if request.request[:sender].equal?(sender)
      end
      failed.each do |id, request|
        request_delete(id)
        begin
          request.response.call(id: id, error: error)
        rescue StandardError
        end
      end
      failed.map(&:first)
    end

    def make_request(path, body, context, response, sender = nil)
      id = next_id
      request = { id: id, path: path, body: body, context: context }
      # The sender stays local: it must not be serialized onto the wire.
      pending = PendingRequest.new(request.merge(sender: sender), response)
      request_set(id, pending)
      request
    end

    def make_stream(stream, body, context, response, sender = nil)
      id = next_id
      request = { id: id, stream: stream, body: body, context: context }
      # The sender stays local: it must not be serialized onto the wire.
      pending = PendingRequest.new(request.merge(sender: sender), response)
      request_set(id, pending)
      request
    end

    def make_subscription(sub, body, context, response, sender = nil)
      id = next_id
      request = { id: id, sub: sub, body: body, context: context }
      # The sender stays local: it must not be serialized onto the wire.
      pending = PendingRequest.new(request.merge(sender: sender), response)
      request_set(id, pending)
      request
    end

    def cancel(id)
      request_delete(id)
    end

    def receive(some, controller)
      if some.is_a?(Array)
        some.each { |one| receive_one(one, controller) }
      else
        receive_one(some, controller)
      end
    end

    def receive_one(some, controller)
      message = some
      if present_key?(message, :path)
        receive_path(message, controller)
      elsif present_key?(message, :stream)
        receive_stream(message, controller)
      elsif key?(message, :cancel)
        receive_cancel(message, controller)
      elsif present_key?(message, :sub)
        receive_subscription(message, controller)
      elsif present_key?(message, :unsub)
        controller[:unsubscribe].call(read_key(message, :unsub))
      elsif present_key?(message, :topic)
        receive_topic(message, controller)
      elsif key?(message, :id)
        receive_response(message)
      end
    end

    def events(events, prefix = '')
      @events_mutex.synchronize do
        @events_api ||= {}
        if events.is_a?(Hash)
          Subscription.parse(events, prefix.to_s, @events_api)
        else
          raise ArgumentError, 'events expects a Hash of subscriptions'
        end
      end
      self
    end

    def api(api, prefix = '')
      parse_api(api, prefix)
      self
    end

    def merge(other_channel, prefix = '')
      p = prefix.to_s.empty? ? '' : "#{prefix}/"
      other_events = other_channel.events_api
      events(other_events, prefix) if other_events

      other_channel.post_api.each do |key, value|
        post("#{p}#{key}", value)
      end
      other_channel.stream_api.each do |key, value|
        stream("#{p}#{key}", value)
      end
      scoped = lambda do |match|
        p.empty? ? match : ->(path) { path.start_with?(p) && match.call(path[p.length..]) }
      end
      other_channel.other_post_api.each { |item| post_other(scoped.call(item[:path]), item[:request]) }
      other_channel.other_stream_api.each { |item| stream_other(scoped.call(item[:path]), item[:request]) }
      self
    end

    private

    def next_id
      @id_mutex.synchronize do
        current = @id
        @id += 1
        current
      end
    end

    def request_set(id, pending)
      @requests_mutex.synchronize { @requests[id] = pending }
    end

    def request_get(id)
      @requests_mutex.synchronize { @requests[id] }
    end

    def request_delete(id)
      @requests_mutex.synchronize { @requests.delete(id) }
    end

    def request_each(&block)
      snapshot = @requests_mutex.synchronize { @requests.dup }
      snapshot.each(&block)
    end

    def parse_api(api, prefix)
      api.each do |key, value|
        path = key.to_s == '_' ? prefix : (prefix.to_s.empty? ? key.to_s : "#{prefix}/#{key}")
        if value.respond_to?(:call)
          post(path, value)
        elsif value.is_a?(Hash)
          parse_api(value, path)
        end
      end
    end

    def key?(message, key)
      message.is_a?(Hash) && (message.key?(key) || message.key?(key.to_s))
    end

    def read_key(message, key)
      return unless message.is_a?(Hash)

      return message[key] if message.key?(key)

      message[key.to_s]
    end

    def present_key?(message, key)
      value = read_key(message, key)
      !(value.nil? || value == '')
    end

    def receive_path(message, controller)
      id = read_key(message, :id)
      path = read_key(message, :path).to_s
      api = @post_api[path]
      api ||= @other_post_api.find { |item| item[:path].call(path) }&.dig(:request)

      task = nil
      request_sender = controller[:sender]
      response_sender = controller[:response]
      begin
        raise ChannelError, 'api not found' unless api

        payload = {
          body: read_key(message, :body),
          context: read_key(message, :context) || {},
          sender: request_sender,
          state: controller[:state],
          path: path,
          task: task
        }

        if id.nil?
          api.call(payload)
          return
        end

        task = CancellableRequest.new(proc { response_sender.call(id: id, error: 'cancelled') })
        payload[:task] = task

        if controller[:inline_requests] && read_key(message, :sync)
          value = api.call(payload)
          value = async_result(value) if async_value?(value)
          response_sender.call(id: id, body: value) unless task.cancelled?
          return
        end

        request_sender.request_set(id, task)
        Thread.new do
          begin
            value = api.call(payload)
            value = async_result(value) if async_value?(value)
            response_sender.call(id: id, body: value) unless task.cancelled?
          rescue StandardError => error
            response_sender.call(id: id, error: error.to_s) unless task.cancelled?
          ensure
            request_sender.request_delete(id)
          end
        end
      rescue StandardError => e
        if !id.nil?
          response_sender.call(id: id, error: e.to_s) unless task&.cancelled?
          request_sender.request_delete(id)
        end
      end
    end

    def async_value?(value)
      value.is_a?(Thread) || value.respond_to?(:wait)
    end

    def async_result(value)
      return value.value if value.is_a?(Thread)

      value.wait if value.respond_to?(:wait)
      return value.value if value.respond_to?(:value)

      value
    end

    def receive_stream(message, controller)
      id = read_key(message, :id)
      stream_name = read_key(message, :stream).to_s
      api = @stream_api[stream_name]
      api ||= @other_stream_api.find { |item| item[:path].call(stream_name) }&.dig(:request)

      begin
        raise ChannelError, 'api not found' unless api
        raise ChannelError, 'stream requires id' if id.nil?

        stream_request(id, controller, stream_name, read_key(message, :body), read_key(message, :context) || {}, api)
      rescue StandardError => e
        controller[:response].call(id: id, error: e.to_s) unless id.nil?
      end
    end

    def stream_request(id, controller, path, body, context, stream_handler)
      stream_thread = Thread.new do
        begin
          payload = {
            body: body,
            sender: controller[:sender],
            state: controller[:state],
            path: path,
            context: context
          }

          values = stream_handler.call(payload)
          values.each do |value|
            controller[:response].call(id: id, body: value)
          end
          controller[:response].call(id: id, done: true)
        rescue StandardError => e
          controller[:response].call(id: id, error: e.to_s)
        ensure
          controller[:sender].stream_delete(id)
        end
      end

      controller[:sender].stream_set(id, stream_thread)
    end

    def receive_cancel(message, controller)
      id = read_key(message, :cancel)
      return if id.nil?

      stream = controller[:sender].stream_get(id)
      if stream
        begin
          stream.kill if stream.respond_to?(:kill)
        rescue StandardError
        end
        controller[:sender].stream_delete(id)
      end

      request = controller[:sender].request_get(id)
      if request
        request.cancel
        controller[:sender].request_delete(id)
      end
    end

    def receive_subscription(message, controller)
      id = read_key(message, :id)
      sub = read_key(message, :sub).to_s
      subscription = @events_api && @events_api[sub]

      begin
        raise ChannelError, 'subscription not found' unless subscription

        request_body = read_key(message, :body)
        topic = subscription.topic_for(request_body)
        full_topic = if topic.to_s.empty?
                       subscription.prefix
                     else
                       "#{subscription.prefix}/#{topic}"
                     end

        body = subscription.body_for(request_body)
        body = async_result(body) if async_value?(body)

        controller[:subscribe].call(full_topic)
        controller[:response].call(id: id, topic: full_topic, body: body)
      rescue StandardError => e
        controller[:response].call(id: id, error: e.to_s) unless id.nil?
      end
    end

    def receive_topic(message, controller)
      id = read_key(message, :id)
      unless id.nil?
        request = request_get(id)
        request_delete(id)
        request&.response&.call(message)
      end

      controller[:event].call(read_key(message, :topic), read_key(message, :body)) if key?(message, :body)
    end

    def receive_response(message)
      id = read_key(message, :id)
      request = request_get(id)
      return unless request

      unless request.request.key?(:stream) && !read_key(message, :done)
        request_delete(id)
      end
      request.response.call(message)
    end
  end
end
