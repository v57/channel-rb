# frozen_string_literal: true

require 'thread'

module ChannelRB
  class ChannelError < StandardError; end

  class CancellableRequest
    attr_reader :cancel_actions

    def initialize(on_cancel)
      @on_cancel = on_cancel
      @cancelled = false
      @cancel_actions = []
      @mutex = Mutex.new
    end

    def cancelled?
      @mutex.synchronize { @cancelled }
    end

    def on_cancel(&block)
      return unless block

      @mutex.synchronize do
        @cancel_actions << block
      end
    end

    def cancel
      already_cancelled = false
      actions = nil

      @mutex.synchronize do
        already_cancelled = @cancelled
        unless @cancelled
          @cancelled = true
          actions = @cancel_actions.dup
        end
      end

      return if already_cancelled

      actions.each do |action|
        begin
          action.call
        rescue StandardError
        end
      end

      @on_cancel.call if @on_cancel
    end
  end

  class SendingRequest
    def initialize(response_queue, on_cancel)
      @response_queue = response_queue
      @on_cancel = on_cancel
    end

    def response
      status, value = @response_queue.pop
      raise ChannelError, value.to_s if status == :error

      value
    end

    def cancel
      @on_cancel.call
    end
  end

  class SubscriptionCancellable
    def initialize(&block)
      @cancel = block
    end

    def cancel
      @cancel.call
    end
  end

  class Sender
    attr_reader :streams, :requests

    def initialize(channel, connection)
      @channel = channel
      @connection = connection
      @streams = {}
      @requests = {}
      @streams_mutex = Mutex.new
      @requests_mutex = Mutex.new
    end

    def send(path, body = nil, context = nil)
      if @connection.respond_to?(:inline_requests?) && @connection.inline_requests?
        response_status = :pending
        response_value = nil

        request = @channel.make_request(path, body, context, proc { |response|
          error = read_key(response, :error)
          if error.nil?
            response_status = :ok
            response_value = read_key(response, :body)
          else
            response_status = :error
            response_value = error
          end
        }, self)
        request[:sync] = true

        transport_id = @connection.send(request)
        @connection.sent(transport_id) unless transport_id.nil?

        raise ChannelError, response_value.to_s if response_status == :error

        return response_value
      end

      queue = Queue.new
      transport_id = nil
      request = @channel.make_request(path, body, context, proc { |response|
        error = read_key(response, :error)
        if error.nil?
          queue << [:ok, read_key(response, :body)]
        else
          queue << [:error, error]
        end
        @connection.sent(transport_id) unless transport_id.nil?
      }, self)
      transport_id = @connection.send(request)
      status, value = queue.pop
      raise ChannelError, value.to_s if status == :error

      value
    end

    def request(path, body = nil, context = nil)
      queue = Queue.new
      transport_id = nil
      request_id = nil

      request = @channel.make_request(path, body, context, proc { |response|
        error = read_key(response, :error)
        if error.nil?
          queue << [:ok, read_key(response, :body)]
        else
          queue << [:error, error]
        end
        unless transport_id.nil?
          @connection.sent(transport_id)
          transport_id = nil
        end
      }, self)

      request_id = request[:id]
      transport_id = @connection.send(request)

      SendingRequest.new(queue, proc {
        if transport_id.nil?
          @connection.notify(cancel: request_id) unless request_id.nil?
        else
          cancelled = @connection.cancel(transport_id)
          @connection.notify(cancel: request_id) if !cancelled && !request_id.nil?
        end

        @channel.cancel(request_id) unless request_id.nil?
        transport_id = nil
        request_id = nil
      })
    end

    def values(path, body = nil, context = nil)
      transport_id = nil
      Values.new(
        channel: @channel,
        path: path,
        body: body,
        context: context,
        on_send: proc { |request|
          transport_id = @connection.send(request)
        },
        on_cancel: proc { |request_id|
          if transport_id.nil?
            @connection.send(cancel: request_id)
          else
            unless @connection.cancel(transport_id)
              @connection.send(cancel: request_id)
            end
          end
          @channel.cancel(request_id)
          transport_id = nil
        },
        sender: self
      )
    end

    def subscribe(path, body = nil, context = nil, &event)
      queue = Queue.new

      request = @channel.make_subscription(path, body, context, proc { |response|
        error = read_key(response, :error)
        if error.nil?
          queue << [:ok, response]
        else
          queue << [:error, error]
        end
      }, self)

      @connection.send(request)
      status, value = queue.pop

      raise ChannelError, value.to_s if status == :error

      topic = read_key(value, :topic)
      remove = @connection.add_topic(topic, &event)
      SubscriptionCancellable.new do
        should_notify = remove.call
        @connection.notify(unsub: topic) if should_notify
      end
    end

    def stop
      @connection.stop
    end

    def stream_set(id, stream)
      @streams_mutex.synchronize { @streams[id] = stream }
    end

    def stream_get(id)
      @streams_mutex.synchronize { @streams[id] }
    end

    def stream_delete(id)
      @streams_mutex.synchronize { @streams.delete(id) }
    end

    def stream_each(&block)
      snapshot = @streams_mutex.synchronize { @streams.dup }
      snapshot.each(&block)
    end

    def stream_clear
      @streams_mutex.synchronize { @streams.clear }
    end

    def request_set(id, request)
      @requests_mutex.synchronize { @requests[id] = request }
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

    def request_clear
      @requests_mutex.synchronize { @requests.clear }
    end

    private

    def read_key(hash, key)
      return unless hash.is_a?(Hash)

      return hash[key] if hash.key?(key)

      hash[key.to_s]
    end
  end

  module SenderFactory
    module_function

    def make_sender(channel, connection)
      Sender.new(channel, connection)
    end
  end
end
