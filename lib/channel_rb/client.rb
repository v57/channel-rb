# frozen_string_literal: true

require 'set'
require 'uri'
require 'thread'
require_relative 'runtime'
require_relative 'json'
require_relative 'sender'

module ChannelRB
  class ClientSender
    attr_reader :ws

    def initialize(sender, ws)
      @sender = sender
      @ws = ws
    end

    def send(path, body = nil, context = nil)
      @sender.send(path, body, context)
    end

    def request(path, body = nil, context = nil)
      @sender.request(path, body, context)
    end

    def values(path, body = nil, context = nil)
      @sender.values(path, body, context)
    end

    def subscribe(path, body = nil, context = nil, &event)
      @sender.subscribe(path, body, context, &event)
    end

    def stop
      @sender.stop
    end

    def streams
      @sender.streams
    end

    def requests
      @sender.requests
    end
  end

  class WebSocketClientConnection
    attr_writer :on_open, :on_close, :on_message

    def initialize(address, headers_provider = nil)
      @address = normalize_address(address)
      @headers_provider = headers_provider

      @id = 0
      @pending = {}
      @pending_mutex = Mutex.new

      @subscribed = {}
      @subscribed_mutex = Mutex.new
      @topic_id = 0

      @connection = nil
      @connected = false
      @running = true
      @starting = false
      @state_mutex = Mutex.new

      @queue = []
      @queue_mutex = Mutex.new

      @on_open = nil
      @on_close = nil
      @on_message = nil

      start
    end

    def start
      should_start = false
      @state_mutex.synchronize do
        return if !@running || @starting
        @starting = true
        should_start = true
      end
      return unless should_start

      IodineRuntime.ensure_started
      Iodine.connect(url: decorated_url, handler: ClientConnectionHandler.new(self), ping: 40)
    ensure
      @state_mutex.synchronize { @starting = false }
    end

    def stop
      connection = nil
      @state_mutex.synchronize do
        @running = false
        @connected = false
        connection = @connection
        @connection = nil
      end

      @pending_mutex.synchronize { @pending.clear }

      begin
        connection&.close
      rescue StandardError
      end
    end

    def send(body)
      id = nil
      @pending_mutex.synchronize do
        id = @id
        @id += 1
        @pending[id] = body
      end

      connection = connected_connection
      write_payload(connection, body) unless connection.nil?
      id
    end

    def cancel(id)
      @state_mutex.synchronize do
        return false if @connected
      end

      @pending_mutex.synchronize do
        !!@pending.delete(id)
      end
    end

    def notify(body)
      connection = connected_connection
      if connection
        write_payload(connection, body)
      else
        enqueue_payload(JSONCodec.dump(body))
      end
    end

    def sent(id)
      @pending_mutex.synchronize { @pending.delete(id) }
    end

    def throttle
      # Keep API parity with JS client. Batching is already event-loop buffered by iodine.
      true
    end

    def connected?
      !connected_connection.nil?
    end

    def add_topic(topic, &event)
      handler_id = nil
      @subscribed_mutex.synchronize do
        handler_id = @topic_id
        @topic_id += 1
        map = (@subscribed[topic] ||= {})
        map[handler_id] = event
      end

      proc do
        removed_last = false
        @subscribed_mutex.synchronize do
          map = @subscribed[topic]
          if map
            map.delete(handler_id)
            if map.empty?
              @subscribed.delete(topic)
              removed_last = true
            end
          end
        end
        removed_last
      end
    end

    def received_event(topic, body)
      callbacks = nil
      @subscribed_mutex.synchronize do
        callbacks = (@subscribed[topic] || {}).values.dup
      end
      callbacks.each do |callback|
        begin
          callback.call(body)
        rescue StandardError
        end
      end
    end

    def handle_open(connection)
      pending = nil

      @state_mutex.synchronize do
        @connection = connection
        @connected = true
      end

      @pending_mutex.synchronize do
        pending = @pending.values.dup
      end

      begin
        @on_open.call if @on_open
      rescue StandardError
      end

      if !pending.empty?
        payload = pending.length == 1 ? pending.first : pending
        write_payload(connection, payload)
      end

      flush_queue
    end

    def handle_message(message)
      parsed = JSONCodec.load(message)
      @on_message.call(parsed) if @on_message
    rescue StandardError
      # Malformed JSON should not crash client callbacks.
    end

    def handle_close
      should_reconnect = false

      @state_mutex.synchronize do
        @connected = false
        @connection = nil
        should_reconnect = @running
      end

      begin
        @on_close.call if @on_close
      rescue StandardError
      end

      return unless should_reconnect

      Thread.new do
        sleep 0.1
        begin
          start
        rescue StandardError
        end
      end
    end

    private

    def normalize_address(address)
      return "ws://127.0.0.1:#{address}" if address.is_a?(Integer)
      return address if address.to_s.start_with?('ws://', 'wss://')

      "ws://#{address}"
    end

    def decorated_url
      return @address unless @headers_provider

      headers = @headers_provider.call
      return @address unless headers.is_a?(Hash) && !headers.empty?

      uri = URI.parse(@address)
      params = URI.decode_www_form(uri.query.to_s)
      headers.each do |key, value|
        next if value.nil? || value.to_s.empty?

        params << [key.to_s, value.to_s]
      end
      uri.query = URI.encode_www_form(params)
      uri.to_s
    rescue StandardError
      @address
    end

    def enqueue_payload(payload)
      @queue_mutex.synchronize do
        @queue << payload
      end
    end

    def write_payload(connection, body)
      payload = JSONCodec.dump(body)
      connection.write(payload)
    rescue StandardError
      enqueue_payload(payload)
    end

    def flush_queue
      connection = nil
      @state_mutex.synchronize { connection = @connection }
      return unless connection

      items = nil
      @queue_mutex.synchronize do
        return if @queue.empty?

        items = @queue
        @queue = []
      end

      items.each do |payload|
        begin
          connection.write(payload)
        rescue StandardError
          break
        end
      end
    end

    def connected_connection
      @state_mutex.synchronize do
        connection = @connection
        return connection if @connected && !connection.nil?

        nil
      end
    end

    class ClientConnectionHandler
      def initialize(client)
        @client = client
      end

      def on_open(connection)
        @client.handle_open(connection)
      end

      def on_message(_connection, message)
        @client.handle_message(message)
      end

      def on_close(_connection)
        @client.handle_close
      end
    end
  end

  class Channel
    def connect(address, options = {})
      headers = options[:headers]
      ws = WebSocketClientConnection.new(address, headers)
      sender = SenderFactory.make_sender(self, ws)
      topics = Set.new
      state = {}

      controller = {
        response: proc { |body| ws.notify(body) },
        subscribe: proc { |topic| topics << topic },
        unsubscribe: proc { |topic| topics.delete(topic) },
        event: proc { |topic, body| ws.received_event(topic, body) },
        sender: sender,
        state: state
      }

      publishers = []
      removed_publishers = false
      cleanup_publishers = proc do
        next if removed_publishers

        removed_publishers = true
        publishers.each do |entry|
          subscription = entry[:subscription]
          publisher = entry[:publisher]
          subscription.publishers.delete(publisher)
        end
      end

      ws.on_open = proc do
        on_connect = options[:on_connect]
        on_connect.call(sender) if on_connect
      end

      ws.on_close = proc do
        cleanup_publishers.call
        disconnect(state, sender)
      end

      ws.on_message = proc do |message|
        receive(message, controller)
      end

      (@events_api || {}).each_value do |subscription|
        publisher = Publisher.new do |event|
          next unless topics.include?(event[:topic])

          ws.send(event)
        end
        subscription.publishers << publisher
        publishers << { subscription: subscription, publisher: publisher }
      end

      ClientSender.new(sender, ws)
    end
  end

  class Publisher
    def initialize(&block)
      @block = block
    end

    def publish(event)
      @block.call(event)
    end
  end
end
