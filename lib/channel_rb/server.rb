# frozen_string_literal: true

require 'uri'
require 'set'
require_relative 'runtime'
require_relative 'json'
require_relative 'sender'

module ChannelRB
  # Largest WebSocket message accepted by the server: 16 MiB, the TS/Bun default.
  DEFAULT_MAX_PAYLOAD = 16 * 1024 * 1024 unless const_defined?(:DEFAULT_MAX_PAYLOAD)

  class TopicSubscriptions
    def initialize
      @id = 0
      @subscribed = {}
      @mutex = Mutex.new
    end

    def add_topic(topic, &event)
      local_id = nil
      @mutex.synchronize do
        local_id = @id
        @id += 1
        map = (@subscribed[topic] ||= {})
        map[local_id] = event
      end

      proc do
        removed_last = false
        @mutex.synchronize do
          map = @subscribed[topic]
          if map
            map.delete(local_id)
            if map.empty?
              @subscribed.delete(topic)
              removed_last = true
            end
          end
        end
        removed_last
      end
    end

    def received_event(topic, event)
      callbacks = nil
      @mutex.synchronize do
        callbacks = (@subscribed[topic] || {}).values.dup
      end

      callbacks.each do |callback|
        begin
          callback.call(event)
        rescue StandardError
        end
      end
    end
  end

  class ServerHandle
    def initialize(cleanup)
      @cleanup = cleanup
      @stopped = false
      @mutex = Mutex.new
    end

    def stop
      do_cleanup = false
      @mutex.synchronize do
        next if @stopped

        @stopped = true
        do_cleanup = true
      end
      return unless do_cleanup

      @cleanup.call
    end
  end

  class ServerConnectionAdapter
    attr_reader :state, :sender, :subscriptions

    def initialize(channel, client, state, options)
      @channel = channel
      @client = client
      @state = state
      @options = options
      @subscriptions = TopicSubscriptions.new
      @topics = Set.new
      @topics_mutex = Mutex.new

      connection = Connection.new(client, @subscriptions)
      @sender = SenderFactory.make_sender(channel, connection)

      @controller = {
        response: proc { |body| @client.write(JSONCodec.dump(body)) },
        subscribe: proc { |topic| subscribe_topic(topic) },
        unsubscribe: proc { |topic| unsubscribe_topic(topic) },
        event: proc { |topic, body| @subscriptions.received_event(topic, body) },
        sender: @sender,
        state: @state
      }
    end

    def on_open
      callback = @options[:on_connect]
      callback.call(state: @state, sender: @sender, subscriptions: @subscriptions) if callback
    end

    def on_message(message)
      payload = JSONCodec.load(message)
      @channel.receive(payload, @controller)
    rescue StandardError
      @client.close
    end

    def on_close
      @channel.disconnect(@state, @sender)
      callback = @options[:on_disconnect]
      callback.call(state: @state, sender: @sender, subscriptions: @subscriptions) if callback
    end

    def publish_event(event)
      topic = event[:topic]
      should_send = false
      @topics_mutex.synchronize do
        should_send = @topics.include?(topic)
      end
      return unless should_send

      @client.write(JSONCodec.dump(event))
    rescue StandardError
      # Ignore write failures during disconnect races.
    end

    private

    def subscribe_topic(topic)
      @topics_mutex.synchronize { @topics.add(topic) }
    end

    def unsubscribe_topic(topic)
      @topics_mutex.synchronize { @topics.delete(topic) }
    end

    class Connection
      def initialize(client, subscriptions)
        @client = client
        @subscriptions = subscriptions
      end

      def send(body)
        @client.write(JSONCodec.dump(body))
        0
      end

      def sent(_id); end

      def cancel(_id)
        false
      end

      def notify(body)
        @client.write(JSONCodec.dump(body))
      end

      def add_topic(topic, &event)
        @subscriptions.add_topic(topic, &event)
      end

      def stop
        @client.close
      end
    end
  end

  class ConnectionRegistry
    def initialize
      @connections = []
      @mutex = Mutex.new
    end

    def add(connection)
      @mutex.synchronize { @connections << connection }
    end

    def remove(connection)
      @mutex.synchronize { @connections.delete(connection) }
    end

    def each(&block)
      snapshot = @mutex.synchronize { @connections.dup }
      snapshot.each(&block)
    end
  end

  class ServerProtocol
    def initialize(channel, options, env, registry)
      @channel = channel
      @options = options
      @env = env
      @registry = registry
      @adapter = nil
    end

    def on_open(client)
      state_factory = @options[:state]
      state = if state_factory
                state_factory.call(extract_headers(@env))
              else
                {}
              end
      @adapter = ServerConnectionAdapter.new(@channel, client, state, @options)
      @registry.add(@adapter)
      @adapter.on_open
    end

    def on_message(_client, message)
      @adapter&.on_message(message)
    end

    def on_close(_client)
      @adapter&.on_close
      @registry.remove(@adapter) if @adapter
      @adapter = nil
    end

    private

    # Query parameters first, then real HTTP headers, so headers win (TS parity: an
    # authenticating proxy's header can't be overridden with `?x-user-id=...`).
    # Header names are lowercased like TS/Bun (`authorization`, `x-user-id`).
    def extract_headers(env)
      headers = {}

      query = env['QUERY_STRING'].to_s
      unless query.empty?
        begin
          URI.decode_www_form(query).each do |key, value|
            headers[key] = value
          end
        rescue ArgumentError
          # Malformed query string: ignore it rather than failing the handshake.
        end
      end

      env.each do |key, value|
        # HTTP_VERSION is the request line's protocol (Rack/CGI), not a header.
        next unless key.start_with?('HTTP_') && key != 'HTTP_VERSION'

        headers[key.sub('HTTP_', '').downcase.tr('_', '-')] = value
      end

      headers
    end
  end

  class Channel
    def listen(address, options = {})
      host, port = parse_address(address)
      registry = ConnectionRegistry.new

      app = proc do |env|
        upgrade = env['rack.upgrade?'.freeze]
        if upgrade == :websocket || upgrade == :sse
          env['rack.upgrade'.freeze] = ServerProtocol.new(self, options, env, registry)
          [0, {}, []]
        else
          [404, { 'Content-Type' => 'text/plain', 'Content-Length' => '0' }, []]
        end
      end

      # max_msg is in KiB: accept WebSocket messages up to 16 MiB (TS/Bun default) instead of
      # iodine's ~250 KiB default. `max_payload:` (bytes) overrides it.
      max_payload = options[:max_payload] || DEFAULT_MAX_PAYLOAD
      Iodine.listen(service: :http, address: host, port: port, handler: app,
                    max_msg: (max_payload + 1023) / 1024)

      publisher = Publisher.new do |event|
        registry.each { |connection| connection.publish_event(event) }
      end

      (@events_api || {}).each_value { |subscription| subscription.publishers << publisher }

      IodineRuntime.ensure_started

      ServerHandle.new(proc {
        (@events_api || {}).each_value { |subscription| subscription.publishers.delete(publisher) }
        IodineRuntime.stop
      })
    end

    private

    def parse_address(address)
      if address.is_a?(Integer)
        ['127.0.0.1', address]
      else
        host, port = address.to_s.split(':', 2)
        [host.nil? || host.empty? ? '127.0.0.1' : host, (port || '80').to_i]
      end
    end
  end
end
