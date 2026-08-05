# frozen_string_literal: true

require 'thread'

module ChannelRB
  class Values
    include Enumerable

    def initialize(channel:, path:, body:, context:, on_send:, on_cancel:, sender:)
      @channel = channel
      @path = path
      @body = body
      @context = context
      @on_send = on_send
      @on_cancel = on_cancel
      @sender = sender

      @running = false
      @queue = Queue.new
      @rid = nil
      @mutex = Mutex.new
      @finished = false
    end

    def each
      return enum_for(:each) unless block_given?

      start

      begin
        loop do
          response = @queue.pop
          error = read_key(response, :error)
          if !error.nil?
            @finished = true
            raise ChannelError, error.to_s
          end
          if read_key(response, :done)
            @finished = true
            break
          end
          yield read_key(response, :body)
        end
      ensure
        cancel unless @finished
      end
    end

    def cancel
      rid = nil
      @mutex.synchronize do
        rid = @rid
        @rid = nil
      end
      return unless rid

      @on_cancel.call(rid)
    end

    def return
      cancel
      { value: nil, done: true }
    end

    private

    def start
      @mutex.synchronize do
        return if @running

        @running = true
        request = @channel.make_stream(@path, @body, @context, proc { |response|
          @queue << response
        }, @sender)
        @rid = request[:id]
        @on_send.call(request)
      end
    end

    def read_key(hash, key)
      return unless hash.is_a?(Hash)

      return hash[key] if hash.key?(key)

      hash[key.to_s]
    end
  end
end
