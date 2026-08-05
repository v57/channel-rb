# frozen_string_literal: true

require 'thread'
require 'iodine'

module ChannelRB
  module IodineRuntime
    module_function

    def ensure_started
      mutex.synchronize do
        return if @started

        Iodine.threads = 1
        Iodine.workers = 1

        @thread = Thread.new do
          begin
            Iodine.start
          rescue StandardError
            # The process might be stopping while tests finish.
          end
        end

        @started = true
      end
    end

    def stop
      thread = nil
      mutex.synchronize do
        return unless @started

        @started = false
        thread = @thread
        @thread = nil
      end

      begin
        Iodine.stop
      rescue StandardError
      end

      begin
        thread&.join(2)
      rescue StandardError
      end
    end

    def running?
      mutex.synchronize { @started }
    end

    def mutex
      @mutex ||= Mutex.new
    end

    private_class_method :mutex
  end
end

at_exit do
  ChannelRB::IodineRuntime.stop
end
