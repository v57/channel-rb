# frozen_string_literal: true

require_relative 'sender'

module ChannelRB
  class ProcessInterface
    attr_accessor :controller

    def initialize(channel)
      @channel = channel
      @controller = nil
    end

    def send(body)
      @channel.receive(body, @controller) if @controller
      0
    end

    def sent(_id); end

    def cancel(_id)
      false
    end

    def inline_requests?
      true
    end

    def notify(body)
      @channel.receive(body, @controller) if @controller
    end

    def add_topic(_topic)
      proc { false }
    end

    def stop
      return unless @controller

      state = @controller[:state]
      sender = @controller[:sender]
      @controller = nil
      @channel.disconnect(state, sender)
    end
  end

  class Channel
    def process
      connection = ProcessInterface.new(self)
      sender = SenderFactory.make_sender(self, connection)
      state = {}
      connection.controller = {
        response: proc { |body| receive(body, connection.controller) },
        subscribe: proc { |_topic| },
        unsubscribe: proc { |_topic| },
        event: proc { |_topic, _body| },
        inline_requests: true,
        sender: sender,
        state: state
      }
      sender
    end
  end
end
