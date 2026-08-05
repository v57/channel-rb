# frozen_string_literal: true

module ChannelRB
  class Subscription
    attr_accessor :prefix
    attr_reader :publishers

    def self.parse(object, prefix, map)
      object.each do |key, value|
        k = key.to_s
        if value.is_a?(Subscription)
          value.prefix = prefix.empty? ? k : "#{prefix}/#{k}"
          map["#{prefix}#{k}"] = value
        elsif value.is_a?(Hash)
          parse(value, "#{prefix}#{k}/", map)
        end
      end
    end

    def initialize
      @publishers = []
      @prefix = ''
      @topic_proc = proc { |request| request.nil? ? '' : request.to_s }
      @body_proc = proc { |_request| nil }
    end

    def topic(&block)
      @topic_proc = block if block
      self
    end

    # Keep parity with TS behavior (body replaces topic formatter).
    def body(&block)
      @topic_proc = block if block
      self
    end

    def set_body(&block)
      @body_proc = block if block
      self
    end

    def topic_for(request)
      @topic_proc.call(request)
    end

    def body_for(request)
      @body_proc.call(request)
    end

    def send_event(request, body = :__channel_rb_no_body__)
      topic = @topic_proc.call(request)
      payload = if body == :__channel_rb_no_body__
                  @body_proc.call(request)
                else
                  body
                end
      publish(topic, payload)
    end

    def publish(topic, body = nil)
      return if body.nil?

      full_topic = if @prefix.empty?
                     topic.to_s
                   elsif topic.to_s.empty?
                     @prefix
                   else
                     "#{@prefix}/#{topic}"
                   end

      event = { topic: full_topic, body: body }
      @publishers.each { |publisher| publisher.publish(event) }
    end
  end
end
