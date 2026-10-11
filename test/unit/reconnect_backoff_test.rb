# frozen_string_literal: true

require 'socket'
require_relative '../test_helper'

# The client used to retry every 100 ms forever while the server was down (TS parity: channel docs/client.md).
class ReconnectBackoffTest < Minitest::Test
  def test_reconnect_delay_doubles_up_to_5_seconds
    delays = (0...8).map { |attempt| ChannelRB::WebSocketClientConnection.reconnect_delay(attempt, 1) }
    [0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 5.0, 5.0].zip(delays).each { |expected, delay| assert_in_delta(expected, delay) }
    assert_in_delta(0.05, ChannelRB::WebSocketClientConnection.reconnect_delay(0, 0))
    assert_in_delta(2.5, ChannelRB::WebSocketClientConnection.reconnect_delay(1000, 0))
  end

  def test_client_backs_off_while_the_server_refuses_the_upgrade
    # Every request is refused with HTTP 500: each connection is one failed attempt.
    server = TCPServer.new('127.0.0.1', 0)
    attempts = 0
    acceptor = Thread.new do
      loop do
        socket = server.accept
        attempts += 1
        socket.readpartial(4096) rescue nil
        socket.write("HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        socket.close
      end
    rescue IOError, Errno::EBADF
    end

    client = ChannelRB::WebSocketClientConnection.new("ws://127.0.0.1:#{server.addr[1]}")
    sleep 2
    client.stop
    server.close
    acceptor.join(1)

    # 100 ms retries would make ~20 attempts in 2 s; backoff allows at most 1 + 6 (waits of 50..100, 100..200, ...).
    assert_includes(2..8, attempts)
  end
end
