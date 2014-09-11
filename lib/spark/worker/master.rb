#!/usr/bin/env ruby

$PROGRAM_NAME = "RubySparkMaster"

require "socket"
require "io/wait"
require "nio"

require_relative "special_constant"
require_relative "worker"

class Master

  include SpecialConstant

  def self.init
    @worker_type = ENV["WORKER_TYPE"]
    @worker_arguments = ENV["WORKER_ARGUMENTS"]
    @port = ENV["SERVER_PORT"]

    @socket = TCPSocket.open("localhost", @port)
  end

  def self.run
    init

    selector = NIO::Selector.new
    monitor = selector.register(@socket, :r)
    monitor.value = Proc.new { receive_message }

    loop {
      selector.select {|monitor| monitor.value.call}
    }
  end

  def self.receive_message
    # Read int
    command = @socket.read(4).unpack("l>")[0]

    case command
    when CREATE_WORKER
      create_worker
    end
  end

  def self.create_worker
    pid = Process.spawn("ruby #{@worker_arguments} worker.rb #{@port}")

    # Detach child from master to avoid kill zombie process
    Process.detach(pid)
  end
end

Master.run
