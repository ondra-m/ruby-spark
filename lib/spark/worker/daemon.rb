#!/usr/bin/env ruby

require "socket"

def log(message=nil)
  puts %{==> [#{Process.pid}] [#{Time.now.strftime("%H:%M")}] RUBY WORKER: #{message}}
end

# ==============================================================================
# SocketHelper
# ==============================================================================

module SocketHelper

  def to_stream(data)
    if data.is_a?(Integer)
      [data].pack("l>")
    end
  end

end



# ==============================================================================
# Master
# ==============================================================================

class Master

  include SocketHelper

  POOL_SIZE = 1

  attr_accessor :port, :server_socket, :pool

  def initialize(address='127.0.0.1', port=0)
    self.server_socket = TCPServer.new(address, port)
    self.port = server_socket.addr[1]
    self.pool = []
  end

  def send_info
    $stdout.write(to_stream(port))
  end

  def run
    log "Master INIT"

    POOL_SIZE.times { create_pool_master }
    # server_socket.close
    pool.each {|t| t.join}
  end

  def create_pool_master
    pool << Thread.new do
      PoolMaster.new(server_socket).run
    end
  end

end



# ==============================================================================
# PoolMaster
# ==============================================================================

class PoolMaster

  attr_accessor :server_socket
  
  def initialize(server_socket)
    self.server_socket = server_socket
  end

  def run
    loop {
      client_socket = server_socket.accept
      create_worker(client_socket)
    }
  end

  def create_worker(client_socket)
    Thread.new do
      Worker.new(client_socket).run
    end
  end

end



# ==============================================================================
# Worker
# ==============================================================================

class Worker

  include SocketHelper

  attr_accessor :client_socket

  def initialize(client_socket)
    self.client_socket = client_socket
  end

  def run
    split_index = read_int
    command_size = read_int
    command = Marshal.load(read(command_size))
    iterator = load_stream

    eval(command[0]) # original lambda

    result = eval(command[1]).call(split_index, iterator)

    # log "SPLIT INDEX: #{split_index}"
    # log "COMMAND SIZE: #{command_size}"
    # log "COMMAND: #{command}"
    # log "ITERATOR: #{iterator}"
    # log "RESULT: #{result}"

    write_stream(result)
    write_int(0)
  end

  private

    def read(size)
      client_socket.read(size)
    end

    def send(data)
      client_socket.send(data, 0)
    end

    def read_int
      read(4).unpack("l>")[0] 
    end

    def write_int(data)
      send(to_stream(data))
    end

    def load_stream
      result = []
      loop { 
        result << begin
                    data = read(read_int).force_encoding(@encoding) rescue break # end of stream
                    Marshal.load(data) rescue data # data cannot be mashaled (e.g. first input)
                  end
      }
      result
    end

    def write_stream(data)
      data.each do |x|
        serialized = Marshal.dump(x)

        write_int(serialized.size)
        send(serialized)
      end
    end

end



# ==============================================================================
# INIT
# ==============================================================================

master = Master.new
master.send_info
master.run
