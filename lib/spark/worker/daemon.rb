#!/usr/bin/env ruby

# $stderr.reopen("/ruby_spark/err.txt", "w")

require "socket"

dir = File.expand_path(File.join("..", "serializer"), File.dirname(__FILE__))
Dir.glob(File.join(dir, "*.rb")) { |file| require file  }

def log(message=nil)
  puts %{==> [#{Process.pid}##{Thread.current.object_id}] [#{Time.now.strftime("%H:%M")}] #{message}}
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

  POOL_SIZE = 2

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

    log "Master SHUTDOWN"
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

  attr_accessor :server_socket, :workers
  
  def initialize(server_socket)
    self.server_socket = server_socket
    self.workers = []
  end

  def run
    log "Init POOLMASTER"
    loop {
      client_socket = server_socket.accept
      create_worker(client_socket)
      # client_socket.close # not for thread
    }
    workers.each {|t| t.join}
    log "Shutdown POOLMASTER"
  end

  def create_worker(client_socket)
    workers << Thread.new do
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
    log "Init WORKER"

    load_split_index
    load_command
    load_iterator

    compute

    serialize_result
    send_result
    finish

    log "Shutdown WORKER"
  end

  private

    def read(size)
      client_socket.read(size)
    end

    def read_int
      read(4).unpack("l>")[0] 
    end

    def load_split_index
      @split_index = read_int
    end

    def load_command
      command = Marshal.load(read(read_int))

      @commands = command[0].map!{|x| [eval(x[0]), eval(x[1])]}
      @serializer = eval(command[1])
    end

    def load_iterator
      @iterator = @serializer.load_from_io(client_socket)
    end

    def compute
      @commands.each do |command|
        @__function__ = command[0]
        @iterator = command[1].call(@split_index, @iterator)
      end
    end

    def serialize_result
      @serializer.dump_for_io(@iterator)
    end

    def send_result
      client_socket.write(@iterator.join)
    end

    def finish
      client_socket.write(to_stream(0))
      client_socket.flush

      while true
        # Empty string is returned upon EOF (and only then).
        if client_socket.recv(4096) == ''
          break
        end
      end

    end

end








# ==============================================================================
# INIT
# ==============================================================================

master = Master.new
master.send_info
master.run
