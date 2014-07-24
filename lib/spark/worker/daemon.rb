#!/usr/bin/env ruby

require "socket"

# Require all serializers
dir = File.expand_path(File.join("..", "serializer"), File.dirname(__FILE__))
Dir.glob(File.join(dir, "*.rb")) { |file| require file  }
require File.expand_path(File.join("..", "command", "template"), File.dirname(__FILE__))

def log(message=nil)
  puts %{==> [#{Process.pid}::#{Thread.current.object_id}] [#{Time.now.strftime("%H:%M")}] #{message}}
end



# ==============================================================================
# Master
# ==============================================================================

class Master

  include Spark::Serializer::Helper

  POOL_SIZE = 2

  attr_accessor :port, :server_socket, :pool

  def initialize(address='127.0.0.1', port=0)
    self.server_socket = TCPServer.new(address, port)
    self.port = server_socket.addr[1]
    self.pool = []
  end

  def send_info
    $stdout.write(pack_int(port))
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

  include Spark::Serializer::Helper

  attr_accessor :client_socket

  def initialize(client_socket)
    self.client_socket = client_socket
  end

  def run
    load_split_index
    load_command
    load_iterator

    compute

    send_result
    finish
  end

  private

    def read(size)
      client_socket.read(size)
    end

    def read_int
      unpack_int(read(4))
    end

    def load_split_index
      @split_index = read_int
    end

    def load_command
      @command = Marshal.load(read(read_int))
    end

    def load_iterator
      @iterator = @command.deserializer.load(client_socket)
    end

    def compute
      begin
        @command.library.each{|lib| require lib}
        @command.pre.each{|pre| eval(pre)}

        @command.stages.each do |stage|
          eval(stage.pre)
          @iterator = eval(stage.main).call(@iterator, @split_index)
        end
      rescue => e
        client_socket.write(pack_int(-1))
        client_socket.write(pack_int(e.message.size))
        client_socket.write(e.message)

        # Thread.kill
      end
    end

    def send_result
      @command.serializer.dump(@iterator, client_socket)
    end

    def finish
      client_socket.write(pack_int(0))
      client_socket.flush

      loop { break if client_socket.recv(4096) == '' }
    end

end



# ==============================================================================
# INIT
# ==============================================================================

master = Master.new
master.send_info
master.run
