#!/usr/bin/env ruby

require "socket"

# Require all serializers
dir = File.expand_path(File.join("..", "serializer"), File.dirname(__FILE__))
Dir.glob(File.join(dir, "*.rb")) { |file| require file  }

# Require template file
require File.expand_path(File.join("..", "command", "template.rb"), File.dirname(__FILE__))

require_relative "pool_master"
require_relative "worker"

def log(message=nil)
  puts %{==> [#{Process.pid}::#{Thread.current.object_id}] [#{Time.now.strftime("%H:%M")}] #{message}}
  $stdout.flush
end


# ==============================================================================
# Master
# ==============================================================================
class Master

  include Spark::Serializer::Helper

  POOL_SIZE = 2

  attr_accessor :port, :server_socket

  def self.create(address='127.0.0.1', port=0)
    case ENV['WORKER_TYPE'].downcase
    when 'thread'
      MasterThread.new(address, port)
    when 'fork'
      MasterFork.new(address, port)
    when 'simple'
    end
  end

  def initialize(address, port)
    self.server_socket = TCPServer.new(address, port)
    self.port = server_socket.addr[1]
  end

  def send_info
    $stdout.write(pack_int(port))
    $stdout.flush # need it for MRI
  end

  def run
    log "Master INIT"

    POOL_SIZE.times { create_pool_master }
    wait

    log "Master SHUTDOWN"
  end

end

#
# MasterThread
#
class MasterThread < Master
  
  attr_accessor :pool_threads

  def initialize(address, port)
    super(address, port)

    self.pool_threads = []

    $mutex = Mutex.new
  end

  def create_pool_master
    pool_threads << Thread.new {
                      PoolMasterThread.new(server_socket).run
                    }
  end

  def wait
    pool_threads.each {|t| t.join}
  end

end

#
# MasterFork
#
class MasterFork < Master
  
  def create_pool_master
    fork do
      PoolMasterFork.new(server_socket).run
    end
  end

  def wait
    server_socket.close
    Process.wait
  end

end


master = Master.create
master.send_info
master.run
