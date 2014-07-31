#!/usr/bin/env ruby

# TODO: restart poolmaster if crash

$PROGRAM_NAME = "RubySparkMaster"

require "socket"

# Require all serializers
dir = File.expand_path(File.join("..", "serializer"), File.dirname(__FILE__))
Dir.glob(File.join(dir, "*.rb")) { |file| require file  }

# Require template file
require File.expand_path(File.join("..", "command", "template.rb"), File.dirname(__FILE__))

require_relative "pool_master"
require_relative "worker"

def log(message=nil)
  $stdout.write %{==> [#{Process.pid}::#{Thread.current.object_id}] [#{Time.now.strftime("%H:%M")}] #{message}}
  $stdout.flush
end

# New process group
Process.setpgrp


# =================================================================================================
# Master of all workers
# Create and controll process/threads
# Trap signal
#
module Master

  # Create a master class by type
  def self.create(address='127.0.0.1', port=0)
    case ENV['WORKER_TYPE'].downcase
    when 'process'
      Master::Process.new(address, port)
    when 'thread'
      Master::Thread.new(address, port)
    when 'simple'
      # not yet
    end
  end

  # ===============================================================================================
  # Base class
  # Used just as parent
  #
  class Base
    include Spark::Serializer::Helper

    POOL_SIZE = 2

    attr_accessor :port, :server_socket

    # Create new Socket server
    def initialize(address, port)
      self.server_socket = TCPServer.new(address, port)
      self.port = server_socket.addr[1]

      # Send to Spark port of Socket server
      $stdout.write(pack_int(self.port))
      $stdout.flush

      # $stderr.reopen($stdout)
    end

    # Create PollMasters
    def run
      log "Master INIT"
      before_start

      POOL_SIZE.times do
        create_pool_master
      end

      before_end
      log "Master SHUTDOWN"
    end

    private
      def before_start
      end

      def before_end
      end

  end

  # ===============================================================================================
  # Master::Process
  #
  # New workers are created via process
  # Available only on UNIX on non-java ruby
  #
  class Process < Base

    attr_accessor :pids

    private

      def before_start
        self.pids = []
      end

      def before_end
        server_socket.close

        Signal.trap("TERM") { 
          ::Process.kill("HUP", 0)
          exit
        }

        loop {
          sleep(2)
          if $stdin.closed?
            ::Process.kill("HUP", 0)
          end
        }
      end

      def create_pool_master
        pids << fork {
                  PoolMaster::Process.new(server_socket).run
                }
      end

  end

  # ===============================================================================================
  # Master::Thread
  #
  # New workers are created via threads
  # Somethings are faster but it can be danger
  #
  class Thread < Base
    attr_accessor :pool_threads

    private

      def before_start
        self.pool_threads = []

        # For synchronous access to socket IO
        $mutex = Mutex.new
      end

      def before_end
        pool_threads.each {|t| t.join}
      end

      def create_pool_master
        pool_threads << Thread.new {
                          PoolMaster::Thread.new(server_socket).run
                        }
      end

  end

end


# Create master by ENV['WORKER_TYPE']
Master.create.run
