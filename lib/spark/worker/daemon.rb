#!/usr/bin/env ruby

# $stderr.reopen("/ruby_spark/err.txt", "w")

require "socket"

def log(message=nil)
  puts %{==> [#{Process.pid}] [#{Time.now.strftime("%H:%M")}] RUBY WORKER: #{message}}
end

def realtime
  t1 = Time.now
  yield
  Time.now - t1
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

# class Worker

#   include SocketHelper

#   attr_accessor :client_socket

#   def initialize(client_socket)
#     self.client_socket = client_socket
#   end

#   def run
#     time = Time.now

#     split_index = read_int

#     command = Marshal.load(read(read_int))

#     log "1: #{-1*(time - (time=Time.now))*1000}ms"
#     iterator = load_iterator

#     log "2: #{-1*(time - (time=Time.now))*1000}ms"
#     eval(command[0]) # original lambda

#     log "3: #{-1*(time - (time=Time.now))*1000}ms"
#     result = eval(command[1]).call(split_index, iterator)
#     log "4: #{-1*(time - (time=Time.now))*1000}ms"
    
#     write_stream(result)
#     log "5: #{-1*(time - (time=Time.now))*1000}ms"

#     write_int(0)
#   end

#   private

#     def read(size)
#       client_socket.read(size)
#     end

#     def send(data)
#       client_socket.send(data, 0)
#     end

#     def read_int
#       read(4).unpack("l>")[0] 
#     end

#     def write_int(data)
#       send(to_stream(data))
#     end

#     def load_iterator(&block)
#       # result = []
#       # loop { 
#       #   result << begin
#       #               data = read(read_int).force_encoding(@encoding) rescue break # end of stream
#       #               Marshal.load(data) rescue data # data cannot be mashaled (e.g. first input)
#       #             end
#       # }
#       # result

#       Enumerator.new do |e|
#         while true
#           begin
#             e.yield(read(read_int))
#           rescue
#             break
#           end
#         end
#       end.each(&block)

#     end

#     def write_stream(data)
#       data.each do |x|
#         serialized = Marshal.dump(x)

#         write_int(serialized.size)
#         send(serialized)
#       end
#     end

# end








class Worker

  include SocketHelper

  attr_accessor :client_socket

  def initialize(client_socket)
    self.client_socket = client_socket

    @iterator = []
    @point = Time.now
  end

  def run

    split_index = read_int

    command = Marshal.load(read(read_int))

    report("Load command")

    load_iterator

    report("Load iterator")

    eval(command[0]) # original lambda

    report("Eval original lambda")

    @result = eval(command[1]).call(split_index, @iterator)

    report("Run")
    
    write_stream

    report("Write stream")

    write_int(0)
  end

  private

    def report(message)
      log("#{message}: #{(Time.now - @point)*1000}ms")
      @point = Time.now
    end

    def read(size)
      client_socket.read(size)
    end

    def send(data)
      client_socket.write(data)
    end

    def read_int
      read(4).unpack("l>")[0] 
    end

    def write_int(data)
      send(to_stream(data))
    end

    def load_iterator

      @iterator = []
      loop { 
        @iterator << begin
                       # data = read(read_int).force_encoding(@encoding) rescue break # end of stream
                       data = read(read_int) rescue break # end of stream
                       # Marshal.load(data) rescue data # data cannot be mashaled (e.g. first input)
                     end
      }

      # Enumerator.new do |e|
      #   while true
      #     begin
      #       e.yield(read(read_int))
      #     rescue
      #       break
      #     end
      #   end
      # end.each(&block)

    end

    def write_stream
      @result.each{|x|
        serialized = Marshal.dump(x)

        write_int(serialized.size)
        send(serialized)
      }
    end

end








# ==============================================================================
# INIT
# ==============================================================================

master = Master.new
master.send_info
master.run
