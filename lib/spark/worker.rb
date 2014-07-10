#!/usr/bin/env ruby

# -------------------------------------------------------------------------------
#
# TODO: create Serializer class
# TODO: add constant for log, end stream
#
# -------------------------------------------------------------------------------


require "socket"

# -------------------------------------------------------------------------------
# Function
# -------------------------------------------------------------------------------

def log(message=nil)
  puts %{==> [#{Process.pid}] [#{Time.now.strftime("%H:%M")}] RUBY WORKER: #{message}}
end

class SparkSocket < TCPSocket
  
  def initialize(port, address='127.0.0.1', encoding='UTF-8')
    @encoding = encoding

    super(address, port)

    set_encoding(@encoding)
  end

  # 4 byte, big endian
  def read_int
    read(4).unpack("l>")[0] 
  end

  def write_int(x)
    send([x].pack("l>"), 0)
  end

  # first read size of item
  # then load command
  # TODO: too many rescue
  def load_stream
    result = []
    loop { result << _next rescue break }
    result
  end

  def _next
    data = read(read_int).force_encoding(@encoding)
    Marshal.load(data) rescue data
  end

  def write_stream(data)
    data.each do |x|
      serialized = Marshal.dump(x)

      write_int(serialized.size)
      send(serialized, 0)
    end
  end

end




# -------------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------------

# log "INIT"

port = $stdin.readline.to_i

s = SparkSocket.new(port)

split_index = s.read_int
command_size = s.read_int
command = Marshal.load(s.read(command_size))
iterator = s.load_stream

eval(command[0]) # original lambda

result = eval(command[1]).call(split_index, iterator)

# log "SPLIT INDEX: #{split_index}"
# log "COMMAND SIZE: #{command_size}"
# log "COMMAND: #{command}"
# log "ITERATOR: #{iterator}"
# log "RESULT: #{result}"

s.write_stream(result)
s.write_int(0)

s.close
