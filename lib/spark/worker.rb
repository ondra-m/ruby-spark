#!/usr/bin/env ruby

require "socket"

# -------------------------------------------------------------------------------
# Function
# -------------------------------------------------------------------------------

def log(message=nil)
  puts %{==> [#{Time.now.strftime("%H:%M")}] RUBY WORKER: #{message}}
end

def load_stream(s)
  result = []

  loop { 
    begin
      size = s.read(4).unpack("l>")[0]
      result << s.read(size).force_encoding('UTF-8')
    rescue
      break
    end
  }

  result
end



# -------------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------------

log "INIT"

port = $stdin.readline.to_i

log "PORT: #{port}"

s = TCPSocket.new('127.0.0.1', port)
s.set_encoding 'UTF-8'

split_index = s.read(4).unpack("l>")[0] # 4 byte, big endian
command_size = s.read(4).unpack("l>")[0]
command = Marshal.load(s.read(command_size))
iterator = load_stream(s)

eval(command[0]) # original lambda

result = eval(command[1]).call(split_index, iterator)

log "SPLIT INDEX: #{split_index}"
log "COMMAND SIZE: #{command_size}"
log "COMMAND: #{command}"
log "ITERATOR: #{iterator}"
log "RESULT: #{result}"

result = Marshal.dump(result)

log result.size

s.send([result.size].pack("l>"), 0)
s.send(result, 0)

s.send([0].pack("l>"), 0)

s.close
