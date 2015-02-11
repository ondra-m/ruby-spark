if !ENV.has_key?("JAVA_HOME")
  raise "Environment variable JAVA_HOME is not set" 
end

require 'socket'
require 'rjb'

Rjb.load("#{ENV['SCALA_HOME']}/lib/scala-library.jar:scala.jar")
Rjb.primitive_conversion = true

ScalaObject = Rjb.import('ruby.ScalaObject')

@port = nil

t = Thread.new do
  server = TCPServer.new(0)
  @port = server.addr[1]

  connection = server.accept

  puts "ruby: received #{connection.read(4).unpack('l>').first}"

  to_send = 5

  puts "ruby: sending #{to_send}"

  connection.write([to_send].pack('l>'))
  connection.flush

  puts "ruby: sent"
end

while @port.nil?
  sleep(0.1)
end

ScalaObject.start(@port)

t.join
