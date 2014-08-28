require "benchmark"
require "yaml"
require "msgpack"
require "oj"
# require "thrift"
 
puts "Simple"

data = (0..100000).to_a

Benchmark.bmbm do |x|
  x.report("YAML") do
    serialized = YAML.dump(data)
    deserialized = YAML.load(serialized)
    puts "Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  x.report("Marshal") do
    serialized = Marshal.dump(data)
    deserialized = Marshal.load(serialized)
    puts "Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  x.report("MessagePack") do
    serialized = MessagePack.dump(data)
    deserialized = MessagePack.load(serialized)
    puts "Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  x.report("Oj") do
    serialized = Oj.dump(data)
    deserialized = Oj.load(serialized)
    puts "Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  # x.report("Thrift") do
  #   serializer = Thrift::Serializer.new
  #   deserializer = Thrift::Deserializer.new

  #   serialized = serializer.serialize(data)
  # end
end

puts ""
puts "More complex"

data = Array.new(10000000) { 
  [rand(97..122).chr, rand(10000000)]
}

Benchmark.bm do |x|
  # Take too long
  # x.report("YAML") do
  #   serialized = YAML.dump(data)
  #   YAML.load(serialized)
  # end

  x.report("Marshal") do
    serialized = Marshal.dump(data)
    deserialized = Marshal.load(serialized)
    puts " Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  x.report("MessagePack") do
    serialized = MessagePack.dump(data)
    deserialized = MessagePack.load(serialized)
    puts " Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  x.report("Oj") do
    serialized = Oj.dump(data)
    deserialized = Oj.load(serialized)
    puts " Size: #{serialized.size}, Equal: #{deserialized == data}"
  end

  # x.report("Thrift") do
  #   serializer = Thrift::Serializer.new
  #   deserializer = Thrift::Deserializer.new

  #   serialized = serializer.serialize(data)
  # end
end
