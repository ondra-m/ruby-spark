require 'benchmark'
require 'benchmark/ips'

def pack_int(data)
  [data].pack('l>')
end

def pack_long(data)
  [data].pack('q>')
end

def pack_doubles(data)
  data.pack('G*')
end

module Standard
  class LabeledPoint
    def initialize(label, features)
      @label = label
      @features = Standard::Vector.new(features)
    end

    def marshal_dump
      [@label, @features]
    end

    def marshal_load(*)
    end
  end

  class Vector
    def initialize(array)
      @values = array
    end

    def marshal_dump
      [@values]
    end

    def marshal_load(*)
    end
  end
end

module Custom
  class LabeledPoint
    def initialize(label, features)
      @label = label
      @features = Custom::Vector.new(features)
    end

    def _dump(*)
      pack_long(@label) + @features._dump
    end

    def self._load(*)
    end
  end

  class Vector
    def initialize(array)
      @values = array
    end

    def _dump(*)
      result = 'v'
      result << pack_int(@values.size)
      result << pack_doubles(@values)
      result.encode(Encoding::ASCII_8BIT)
    end

    def self._load(*)
    end
  end
end

data_size = 10_000
vector_size = 1_000
values = Array.new(vector_size) { |x| rand(10_000..100_000) }

@data1 = Array.new(data_size) {|i| Standard::LabeledPoint.new(i, values)}
@data2 = Array.new(data_size) {|i| Custom::LabeledPoint.new(i, values)}

Benchmark.ips do |r|
  r.report('standard') do
    Marshal.dump(@data1)
  end

  r.report('custom') do
    Marshal.dump(@data2)
  end

  r.compare!
end
