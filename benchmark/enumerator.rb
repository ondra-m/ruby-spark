require "benchmark"

class Enumerator
  def defer(&blk)
    self.class.new do |y|
      each do |*input|
        blk.call(y, *input)
      end
    end
  end
end

ARRAY_SIZE = 50_000_000

def type_yield
  return to_enum(__callee__) unless block_given?

  ARRAY_SIZE.times { |i|
    yield i
  }
end

def yield_map_x2(enum)
  return to_enum(__callee__, enum) unless block_given?
  
  enum.each do |item|
    yield item*2
  end
end

def type_enumerator_new
  Enumerator.new do |e|
    ARRAY_SIZE.times { |i|
      e << i
    }
  end
end

def enumerator_new_map_x2(enum)
  Enumerator.new do |e|
    enum.each do |item|
      e << item*2
    end
  end
end

def enumerator_defer_x2(enum)
  enum.defer do |out, inp|
    out << inp*2
  end
end

Benchmark.bm(26) do |x|
  x.report("yield max") do
    type_yield.max
  end

  x.report("yield sum") do
    type_yield.reduce(:+)
  end

  x.report("yield map x*2 sum") do
    yield_map_x2(type_yield).reduce(:+)
  end

  x.report("yield defer map x*2 sum") do
    enumerator_defer_x2(type_yield).reduce(:+)
  end

  x.report("-----"){}

  x.report("Enum.new max") do
    type_enumerator_new.max
  end

  x.report("Enum.new sum") do
    type_enumerator_new.reduce(:+)
  end

  x.report("Enum.new map x*2 sum") do
    enumerator_new_map_x2(type_enumerator_new).reduce(:+)
  end

  x.report("Enum.new defer map x*2 sum") do
    enumerator_defer_x2(type_enumerator_new).reduce(:+)
  end

end
