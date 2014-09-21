require "benchmark"

array = []
1000.times { 
  array << {:bar => rand(1000)} 
}

n = 500
Benchmark.bm(20) do |x|
  x.report("sort")               { n.times { array.sort{ |a,b| b[:bar] <=> a[:bar] } } }
  x.report("sort reverse")       { n.times { array.sort{ |a,b| a[:bar] <=> b[:bar] }.reverse } }
  x.report("sort_by -a[:bar]")   { n.times { array.sort_by{ |a| -a[:bar] } } }
  x.report("sort_by a[:bar]*-1") { n.times { array.sort_by{ |a| a[:bar]*-1 } } }
  x.report("sort_by.reverse!")   { n.times { array.sort_by{ |a| a[:bar] }.reverse } }
end


array = Array.new(10000) { Array.new(rand(1..10)){(97+rand(26)).chr}.join }

Benchmark.bm(20) do |x|
  x.report("sort asc")         { n.times { array.sort } }
  x.report("sort asc block")   { n.times { array.sort{|a,b| a <=> b} } }
  x.report("sort desc")        { n.times { array.sort{|a,b| b <=> a} } }
  x.report("sort asc reverse") { n.times { array.sort.reverse } }
end


key_value = Struct.new(:key, :value) do
  def <=>(other)
    key <=> other.key
  end
end

count = 10000
item_range = 1000000
array1 = Array.new(count) { [rand(item_range), rand(item_range)] }
array2 = Array.new(count) { key_value.new rand(item_range), rand(item_range) }

Benchmark.bm(20) do |x|
  x.report("sort_by")       { n.times { array1.sort_by {|a| a[0]} } }
  x.report("sort struct")   { n.times { array2.sort } }
end

