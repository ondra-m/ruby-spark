require 'benchmark'
require 'benchmark/ips'

data = 0..1_000_000
zero_value = rand(100_000)
function = Proc.new{|sum, n| sum+n}

Benchmark.ips do |r|  
  r.report('each') do
    sum = zero_value
    data.each do |n|
      sum += n
    end
  end

  r.report('reduce') do
    data.reduce(zero_value){|sum, n| sum+n}
  end

  r.report('each with function') do
    sum = zero_value
    data.each do |n|
      sum = function.call(sum, n)
    end
  end

  r.report('reduce with function') do
    data.reduce(zero_value, &function)
  end

  r.compare!
end

