require "benchmark"

def bisect_left1(a, x, opts={})
  return nil if a.nil?
  return 0 if a.empty?

  lo = (opts[:lo] || opts[:low]).to_i
  hi = opts[:hi] || opts[:high] || a.length

  while lo < hi
    mid = (lo + hi) / 2
    v = a[mid]
    if v < x
      lo = mid + 1
    else
      hi = mid
    end
  end
  return lo
end

def bisect_left2(list, item)
  count = 0
  list.each{|i|
    return count if i >= item
    count += 1
  }
  nil
end

def bisect_left3(list, item, lo = 0, hi = list.size)
  while lo < hi
    i = (lo + hi - 1) >> 1

    if 0 <= (list[i] <=> item)
      hi = i
    else
      lo = i + 1
    end
  end
  return hi
end

array = Array.new(1000000) { rand(0..1000000) };
to_find = Array.new(500) { rand(0..10000) };

Benchmark.bm(20) do |x|
  x.report("bisect_left1") do
    to_find.each do |item|
      bisect_left1(array, item)
    end
  end

  x.report("bisect_left2") do
    to_find.each do |item|
      bisect_left2(array, item)
    end
  end

  x.report("bisect_left3") do
    to_find.each do |item|
      bisect_left3(array, item)
    end
  end
end

array = Array.new(100000) { Array.new(rand(1..10)){(97+rand(26)).chr}.join };
to_find = Array.new(500) { (97+rand(26)).chr };

Benchmark.bm(20) do |x|
  x.report("bisect_left1") do
    to_find.each do |item|
      bisect_left1(array, item)
    end
  end

  x.report("bisect_left2") do
    to_find.each do |item|
      bisect_left2(array, item)
    end
  end

  x.report("bisect_left3") do
    to_find.each do |item|
      bisect_left3(array, item)
    end
  end
end
