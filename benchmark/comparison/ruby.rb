#!/usr/bin/env ruby

lib = File.expand_path(File.dirname(__FILE__) + '/../../lib')
$LOAD_PATH.unshift(lib) if File.directory?(lib) && !$LOAD_PATH.include?(lib)

require 'ruby-spark'
require 'benchmark'

Spark.start
sc = Spark.context

$log_file = File.open(ENV['RUBY_LOG'], 'w')

def log(*values)
  $log_file.puts(values.join(';'))
end

workers = ENV['WORKERS'].to_i
numbers_count = ENV['NUMBERS_COUNT'].to_i
text_file = ENV['TEXT_FILE']

numbers = (0...numbers_count).to_a
floats = numbers.map(&:to_f)
strings = File.read(text_file).split("\n")


# =============================================================================
# Serialization
# =============================================================================

time = Benchmark.realtime do
  @rdd_numbers = sc.parallelize(numbers, workers)
end

log('NumbersSerialization', time)


time = Benchmark.realtime do
  @rdd_floats = sc.parallelize(floats, workers)
end

log('FloatsSerialization', time)


time = Benchmark.realtime do
  @rdd_strings = sc.parallelize(strings, workers)
end

log('StringsSerialization', time)


# =============================================================================
# Computing
# =============================================================================


# --- Is prime? ---------------------------------------------------------------

is_prime = Proc.new do |x|
  case
  when x < 2
    [x, false]
  when x == 2
    [x, true]
  when x % 2 == 0
    [x, false]
  else
    upper = Math.sqrt(x.to_f).to_i
    result = true

    i = 3
    while i <= upper
      if x % i == 0
        result = false
        break
      end

      i += 2
    end

    [x, result]
  end
end

time = Benchmark.realtime do
  @rdd_numbers.map(is_prime).collect
end

log('IsPrime', time)


# --- Matrix multiplication ---------------------------------------------------

matrix_size = ENV['MATRIX_SIZE'].to_i

matrix = Array.new(matrix_size) do |row|
  Array.new(matrix_size) do |col|
    row+col
  end
end;

multiplication_func = Proc.new do |matrix|
  size = matrix.size

  Array.new(size) do |row|
    Array.new(size) do |col|
      matrix[row]

      result = 0
      size.times do |i|
        result += matrix[row][i] * matrix[col][i]
      end
      result
    end
  end
end

time = Benchmark.realtime do
  rdd = sc.parallelize(matrix, 1)
  rdd.map_partitions(multiplication_func).collect
end

log('MatrixMultiplication', time)


# --- Pi digits ---------------------------------------------------------------
# http://rosettacode.org/wiki/Pi#Ruby

pi_digit = ENV['PI_DIGIT'].to_i

pi_func = Proc.new do |size|
  size = size.first
  result = ''

  q, r, t, k, n, l = 1, 0, 1, 1, 3, 3
  while size > 0
    if 4*q+r-t < n*t
      result << n.to_s
      size -= 1
      nr = 10*(r-n*t)
      n = ((10*(3*q+r)) / t) - 10*n
      q *= 10
      r = nr
    else
      nr = (2*q+r) * l
      nn = (q*(7*k+2)+r*l) / (t*l)
      q *= k
      t *= l
      l += 2
      k += 1
      n = nn
      r = nr
    end
  end

  [result]
end

time = Benchmark.realtime do
  rdd = sc.parallelize([pi_digit], 1)
  rdd.map_partitions(pi_func).collect
end

log('PiDigit', time)


$log_file.close
