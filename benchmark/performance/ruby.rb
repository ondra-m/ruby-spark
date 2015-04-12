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

@workers = ENV['WORKERS'].to_i
@numbers = (0...ENV['NUMBERS_COUNT'].to_i).to_a
@random_file_path = ENV['RANDOM_FILE_PATH']
@random_strings = File.read(@random_file_path).split


# =============================================================================
# Serialization
# =============================================================================


time = Benchmark.realtime do
  @rdd_numbers = sc.parallelize(@numbers, @workers)
end

log('NumbersSerialization', time)


time = Benchmark.realtime do
  @rdd_strings = sc.parallelize(@random_strings, @workers)
end

log('RandomStringSerialization', time)


time = Benchmark.realtime do
  @rdd_file_string = sc.text_file(@random_file_path, @workers)
end

log('TextFileSerialization', time)









$log_file.close
