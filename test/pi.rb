#!/bin/ruby

require 'ruby-spark'

puts Spark.root
Dir.glob(File.join(File.expand_path  "../target/*", File.dirname(__FILE__))){|file| require file }
require File.expand_path("../target/ruby-spark.jar", File.dirname(__FILE__))

sc = Spark::Context.new(app_name: "RubySpark", master: "local")

slices = 3

n = 100000 * slices

f = lambda {
  x = rand() * 2 - 1
  y = rand() * 2 - 1
  x ** 2 + y ** 2 < 1 ? 1 : 0
}
count = sc.parallelize((1..n).to_a, slices).map(f).collect.reduce(:+)
print "Pi is roughly %f" % (4.0 * count / n)
