# require 'simplecov'
# SimpleCov.start

$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"
require "generator"

# Keep it on method because its called from config test
def spark_start
  Spark::Logger.disable
  Spark.config do
    set "spark.ruby.parallelize_strategy", "deep_copy"
    set "spark.ruby.batch_size", 100
  end
  Spark.start
  $sc = Spark.context
end

RSpec.configure do |config|
  config.default_formatter = "doc"
  config.color = true
  config.tty   = true

  config.before(:suite) do
    spark_start
  end
  config.after(:suite) do
    Spark.stop
  end
end
