$:.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"

class Example
  attr_accessor :data, :file, :function, :result

  include RSpec::Matchers

  def initialize
    @function = []
  end

  def rdd
    if file
      rdd = $sc.text_file(file, @worker_count)
    elsif data
      rdd = $sc.parallelize(data, @worker_count)
    else
      raise "Choose file or data"
    end

    @function.each do |method, arg|
      if arg
        rdd = rdd.send(method.to_sym, arg)
      else
        rdd = rdd.send(method.to_sym)
      end
    end

    rdd
  end

  def workers(worker_count)
    @worker_count = worker_count
    self
  end

  def run(result=nil)
    if result
      expect(rdd.collect).to eq(result)
    else
      expect(rdd.collect).to eq(@result)
    end
  end
end

RSpec.configure do |config|
  config.color = true
  config.tty   = true

  config.before(:suite) do
    Spark.disable_log
    $sc = Spark::Context.new(app_name: "RubySpark", master: "local[*]")
  end
  config.after(:suite) do
    Spark.destroy_all
  end
end
