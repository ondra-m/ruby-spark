$:.unshift File.dirname(__FILE__) + '/../lib'
require "ruby-spark"

RSpec.configure do |config|
  config.color = true
  config.tty = true

  config.before(:suite) do
    $sc = Spark::Context.new(app_name: "RubySpark", master: "local")
  end
  config.after(:suite) do
  end
end
