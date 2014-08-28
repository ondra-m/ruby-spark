# Just declaration
module Spark; end
module Spark::Command; end

class Spark::Command::Template
  attr_accessor :serializer, :deserializer, :stages, :library, :before, :after

  def initialize
    @serializer   = nil
    @deserializer = nil
    @stages  = []
    @library = []
    @before  = []
    @after   = []
  end
end

class Spark::Command::Stage
  attr_accessor :before, :after, :main

  def initialize
    @main = nil
    @before = []
    @after  = []
  end
end
