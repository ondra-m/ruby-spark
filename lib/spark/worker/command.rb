module Spark
  class Command

    attr_accessor :serializer, :deserializer
    attr_accessor :libraries

    def initialize
      @tasks = []
      @libraries = []
    end

    def add_task(*args)
      args.each do |arg|
        @tasks << arg
      end
    end

    def execute(iterator, split_index)
      # Require necessary libraries
      libraries.each{|lib| require lib}

      # Run all task
      @tasks.each do |task|
        iterator = task.execute(iterator, split_index)
      end

      # Return changed iterator. This is not be necessary for some tasks
      # because of using inplace changing but some task can return
      # only one value (for example reduce).
      iterator
    end

    def last
      @tasks.last
    end

  end
end

class Spark::Command
  class Task
    include Spark::Serializer::Helper

    attr_accessor :before
    attr_accessor :exec_function

    def initialize
      @before = ""
      @exec_function = ""
    end

    def <<(data)
      @before << data
    end

    def execute(iterator, split_index)
      # Eval pre initialize functions
      eval(before)

      # Cmpute this task
      eval(exec_function).call(iterator, split_index)
    end
  end
end

