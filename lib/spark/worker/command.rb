module Spark
  class Command

    attr_accessor :serializer, :deserializer
    attr_accessor :libraries

    def initialize
      @tasks = []
      @libraries = []
    end

    def build
      @tasks.each(&:build)
    end

    def deep_copy
      Marshal.load(Marshal.dump(self))
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
  class Base
    include Spark::Serializer::Helper

    def build
    end
  end

  class SimpleTask < Base

    attr_accessor :before
    attr_accessor :exec_function

    def initialize(args)
      @before = ""
      @exec_function = args.is_a?(Array) ? args.first : args
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

  class SampleTask < Base

    attr_accessor :sampler

    def initialize(sampler)
      @sampler = sampler
    end

    def build
      @sampler = Marshal.dump(@sampler)
    end

    def execute(iterator, *)
      # First require Spark::Sampler, otherwise Marshal.load raise error
      # for non-existing class
      require_sampler
      
      # Restore sampler
      @sampler = Marshal.load(@sampler)

      # Get sample
      @sampler.sample(iterator)
    end

    private

      def require_sampler
        require File.expand_path(File.join("..", "sampler"), File.dirname(__FILE__))
      end

  end
end

