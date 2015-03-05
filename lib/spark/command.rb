module Spark
  class Command

    attr_accessor :serializer, :deserializer
    attr_accessor :libraries, :accumulators, :bound_objects

    def initialize
      @serializer = nil
      @deserializer = nil
      @commands = []
      @libraries = []
      @accumulators = []
      @bound_objects = {}
    end

    def build
      @commands.each(&:build)
    end

    def add_command(*args)
      args.each do |arg|
        @commands << arg
      end
    end

    def execute(iterator, split_index)
      # Require necessary libraries
      libraries.each{|lib| require lib}

      # Prepare bound objects
      @commands.each do |command|
        command.__objects__ = @bound_objects
      end

      # Prepare for running
      @commands.each(&:prepare)

      # Run all task
      @commands.each do |command|
        iterator = command.execute(iterator, split_index)
      end

      # Return changed iterator. This is not be necessary for some tasks
      # because of using inplace changing but some task can return
      # only one value (for example reduce).
      iterator
    end

    def last
      @commands.last
    end

  end
end

require "spark/command/base"
require "spark/command/basic"
require "spark/command/pair"
require "spark/command/statistic"
require "spark/command/sort"
