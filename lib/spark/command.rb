module Spark
  ##
  # Container which includes all commands and other things for worker
  # Every RDD have own copy of Command
  #
  class Command

    attr_accessor :serializer, :deserializer, :commands, :libraries, :bound_objects

    def initialize
      @serializer = nil
      @deserializer = nil
      @commands = []
      @libraries = []
      @bound_objects = {}
    end

    def execute(iterator, split_index)
      # Require necessary libraries
      libraries.each{|lib| require lib}

      # Prepare bound objects
      @commands.each do |command|
        command.__objects__ = bound_objects
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

    def bound_objects
      # Objects from users
      # Already initialized objects on worker
      return @bound_objects if @bound_objects

      if @serialized_bound_objects
        # Still serialized
        @bound_objects = Marshal.load(@serialized_bound_objects)
      else
        # Something else
        @bound_objects = {}
      end
    end

    # Bound objects can depend on library which is loaded during @execute
    # In that case worker raise "undefined class/module"
    def marshal_dump
      [@serializer, @deserializer, @commands, @libraries, serialized_bound_objects]
    end

    def marshal_load(array)
      @serializer = array.shift
      @deserializer = array.shift
      @commands = array.shift
      @libraries = array.shift
      @serialized_bound_objects = array.shift
    end

    private

      def serialized_bound_objects
        @serialized_bound_objects ||= Marshal.dump(@bound_objects)
      end

  end
end

require 'spark/command/base'
require 'spark/command/basic'
require 'spark/command/pair'
require 'spark/command/statistic'
require 'spark/command/sort'
