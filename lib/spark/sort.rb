module Spark
  module InternalSorter
    class Base
      def initialize(key_function)
        @key_function = key_function
      end
    end

    class Ascending < Base
      def sort(data)
        data.sort_by!(&@key_function)
      end
    end

    class Descending < Ascending
      def sort(data)
        super
        data.reverse!
      end
    end

    def self.get(ascending, key_function)
      if ascending
        type = Ascending
      else
        type = Descending
      end

      type.new(key_function)
    end
  end
end


module Spark
  class ExternalSorter

    include Spark::Helper::System

    # Items from GC cannot be destroyed so #make_parts need some reserve
    MEMORY_RESERVE = 50 # %

    # How big will be chunk for adding new memory because GC not cleaning
    # immediately un-referenced variables
    MEMORY_FREE_CHUNK = 10 # %

    # How many items will be evaluate from iterator at start
    START_SLICE_SIZE = 10

    # Maximum of slicing. Memory control can be avoided by large value.
    MAX_SLICE_SIZE = 10_000

    # How many values will be taken from each enumerator.
    EVAL_N_VALUES = 10

    # Default key function
    KEY_FUNCTION = lambda{|item| item}

    attr_reader :total_memory, :memory_limit, :memory_chunk, :serializer

    def initialize(total_memory, serializer)
      @total_memory = total_memory
      @memory_limit = total_memory * (100-MEMORY_RESERVE)    / 100
      @memory_chunk = total_memory * (100-MEMORY_FREE_CHUNK) / 100
      @serializer   = serializer
    end

    def add_memory!
      @memory_limit += memory_chunk
    end

    def sort_by(iterator, ascending=true, key_function=KEY_FUNCTION)
      return to_enum(__callee__, iterator, key_function) unless block_given?

      create_temp_folder
      internal_sorter = Spark::InternalSorter.get(ascending, key_function)

      # Make N sorted enumerators
      parts = make_parts(iterator, internal_sorter)

      return [] if parts.empty?

      # Need new key function because items have new structure
      # From: [1,2,3] to [[1, Enumerator],[2, Enumerator],[3, Enumerator]]
      key_function_with_enum = lambda{|(key, _)| key_function[key]}
      internal_sorter = Spark::InternalSorter.get(ascending, key_function_with_enum)

      heap  = []
      enums = []

      # Load first items to heap
      parts.each do |part|
        EVAL_N_VALUES.times {
          begin
            heap << [part.next, part]
          rescue StopIteration
            break
          end
        }
      end

      # Parts can be empty but heap not
      while parts.any? || heap.any?
        internal_sorter.sort(heap)

        # Since parts are sorted and heap contains EVAL_N_VALUES method
        # can add EVAL_N_VALUES items to the result
        EVAL_N_VALUES.times {
          break if heap.empty?

          item, enum = heap.shift
          enums << enum

          yield item
        }

        # Add new element to heap from part of which was result item
        while (enum = enums.shift)
          begin
            heap << [enum.next, enum]
          rescue StopIteration
            parts.delete(enum)
            enums.delete(enum)
          end
        end
      end

    ensure
      destroy_temp_folder
    end

    private

      def create_temp_folder
        @dir = Dir.mktmpdir
      end

      def destroy_temp_folder
        FileUtils.remove_entry_secure(@dir) if @dir
      end

      # New part is created when current part exceeds memory limit (is variable)
      # Every new part have more memory because of ruby GC
      def make_parts(iterator, internal_sorter)
        slice = START_SLICE_SIZE

        parts = []
        part  = []

        loop do
          begin
            # Enumerator does not have slice method
            slice.times { part << iterator.next }
          rescue StopIteration
            break
          end

          # Carefully memory_limit is variable
          if memory_usage > memory_limit
            # Sort current part with origin key_function
            internal_sorter.sort(part)
            # Tempfile for current part
            # will be destroyed on #destroy_temp_folder
            file = Tempfile.new("part", @dir)
            serializer.dump(part, file)
            # Peek is at the end of file
            file.seek(0)
            parts << serializer.load(file)

            # Some memory will be released but not immediately
            # need some new memory for start
            part.clear
            add_memory!
          else
            slice = [slice*2, MAX_SLICE_SIZE].min
          end
        end

        # Last part which is not in the file
        if part.any?
          internal_sorter.sort(part)
          parts << part.each
        end

        parts
      end

  end # ExternalSorter
end # Spark
