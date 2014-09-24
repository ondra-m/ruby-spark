# require "algorithms"

module Spark
  module ExternalSorter

    include Spark::Helper::System

    # Items from GC can not be completely free so we need some reserve
    MEMORY_RESERVE = 20

    # How many items will be evaluate from iterator
    SLICE_SIZE = 10

    EVAL_N_VALUES = 10

    attr_reader :total_memory, :memory_limit, :serializer

    def initialize(total_memory, serializer)
      @total_memory = total_memory
      @memory_limit = total_memory * (100-MEMORY_RESERVE)
      @serializer   = serializer
    end

    def sort_by(iterator, key_function)
      return to_enum(__callee__, iterator) unless block_given?

      init_temp_folder

      parts = make_parts(iterator, key_function)

      return [] if parts.empty?

      eval_n_values = EVAL_N_VALUES
      eval_n_values = parts.size if parts.size > EVAL_N_VALUES

      heap  = []
      enums = []

      parts.each do |part|
        eval_n_values.times {
          begin
            heap << [part.next, part]
          rescue StopIteration
          end
        }
      end

      while parts.any? || heap.any?
          heap.sort_by!(&key_function)

          eval_n_values.times {
            break if heap.empty?

            item, enum = heap.shift

            yield item

            enums << enum
          }

        while (enum = enums.shift)
          begin
            heap << [enum.next, enum]
          rescue StopIteration
            data.delete(enum)
            enums.delete(enum)
          end
        end
      end

    ensure
      destroy_temp_folder
    end

    private

      def init_temp_folder
        @dir = Dir.mktmpdir
      end

      def destroy_temp_folder
        FileUtils.remove_entry_secure(@dir)
      end

      def make_parts(iterator, key_function)
        parts = []
        tmp   = []

        iterator.each_slice(SLICE_SIZE).with_index do |part, i|
          tmp += part

          if memory_usage > memory_limit
            tmp.sort_by!(&block)
            file = Tempfile.new("part_#{i}", @dir)
            serializer.dump(tmp, file)
            parts << serializer.load_as_enum(file)

            # Some memory will be released
            tmp.clear
          end
        end

        if tmp.any?
          parts << tmp.each
        end

        parts
      end

  end # ExternalSorter
end # Spark
