module Spark
  module Helper
    module Parser
      
      def self.included(base)
        base.send :extend,  Methods
        base.send :include, Methods
      end
     
      module Methods
        def to_java_hash(hash)
          hash_map = HashMap.new
          hash.each_pair do |key, value|
            begin
              # RJB raise Object is NULL (but new record is put correctly)
              hash_map.put(key, value)
            rescue RuntimeError
            end
          end
          hash_map
        end

        def convert_to_java_int(data)
          if data.is_a?(Array)
            data.map{|x| JInteger.new(x)}
          else
            JInteger.new(data)
          end
        end

        def to_java_array_list(array)
          array_list = ArrayList.new
          array.each do |item|
            array_list.add(item)
          end
          array_list
        end

        # Parse and convert memory size. Shifting be better but Float doesn't support it.
        #
        # == Examples:
        #   to_memory_size("512mb")
        #   # => 524288
        #
        #   to_memory_size("512 MB")
        #   # => 524288
        #
        #   to_memory_size("512mb", "GB")
        #   # => 0.5
        #
        def to_memory_size(memory, result_unit="KB")
          match = memory.match(/([\d]+)[\s]*([\w]*)/)
          if match.nil?
            raise Spark::ParseError, "Memory has wrong format. Use: 'SIZE UNIT'"
          end

          size = match[1].to_f
          unit = match[2]

          size *= memory_multiplier_based_kb(unit)
          size /= memory_multiplier_based_kb(result_unit)
          size.round(2)
        end

        # Based to KB
        def memory_multiplier_based_kb(type)
          case type.to_s.upcase
          when "G", "GB"
            1048576
          when "M", "MB"
            1024
          when "K", "KB"
            1
          else
            raise Spark::ParseError, "Unsupported type #{type}"
          end
        end

      end # Methods

    end # Parser
  end # Helper
end # Spark


