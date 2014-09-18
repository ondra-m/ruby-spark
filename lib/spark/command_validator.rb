module Spark
  module CommandValidator

    def validate(value, options)
      validate_type(value, options[:type])
    end

    def valid?(value, options)
      begin
        validate(value, options)
        return true
      rescue
        return false
      end
    end

    def validate_type(value, type)
      if !value.is_a?(type)
        error "Value: #{value} should be a #{type} but is #{value.class}."
      end
    end

    def validate_size(array1, array2)
      if array1.size != array2.size
        error "Wrong number of arguments (#{array1.size} for #{array2.size})"
      end
    end

  end
end
