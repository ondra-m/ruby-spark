module Spark
  module Helper
    module Partition
      
      def self.included(base)
        base.send :extend,  Methods
        base.send :include, Methods
      end
     
      module Methods
        # Determine bound of partitioning
        #
        # == Example:
        #   data = [0,1,2,3,4,5,6,7,8,9,10]
        #   determine_bounds(data, 3)
        #   # => [2, 5, 8]
        #
        def determine_bounds(data, num_partitions)
          bounds = []
          count = data.size
          (0...(num_partitions-1)).each do |index|
            bounds << data[count * (index+1) / num_partitions]
          end
          bounds
        end
      end # Methods

    end # Partition
  end # Helper
end # Spark



