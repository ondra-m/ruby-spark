require "sourcify"

# Resilient Distributed Dataset

module Spark
  class RDD

    attr_reader :jrdd, :context

    def initialize(jrdd, context)
      @jrdd = jrdd
      @context = context

      @cached = false
      @checkpointed = false
    end



    # =======================================================================    
    # Variables 
    # =======================================================================   

    def default_reduce_partitions
      if @context.conf.contains("spark.default.parallelism")
        @context.default_parallelism
      else
        @jrdd.partitions.size
      end
    end



    # =======================================================================    
    # Compute functions    
    # =======================================================================        


    # jrdd.collect() -> ArrayList
    #     .to_a -> Arrays in Array
    def collect
      Spark::Serializer::UTF8.load(jrdd.collect.to_a)
      # Spark::Serializer::UTF8.load_from_itr(jrdd.collect.iterator)
    end




    def map(f)
      # function = [f, Proc.new {|split, iterator| Enumerator.new{|e| iterator.map{|i| e.yield(@_f.call(i))} }}]
      function = [f, Proc.new {|split, iterator| iterator.map{|i| @_f.call(i)} }]
      PipelinedRDD.new(self, function)
    end

    def flat_map(f)
      function = [f, Proc.new {|split, iterator| iterator.flat_map{|i| @_f.call(i)} }]
      map_partitions_with_index(function)
    end

    def reduce_by_key(f, num_partitions=nil)
      combine_by_key(lambda {|x| x}, f, f, num_partitions)
    end

    def combine_by_key(create_combiner, merge_value, merge_combiners, num_partitions=nil)
      num_partitions ||= default_reduce_partitions
    end

    def map_partitions_with_index(f)
      PipelinedRDD.new(self, f)
    end



    # Aliases
    alias_method :flatMap, :flat_map
    alias_method :reduceByKey, :reduce_by_key
    alias_method :combineByKey, :combine_by_key
    alias_method :mapPartitionsWithIndex, :map_partitions_with_index


    private

      # def collect_through_file(iterator, &block)
      #     # tempFile = NamedTemporaryFile(delete=False, dir=self.ctx._temp_dir)
      #     # tempFile.close()
      #     # self.ctx._writeToFile(iterator, tempFile.name)
      #     # # Read the data into Python and deserialize it:
      #     # with open(tempFile.name, 'rb') as tempFile:
      #     #     for item in self._jrdd_deserializer.load_stream(tempFile):
      #     #         yield item
      #     # os.unlink(tempFile.name)

      #   time = Time.now

      #   file = Tempfile.new("bytes_from_java")
      #   file.close(false) # not unlink_now


      #   puts "2: #{-1*(time - (time=Time.now))*1000}ms"

      #   PythonRDD.writeToFile(iterator, file.path)

      #   puts "3: #{-1*(time - (time=Time.now))*1000}ms"

      #   f = File.open(file.path, "rb")

      #   # it = Enumerator.new do |e|
      #   #   while !f.eof?
      #   #     e.yield(
      #   #       Marshal.load(
      #   #         f.read(
      #   #           f.read(4).unpack("l>")[0] 
      #   #         )
      #   #       )
      #   #     )
      #   #   end
      #   # end.each(&block)

      #   it = []
      #   while !f.eof?
      #     it << Marshal.load(
      #             f.read(
      #               f.read(4).unpack("l>")[0]
      #             )
      #           )
      #   end

      #   puts "4: #{-1*(time - (time=Time.now))*1000}ms"

      #   file.unlink

      #   it


      # end

  end


  class PipelinedRDD < RDD

    def initialize(prev, function)
      @function = function
      @prev_jrdd = prev.jrdd

      @cached = false
      @checkpointed = false

      @context = prev.context
    end

    def jrdd
      return @jrdd_values if @jrdd_values

      command = Marshal.dump(["@_f=#{@function[0].to_source}", @function[1].to_source]).bytes.to_a
      env = @context.environment
      class_tag = @prev_jrdd.classTag

      ruby_rdd = RubyRDD.new(@prev_jrdd.rdd, command, env, Spark.worker_dir, class_tag)
      @jrdd_values = ruby_rdd.asJavaRDD()
      @jrdd_values
    end

  end
end
