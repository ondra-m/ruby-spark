class Enumerator

  if !method_defined?(:defer)
    def defer(&blk)
      self.class.new do |y|
        each do |*input|
          blk.call(y, *input)
        end
      end
    end
  end

end
