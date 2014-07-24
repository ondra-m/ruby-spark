require "spec_helper"

describe "Spark::RDD.map_partitions" do

  context "throught parallelize" do
    it "one worker" do
      rdd = $sc.parallelize(0..5)
      result = rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect

      expect(result).to eql([(0..5).to_a.reduce(:+)])
    end

    it "5 workers" do
      rdd = $sc.parallelize(0...100, 5)
      result = rdd.map_partitions(lambda{|part| part.reduce(:+)}).collect

      expect(result).to eql((0...100).each_slice(20).map{|x| x.reduce(:+)})
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_10.txt") }

    it "one worker" do
      rdd = $sc.text_file(file)
      result = rdd.map_partitions(lambda{|part| part.map(&:to_i).reduce(:+)}).collect

      expect(result).to eq([55])
    end
    
    it "2 worker" do
      rdd = $sc.text_file(file, 2)
      result = rdd.map_partitions(lambda{|part| part.map(&:to_i).reduce(:+)}).collect

      expect(result).to eq([15, 40])
    end
  end

end
