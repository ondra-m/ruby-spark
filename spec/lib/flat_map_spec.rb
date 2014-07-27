require "spec_helper"

describe "Spark::RDD.flat_map" do

  context "throught parallelize" do
    it "one worker" do
      rdd = $sc.parallelize(0..5)
      result = rdd.flat_map(lambda {|x| [x*2, x]}).collect

      expect(result).to eq([0, 0, 2, 1, 4, 2, 6, 3, 8, 4, 10, 5])
    end
    
    it "5 worker" do
      rdd = $sc.parallelize(0..100, 5)
      result = rdd.flat_map(lambda {|x| [x*2, x]}).collect

      expect(result).to eq((0..100).to_a.map{|x| [x*2, x]}.flatten)
    end
  end

  context "throught text_file" do
    let(:file) { File.join("spec", "inputs", "numbers_0_10.txt") }

    it "one worker" do
      rdd = $sc.text_file(file)
      result = rdd.flat_map(lambda {|x| [x.to_i*2, 1]}).collect

      expect(result).to eq([0, 1, 2, 1, 4, 1, 6, 1, 8, 1, 10, 1, 12, 1, 14, 1, 16, 1, 18, 1, 20, 1])
    end

    it "4 workers" do
      rdd = $sc.text_file(file, 4)
      result = rdd.flat_map(lambda {|x| [x.to_i*2, 1]}).collect

      expect(result).to eq([0, 1, 2, 1, 4, 1, 6, 1, 8, 1, 10, 1, 12, 1, 14, 1, 16, 1, 18, 1, 20, 1])
    end
  end

end
