require "spec_helper"

describe Spark::RDD do

  context "map" do
    context "throught parallelize" do
      it "one worker" do
        rdd = $sc.parallelize(0..5)
        result = rdd.map(lambda {|x| x*2}).collect

        expect(result).to eq([0, 2, 4, 6, 8, 10])
      end
      
      it "5 worker" do
        rdd = $sc.parallelize(0..100, 5)
        result = rdd.map(lambda {|x| x*2}).collect

        expect(result).to eq((0..100).to_a.map{|x| x*2})
      end
    end

    context "throught text_file" do
      let(:file) { File.join("spec", "inputs", "numbers_0_10.txt") }

      it "one worker" do
        rdd = $sc.text_file(file)
        result = rdd.map(lambda {|x| x.to_i*2}).collect

        expect(result).to eq([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20])
      end
      
      it "3 worker" do
        rdd = $sc.text_file(file, 3)
        result = rdd.map(lambda {|x| x.to_i*2}).collect

        expect(result).to eq([0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20])
      end
    end
  end

end
