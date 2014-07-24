require "spec_helper"

describe Spark::RDD do

  context "map" do
    it "first" do
      rdd = $sc.parallelize(0..5)
      result = rdd.map(lambda {|x| x*2}).collect

      expect(result).to eq([0, 2, 4, 6, 8, 10])
    end
  end

end
