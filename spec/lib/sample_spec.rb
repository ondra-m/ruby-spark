require "spec_helper"

# Sample method can not be tested because of random generator
# Just test it for raising error

RSpec::describe "Spark::RDD.sample" do
  
  it "with replacement" do
    rdd = $sc.parallelize(0..100)
    rdd = rdd.sample(true, 0.5)

    expect { rdd.collect }.to_not raise_error
  end

  it "without replacement" do
    rdd = $sc.parallelize(0..100)
    rdd = rdd.sample(false, 0.5)

    expect { rdd.collect }.to_not raise_error
  end

end
