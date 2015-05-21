require 'spec_helper'

RSpec.shared_examples 'a sorting' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers)

    rdd2 = rdd2.flat_map(split)
    result = lines.flat_map(&split)

    # Sort by self
    rdd3 = rdd2.map(map).sort_by_key
    result2 = result.map(&map).sort_by{|(key, _)| key}

    expect(rdd3.collect).to eql(result2)

    # Sort by len
    rdd3 = rdd2.map(len_map).sort_by_key
    result2 = result.map(&len_map).sort_by{|(key, _)| key}

    expect(rdd3.collect).to eql(result2)
  end
end


RSpec.describe 'Spark::RDD' do
  let(:split)   { lambda{|x| x.split} }
  let(:map)     { lambda{|x| [x.to_s, 1]} }
  let(:len_map) { lambda{|x| [x.size, x]} }

  context 'throught parallelize' do
    context '.map' do
      let(:lines) { Generator.lines }

      def rdd(workers)
        $sc.parallelize(lines, workers)
      end

      it_behaves_like 'a sorting', 1
      it_behaves_like 'a sorting', 2
      # it_behaves_like 'a sorting', nil
      # it_behaves_like 'a sorting', rand(2..10)
    end
  end

  context 'throught text_file' do
    context '.map' do
      let(:file)  { File.join('spec', 'inputs', 'lorem_300.txt') }
      let(:lines) { File.readlines(file).map(&:strip) }

      def rdd(workers)
        $sc.text_file(file, workers)
      end

      it_behaves_like 'a sorting', 1
      it_behaves_like 'a sorting', 2
      # it_behaves_like 'a sorting', nil
      # it_behaves_like 'a sorting', rand(2..10)
    end
  end
end
