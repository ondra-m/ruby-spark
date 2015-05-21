require 'spec_helper'

RSpec.shared_examples 'a whole_text_files' do |workers|
  it "with #{workers || 'default'} worker" do
    rdd2 = rdd(workers).map(get_numbers)
    result = files.size

    expect(rdd2.collect.size).to eql(result)

    rdd3 = rdd(workers)
    rdd3 = rdd3.flat_map(get_numbers)

    result = 0
    files.each{|f| result += File.read(f).split.map(&:to_i).reduce(:+)}

    expect(rdd3.sum).to eql(result)
  end
end

RSpec.describe 'Spark::Context' do
  let(:get_numbers) { lambda{|file, content| content.split.map(&:to_i)} }

  let(:dir)   { File.join('spec', 'inputs', 'numbers') }
  let(:files) { Dir.glob(File.join(dir, '*')) }

  def rdd(workers)
    $sc.whole_text_files(dir, workers)
  end

  it_behaves_like 'a whole_text_files', 1
  it_behaves_like 'a whole_text_files', 2
  # it_behaves_like 'a whole_text_files', nil
  # it_behaves_like 'a whole_text_files', rand(2..10)
end
