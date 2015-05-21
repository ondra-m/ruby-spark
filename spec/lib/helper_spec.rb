require 'spec_helper'

RSpec.configure do |c|
  c.include Spark::Helper::Parser
end

RSpec.describe Spark::Helper do

  it 'memory size' do
    expect(to_memory_size('512mb')).to eql(524288.0)
    expect(to_memory_size('1586 mb')).to eql(1624064.0)
    expect(to_memory_size('3 MB')).to eql(3072.0)
    expect(to_memory_size('9gb')).to eql(9437184.0)
    expect(to_memory_size('9gb', 'mb')).to eql(9216.0)
    expect(to_memory_size('9mb', 'gb')).to eql(0.01)
    expect(to_memory_size('6652548796kb', 'mb')).to eql(6496629.68)
  end

end
