require 'spec_helper'
require 'zlib'

RSpec.describe Spark::Serializer do
  let(:data) { [1, 'test', 2.0, [3], {key: 'value'}, :test, String] }

  it 'find' do
    expect(described_class.find('not_existed_class')).to eql(nil)

    expect(described_class.find('Marshal')).to eq(described_class::Marshal)
    expect(described_class.find('marshal')).to eq(described_class::Marshal)
    expect(described_class.find(:marshal)).to eq(described_class::Marshal)
    expect(described_class.find('batched')).to eq(described_class::Batched)
    expect(described_class.find('base')).to eq(described_class::Simple)
    expect(described_class.find('basic')).to eq(described_class::Simple)
    expect(described_class.find('simple')).to eq(described_class::Simple)
  end

  it 'find!' do
    expect { expect(described_class.find!('not_existed_class')) }.to raise_error(Spark::SerializeError)
    expect { expect(described_class.find!('marshal')) }.to_not raise_error
    expect { expect(described_class.find!('simple')) }.to_not raise_error
  end

  it 'register' do
    NewSerializer = Class.new

    expect(described_class.find('new_serializer_1')).to eql(nil)
    expect(described_class.find('new_serializer_2')).to eql(nil)
    expect(described_class.find('new_serializer_3')).to eql(nil)

    described_class.register('new_serializer_1', 'new_serializer_2', 'new_serializer_3', NewSerializer)

    expect(described_class.find('new_serializer_1')).to eql(NewSerializer)
    expect(described_class.find('new_serializer_2')).to eql(NewSerializer)
    expect(described_class.find('new_serializer_3')).to eql(NewSerializer)
  end

  it '==' do
    # One class
    marshal1 = described_class::Marshal.new
    marshal2 = described_class::Marshal.new

    expect(marshal1).to eq(marshal1)
    expect(marshal1).to eq(marshal2)

    # Two classes
    batched1 = described_class::Batched.new(marshal1, 1)
    batched2 = described_class::Batched.new(marshal1, 2)
    batched3 = described_class::Batched.new(marshal2, 1)

    expect(batched1).to eq(batched1)
    expect(batched1).to eq(batched3)
    expect(batched1).to_not eq(batched2)

    # Three classes
    simple1 = described_class::Simple.new(batched1)
    simple2 = described_class::Simple.new(batched2)
    simple3 = described_class::Simple.new(batched3)

    expect(simple1).to eq(simple3)
    expect(simple1).to_not eq(simple2)
  end

  it 'build' do
    marshal1 = described_class::Marshal.new
    compressed1 = described_class::Compressed.new(marshal1)
    batched1 = described_class::Batched.new(compressed1, 1)

    expect(described_class.build{ marshal }).to eq(marshal1)
    expect(described_class.build{ compressed(marshal) }).to eq(compressed1)
    expect(described_class.build{ batched(compressed(marshal), 1) }).to eq(batched1)
  end


  it 'serialization' do
    marshal1 = described_class.build{ marshal }
    compressed1 = described_class.build{ compressed(marshal) }

    expect(marshal1.dump(data)).to eq(Marshal.dump(data))
    expect(compressed1.dump(data)).to eq(
      Zlib::Deflate.deflate(Marshal.dump(data))
    )
  end
end
