require 'spec_helper'

RSpec.describe Array do

  it '.deep_copy' do
    data = ['a', 'b', 'c']
    new_data = data.dup

    data[0] << 'a'

    expect(data).to eql(new_data)

    new_data = data.deep_copy

    data[1] << 'b'

    expect(data).to_not eql(new_data)
  end

end

RSpec.describe Hash do

  it '.stringify_keys!' do
    data = {
      a: 'a',
      b: 'b',
      c: 'c'
    }

    data.stringify_keys!

    expect(data).to eql({
      'a' => 'a',
      'b' => 'b',
      'c' => 'c'
    })
  end

end

RSpec.describe String do

  it '.camelize' do
    data = 'aaa_bbb_ccc'.camelize
    expect(data).to eql('AaaBbbCcc')
  end

end

RSpec.describe IO do

  it 'serialize' do
    file = Tempfile.new('serialize')
    file.binmode

    file.write_int(1)
    file.write_long(2)
    file.write_string('3')
    file.write_data([4])

    file.rewind

    expect(file.read_int).to eq(1)
    expect(file.read_long).to eq(2)
    expect(file.read_string).to eq('3')
    expect(file.read_data).to eq([4])

    file.unlink
  end

end
