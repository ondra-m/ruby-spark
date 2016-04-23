require 'spec_helper'

RSpec.shared_examples 'binary comparison' do |op|
  it "#{op}" do
    to_test = 20

    result = df.select('age').where( df.age.__send__(op, to_test) ).values.flatten
    result.each do |item|
      if op == '!='
        expect(item).to_not eq(to_test)
      else
        expect(item).to be.__send__(op, to_test)
      end
    end
  end
end

RSpec.describe Spark::SQL::Column do

  let(:file) { File.join('spec', 'inputs', 'people.json') }
  let(:df) { $sql.read.json(file) }

  let(:data) do
    # Data are line delimited
    result = []
    File.readlines(file).each do |line|
      result << JSON.parse(line)
    end
    result
  end

  context 'operators' do
    it 'func' do
      result = df.select( df.id, df.active, ~df.id, !df.active ).collect_as_hash.map(&:values)
      result.each do |item|
        expect(item[0]).to eq(-item[2])
        expect(item[1]).to eq(!item[3])
      end
    end

    context 'binary' do
      it 'arithmetic' do
        result = df.select( df.id, df.id+1, df.id-1, df.id*2, df.id/2, df.id%2 ).collect_as_hash.map(&:values)
        result.each do |item|
          expect(item[1]).to eq(item[0]+1)
          expect(item[2]).to eq(item[0]-1)
          expect(item[3]).to eq(item[0]*2)
          expect(item[4]).to eq(item[0]/2.0)
          expect(item[5]).to eq(item[0]%2)
        end
      end

      # comparison
      it_behaves_like 'binary comparison', '=='
      it_behaves_like 'binary comparison', '!='
      it_behaves_like 'binary comparison', '<'
      it_behaves_like 'binary comparison', '<='
      it_behaves_like 'binary comparison', '>'
      it_behaves_like 'binary comparison', '>='

      it 'logical' do
        result = df.select('id').where( (df.id >= 20) & (df.id <= 30) ).values.flatten
        expect(result).to all( be_between(20, 30) )

        result = df.select('id').where( (df.id == 1) | (df.id == 2) ).values.flatten
        expect(result).to eq([1, 2])
      end

      it 'like' do
        result = df.select('email').where( df.email.like('%com%') ).values.flatten
        expect(result).to all( include('com') )
      end

      it 'null' do
        result1 = df.select('address').where( df.address.is_null ).values.flatten
        result2 = df.select('address').where( df.address.is_not_null ).values.flatten

        expect(result1).to all( be_nil )
        expect(result2).to all( be_an(String) )
      end
    end
  end

  it 'substr' do
    result = df.select( df.name.substr(1, 3) ).values.flatten
    result.each do |item|
      expect(item.size).to eq(3)
    end
  end

  it 'isin' do
    result = df.select('age').where( df.age.isin(20, 21, 22) ).values.flatten
    expect(result).to all( eq(20).or eq(21).or eq(22) )
  end

  it 'alias' do
    result = df.select( df.id.as('id2') ).collect_as_hash.map(&:keys).flatten
    expect(result).to all( eq('id2') )
  end

  it 'cast' do
    result = df.select( df.id, df.id.cast('string').alias('age2') ).values
    result.each do |item|
      expect(item[0]).to be_an(Integer)
      expect(item[0].to_s).to eq(item[1])
    end
  end

  it 'when, otherwise' do
    result = df.select(df.id, Spark::SQL::Column.when(df.id <= 20, 1).when(df.id >= 30, 3).otherwise(2)).values
    result.each do |item|
      id = item[0]
      value = item[1]

      if id <= 20
        expect(value).to eq(1)
      elsif id >= 30
        expect(value).to eq(3)
      else
        expect(value).to eq(2)
      end
    end
  end

end
