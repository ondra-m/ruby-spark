require 'spec_helper'

RSpec.describe Spark::SQL::DataFrame do

  let(:file) { File.join('spec', 'inputs', 'people.json') }
  let(:df) { $sql.read.json(file) }

  context '[]' do

    it 'String' do
      value = df['age']
      expect(value).to be_a(Spark::SQL::Column)
      expect(value.to_s).to eq('Column("age")')
    end

    it 'Array' do
      value = df[ ['name', 'age'] ]
      expect(value).to be_a(Spark::SQL::DataFrame)
      expect(value.columns).to eq(['name', 'age'])
    end

    it 'Numeric' do
      value = df[0]
      expect(value).to be_a(Spark::SQL::Column)
      expect(value.to_s).to eq('Column("active")')
    end

    it 'Column' do
      value = df[ df[0] == true ]
      expect(value).to be_a(Spark::SQL::DataFrame)
    end

  end

  it 'columns' do
    expect(df.columns).to eq(['active', 'address', 'age', 'email', 'id', 'ip_address', 'name'])
  end

  it 'schema' do
    schema = df.schema
    expect(schema).to be_a(Spark::SQL::StructType)
    expect(schema.type_name).to eq('struct')
  end

  it 'show_string' do
    expect(df.show_string).to start_with('+--')
  end

  it 'dtypes' do
    expect(df.dtypes).to eq([['active', 'boolean'], ['address', 'string'], ['age', 'long'], ['email', 'string'], ['id', 'long'], ['ip_address', 'string'], ['name', 'string']])
  end

  it 'take' do
    expect(df.take(10).size).to eq(10)
  end

  it 'count' do
    expect(df.count).to eq(100)
  end

  context 'select' do

    it '*' do
      row = df.select('*').first
      expect(row.data.keys).to eq(['active', 'address', 'age', 'email', 'id', 'ip_address', 'name'])
    end

    it 'with string' do
      row = df.select('name', 'age').first
      expect(row.data.keys).to eq(['name', 'age'])
    end

    it 'with column' do
      row = df.select(df.name, df.age).first
      expect(row.data.keys).to eq(['name', 'age'])
    end

  end

  context 'where' do

    it 'with string' do
      eq_20 = df.filter('age = 20').collect
      expect(eq_20.map{|c| c['age']}).to all(be == 20)
    end

    it 'with column' do
      nil_values = df.where(df.age.is_null).collect
      greater_or_eq_20 = df.where(df.age >= 20).collect
      lesser_than_20 = df.where(df.age < 20).collect

      expect(nil_values.size + greater_or_eq_20.size + lesser_than_20.size).to eq(df.count)

      expect(nil_values.map{|c| c['age']}).to all(be_nil)
      expect(greater_or_eq_20.map{|c| c['age']}).to all(be >= 20)
      expect(lesser_than_20.map{|c| c['age']}).to all(be < 20)
    end

  end

end
