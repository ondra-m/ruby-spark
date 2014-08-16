require "spec_helper"

RSpec::describe Spark::Serializer do
  
  it ".get" do
    expect(described_class.get(nil)).to eql(nil)
    expect(described_class.get("MARSHAL")).to eql(nil)
    expect(described_class.get("Marshal")).to eql(described_class::Marshal)
    expect(described_class.get("marshal")).to eql(described_class::Marshal)
    expect(described_class.get("message_pack")).to eql(described_class::MessagePack)
  end

end
