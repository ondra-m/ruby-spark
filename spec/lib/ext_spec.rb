require "spec_helper"

RSpec::describe Array do

  it ".deep_copy" do
    data = ["a", "b", "c"]
    new_data = data.dup

    data[0] << "a"

    expect(data).to eql(new_data)

    new_data = data.deep_copy

    data[1] << "b"

    expect(data).to_not eql(new_data)
  end

end

RSpec::describe Hash do

  it ".stringify_keys!" do
    data = {
      a: "a",
      b: "b",
      c: "c"
    }

    data.stringify_keys!

    expect(data).to eql({
      "a" => "a",
      "b" => "b",
      "c" => "c"
    })
  end

end

RSpec::describe String do
  
  it ".camelize" do
    data = "aaa_bbb_ccc".camelize
    expect(data).to eql("AaaBbbCcc")
  end

end
