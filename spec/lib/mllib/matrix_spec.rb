require 'spec_helper'

RSpec.describe 'Spark::Mllib::Matrix' do
  context 'dense' do
    it 'construct' do
      values = [[1.0, 0.0, 4.0], [0.0, 3.0, 5.0], [2.0, 0.0, 6.0]]
      matrix = DenseMatrix.new(3, 3, [[1.0, 0.0, 4.0], [0.0, 3.0, 5.0], [2.0, 0.0, 6.0]])

      expect(matrix.shape).to eq([3, 3])
      expect(matrix.values).to eq([[1.0, 0.0, 4.0], [0.0, 3.0, 5.0], [2.0, 0.0, 6.0]])
    end
  end

  context 'sparse' do
    it 'construct' do
      values = [1.0, 2.0, 4.0, 5.0]
      column_pointers = [0, 2, 2, 4, 4]
      row_indices = [1, 2, 1, 2]

      matrix = SparseMatrix.new(3, 4, column_pointers, row_indices, values)

      expect(matrix.shape).to eq([3, 4])
      expect(matrix.to_a).to eq(
        [
          [0.0, 0.0, 0.0, 0.0],
          [1.0, 0.0, 4.0, 0.0],
          [2.0, 0.0, 5.0, 0.0]
        ]
      )
    end
  end
end
