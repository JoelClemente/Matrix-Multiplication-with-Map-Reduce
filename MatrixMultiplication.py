from mrjob.job import MRJob
from operator import itemgetter

class MatrixMultiplication(MRJob):

    def configure_args(self):
        super(MatrixMultiplication, self).configure_args()
        self.add_passthru_arg('--row_a', type=int, help='Number of rows in matrix A')
        self.add_passthru_arg('--col_b', type=int, help='Number of columns in matrix B')

    def mapper(self, _, line):
        matrix, row, col, value = line.strip().split(",")

        if matrix == "A":
            for i in range(self.options.col_b):
                key = f"{row},{i}"
                yield key, (col, value)
        else:
            for j in range(self.options.row_a):
                key = f"{j},{col}"
                yield key, (row, value)

    def reducer(self, key, values):
        value_list = list(values)
        value_list = sorted(value_list, key=itemgetter(0))
        i = 0
        result = 0

        while i < len(value_list) - 1:
            if value_list[i][0] == value_list[i + 1][0]:
                result += int(value_list[i][1]) * int(value_list[i + 1][1])
                i += 2
            else:
                i += 1

        yield key, result

if __name__ == '__main__':
    MatrixMultiplication.run()
