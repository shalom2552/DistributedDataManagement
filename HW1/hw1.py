import sys
from mrjob.job import MRJob
from mrjob.step import MRStep


class MRValueMean(MRJob):
    def mapper(self, _, line):
        yield "StartedOn", line.split(',')[0].split('/')[1]
        print(line.split(',')[0].split('/')[1])
        yield "value", line.split(',')[1]
        yield "lines", 1

    def reducer(self, day, values):
        yield day, sum(values)


def main(args):

    # MRValueMean().run()
    # path = args[1]
    path = "data_HW1.csv"

    with open(path) as f:
        data = f.readlines()

    for line in data[:10]:
        print(line.replace("\n", ''))

    MRValueMean.run()
    pass


def ex1(data):
    # mean open value of a curtain in a day is a sum of all value of the curtain in a day divided by number of values
    # for each month need to calculate min and max 'mean open' per day
    # return the min and max for each month

    pass


if __name__ == '__main__':
    main(sys.argv)
