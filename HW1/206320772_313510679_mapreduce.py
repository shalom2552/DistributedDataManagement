from mrjob.job import MRJob
from mrjob.step import MRStep
import statistics

DATE, CURTAIN, VALUE = 0, 3, 1


class MRValueMean(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_mean_month),
            MRStep(mapper=self.mapper_sum_days, reducer=self.reducer_)
        ]

    def mapper(self, _, line):
        line = line.split(',')
        if line[0] != 'StartedOn':
            date, curtain, value = [line[DATE].split('/')[0], line[DATE].split('/')[1]], line[CURTAIN], line[VALUE]
            yield [int(date[0]), int(date[1]), curtain], float(value)

    def reducer_mean_month(self, key, value):
        yield key, statistics.mean(value)

    def mapper_sum_days(self, key, value):
        yield (key[1], key[2]), value

    def reducer_(self, key, value):
        values = []
        for val in value:
            values.append(val)
        yield key, round(max(values), 7)
        yield key, round(min(values), 7)


if __name__ == '__main__':
    MRValueMean().run()
