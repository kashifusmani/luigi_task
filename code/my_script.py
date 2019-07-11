from __future__ import division
import luigi
import random
from collections import defaultdict

from luigi import six
from string import punctuation
from json import dumps, loads
from math import log10
import logging

logger = logging.getLogger('luigi-interface')


class CleanUp(luigi.Task):

    separator = '%'

    def output(self):
        return luigi.LocalTarget("documents_cleanup.txt")

    def run(self):
        stripped_lines = []
        unique_words = set()
        with open('documents_1.txt', 'r') as f:
            prev_doc = ''
            for line in f:
                if not line.strip() == self.separator:
                    for p in punctuation:
                        line = line.replace(p, '')
                    prev_doc = prev_doc + ' ' + line.replace('\n', '')
                else:
                    stripped_lines.append(prev_doc.lower())
                    prev_doc = ''
        with self.output().open('w') as out_file:
            for line in stripped_lines:
                out_file.write(line.strip() + '\n')

        with open('documents_unique', 'w') as out_file:
            for line in stripped_lines:
                for word in line.split(' '):
                    if not word == '':
                        unique_words.add(word)
            for item in unique_words:
                out_file.write(item + "\n")



class ComputeTf(luigi.Task):
    def requires(self):
        return [CleanUp()]

    def output(self):
        return luigi.LocalTarget("documents_tf.txt")

    def run(self):
        all_tfs = []
        for t in self.input():
            print("==========")
            print(t)
            with t.open('r') as in_file:
                for doc in in_file:
                    result = {}
                    count_dt, total_counts = self.count_elems(doc.replace('\n', '').split(' '))
                    for word in count_dt:
                        result[word] = count_dt[word] / total_counts
                    all_tfs.append(result)
        with self.output().open('w') as out_file:
            for item in all_tfs:
                out_file.write(dumps(item) + '\n')

    def count_elems(self, ls):
        ret = {}
        total_counts = 0
        for elem in ls:
            if not elem == '':
                if elem in ret:
                    ret[elem] += 1
                    total_counts +=1
                else:
                    ret[elem] = 1
                    total_counts +=1
        return (ret, total_counts)


class ComputeIdf(luigi.Task):
    def requires(self):
        return [ComputeTf()]

    def output(self):
        return luigi.LocalTarget("documents_idf.txt")

    def run(self):
        tfs = []
        for t in self.input():
            with t.open('r') as in_file:
                for doc in in_file:
                    tfs.append(loads(doc[0:len(doc)-1]))
        counts = {}
        with open('documents_unique', 'r') as in_file:
            for word in in_file:
                word = word[0: len(word) - 1]
                count = 0
                for tf in tfs:
                    if word in tf:
                        count += 1
                counts[word] = log10(len(tfs) / count)
        with self.output().open('w') as out_file:
            out_file.write(dumps(counts))

'''


class Agg(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.Streams.output` and
    writes the result into its :py:meth:`~.AggregateArtists.output` target (local file).
    """

    date_interval = luigi.DateIntervalParameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("data/artist_streams_{}.tsv".format(self.date_interval))

    def requires(self):
        """
        This task's dependencies:

        * :py:class:`~.Streams`

        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [Streams(date) for date in self.date_interval]

    def run(self):
        artist_count = defaultdict(int)

        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    _, artist, track = line.strip().split()
                    artist_count[artist] += 1

        with self.output().open('w') as out_file:
            for artist, count in six.iteritems(artist_count):
                out_file.write('{}\t{}\n'.format(artist, count))


class Streams(luigi.Task):
    """
    Faked version right now, just generates bogus data.
    """
    date = luigi.DateParameter()


    def run(self):
        """
        Generates bogus data and writes it into the :py:meth:`~.Streams.output` target.
        """
        with self.output().open('w') as output:
            for _ in range(1000):
                output.write('{} {} {}\n'.format(
                    random.randint(0, 999),
                    random.randint(0, 999),
                    random.randint(0, 999)))

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in the local file system.

        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget(self.date.strftime('data/streams_%Y_%m_%d_faked.tsv'))
'''
if __name__ == "__main__":
    luigi.run()
