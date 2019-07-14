from __future__ import division

from json import dumps, loads
from math import log10, sqrt, pow
from string import punctuation
import os

import luigi

# temporary documents
cleanup_doc = 'documents_cleanup.txt'
unique_words_doc = 'documents_unique.txt'
tf_documents = 'documents_tf.txt'
idf_documents = 'documents_idf.txt'
tf_idf_documents = 'documents_tf_idf.txt'


class CleanUp(luigi.Task):
    separator = '%'

    input_file_path = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(cleanup_doc)

    def run(self):
        stripped_lines = []
        unique_words = set()
        with open(self.input_file_path, 'r') as f:
            prev_doc = ''
            for line in f:
                if not line.strip() == self.separator:
                    for p in punctuation:
                        line = line.replace(p, '')
                    prev_doc = prev_doc + ' ' + line.replace('\n', '').replace('\t', '')
                else:
                    stripped_lines.append(prev_doc.lower())
                    prev_doc = ''
        with self.output().open('w') as out_file:
            for line in stripped_lines:
                out_file.write(line.strip() + '\n')

        with open(unique_words_doc, 'w') as out_file:
            for line in stripped_lines:
                for word in line.strip().split(' '):
                    if not word == '' and not word == ' ':
                        unique_words.add(word.strip())
            for item in unique_words:
                out_file.write(item + "\n")


class ComputeTf(luigi.Task):
    input_file_path = luigi.Parameter()

    def requires(self):
        return [CleanUp(self.input_file_path)]

    def output(self):
        return luigi.LocalTarget(tf_documents)

    def run(self):
        all_tfs = []
        for t in self.input():
            with t.open('r') as in_file:
                for doc in in_file:
                    result = {}
                    count_dt, total_counts = count_elems(doc.replace('\n', '').split(' '))
                    for word in count_dt:
                        result[word] = count_dt[word] / total_counts
                    all_tfs.append(result)
        with self.output().open('w') as out_file:
            for item in all_tfs:
                out_file.write(dumps(item) + '\n')


class ComputeIdf(luigi.Task):
    input_file_path = luigi.Parameter()

    def requires(self):
        return [ComputeTf(self.input_file_path)]

    def output(self):
        return luigi.LocalTarget(idf_documents)

    def run(self):
        tfs = []
        for t in self.input():
            with t.open('r') as in_file:
                for doc in in_file:
                    tfs.append(loads(doc[0:len(doc) - 1]))
        counts = {}
        with open(unique_words_doc, 'r') as in_file:
            for word in in_file:
                word = word[0: len(word) - 1]
                count = 0
                for tf in tfs:
                    if word in tf:
                        count += 1
                counts[word] = log10(len(tfs) / count)
        with self.output().open('w') as out_file:
            out_file.write(dumps(counts))


class ComputeTfIdf(luigi.Task):
    input_file_path = luigi.Parameter()

    def requires(self):
        return [ComputeIdf(self.input_file_path)]

    def output(self):
        return luigi.LocalTarget(tf_idf_documents)

    def run(self):
        with open(idf_documents, 'r') as idfs_file:
            idfs = loads(idfs_file.read())
        tfs = []
        with open(tf_documents, 'r') as tfs_file:
            for line in tfs_file:
                tfs.append(loads(line))

        for tf in tfs:
            result = {}
            for term in tf:
                result[term] = tf[term] * idfs[term]
            with open(tf_idf_documents, 'a') as out_file:
                out_file.write(dumps(result) + '\n')


class ComputeSimilarity(luigi.Task):
    input_file_path = luigi.Parameter()
    output_file_path = luigi.Parameter()

    def requires(self):
        return [ComputeTfIdf(self.input_file_path)]

    def output(self):
        return luigi.LocalTarget(self.output_file_path)

    def run(self):
        def take_third(elem):
            return elem[2]

        input = []
        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    input.append(loads(line))
        corpus_len = len(input)
        result = []
        for i in range(0, corpus_len):
            rest_len = corpus_len - i - 1
            principle_elem = input[rest_len]
            for k in range(0, rest_len):
                compare_to = input[rest_len - k - 1]
                principle_elem_index = rest_len
                compare_to_elem_index = rest_len - k - 1
                euclidean_distance = compute_euclidean(principle_elem, compare_to)
                result.append((compare_to_elem_index, principle_elem_index, euclidean_distance))

        with self.output().open('w') as out_file:
            for item in sorted(result, key=take_third):
                out_file.write("%s,%s,%s\n" % (str(item[0]), str(item[1]), str(item[2])))

        delete_file(cleanup_doc)
        delete_file(unique_words_doc)
        delete_file(tf_documents)
        delete_file(idf_documents)
        delete_file(tf_idf_documents)


def delete_file(file_path):
    if os.path.isfile(file_path):
        os.remove(file_path)

def count_elems(ls):
    ret = {}
    total_counts = 0
    for elem in ls:
        if not elem == '':
            if elem in ret:
                ret[elem] += 1
                total_counts += 1
            else:
                ret[elem] = 1
                total_counts += 1
    return (ret, total_counts)


def compute_euclidean(principle_elem, compare_to):
    sum_so_far = 0
    for term in principle_elem:
        tf_idf_other = 0
        if term in compare_to:
            tf_idf_other = compare_to[term]
        sum_so_far = sum_so_far + pow((principle_elem[term] - tf_idf_other), 2)
    return sqrt(sum_so_far)


if __name__ == "__main__":
    luigi.run()
