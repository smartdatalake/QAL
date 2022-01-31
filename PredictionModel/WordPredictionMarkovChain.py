import re
from nltk.tokenize import word_tokenize
from collections import defaultdict, Counter
import Tools
from flask import Flask
from flask import request
from pandas import DataFrame
import numpy as np
import tensorflow as tf
import sys
from keras.models import model_from_json

dataName = "processWord_1800Gap_2processMinLength_10000maxQueryRepetition_1featureMinFRQ_10reserveFeature_2007fromYear"
parentDir = "/home/hamid/QAL/DensityCluster/"
dataPath = parentDir + dataName
infoPath = parentDir + "processWord_1800Gap_2processMinLength_10000maxQueryRepetition_1featureMinFRQ_10reserveFeature_2007fromYear" + "_vec2FeatureAndWordAndFRQ"
padding = 5


class MarkovChain:
    def __init__(self):
        self.lookup_dict = defaultdict(list)

    def padder(self, process):
        if len(process) < 1:
            return ""
        begin = ''.join(["S" + str(x) + " " for x in range(1, padding + 1)])
        end = ''.join([" E" + str(x) for x in range(1, padding + 1)])
        return begin + process + end

    def _preprocess(self, string):
        cleaned = re.sub(r"\W+", ' ', '\n'.join(list(map(self.padder, string.split('\n'))))).lower()
        tokenized = word_tokenize(cleaned)
        return tokenized

    def __generate_tuple_keys(self, data):
        if len(data) < 1:
            return
        for i in range(len(data) - 1):
            if not data[i].__contains__("e") and not data[i + 1].__contains__("s"):
                yield [data[i], data[i + 1]]

    def add_document_fixed(self, string):
        preprocessed_list = self._preprocess(string)
        p = self.__generate_Ntuple_To_Ntuple(preprocessed_list, padding)
        for pair in p:
            print(pair)
            self.lookup_dict[pair[0]].append(pair[1])

    def add_document(self, string):
        preprocessed_list = self._preprocess(string)
        pairs = self.__generate_tuple_keys(preprocessed_list)
        for pair in pairs:
            self.lookup_dict[pair[0]].append(pair[1])
        pairs2 = self.__generate_2tuple_keys(preprocessed_list)
        for pair in pairs2:
            self.lookup_dict[tuple([pair[0], pair[1]])].append(pair[2])
        pairs3 = self.__generate_3tuple_keys(preprocessed_list)
        for pair in pairs3:
            self.lookup_dict[tuple([pair[0], pair[1], pair[2]])].append(pair[3])

    # to add two words tuple as key and the next word as value
    def __generate_2tuple_keys(self, data):
        if len(data) < 2:
            return
        for i in range(len(data) - 2):
            if not data[i].__contains__("e") and not data[i + 2].__contains__("s"):
                yield [data[i], data[i + 1], data[i + 2]]

    # to add three words tuple as key and the next word as value
    def __generate_3tuple_keys(self, data):
        if len(data) < 3:
            return
        for i in range(len(data) - 3):
            if not data[i].__contains__("e") and not data[i + 3].__contains__("s"):
                yield [data[i], data[i + 1], data[i + 2], data[i + 3]]

    def __generate_Ntuple_To_Ntuple(self, data, n):
        if len(data) < n + n:
            return
        for i in range(len(data) - n):
            if not data[i].__contains__("e") and not data[i + n - 1].__contains__("e") and not data[i + n].__contains__(
                    "s"):
                yield [tuple(data[i + x] for x in range(0, n)), tuple(data[i + n + x] for x in range(0, n))]

    def oneword(self, string):
        return Counter(self.lookup_dict[string]).most_common()[:3]

    def twowords(self, string):
        suggest = Counter(self.lookup_dict[tuple(string)]).most_common()[:3]
        if len(suggest) == 0:
            return self.oneword(string[-1])
        return suggest

    def threewords(self, string):
        suggest = Counter(self.lookup_dict[tuple(string)]).most_common()[:3]
        if len(suggest) == 0:
            return self.twowords(string[-2:])
        return suggest

    def morewords(self, string):
        suggest = Counter(self.lookup_dict[tuple(string)]).most_common()[:padding]
        if len(suggest) == 0:
            return self.twowords(string[-2:])
        return suggest

    def generate_text(self, strin):
        begin = ''.join(["s" + str(x) + " " for x in range(1 + len(strin.split(" ")), padding + 1)])
        string = begin + strin
        if len(self.lookup_dict) > 0:
            if len(string) == 0:
                return "Next word suggestions:" + str(self.threewords(("s1 " + "s2 " + "s3").split(" "))).upper()
            tokens = string.split(" ")
            if len(tokens) == 1:
                return "Next word suggestions:" + str(self.threewords(("s2 " + "s3 " + string).split(" "))).upper()
            elif len(tokens) == 2:
                return "Next word suggestions:" + str(self.threewords(("s3 " + string).split(" "))).upper()
            elif len(tokens) == 3:
                return "Next word suggestions:" + str(self.threewords(string.split(" "))).upper()
            elif len(tokens) > 3:
                return self.morewords(string.split(" "))
        return ""


# x=Tools.pokh_with_padding(open(dataPath, "r").read(),1,4)
# x = Tools.wordsToOneHot(open(dataPath, "r").readline(), 1, 4)

(wordsInfo, featuresInfo, vectorInfo) = Tools.readWordToInfo(infoPath)
my_next_word = MarkovChain()
my_next_word.add_document_fixed(open(dataPath, "r").read())

api = Flask(__name__)


@api.route('/vec', methods=['GET'])
def get_companies():
    input = str(request.args.get('vec'))
    queries = input.split(";")
    print(queries)
    begin = ''.join(["s" + str(x) + " " for x in range(1 + len(queries), padding + 1)])
    if len(queries) == padding:
        begin = ""
        for q in queries:
            begin += (vectorInfo.get(q).lower() + " ")
        begin = begin[:-1]
    else:
        begin = begin[:-1]
        for q in queries:
            begin += (" " + vectorInfo.get(q).lower())
    print(begin)
    pred = my_next_word.generate_text(begin)
    print(pred)
    t = ""
    if len(pred) >= 3:
        for x in pred[2][0]:
            t += str(x) + " "
    if len(pred) >= 2:
        for x in pred[1][0]:
            t += str(x) + " "
    if len(pred) >= 1:
        for x in pred[0][0]:
            t += str(x) + " "
    t = t[:-1]
    print(t)
    res = ""
    if t=="":
        return ""
    for q in t.split(" "):
        if not q.__contains__("e") and not q.__contains__("s"):
            res += wordsInfo.get(q)[2]
    if res == "":
        return ""
    print(res)
    return res
    # vecs = padVecs(input, windowSize, featureCNT, reserve)
    # a = DataFrame(vecs)
    docX = []
    # docX.append(a.iloc[:].values)
    # pred = model.predict(np.array(docX).astype('float32'))
    res = ""
    # for bit in np.nditer(tf.cast(tf.greater_equal(pred, tf.constant(0.15)), tf.int32)):
    #    res += str(bit)
    return 0  # res


if __name__ == '__main__':
    print("I am.")
    api.run()

print("Give a query:")
for i in range(1, 15):
    # print(Tools.wordsToFeatures(
    #    my_next_word.generate_text(Tools.featuresToWords(input().strip().lower(), featuresInfo).strip().lower()),
    #   wordsInfo))
    print("Q" + str(i))
    print(my_next_word.generate_text("q591 q591 q1477 q1637"))
