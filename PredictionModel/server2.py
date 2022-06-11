import Tools
from os import listdir
from os.path import isfile, join
from flask import Flask
from flask import request
from pandas import DataFrame
import numpy as np
import tensorflow as tf
import sys
from keras.models import model_from_json

# windowSize = int(sys.argv[1])  # 5
# futureSize = int(sys.argv[2])  # 5
reserve = 15  # int(sys.argv[3])  # 15
threshold = 0.15  # float(sys.argv[4])
setup = ["2WeekTo2Week", "4YearTo6Month", "4YearTo2Week"]  # 1875
# "1DayTo1Day"  "4YearTo2Week", , "1DayTo1Day"
parentDir = "/home/hamid/IdeaProjects/PredictionModel/modelsTest/"
models = {}


def load_model(modelPath, weightPath):
    json_file = open(modelPath, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    loaded_model.load_weights(weightPath)
    loaded_model.compile(loss="binary_crossentropy", optimizer='adam',
                         metrics=[Tools.binary_fbeta])
    return loaded_model


for tag in setup:
    files = [f for f in listdir(parentDir) if (
            isfile(join(parentDir, f)) and f.__contains__(tag) and f.__contains__("_LSTM") and f.__contains__(
        ".json"))]
    for file in files:
        print(file)
        models[tag + "_" + file.split('_')[2] + "_" + file.split('_')[3]] = load_model(parentDir + file,
                                                                                       parentDir + file.split('.')[
                                                                                           0] + ".h5")


def padVecs(vecs, windowSize, featureSize, reserve):
    vectors = vecs.split(";")
    a = []
    if len(vectors) > windowSize:
        for vec in vectors[-windowSize:]:
            a.append([int(x) for x in vec])
    elif len(vectors) == windowSize:
        for vec in vectors:
            a.append([int(x) for x in vec])
    else:
        for i in range(0, min(windowSize, reserve)):
            a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
        for vec in vectors:
            a.append([int(x) for x in vec])
    return a[-windowSize:]


api = Flask(__name__)


def predicate(model, vecs):
    a = DataFrame(vecs)
    docX = []
    docX.append(a.iloc[:].values)
    pred = model.predict(np.array(docX).astype('float32'))
    res = ""
    for bit in np.nditer(tf.cast(tf.greater_equal(pred, tf.constant(threshold)), tf.int32)):
        res += str(bit)
    return res


def test(model, vectors, h):
    (X_test, Y_test) = Tools.read(vectors, int(h), int(h), reserve, len(vectors.split("P")[0].split(';')[0]), ';', 'P')
    if X_test.size == 0:  # and len(vectors.split(';')) >= int(h):
        print("zero vectors.")
        return "0"
    scores = model.evaluate(X_test, Y_test, verbose=0, use_multiprocessing=True, workers=4)
    return str(round(scores[1] * 100))


api = Flask(__name__)


@api.route('/test', methods=['GET'])
def get_company():
    (tag, m, input, h) = str(request.args.get('vec')).split("X")
    if tag + "_M" + m + "_" + h in models:
        return test(models[tag + "_M" + m + "_" + h], input, h)
    print(tag + "_M" + m + " is not detected.")
    return ""


@api.route('/vec', methods=['GET'])
def get_companies():
    (tag, m, input, h) = str(request.args.get('vec')).split("X")
    featureCNT = len(input.split(';')[0])
    if tag + "_M" + m + "_" + h in models:
        vec = padVecs(input, int(h), featureCNT, reserve)
        res = predicate(models[tag + "_M" + m + "_" + h], vec)
        print(res)
        return res
    else:
        return print(tag + "_M" + m + "_h" + h + " is not detected.")


if __name__ == '__main__':
    print("I am.")
    api.run()
