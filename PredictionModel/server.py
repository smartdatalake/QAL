from flask import Flask
from flask import request
from pandas import DataFrame
import numpy as np
import tensorflow as tf
import sys
from keras.models import model_from_json

# = "WordPredictionLSTM.json"
# = "WordPredictionLSTM.h5"
featureCNT = int(sys.argv[1])  # 138
windowSize = int(sys.argv[2])  # 5
futureSize = int(sys.argv[3])  # 5
reserve = int(sys.argv[4])  # 10
modelPath = sys.argv[5]  # 10
weightPath = sys.argv[6]  # 10
threshold = float(sys.argv[7])  # 10


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


def load_model(modelPath, weightPath):
    json_file = open(modelPath, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    loaded_model.load_weights(weightPath)
    return loaded_model


model = load_model(modelPath, weightPath)

api = Flask(__name__)


@api.route('/vec', methods=['GET'])
def get_companies():
    input = str(request.args.get('vec'))
    vecs = padVecs(input, windowSize, featureCNT, reserve)
    a = DataFrame(vecs)
    docX = []
    docX.append(a.iloc[:].values)
    pred = model.predict(np.array(docX).astype('float32'))
    res = ""
    for bit in np.nditer(tf.cast(tf.greater_equal(pred, tf.constant(threshold)), tf.int32)):
        res += str(bit)
    return res


if __name__ == '__main__':
    print("I am.")
    api.run()
