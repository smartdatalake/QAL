from flask import Flask
from flask import request
import sys
from keras.models import model_from_json
import Tools
from os import listdir
from os.path import isfile, join

windowSize = int(sys.argv[1])  # 5
futureSize = int(sys.argv[2])  # 5
reserve = int(sys.argv[3])  # 15
setup = ["4YearTo2Week", "2WeekTo2Week", "4YearTo6Month", "1DayTo1Day"] # 1875
# "1DayTo1Day"
parentDir = "/home/hamid/IdeaProjects/PredictionModel/models/"
models = {}


def load_model(modelPath, weightPath):
    json_file = open(modelPath, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    loaded_model.load_weights(weightPath)
    loaded_model.compile(loss="binary_crossentropy", optimizer='adam',
                         metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    return loaded_model


for tag in setup:
    files = [f for f in listdir(parentDir) if (isfile(join(parentDir, f)) and f.__contains__(tag) and f.__contains__(
        "_" + str(windowSize) + "_" + str(futureSize) + "_") and f.__contains__("_LSTM") and f.__contains__(".json"))]
    for file in files:
        print(file)
        models[tag + "_" + file.split('_')[2]] = load_model(parentDir + file, parentDir + file.split('.')[0] + ".h5")


def test(model, vectors):
    (X_test, Y_test) = Tools.read(vectors, windowSize, futureSize, reserve, len(vectors.split("P")[0].split(';')[0]), ';', 'P')
    if X_test.size == 0 and len(vectors.split(';')) >= windowSize:
        print("zero vectors.")
        return "0"
    scores = model.evaluate(X_test, Y_test, verbose=0, use_multiprocessing=True, workers=4)
    return str(round(scores[1] * 100))


api = Flask(__name__)


@api.route('/vec', methods=['GET'])
def get_companies():
    (tag, m, input) = str(request.args.get('vec')).split("X")
    if tag + "_M" + m in models:
        return test(models[tag + "_M" + m], input)
    print(tag+"_M"+m+" is not detected.")
    return ""


if __name__ == '__main__':
    print("I am.")
    api.run()
