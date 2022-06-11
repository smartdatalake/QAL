import Tools
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Reshape
from tensorflow.keras.layers import LSTM
from tensorflow.keras.layers import Dropout
import sys
import os.path

# sys.stdout = open("out", "a+")
# from tensorflow.python.client import device_lib
# print(device_lib.list_local_devices())
# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
modelPath = "WordPredictionWithTimeImprovedLSTM.json"
weightPath = "WordPredictionWithTimeImprovedLSTM.h5"
testDataPath = "/home/hamid/QAL/DensityCluster/processVec_test_1800Gap_5processMinLength_10000maxQueryRepetition_1featureMinFRQ_20reserveFeature_2007fromYear_20202toYear"
trainDataPath = sys.argv[1]
windowSize = int(sys.argv[2])
predictionWindowSize = int(sys.argv[3])
batchSize = int(sys.argv[4])
nbEpoch = int(sys.argv[5])
hidden_neurons = int(sys.argv[6])  # int((feature_count + feature_count))*2
loss = sys.argv[7]
testSize = float(sys.argv[8])
validationSplit = float(sys.argv[9])
testStep = int(sys.argv[10])
inputMaxSize = int(sys.argv[11])
featureSize = int(sys.argv[12])  # 412
fT = False  # bool(sys.argv[13])  # 412
reserved = 15
columnSize = 261
socketPort = 4445

nbEpoch = 20
threshold = 0.3

output = str(hidden_neurons) + "\t\t" + str(
    loss) + "\t\t" + str(windowSize) + "\t" + str(predictionWindowSize) + "\t" + str(nbEpoch) + "\t" + str(
    batchSize) + "\t" + str(testStep) + "\t" + str(inputMaxSize)


def trainAndStoreVecToVecModel(output):
    model = Sequential()
    n = (int)((2 * (featureSize - reserved) * (windowSize + predictionWindowSize)) / 200)
    n = featureSize - reserved
    n = 300
    print(n)
    model.add(LSTM(n, return_sequences=True, dropout=0.3, input_shape=(windowSize, featureSize),
                   kernel_initializer='he_uniform'))
    model.add(Reshape((windowSize * n,)))
    model.add(Dropout(0.3))
    model.add(Dense(predictionWindowSize * (featureSize - reserved), activation='sigmoid'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, verbose=1,
              shuffle=True)
    scores = model.evaluate(X_test, Y_test, verbose=0)
    output += "\t" + str(round(scores[1] * 100))
    output += "\t" + str(round(scores[2] * 100))
    output += "\t" + str(round(scores[3] * 100))
    output += "\t" + str(round(scores[4] * 100))
    print(output)
    Tools.save_model(model, str(predictionWindowSize) + str(featureSize) + modelPath,
                     str(predictionWindowSize) + str(featureSize) + weightPath)
    return model

testDataPath = "/home/hamid/QAL/DensityCluster/processVec_test_1800Gap_5processMinLength_10000maxQueryRepetition_1featureMinFRQ_20reserveFeature_2007fromYear_20202toYear"
trainDataPath = "/home/hamid/QAL/DensityCluster/processVec_1800Gap_5processMinLength_10000maxQueryRepetition_1featureMinFRQ_20reserveFeature_2007fromYear_20202toYear_shuf"
featureSize=555
predictionWindowSize = 3
windowSize = 3
(X_train, Y_train) = Tools.read_process_with_nonempty_padding(open(trainDataPath, "r").read(), windowSize,
                                                              predictionWindowSize, reserved, fT,
                                                              featureSize)
(X_test, Y_test) = Tools.read_process_with_nonempty_padding(open(testDataPath, "r").read(), windowSize,
                                                            predictionWindowSize, reserved, fT, featureSize)
model = trainAndStoreVecToVecModel(output)
predictionWindowSize = 5
windowSize = 5
(X_train, Y_train) = Tools.read_process_with_nonempty_padding(open(trainDataPath, "r").read(), windowSize,
                                                              predictionWindowSize, reserved, fT,
                                                              featureSize)
(X_test, Y_test) = Tools.read_process_with_nonempty_padding(open(testDataPath, "r").read(), windowSize,
                                                            predictionWindowSize, reserved, fT, featureSize)
model = trainAndStoreVecToVecModel(output)
predictionWindowSize = 10
windowSize = 10
(X_train, Y_train) = Tools.read_process_with_nonempty_padding(open(trainDataPath, "r").read(), windowSize,
                                                              predictionWindowSize, reserved, fT,
                                                              featureSize)
(X_test, Y_test) = Tools.read_process_with_nonempty_padding(open(testDataPath, "r").read(), windowSize,
                                                            predictionWindowSize, reserved, fT, featureSize)
model = trainAndStoreVecToVecModel(output)
predictionWindowSize = 15
windowSize = 15
(X_train, Y_train) = Tools.read_process_with_nonempty_padding(open(trainDataPath, "r").read(), windowSize,
                                                              predictionWindowSize, reserved, fT,
                                                              featureSize)
(X_test, Y_test) = Tools.read_process_with_nonempty_padding(open(testDataPath, "r").read(), windowSize,
                                                            predictionWindowSize, reserved, fT, featureSize)
model = trainAndStoreVecToVecModel(output)
