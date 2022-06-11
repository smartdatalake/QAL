from keras.layers import GRU
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.callbacks import ModelCheckpoint

import Tools
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Reshape
from tensorflow.keras.layers import LSTM
from tensorflow.keras.layers import Dropout
import sys
import os.path
import time

callbacks = [
    EarlyStopping(monitor='loss', patience=1, verbose=0),
]

# sys.stdout = open("out", "a+")
# from tensorflow.python.client import device_lib
# print(device_lib.list_local_devices())
# os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
# 100 to 154 done
verbose = 0
threshold = 0.3
reserved = 15
modelPath = "VectorPredictionWithTime.json"
weightPath = "VectorPredictionWithTime.h5"
modelDir = "models/"
setup = [ ("2WeekTo2Week", 300, 3000,32, 100), ("4YearTo6Month", 22,100, 32, 25)]
# ("4YearTo2Week", 1000, 32, 25) , ("1DayTo1Day", 10000, 32, 100), ("2WeekTo2Week", 500, 32, 100), ("4YearTo6Month", 30, 32, 25)
# ("1DayTo1Day", 10000, 32, 100), ("2WeekTo2Week", 500, 32, 100), ("4YearTo6Month", 30, 32, 25),
# ("2WeekTo2Week", 400, 32, 50),("1WeekTo1Week", 10000, 32, 70), ("1DayTo1Day", 10000, 32, 100),("1MonthTo1Month", 1000, 32, 30),("3MonthTo3Month", 1000, 32, 30),("2YearTo1Month", 1000, 32, 20), ("4YearTo1Month", 1000, 32, 15), ("1MonthTo2Week", 1000, 32, 35),
# ("3MonthTo2Week", 1000, 32, 25), ("6MonthTo2Week", 1000, 32, 25), ("1YearTo2Week", 1000, 32, 25),
# ("4YearTo6Month", 30, 16, 10), ("3MonthTo3Month", 60, 32, 25), ("6MonthTo3Month", 60, 32, 25),
# ("1YearTo3Month", 60, 32, 25), ("2YearTo3Month", 60, 32, 25), ("4YearTo3Month", 60, 32, 25),
# ("BeginTo6Month", 36, 16, 10), ("6MonthTo6Month", 36, 16, 30), ("BeginTo3Month", 60, 32, 10),
# ("BeginTo1Month", 200, 32, 10)
#         ("1YearTo6Month", 36, 32, 25), ("1YearTo3Month", 51, 32, 25), ("BeginTo2Year", 7, 32, 15),
#          ("BeginTo1Year", 14, 32, 15),
#         ("4YearTo2Year", 5, 32, 15), ("2YearTo3Month", 45, 32, 20), ("2YearTo6Month", 23, 32, 20),
#         ("2YearTo1Year", 12, 32, 20), ("2WeekTo2Week", 339, 32, 50),
#         ("3MonthTo3Month", 52, 32, 25), ("6MonthTo6Month", 26, 32, 25), ("1YearTo1Year", 13, 32, 25),
#         ("2YearTo2Year", 6, 32, 20), ("4YearTo4Year", 3, 32, 20), ("BeginTo6Month", 26, 32, 15),("4YearTo3Month", 37, 32, 10), ,]
parentDir = sys.argv[1]  # /home/hamid/QAL/DensityCluster/
tagPostfix = "_gap18000000_processMinLength5_maxQueryRepetition10000000_featureMinFRQ1_reserveFeature15"
loss = sys.argv[3]
featureSize = 0
dropout = 0.3


def trainLSTM(windowSize, predictionWindowSize, dropout, featureSize, reserved, X_train, Y_train, nbEpoch, batchSize):
    model = Sequential()
    n = round((featureSize * 1) / 6) + 1
    model.add(LSTM(n, return_sequences=True, dropout=dropout, input_shape=(windowSize, featureSize),
                   kernel_initializer='he_uniform'))
    model.add(Reshape((windowSize * n,)))
    model.add(Dropout(dropout))
    model.add(Dense(predictionWindowSize * (featureSize - (reserved + 14)), activation='sigmoid'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, verbose=verbose, use_multiprocessing=True,
              workers=8)

    return model

def trainGRU(windowSize, predictionWindowSize, dropout, featureSize, reserved, X_train, Y_train, nbEpoch, batchSize):
    model = Sequential()
    n = round((featureSize * 1) / 6) + 1
    model.add(GRU(n, return_sequences=True, dropout=dropout, input_shape=(windowSize, featureSize),
                   kernel_initializer='he_uniform'))
    model.add(Reshape((windowSize * n,)))
    model.add(Dropout(dropout))
    model.add(Dense(predictionWindowSize * (featureSize - (reserved + 14)), activation='sigmoid'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, verbose=verbose, use_multiprocessing=True,
              workers=8)

    return model

def trainLSTMDecoder(windowSize, predictionWindowSize, dropout, featureSize, reserved, X_train, Y_train, nbEpoch,
                     batchSize):
    model = Sequential()
    n = round((featureSize * 1) / 6) + 1
    model.add(LSTM(n, return_sequences=True, dropout=dropout, input_shape=(windowSize, featureSize),
                   kernel_initializer='he_uniform'))
    model.add(LSTM(n, return_sequences=True, dropout=dropout, input_shape=(windowSize, featureSize),
                   kernel_initializer='he_uniform'))
    model.add(Reshape((windowSize * n,)))
    model.add(Dropout(dropout))
    model.add(Dense(predictionWindowSize * (featureSize - (reserved + 14)), activation='sigmoid'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, verbose=verbose, use_multiprocessing=True,
              workers=4)

    return model


def trainNN(windowSize, predictionWindowSize, dropout, featureSize, reserved, X_train, Y_train, nbEpoch, batchSize):
    model = Sequential()
    n = round((featureSize * windowSize * 2) / 20) + 1
    model.add(Reshape((windowSize * featureSize,)))
    model.add(Dropout(dropout))
    model.add(Dense(n, activation='relu'))
    model.add(Dropout(dropout))
    model.add(Dense(predictionWindowSize * (featureSize - (reserved + 14)), activation='sigmoid'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, Tools.binary_acc1])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, verbose=verbose)

    return model

for (tag,start, modelNumber, batchSize, nbEpoch) in setup:
    t, c, f, p, r, a = 0.0, 0, 0, 0, 0, 0
    out = ""
    for index in range(start, modelNumber):

        trainFile = tag + "_M" + str(index) + "_train" + tagPostfix
        testFile = tag + "_M" + str(index) + "_test" + tagPostfix
        if not (os.path.isfile(parentDir + trainFile)):
            continue
        with open(parentDir + trainFile + "_vec2Feature", "r") as file:
            featureSize = len(file.readline().split(':')[0])

        for (windowSize, predictionSize) in [(10, 10)]:#in [(3, 3), (5, 5), (10, 10), (15, 15)]:
            output = ""
            cp = time.time()
            (X_train, Y_train) = Tools.read(open(parentDir + trainFile, "r").read(), windowSize, predictionSize,
                                            reserved, featureSize)
            (X_test, Y_test) = Tools.read(open(parentDir + testFile, "r").read(), windowSize, predictionSize, reserved,
                                          featureSize)
            # if (X_train.size == 0 or Y_train.size == 0 or X_test.size == 0 or Y_test.size == 0):
            if (X_train.size == 0 or Y_train.size == 0):
                continue
            modelName = tag + "_LSTM_M" + str(index) + "_" + str(windowSize) + "_" + str(predictionSize) + "_" + str(
                featureSize)

            model = trainLSTM(windowSize, predictionSize, dropout, featureSize, reserved, X_train, Y_train,
                              nbEpoch,
                              batchSize)
            cp = (time.time() - cp)
            t += cp
            if (X_test.size != 0 or Y_test.size != 0):
                scores = model.evaluate(X_test, Y_test, verbose=0, use_multiprocessing=True, workers=4)
                out += modelName + "_" + str(round(cp)) + "_" + str(round(scores[1] * 100)) + "_" + str(
                    round(scores[2] * 100)) + "_" + str(round(scores[3] * 100)) + "_" + str(
                    round(scores[4] * 100)) + "\n"
                f += round(scores[1] * 100)
                p += round(scores[2] * 100)
                r += round(scores[3] * 100)
                a += round(scores[4] * 100)
                c += 1
                output += "\t" + str(round(scores[1] * 100))
                output += "\t" + str(round(scores[2] * 100))
                output += "\t" + str(round(scores[3] * 100))
                output += "\t" + str(round(scores[4] * 100))
                print(output)

            Tools.save_model(model, modelDir + modelName + modelPath, modelDir + modelName + weightPath)

    file_object = open('MLResultSingleTime.txt', 'a')
    file_object.write(out + "\n\n")
    file_object.close()
exit()


for (tag, modelNumber, batchSize, nbEpoch) in setup:
    t, c, f, p, r, a = 0.0, 0, 0, 0, 0, 0
    out = ""
    for index in range(1, modelNumber):

        trainFile = tag + "_M" + str(index) + "_train" + tagPostfix
        testFile = tag + "_M" + str(index) + "_test" + tagPostfix
        if not (os.path.isfile(parentDir + trainFile)):
            continue
        with open(parentDir + trainFile + "_vec2Feature", "r") as file:
            featureSize = len(file.readline().split(':')[0])

        for (windowSize, predictionSize) in [(3, 3), (5, 5), (10, 10), (15, 15)]:
            output = ""
            cp = time.time()
            (X_train, Y_train) = Tools.read(open(parentDir + trainFile, "r").read(), windowSize, predictionSize,
                                            reserved, featureSize)
            (X_test, Y_test) = Tools.read(open(parentDir + testFile, "r").read(), windowSize, predictionSize, reserved,
                                          featureSize)
            # if (X_train.size == 0 or Y_train.size == 0 or X_test.size == 0 or Y_test.size == 0):
            if (X_train.size == 0 or Y_train.size == 0):
                continue
            modelName = tag + "_NN_M" + str(index) + "_" + str(windowSize) + "_" + str(predictionSize) + "_" + str(
                featureSize)

            model = trainNN(windowSize, predictionSize, dropout, featureSize, reserved, X_train, Y_train,
                              nbEpoch,
                              batchSize)
            cp = (time.time() - cp)
            t += cp
            if (X_test.size != 0 or Y_test.size != 0):
                scores = model.evaluate(X_test, Y_test, verbose=0, use_multiprocessing=True, workers=4)
                out += modelName + "_" + str(round(cp)) + "_" + str(round(scores[1] * 100)) + "_" + str(
                    round(scores[2] * 100)) + "_" + str(round(scores[3] * 100)) + "_" + str(
                    round(scores[4] * 100)) + "\n"
                f += round(scores[1] * 100)
                p += round(scores[2] * 100)
                r += round(scores[3] * 100)
                a += round(scores[4] * 100)
                c += 1
                output += "\t" + str(round(scores[1] * 100))
                output += "\t" + str(round(scores[2] * 100))
                output += "\t" + str(round(scores[3] * 100))
                output += "\t" + str(round(scores[4] * 100))
                print(output)

            # Tools.save_model(model, modelDir + modelName + modelPath, modelDir + modelName + weightPath)

    file_object = open('MLResultSingleTime.txt', 'a')
    file_object.write(out + "\n\n")
    file_object.close()

for (tag, modelNumber, batchSize, nbEpoch) in setup:
    t, c, f, p, r, a = 0.0, 0, 0, 0, 0, 0
    out = ""
    for index in range(1, modelNumber):

        trainFile = tag + "_M" + str(index) + "_train" + tagPostfix
        testFile = tag + "_M" + str(index) + "_test" + tagPostfix
        if not (os.path.isfile(parentDir + trainFile)):
            continue
        with open(parentDir + trainFile + "_vec2Feature", "r") as file:
            featureSize = len(file.readline().split(':')[0])

        for (windowSize, predictionSize) in [(3, 3), (5, 5), (10, 10), (15, 15)]:
            output = ""
            cp = time.time()
            (X_train, Y_train) = Tools.read(open(parentDir + trainFile, "r").read(), windowSize, predictionSize,
                                            reserved, featureSize)
            (X_test, Y_test) = Tools.read(open(parentDir + testFile, "r").read(), windowSize, predictionSize, reserved,
                                          featureSize)
            # if (X_train.size == 0 or Y_train.size == 0 or X_test.size == 0 or Y_test.size == 0):
            if (X_train.size == 0 or Y_train.size == 0):
                continue
            modelName = tag + "_LSTM_M" + str(index) + "_" + str(windowSize) + "_" + str(predictionSize) + "_" + str(
                featureSize)

            model = trainLSTM(windowSize, predictionSize, dropout, featureSize, reserved, X_train, Y_train,
                              nbEpoch,
                              batchSize)
            cp = (time.time() - cp)
            t += cp
            if (X_test.size != 0 or Y_test.size != 0):
                scores = model.evaluate(X_test, Y_test, verbose=0, use_multiprocessing=True, workers=4)
                out += modelName + "_" + str(round(cp)) + "_" + str(round(scores[1] * 100)) + "_" + str(
                    round(scores[2] * 100)) + "_" + str(round(scores[3] * 100)) + "_" + str(
                    round(scores[4] * 100)) + "\n"
                f += round(scores[1] * 100)
                p += round(scores[2] * 100)
                r += round(scores[3] * 100)
                a += round(scores[4] * 100)
                c += 1
                output += "\t" + str(round(scores[1] * 100))
                output += "\t" + str(round(scores[2] * 100))
                output += "\t" + str(round(scores[3] * 100))
                output += "\t" + str(round(scores[4] * 100))
                print(output)

            # Tools.save_model(model, modelDir + modelName + modelPath, modelDir + modelName + weightPath)

    file_object = open('MLResultSingleTime.txt', 'a')
    file_object.write(out + "\n\n")
    file_object.close()
#  print(tag + "_" + str(c) + "_" + str(round(f / c)) + "_" + str(round(p / c)) + "_" + str(round(r / c)) + "_" + str(
#      round(a / c)) + "_" + str(round(t + 1 / 60)))
