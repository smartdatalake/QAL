from tensorflow import keras

import Tools
from keras.models import Sequential
from keras.layers import Dense, Bidirectional, RepeatVector, TimeDistributed, Input
from keras.layers import LSTM
from keras.layers import GRU
from keras.layers import Embedding
from keras.layers import Dropout
import sys
import os.path
from sklearn.metrics import accuracy_score

# sys.stdout = open("out", "a+")


os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
modelPath = "WordPredictionWithTimeImprovedLSTM.json"
weightPath = "WordPredictionWithTimeImprovedLSTM.h5"

dataPath = sys.argv[1]
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
dropoutRate = float(sys.argv[13])  # 412
socketPort = 4445
import numpy as np

x = np.array([1, 1, 1, 1])
y = np.array([1, 1, 1, 1])

Tools.binary_fbeta(x, y)

output = dataPath.split('/')[len(dataPath.split('/')) - 1] + "\t\t" + str(hidden_neurons) + "\t\t" + str(
    loss) + "\t\t" + str(windowSize) + "\t" + str(predictionWindowSize) + "\t" + str(nbEpoch) + "\t" + str(
    batchSize) + "\t" + str(testStep) + "\t" + str(inputMaxSize)  + "\t" + str(dropoutRate)


def trainAndStoreVecToVecModel(output):
    model = Sequential()
    # feature_count = len(X_train[0][0])
    # print(feature_count)
    print(X_train.shape)
    # model.add(LSTM(hidden_neurons, return_sequences=True,   input_shape=( windowSize, featureSize), kernel_initializer='he_uniform'))
  #  model.add(LSTM(hidden_neurons, return_sequences=False, input_shape=(windowSize, featureSize),activation="tanh",recurrent_activation="relu"))
    # model.add(LSTM( hidden_neurons,return_sequences=True, activation="relu"))
    # model.add(LSTM( hidden_neurons,return_sequences=False, activation="relu"))
    model.add(Dense(predictionWindowSize * featureSize, activation='relu'))
    model.add(Dropout(dropoutRate))
    model.add(Dense(predictionWindowSize * featureSize, activation='sigmoid'))

    # model.add(Input(shape=(X_train.shape[1], X_train.shape[2])))
    # model.add(LSTM(predictionWindowSize*featureSize,return_sequences=False,  dropout=0.5, activation="relu", kernel_initializer='he_uniform'))
    # model.add(Dropout(0.5))
    # model.add(Dense(predictionWindowSize*featureSize, activation='sigmoid'))
    #model.summary()
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.binary_p, Tools.binary_r, "mean_squared_error", "acc"])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, validation_data=(X_test, Y_test), verbose=1,
              shuffle=True)
    # make a prediction on the test set
    yhat = model.predict(X_test)
    # round probabilities to class labels
    yhat = yhat.round()
    # calculate accuracy
    acc = accuracy_score(Y_test, yhat)
    # store result
    print('>%.3f' % acc)
    scores = model.evaluate(X_train, Y_train, verbose=0)
    output += "\t" + str(round(scores[1] * 100))
    output += "\t" + str(round(scores[2] * 100))
    output += "\t" + str(round(scores[3] * 100))
    output += "\t" + str(round(scores[4] * 100))
    output += "\t" + str(round(scores[5] * 100))

    scores = model.evaluate(X_test, Y_test, verbose=0)
    output += "\t" + str(round(scores[1] * 100))
    output += "\t" + str(round(scores[2] * 100))
    output += "\t" + str(round(scores[3] * 100))
    output += "\t" + str(round(scores[4] * 100))
    output += "\t" + str(round(scores[5] * 100))
    print(output)
    Tools.save_model(model, str(predictionWindowSize) + str(featureSize) + modelPath,
                     str(predictionWindowSize) + str(featureSize) + weightPath)
    return model


train_inputs, train_targets = Tools.read_process_with_nonempty_padding2(open(dataPath, "r").read(), windowSize,
                                                                       predictionWindowSize, 5, featureSize)

(X_train, Y_train), (X_test, Y_test) = Tools.train_test_split_KNFOLD(train_inputs, train_targets,  testStep, testSize)
seq_len = len(X_train)
model = None

# if path.exists(modelPath) and path.exists(weightPath):
#    model = Tools.load_model(modelPath, weightPath)
# else:
model = trainAndStoreVecToVecModel(output)


# for i in range(0, 10000):
#    input_text = input().strip().lower()
#    encoded_text = tokenizer.texts_to_sequences([input_text])[0]
#    pad_encoded = pad_sequences([encoded_text], maxlen=seq_len, truncating='pre')
#    print(encoded_text, pad_encoded)
#    for i in (model.predict(pad_encoded)[0]).argsort()[-3:][::-1]:
#        pred_word = tokenizer.index_word[i]
#        print("Next word suggestion:", pred_word)


def trainAndStoreModel():
    model = Sequential()
    model.add(Embedding(vocabulary_size, seq_len, input_length=seq_len))
    model.add(LSTM(hidden_neurons, return_sequences=True, dropout=0.2))
    model.add(LSTM(hidden_neurons, return_sequences=True, dropout=0.2))
    model.add(LSTM(hidden_neurons, return_sequences=False, dropout=0.2))
    model.add(Dense(hidden_neurons * predictionWindowSize, activation='relu'))
    model.add(Dropout(.2))
    model.add(Dense(hidden_neurons * predictionWindowSize, activation='relu'))
    model.add(Dropout(.2))
    model.add(Dense(hidden_neurons * predictionWindowSize, activation='relu'))
    model.add(Dropout(.2))
    model.add(Dense(vocabulary_size * predictionWindowSize, activation='softmax'))
    model.compile(loss=loss, optimizer='adam',
                  metrics=[Tools.binary_fbeta, Tools.f1, "categorical_accuracy", "binary_accuracy", "acc"])
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, validation_data=(X_test, Y_test), verbose=1,
              shuffle=True)
    Tools.save_model(model, modelPath, weightPath)
    return model
