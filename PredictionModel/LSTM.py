from keras.models import Sequential
from keras.layers.core import Dense
from keras.layers.recurrent import LSTM
import pandas as pd
import numpy as np
import os.path
from os import path
from keras.models import model_from_json
import sys
from keras import backend as K
import tensorflow as tf

#sys.stdout = open("out", "a+")

os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
modelPath = "model.json"
weightPath = "model.h5"
threshold = 0.25
fileName = sys.argv[1]
windowSize = int(sys.argv[2])
predictionWindowSize = int(sys.argv[3])
batchSize = int(sys.argv[4])
nbEpoch = int(sys.argv[5])
hidden_neurons = int(sys.argv[6])  # int((feature_count + feature_count))*2
loss = sys.argv[7]
testSize = float(sys.argv[8])
validationSplit = float(sys.argv[9])

# print("---------------------------------------------------------------------------------------------------------------")
# print("HiddenNeuron: " + str(hidden_neurons))
# print("Loss: " + loss)
# print("WindowSize: " + str(windowSize))
# print("Epoch: " + str(nbEpoch))
# print("BatchSize: " + str(batchSize))

output = fileName + "\t\t" + str(hidden_neurons) + "\t\t" + str(loss) + "\t\t" + str(windowSize) + "\t" + str(
    predictionWindowSize) + "\t" + str(nbEpoch) + "\t" + str(batchSize)


def recall_m(y_true, y_pred):
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
    recall = true_positives / (possible_positives + K.epsilon())
    return recall


def precision_m(y_true, y_pred):
    true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
    predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
    precision = true_positives / (predicted_positives + K.epsilon())
    return precision


def f1_m(y_true, y_pred):
    precision = precision_m(y_true, y_pred)
    recall = recall_m(y_true, y_pred)
    return 2 * ((precision * recall) / (precision + recall + K.epsilon()))


def binary_fbeta(y_true, y_pred):
    beta_squared = 1 ** 2  # squaring beta
    epsilon = 1e-7
    # casting ytrue and ypred as float dtype
    ytrue = tf.cast(y_true, tf.float32)
    ypred = tf.cast(y_pred, tf.float32)

    # setting values of ypred greater than the set threshold to 1 while those lesser to 0
    ypred = tf.cast(tf.greater_equal(ypred, tf.constant(threshold)), tf.float32)
    tp = tf.reduce_sum(ytrue * ypred)  # calculating true positives
    predicted_positive = tf.reduce_sum(ypred)  # calculating predicted positives
    actual_positive = tf.reduce_sum(ytrue)  # calculating actual positives

    precision = tp / (predicted_positive + epsilon)  # calculating precision
    recall = tp / (actual_positive + epsilon)  # calculating recall

    # calculating fbeta
    fb = (1 + beta_squared) * precision * recall / (beta_squared * precision + recall + epsilon)

    return fb


def f1(y_true, y_pred):
    def recall(y_true, y_pred):
        """Recall metric.

        Only computes a batch-wise average of recall.

        Computes the recall, a metric for multi-label classification of
        how many relevant items are selected.
        """
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        possible_positives = K.sum(K.round(K.clip(y_true, 0, 1)))
        recall = true_positives / (possible_positives + K.epsilon())
        return recall

    def precision(y_true, y_pred):
        """Precision metric.

        Only computes a batch-wise average of precision.

        Computes the precision, a metric for multi-label classification of
        how many selected items are relevant.
        """
        true_positives = K.sum(K.round(K.clip(y_true * y_pred, 0, 1)))
        predicted_positives = K.sum(K.round(K.clip(y_pred, 0, 1)))
        precision = true_positives / (predicted_positives + K.epsilon())
        return precision

    precision = precision(y_true, y_pred)
    recall = recall(y_true, y_pred)
    return 2 * ((precision * recall) / (precision + recall + K.epsilon()))


def f1_2(y_true, y_pred):
    y_pred = K.round(y_pred)
    tp = K.sum(K.cast(y_true * y_pred, 'float'), axis=0)
    # tn = K.sum(K.cast((1-y_true)*(1-y_pred), 'float'), axis=0)
    fp = K.sum(K.cast((1 - y_true) * y_pred, 'float'), axis=0)
    fn = K.sum(K.cast(y_true * (1 - y_pred), 'float'), axis=0)

    p = tp / (tp + fp + K.epsilon())
    r = tp / (tp + fn + K.epsilon())

    f1 = 2 * p * r / (p + r + K.epsilon())
    return K.mean(f1)


def save_model(modelIn):
    model_json = modelIn.to_json()
    with open(modelPath, "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    modelIn.save_weights(weightPath)
    print("Saved model to disk")


def load_model():
    json_file = open(modelPath, 'r')
    loaded_model_json = json_file.read()
    json_file.close()
    loaded_model = model_from_json(loaded_model_json)
    loaded_model.load_weights(weightPath)
    return loaded_model


def binary_encoder(vector):
    for i in range(0, vector.size):
        if vector[i] < 0.25:
            vector[i] = 0
        else:
            vector[i] = 1
    return vector


def load_data(data):
    """
    data should be pd.DataFrame()
    """
    docX, docY = [], []
    for i in range(len(data) - (windowSize + predictionWindowSize - 1)):
        docX.append(data.iloc[i:i + windowSize].values)
        docY.append(data.iloc[i + windowSize:i + windowSize + predictionWindowSize].values.flatten())
    alsX = np.array(docX)
    alsY = np.array(docY)
    return alsX, alsY


def train_test_split(data):
    """
    This just splits data to training and testing parts
    """
    ntrn = round(len(data) * (1 - testSize))
    return (load_data(data.iloc[0:ntrn])), (load_data(data.iloc[ntrn:])), (load_data(data.iloc[:len(data) - ntrn]))


# d = pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9], [10, 11, 12], [13, 14, 15], [16, 17, 18]])
# (a, b) = load_data(d)
# print(a)
# print(b)
# d = pd.DataFrame([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
# print(train_test_split(d))
df = pd.read_csv(fileName, sep=',', header=None)
feature_count = df.shape[1]
num_of_vectors = df.shape[0]
# print(df.shape)
np.set_printoptions(threshold=np.inf)

(X_train, Y_train), (X_test, Y_test), (X_test2, Y_test2) = train_test_split(df)
# print(X_train.shape)
model = None
if path.exists(modelPath) and path.exists(weightPath):
    model = load_model()
else:
    model = Sequential()
    model.add(LSTM(hidden_neurons, return_sequences=True, input_shape=(windowSize, feature_count), activation="relu"))
    model.add(LSTM(hidden_neurons, return_sequences=False, input_shape=(windowSize, feature_count), activation="relu"))
    model.add(Dense(predictionWindowSize * feature_count, activation='tanh'))
    model.add(Dense(predictionWindowSize * feature_count, activation='relu'))
    model.add(Dense(predictionWindowSize * feature_count, activation='relu'))
    model.add(Dense(predictionWindowSize * feature_count, activation='relu'))

    model.compile(loss=loss, optimizer="adam", metrics=[binary_fbeta, f1_m, f1, f1_2, "acc"])
    # print(model.summary())
    model.fit(X_train, Y_train, epochs=nbEpoch, batch_size=batchSize, validation_data=(X_test, Y_test), verbose=1)
scores = model.evaluate(X_test, Y_test, verbose=1)

output += "\t" + str(round(scores[1] * 100))
output += "\t" + str(round(scores[2] * 100))
output += "\t" + str(round(scores[3] * 100))
output += "\t" + str(round(scores[4] * 100))
output += "\t" + str(round(scores[5] * 100))
scores = model.evaluate(X_test2, Y_test2, verbose=0)
output += "\t" + str(round(scores[1] * 100))
output += "\t" + str(round(scores[2] * 100))
output += "\t" + str(round(scores[3] * 100))
output += "\t" + str(round(scores[4] * 100))
output += "\t" + str(round(scores[5] * 100))
print(output)
# model.compile(loss="mean_squared_error", optimizer="rmsprop")
# model.fit(X_train, y_train, epochs=nbEpoch, batch_size=batchSize, validation_split=validationSplit)
# save_model(model)
# print(X_test.shape)
predicted = np.apply_along_axis(binary_encoder, 1, model.predict(X_test2))
# print(predicted.shape)
# rmse = np.sqrt(((predicted - Y_test) ** 2).mean(axis=1))
# print(rmse.shape)
# print(rmse)
print(tf.cast(tf.greater_equal(predicted[0], tf.constant(threshold)), tf.float32))
print(Y_test2[0])
print("-----------------------------------------------------------------------------------------------------------")
print(tf.cast(tf.greater_equal(predicted[1], tf.constant(threshold)), tf.float32))
print(Y_test2[1])
print("-----------------------------------------------------------------------------------------------------------")
print(tf.cast(tf.greater_equal(predicted[2], tf.constant(threshold)), tf.float32))
print(Y_test2[2])
print("-----------------------------------------------------------------------------------------------------------")
print(tf.cast(tf.greater_equal(predicted[3], tf.constant(threshold)), tf.float32))
print(Y_test2[3])
print("-----------------------------------------------------------------------------------------------------------")
print(tf.cast(tf.greater_equal(predicted[4], tf.constant(threshold)), tf.float32))
print(Y_test2[4])
print("-----------------------------------------------------------------------------------------------------------")
print(tf.cast(tf.greater_equal(predicted[5], tf.constant(threshold)), tf.float32))
print(Y_test2[5])

# print("-----------------------------------------------------------------------------------------------------------")

# pd.DataFrame(predicted[:99]).plot()
# pd.DataFrame(y_test[:99]).plot()
