import numpy
from tensorflow import keras
from tensorflow.keras.models import model_from_json
from tensorflow.keras import backend as K
import tensorflow as tf
import re
import numpy as np
# from keras.preprocessing.text import Tokenizer
from pandas import DataFrame

threshold = 0.3


def binary_r(y_true, y_pred):
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

    return recall


def binary_p(y_true, y_pred):
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

    return precision


def binary_acc1(y_true, y_pred):
    beta_squared = 1 ** 2  # squaring beta
    epsilon = 1e-7
    # casting ytrue and ypred as float dtype
    ytrue = tf.cast(y_true, tf.float32)
    ypred = tf.cast(y_pred, tf.float32)
    # setting values of ypred greater than the set threshold to 1 while those lesser to 0
    ypred = tf.cast(tf.greater_equal(ypred, tf.constant(threshold)), tf.float32)

    correct = tf.reduce_sum(ytrue * ypred)  # calculating true positives
    total = tf.reduce_sum(ytrue)
    return correct / (total + epsilon)


def binary_acc2(y_true, y_pred):
    beta_squared = 1 ** 2  # squaring beta
    epsilon = 1e-7
    # casting ytrue and ypred as float dtype
    ytrue = tf.cast(y_true, tf.int8)
    ypred = tf.cast(y_pred, tf.float32)
    # setting values of ypred greater than the set threshold to 1 while those lesser to 0
    ypred = tf.cast(tf.greater_equal(ypred, tf.constant(threshold)), tf.int8)

    correct = tf.reduce_sum((ytrue | ypred) & ~(ytrue & ypred))  # calculating true positives
    total = 512
    return tf.cast(correct, tf.float32) / (512.0 + epsilon)


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


def binary_f(y_true, y_pred):
    tp = 0.0
    tn = 0.0
    fp = 0.0
    fn = 0.0
    for i in range(0, y_pred.shape[1]):
        p = 0.0
        l = y_true[i]
        print(tf.keras.get_value(y_pred))
        if y_pred[i] > threshold:
            p = 1.0
        if p == 1.0 and l == 1.0:
            tp += 1.0
        elif p == 1.0 and l == 0.0:
            fp += 1.0
        elif p == 0.0 and l == 1.0:
            fn += 1.0
        else:
            tn += 1.0
        print(y_pred[i])
        print(",")
        print(y_true[i])
        print("\n")
    precision = tp / (tp + fp)
    recall = tp / (tp + fn)
    accuracy = (tp + tn) / (tp + tn + fp + fn)

    # calculating fbeta
    fb = 2 * ((precision * recall) / (precision + recall))

    return fb


def binary_a(y_true, y_pred):
    tp = 0.0
    tn = 0.0
    fp = 0.0
    fn = 0.0

    for i in range(0, y_pred.shape[1]):
        p = 0.0
        l = y_true[i]
        if y_pred[i] > threshold:
            p = 1.0
        if p == 1.0 and l == 1.0:
            tp += 1.0
        elif p == 1.0 and l == 0.0:
            fp += 1.0
        elif p == 0.0 and l == 1.0:
            fn += 1.0
        else:
            tn += 1.0

    return (tp + tn) / (tp + tn + fp + fn)


def binary_pp(y_true, y_pred):
    tp = 0.0
    tn = 0.0
    fp = 0.0
    fn = 0.0

    for i in range(0, y_pred.shape[1]):
        p = 0.0
        l = y_true[i]
        if y_pred[i] > threshold:
            p = 1.0
        if p == 1.0 and l == 1.0:
            tp += 1.0
        elif p == 1.0 and l == 0.0:
            fp += 1.0
        elif p == 0.0 and l == 1.0:
            fn += 1.0
        else:
            tn += 1.0

    return tp / (tp + fp)


def binary_rr(y_true, y_pred):
    tp = 0.0
    tn = 0.0
    fp = 0.0
    fn = 0.0

    for i in range(0, y_pred.shape[1]):
        p = 0.0
        l = y_true[i]
        if y_pred[i] > threshold:
            p = 1.0
        if p == 1.0 and l == 1.0:
            tp += 1.0
        elif p == 1.0 and l == 0.0:
            fp += 1.0
        elif p == 0.0 and l == 1.0:
            fn += 1.0
        else:
            tn += 1.0

    return tp / (tp + fn)


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


def save_model(modelIn, modelPath, weightPath):
    model_json = modelIn.to_json()
    with open(modelPath, "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    modelIn.save_weights(weightPath)
    print("Saved model to disk")


def load_model(modelPath, weightPath):
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


def readWordToInfo(infoPath):
    wordsInfo = dict()
    featuresInfo = dict()
    vectorInfo = dict()
    for line in open(infoPath, "r").readlines():
        info = line.lower().split(':')
        vectorInfo[info[0]] = info[2]
        wordsInfo[info[2]] = (info[1], info[3].replace("\n", ""), info[0])
        featuresInfo[info[1]] = (info[2], info[3].replace("\n", ""), info[0])
    return wordsInfo, featuresInfo, vectorInfo


def featuresToWords(process, featuresInfo):
    if "@" in process:
        out = ""
        for token in process.split(" "):
            out += featuresInfo[token][0] + " "
        return out
    return process


def wordsToFeatures(process, wordsInfo):
    out = ""
    for token in re.split(" |\'", process):
        if token in wordsInfo:
            out += wordsInfo[token][0] + " "
        else:
            out += token + " "
    return out


def pokh(dataPath, windowSize):
    sequences = list()
    for line in open(dataPath, "r").read().split('\n'):
        encoded = line.split()
        for i in range(1, len(encoded)):
            sequence = encoded[:i + 1]
            sequences.append(sequence)
    print('Total Sequences: %d' % len(sequences))


def __generate_3tuple_keys(self, data):
    for i in range(len(data) - 3):
        if not data[i].__contains__("e") and not data[i + 3].__contains__("s"):
            yield [data[i], data[i + 1], data[i + 2], data[i + 3]]


def pokh_with_padding(data, windowSize, futureSize, startKeyword="s", endKeyword="e", tokenDelimiter=' ',
                      processDelimiter='\n'):
    begin = tokenDelimiter.join([startKeyword + str(x) for x in range(1, windowSize + 1)])
    end = tokenDelimiter.join([endKeyword + str(x) for x in range(1, futureSize + 1)])
    docX, docY = [], []
    for line in data.split(processDelimiter):
        if len(line) < 1:
            continue
        words = (begin + tokenDelimiter + line + tokenDelimiter + end).split(tokenDelimiter)
        for i in range(len(words) - (windowSize + futureSize - 1)):
            docX.append(words[i:i + windowSize])
            docY.append(words[i + windowSize:i + windowSize + futureSize])
    alsX = np.array(docX)
    alsY = np.array(docY)
    return alsX, alsY


def wordsToOneHot(data, windowSize, futureSize, startKeyword="s", endKeyword="e", tokenDelimiter=' ',
                  processDelimiter='\n'):
    begin = tokenDelimiter.join([startKeyword + str(x) for x in range(1, windowSize + 1)])
    end = tokenDelimiter.join([endKeyword + str(x) for x in range(1, futureSize + 1)])
    tokenizer = Tokenizer()
    tokenizer.fit_on_texts([begin, end] + data.split(processDelimiter))
    vocabulary_size = len(tokenizer.word_counts) + 1

    docX, docY = [], []
    for line in data.split(processDelimiter):
        if len(line) < 1:
            continue
        words = (begin + tokenDelimiter + line + tokenDelimiter + end).split(tokenDelimiter)
        for i in range(len(words) - (windowSize + futureSize - 1)):
            docX.append(words[i:i + windowSize])
            docY.append(to_categorical(tokenizer.texts_to_sequences(words[i + windowSize:i + windowSize + futureSize]),
                                       num_classes=vocabulary_size).flatten())
    return np.array(tokenizer.texts_to_sequences(docX)), np.array(docY), tokenizer, vocabulary_size


def train_test_split(input, target, testSize=0.1):
    """
    This just splits data to training and testing parts
    """
    ntrn = round(len(input) * (1 - testSize))
    return (input[0:ntrn], target[0:ntrn]), (input[ntrn:], target[ntrn:]), (input[:-ntrn], target[:-ntrn])


def train_test_split2(input, target, testSize=0.1):
    """
    This just splits data to training and testing parts
    """
    ntrn = round(len(input) * (1 - testSize))
    return (input, target), (input[:-ntrn], target[:-ntrn])


def train_test_split_KNFOLD(input, target, step, testSize=0.1):
    """
    This just splits data to training and testing parts
    """
    bucketSize = round(len(input) * testSize)
    return (numpy.concatenate([input[:bucketSize * step], input[bucketSize * (step + 1):]]),
            numpy.concatenate([target[:bucketSize * step], target[bucketSize * (step + 1):]])), (
               input[bucketSize * step:bucketSize * (step + 1)], target[bucketSize * step:bucketSize * (step + 1)])


def read_process_without_padding(data, windowSize, predictionWindowSize, vecDelimiter=';', processDelimiter='\n'):
    docX, docY = [], []
    a = []
    for line in data.split(processDelimiter):
        if len(line.split(vecDelimiter)) < windowSize:
            continue
        if len(line) < 2:
            continue
        for vec in line.split(vecDelimiter):
            if len(vec) > 2:
                a.append([int(x) for x in vec])
    a = DataFrame(a)
    for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
        docX.append(a.iloc[i:i + windowSize].values)
        docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize, 258:-5].values.flatten())
    # docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values.flatten())
    return np.array(docX).astype('float32'), np.array(docY).astype('float32')


def read_process_with_empty_padding(data, windowSize, predictionWindowSize, vecDelimiter=';', processDelimiter='\n'):
    docX, docY = [], []
    a = []
    featureSize = 365  # len(data[:10000].split(processDelimiter)[0])
    for line in data.split(processDelimiter):
        if len(line) < 2:
            continue
        for i in range(0, windowSize):
            a.append([0 for x in range(featureSize)])
        for vec in line.split(vecDelimiter):
            if len(vec) > 2:
                a.append([int(x) for x in vec])
    a = DataFrame(a)
    for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
        if isValid(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values):
            docX.append(a.iloc[i:i + windowSize].values)
            docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values.flatten())
    return np.array(docX).astype('float32'), np.array(docY).astype('float32')


def read(data, windowSize, predictionWindowSize, reserved, featureSize, vecDelimiter=';', processDelimiter='\n'):
    docX, docY, a = [], [], []
    for line in data.split(processDelimiter):
        if len(line.split(vecDelimiter)) < predictionWindowSize:
            continue
        for i in range(0, min(windowSize, reserved)):
            a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
        for vec in line.split(vecDelimiter):
            if len(vec) > 2:
                a.append([int(x) for x in vec])
    a = DataFrame(a)
    for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
        if isValid(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values, featureSize, reserved):
            docX.append(a.iloc[i:i + windowSize].values)
            docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize, :-(reserved+14)].values.flatten())
    return np.array(docX).astype('float32'), np.array(docY).astype('float32')


def read_process_with_nonempty_padding(data, windowSize, predictionWindowSize, reserved, fullTrain, featureSize=412,
                                       vecDelimiter=';', processDelimiter='\n'):
    docX, docY = [], []
    a = []

    if fullTrain:
        for line in data.split(processDelimiter):
            # if len(line.split(vecDelimiter)) < windowSize:
            #    continue
            for i in range(0, min(windowSize, reserved)):
                a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
            for vec in line.split(vecDelimiter):
                if len(vec) > 2:
                    a.append([int(x) for x in vec])
            for i in range(0, predictionWindowSize):
                a.append([0 for x in range(featureSize)])
        a = DataFrame(a)
        for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
            docX.append(a.iloc[i:i + windowSize].values)
            docY.append(
                a.iloc[i + windowSize:i + windowSize + predictionWindowSize, :-reserved].values.reverse().flatten())
    else:
        for line in data.split(processDelimiter):
            if len(line.split(vecDelimiter)) < predictionWindowSize:
                continue
            for i in range(0, min(windowSize, reserved)):
                a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
            for vec in line.split(vecDelimiter):
                if len(vec) > 2:
                    a.append([int(x) for x in vec])
        a = DataFrame(a)
        for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
            if isValid(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values, featureSize, reserved):
                docX.append(a.iloc[i:i + windowSize].values)
                docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize, :-reserved].values.flatten())
    return np.array(docX).astype('float32'), np.array(docY).astype('float32')


def read_process_with_nonempty_padding2(data, windowSize, predictionWindowSize, reserved, featureSize=412,
                                        vecDelimiter=';', processDelimiter='\n'):
    docX, docY = [], []
    a = []
    for line in data.split(processDelimiter):
        if len(line) < 2:
            continue
        for i in range(0, min(windowSize, reserved)):
            a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
        for vec in line.split(vecDelimiter):
            if len(vec) > 2:
                a.append([int(x) for x in vec])
    a = DataFrame(a)
    for i in range(len(a) - (windowSize + predictionWindowSize - 1)):
        if isValid(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values, featureSize, reserved):
            docX.append(a.iloc[i:i + windowSize].values.flatten())
            docY.append(a.iloc[i + windowSize:i + windowSize + predictionWindowSize].values.flatten())
    return np.array(docX).astype('float32'), np.array(docY).astype('float32')


def isValid(window, featureSize, reserved=0):
    for vec in window:
        if not 1 in vec[:featureSize - (reserved+14)]:
            return False
    return True


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
        for i in range(len(vectors), windowSize):
            a.append([1 if x == featureSize - i - 1 else 0 for x in range(featureSize)])
        for vec in vectors:
            a.append([int(x) for x in vec])
    return a
