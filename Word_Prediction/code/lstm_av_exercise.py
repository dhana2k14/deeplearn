import numpy
from keras.models import Sequential
from keras.layers import Dense
from keras.layers import LSTM
from keras.layers import Dropout
from keras.utils import np_utils

# load text file
filename = "macbeth.txt"
text = (open(filename).read()).lower()
unique_chars = sorted(list(set(text)))

char_to_int = {}
int_to_char = {}

for i, c in enumerate(unique_chars):
    #print(i, c)
    char_to_int.update({c:i})
    int_to_char.update({i:c})
    
X = []
Y = []

for i in range(0, len(text)-50, 1):
    sequence = text[i:i+50]
    label = text[i+50]
    X.append([char_to_int[char] for char in sequence])
    Y.append([char_to_int[label]])
    
# reshaping 

x_modified = numpy.reshape(X, (len(X), 50, 1))
x_modified = x_modified / float(len(unique_chars))
Y_modified = np_utils.to_categorical(Y)

# LSTM model network

model = Sequential()
model.add(LSTM(300, input_shape=(x_modified.shape[1], x_modified.shape[2]), return_sequences = True))
model.add(Dropout(0.2))
model.add(LSTM(300))
model.add(Dropout(0.2))
model.add(Dense(Y_modified.shape[1], activation = 'softmax'))
model.compile(loss = 'categorical_crossentropy', optimizer = 'adam')

# fitting the model

model.fit(x_modified, Y_modified, epochs = 1, batch_size = 30)

# pick a random string 

start_index = numpy.random.randint(0, len(X)-1)
new_string = X[start_index]

for i in range(50):
    x = numpy.reshape(new_string, (1, len(new_string), 1))
    x = x / float(len(unique_chars))

    # prediction 
    pred_index = numpy.argmax(model.predict(x, verbose = 0))
    char_out = int_to_char[pred_index]
    seq_in = [int_to_char[value] for value in new_string]
    print(char_out)
    
    new_string.append(pred_index)
    new_string = new_string[1:len(new_string)]



