import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import LSTM, Dense


# data preparation 

stock_data = pd.read_csv("./data/NSE-TATAGLOBAL.csv")
stock_data['Date'] = pd.to_datetime(stock_data['Date'], format = "%Y-%m-%d")
stock_data.index = stock_data['Date']
stock_data.head() 

# plot

plt.figure(figsize = (16,8))
plt.plot(stock_data['Close'], label = "Stock closing price history")

# creating a new dataframe

df = stock_data.sort_index(ascending = True, axis = 0)
data = pd.DataFrame(index = range(0, len(df)), columns = ['Date', 'Close'])

for i in range(0, len(df)):
    data['Date'][i] = df['Date'][i]
    data['Close'][i] = df['Close'][i]
    
data.index = data['Date']
data.drop('Date', axis = 1, inplace = True)
    
# train-test split

dataset = data.values
train = dataset[0:987,:]
test = dataset[987:,:]

# normalise data 

scaler = MinMaxScaler(feature_range = (0, 1))
scaled_data = scaler.fit_transform(dataset)

x_train, y_train = [], []
for i in range(60, len(train)):
    x_train.append(scaled_data[i-60:i,0])
    y_train.append(scaled_data[i,0])
    
x_train, y_train = np.array(x_train), np.array(y_train)
x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

# lstm network

model = Sequential()
model.add(LSTM(50, input_shape=(x_train.shape[1], x_train.shape[2]), return_sequences = True))
model.add(LSTM(50))
model.add(Dense(1))
model.compile(loss = 'mean_squared_error', optimizer = 'adam')
model.fit(x_train, y_train, epochs = 1, batch_size = 1, verbose = 2)

# prediction

inputs = data[len(data) - len(test) - 60:].values
inputs = inputs.reshape(-1, 1)
inputs = scaler.transform(inputs)

X_test = []
for i in range(60, inputs.shape[0]):
    X_test.append(inputs[i-60:i,0])    
X_test = np.array(X_test)
X_test = np.reshape(X_test, (X_test.shape[0], X_test.shape[1], 1))
closing_price = model.predict(X_test)
closing_price = scaler.inverse_transform(closing_price)

# root-mean-squared
rms = np.sqrt(np.mean(np.power((test-closing_price),2)))
print(rms)

# Plot

train = data[:987]
test = data[987:]
test['predictions'] = closing_price
plt.plot(train['Close'])
plt.plot(test[['Close', 'predictions']])

train.to_csv("./output/train-data.csv", index = True)
test.to_csv("./output/test-data.csv", index = True)




