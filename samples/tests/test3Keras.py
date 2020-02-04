from keras.utils import to_categorical
from keras.datasets import mnist
from keras.models import Model
from keras.layers import Conv2D, Dense, MaxPool2D, Flatten, Input
import numpy as np

data=mnist.load_data()
x, y = data[0][0], data[0][1]

x = x/x.max()

#print (x.shape)

inp = Input( (28,28,1) )
layer1 = Conv2D (64, (3,3), activation='relu')(inp)
layer2 = MaxPool2D((3,3))(layer1)
layer3 = Flatten()(layer2)
layer4 = Dense(10, activation='softmax')(layer3)
mdl = Model(input=inp, outputs=layer4)
mdl.summary()

mdl.compile(loss='categorical_crossentropy', optimizer='sgd')
x = np.expand_dims(x, -1)
print (x.shape)

y = to_categorical(y, 10)
print (y.shape)

mdl.fit(x,y, epochs=2)


