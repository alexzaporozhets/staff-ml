# Import Library of Gaussian Naive Bayes model
import numpy as np
from sklearn.naive_bayes import GaussianNB

# assigning predictor and target variables
x = np.array([[-3, 7], [1, 5]])
y = np.array([0, 1])

# Create a Gaussian Classifier
model = GaussianNB()

# Train the model using the training sets
model.fit(x, y)

# Predict Output
predicted = model.predict([[-3, 2], [3, 4], [1, 1]])
print(predicted)
