import pickle

from service import sentimentalClassifier

import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model

model = keras.models.load_model('./gru_review_saved_model_0621/1/')
with open('./tokenizer.pickle', 'rb') as handle:
    tokenizer = pickle.load(handle)

# Create a sentimental classifier service instance
bento_service = sentimentalClassifier()

# Pack the trained model artifact
bento_service.pack("model", model)
bento_service.pack('tokenizer', {'tokenizer': tokenizer})

# Save the prediction service to disk for model serving
saved_path = bento_service.save()
