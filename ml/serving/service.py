import pandas as pd
import numpy as np
import pickle
import json
import matplotlib.pyplot as plt
import re
import io
import urllib.request
from konlpy.tag import Okt
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras.preprocessing.text import Tokenizer
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.models import load_model
from keras_preprocessing.text import tokenizer_from_json

import bentoml
from bentoml import BentoService, api, artifacts, env
from bentoml.frameworks.keras import KerasModelArtifact
from bentoml.service.artifacts.common import PickleArtifact
from bentoml.adapters import JsonInput, JsonOutput

@env(infer_pip_packages=True)
@artifacts([KerasModelArtifact('model'), PickleArtifact('tokenizer')])

class sentimentalClassifier(bentoml.BentoService):
    @api(input=JsonInput(), batch=False)

    def predict(self, parsed_json):
        okt = Okt()
        stopwords = ['도', '는', '다', '의', '가', '이', '은', '한', '에', '하', '고', '을', '를', '인', '듯', '과', '와', '네', '들', '듯', '지', '임', '게', '좀', '으로', '하다', '걍', '네요', '잘']
        maxlen = 80

        new_sentence = parsed_json.get("text")
        new_sentence = re.sub(r'<.+?>', '', new_sentence)
        new_sentence = re.sub(r'[^\uAC00-\uD7A30-9a-zA-Z]', ' ', new_sentence)
        new_sentence = re.sub(r'[^ㄱ-ㅎㅏ-ㅣ가-힣 ]','', new_sentence)
        new_sentence = re.sub('\s+', ' ',new_sentence)
        new_sentence = okt.morphs(new_sentence)
        new_sentence = [word for word in new_sentence if not word in stopwords]

        tokenizer = self.artifacts.tokenizer.get("tokenizer")
        encoded = tokenizer.texts_to_sequences([new_sentence])
        pad_new = pad_sequences(encoded, maxlen=maxlen)

        result = self.artifacts.model.predict(pad_new)

        return result
