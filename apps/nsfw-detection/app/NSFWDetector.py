import threading

from io import BytesIO
from PIL import Image

import numpy as np

import tensorflow_hub as hub
import tensorflow as tf

import keras

lock = threading.Lock()

class NSFWDetector():

    def __init__(self, model_path):

        self.model_path = model_path

        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

        print('Loading NSFW Tensorflow model...')

        self.nsfw_model = keras.models.load_model(self.model_path, compile=False,
                                                  custom_objects={'KerasLayer': hub.KerasLayer})


    def infer(self, image_enc):

        stream = BytesIO(image_enc)
        input_image = Image.open(stream).convert("RGB")
        # stream.close()

        input_image = np.asarray(input_image, dtype=np.float64)
        input_image = np.copy(input_image)

        input_image /= 255
        input_image = input_image[np.newaxis is None,:,:,:]
        model_preds = self.nsfw_model.predict(input_image, verbose=0)[0]

        model_preds_dict = {
            'drawings': model_preds[0].item(),
            'hentai': model_preds[1].item(),
            'neutral': model_preds[2].item(),
            'porn': model_preds[3].item(),
            'sexy': model_preds[4].item()
        }

        return model_preds_dict


    def infer_batch(self, images):

        # print("preparing inference...")
        imgs_ready = None
        for i in images:

            stream = BytesIO(i)
            input_image = Image.open(stream).convert("RGB")
            # stream.close()

            input_image = np.asarray(input_image, dtype=np.float64)
            input_image = np.copy(input_image)

            input_image /= 255
            input_image = input_image[np.newaxis is None,:,:,:]

            if imgs_ready is None:
                imgs_ready = input_image
                continue

            imgs_ready = np.concatenate([imgs_ready, input_image], axis=0)

        # lock.acquire()
        predictions = self.nsfw_model.predict(imgs_ready, verbose=0)
        # predictions = self.nsfw_model.predict(imgs_ready)
        # lock.release()

        ret_predictions = []
        for model_preds in predictions:

            model_preds_dict = {
                'drawings': model_preds[0].item(),
                'hentai': model_preds[1].item(),
                'neutral': model_preds[2].item(),
                'porn': model_preds[3].item(),
                'sexy': model_preds[4].item()
            }

            ret_predictions.append(model_preds_dict)

        return ret_predictions
