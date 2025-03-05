# Pull base image.
FROM aperturedata/workflows-base

ENV APP_NAME=workflows-nsfw-detection

RUN pip3 install tensorflow==2.14 tensorflow-hub==0.15 tensorflow-estimator==2.14  pillow numpy keras==2.14
# RUN pip3 install tensorflow>=2.15 tensorflow-hub==0.15 pillow numpy   keras
# RUN pip3 install tensorflow tensorflow_hub keras
# RUN pip3 install tensorflow==2.14 tensorflow_hub==0.15 keras

COPY requirements.txt /app/requirements.txt

RUN pip3 install -r /app/requirements.txt

COPY app /app/
