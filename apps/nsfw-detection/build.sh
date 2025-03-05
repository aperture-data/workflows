#!/bin/bash

aws s3 sync s3://aperturedb-demos/nsfw_detector app/input/

docker build -t aperturedata/workflows-nsfw-detection .
