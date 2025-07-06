#!/bin/bash

mkdir -p generated

if [ ! -d ../images/generated/one ]; then
    echo "Images must be generated first"
    exit 1
fi

for i in 1 2 3 4 5;
do
    if [ -e generated/movie_${i}.mp4 ]; then
        echo "Movie $i found"
        continue
    fi
    ffmpeg -framerate 1/2 -pattern_type glob -i
    "../images/generated/one/image_${i}*.png" -r 25 -vf scale=640x480 \
        -c:v libx264 -pix_fmt yuv420p  generated/movie_${i}.mp4
    echo "Created movie $i"
done
