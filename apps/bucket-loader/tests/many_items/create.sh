#!/bin/bash

for g in one two three four five;
do
    echo $g
    mkdir "$g"
    python3 ../generateImages.py -c 1500 -o $PWD/$g/image_%% -a "_$g"
done
