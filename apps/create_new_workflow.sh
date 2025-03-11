#!/bin/bash

set -e

NAME=$1

if [ -z "$NAME" ]
    then
    echo "Specify the name of the workflow as the first parameter"
    exit 1
fi

cp -r example $NAME
sed -i -e "s/workflows-example/workflows-${NAME}/g" $NAME/Dockerfile

echo "Workflow $NAME created."
echo "Suggestion: create a commit now to track your changes on the newly created workflow"
echo "            run:"
echo "                 git add $NAME/* && git commit -m'Add $NAME workflow' "
