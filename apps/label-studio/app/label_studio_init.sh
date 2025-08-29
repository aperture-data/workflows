#label_studio_init.sh - run the initialization parts of the label studio image
cd /label-studio
# we may be running in a virtual evn, but the main image doens't install in one,
# so we should exit.
if [ "$VIRTUAL_ENV" != "" ]; then
    unset VIRTUAL_ENV
    export PATH=${PATH#*:}
fi
source /label-studio/deploy/docker-entrypoint.sh
exec_entrypoint "$ENTRYPOINT_PATH/app/"
