#label_studio_init.sh - run the initialization parts of the label studio image
cd /label-studio
source /label-studio/deploy/docker-entrypoint.sh
exec_entrypoint "$ENTRYPOINT_PATH/app/"
