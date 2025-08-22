cd /label-studio
source /label-studio/deploy/docker-entrypoint.sh
exec_entrypoint "$ENTRYPOINT_PATH/app/"
