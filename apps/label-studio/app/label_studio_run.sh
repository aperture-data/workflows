#label_studio_run.sh - run the main entry part of the label studio image
cd /label-studio
source /label-studio/deploy/docker-entrypoint.sh
exec_or_wrap_n_exec uwsgi --ini /label-studio/deploy/uwsgi.ini
