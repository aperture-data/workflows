
docker run \
           --net=host \
           -e RUN_NAME=${RUN_NAME} \
           -e DB_HOST=localhost \
           -e DB_PASS="admin" \
           -e PUSH_TO_S3=true \
           -e POST_TO_SLACK=true \
           -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
           -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
           -e SLACK_BOT_TOKEN=${SLACK_BOT_TOKEN} \
           aperturedata/workflows-example
