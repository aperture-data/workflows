FROM aperturedata/wf-base

# Do not remove this.
# This env variable is needed to correctly
# upload results to S3 buckets.
# It is set by build.sh
ARG APP_NAME
ENV APP_NAME=${APP_NAME}

# Start adding specifics for your wf here:

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

COPY app/ /app/
