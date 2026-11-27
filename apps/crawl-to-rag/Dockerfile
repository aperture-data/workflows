# Name the source images
ARG VERSION=latest
FROM aperturedata/workflows-crawl-website:${VERSION} AS crawl-website
FROM aperturedata/workflows-text-extraction:${VERSION} AS text-extraction
FROM aperturedata/workflows-text-embeddings:${VERSION} AS text-embeddings
FROM aperturedata/workflows-rag:${VERSION} AS rag

FROM aperturedata/workflows-base:${VERSION}

ENV APP_NAME=workflows-crawl-to-rag

# Install dependencies for embeddings
# Install CPU torch first so the embeddings extra does not pull the CUDA build.
RUN pip install --no-cache-dir --index-url https://download.pytorch.org/whl/cpu "torch>=2.0" torchvision
RUN pip install --no-cache-dir "aperturedb-workflows[embeddings]"

# copy in the app directories
COPY --from=crawl-website   /app /workflows/crawl-website
COPY --from=text-extraction /app /workflows/text-extraction
COPY --from=text-embeddings   /app /workflows/text-embeddings
COPY --from=rag     /app /workflows/rag

# copy in the requirements; hope they're compatible
COPY --from=crawl-website   /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir "aperturedb-workflows[text]"
COPY --from=text-embeddings   /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=rag     /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


EXPOSE 8000

COPY app.sh /app/
COPY reconfigure.py /app/
