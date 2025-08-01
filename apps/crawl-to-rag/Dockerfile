# Name the source images
FROM aperturedata/workflows-crawl-website as crawl-website
FROM aperturedata/workflows-text-extraction as text-extraction
FROM aperturedata/workflows-text-embeddings as text-embeddings
FROM aperturedata/workflows-rag as rag

FROM aperturedata/workflows-base

ENV APP_NAME=workflows-crawl-to-rag

# Install dependencies for embeddings
RUN pip install --no-cache-dir -r /app/embeddings/requirements_cpu.txt
RUN pip install --no-cache-dir -r /app/embeddings/requirements.txt

# copy in the app directories
COPY --from=crawl-website   /app /workflows/crawl-website
COPY --from=text-extraction /app /workflows/text-extraction
COPY --from=text-embeddings   /app /workflows/text-embeddings
COPY --from=rag     /app /workflows/rag

# copy in the requirements; hope they're compatible
COPY --from=crawl-website   /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=text-extraction /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /app/text_extraction/requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=text-embeddings   /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
COPY --from=rag     /requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt


EXPOSE 8000

COPY app.sh /app/
