# This builds the dependencies for the crawl-to-rag.

cd ../crawl-website
bash build.sh
cd -

cd ../text-extraction
bash build.sh
cd -

cd ../text-embeddings
bash build.sh
cd -

bash build.sh