# This builds the dependencies for the crawl-to-rag.
set -e

cd ../crawl-website
bash ../build.sh
cd -

cd ../text-extraction
bash ../build.sh
cd -

cd ../text-embeddings
bash ../build.sh
cd -

cd ../rag
bash ../builds.sh
cd -

bash ../build.sh