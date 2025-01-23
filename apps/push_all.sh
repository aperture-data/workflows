
set -e

docker push aperturedata/workflows-base

for d in ../apps/* ; do
  if [ -d "$d" ]; then
    docker push aperturedata/workflows-$(basename $d)
  fi
done
