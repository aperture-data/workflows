
set -e

(cd ../base/docker && ./build.sh $1)

# iterate over all directories in the apps directory

for d in ../apps/* ; do
  if [ -d "$d" ]; then
    (cd $d && ./build.sh)
  fi
done
