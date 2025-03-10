
set -e

(cd ../base/docker && ./build.sh)

# iterate over all directories in the apps directory

for d in ../apps/* ; do
  if [ -d "$d" ]; then
    (cd $d && ./build.sh)
  fi
done
