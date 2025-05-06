
set -e

# STart by changing to the directory of this script
DIR=$(dirname $(readlink -f $0))
cd $DIR

(cd ../base/docker && ./build.sh)

# iterate over all directories in the apps directory

for d in ../apps/* ; do
  if [ -d "$d" ]; then
    (cd $d && ./build.sh)
  fi
done
