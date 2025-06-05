#!/bin/bash

set -e # exit on error

# This script increments the version number supplied as a parameter.
# It is run by the CI/CD pipeline before building the docker images.

# This script uses the "calver" versioning scheme.
# The version number is in the format "vYYYY.MM.PATCH"
# The PATCH number is incremented every time the script is run
# The YEAR and MONTH are taken from the current date
# Calver is a form of semver.

increment_version() {
    OLD_VERSION=$1
    echo >&2 "Old version: $OLD_VERSION"

    # The semver standard does not permit leading zeros in the version number
    CURRENT_YEAR_MONTH=$(date +"v%Y.%-m.")
    CURRENT_YEAR_MONTH_LENGTH=${#CURRENT_YEAR_MONTH}
    OLD_YEAR_MONTH=${OLD_VERSION:0:$CURRENT_YEAR_MONTH_LENGTH}
    OLD_PATCH=${OLD_VERSION:$CURRENT_YEAR_MONTH_LENGTH}

    if [[ "$CURRENT_YEAR_MONTH" != "$OLD_YEAR_MONTH" ]]; then
        NEW_PATCH=0
    else
        NEW_PATCH=$((OLD_PATCH + 1))
    fi

    NEW_VERSION="$CURRENT_YEAR_MONTH$NEW_PATCH"
    echo "$NEW_VERSION"
    echo >&2 "Version incremented to: $NEW_VERSION"
}

# Check $1 is set
if [ -z "$1" ]; then
    echo "Usage: increment_version.sh <old_version>"
    exit 1
fi

increment_version $1