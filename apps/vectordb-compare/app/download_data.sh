#!/bin/bash
#
# download_data.sh - Download vector database benchmark datasets from S3
#
# This script downloads the required datasets from S3 based on the SOURCE parameter.
# Supports: deepimage96, yfcc100m
#
# Usage: ./download_data.sh [SOURCE]
# Environment Variables:
#   SOURCE - Dataset to download (default: deepimage96)
#

set -euo pipefail  # Exit on error, undefined vars, pipe failures
IFS=$'\n\t'        # Secure Internal Field Separator

# Script directory and constants
readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly INPUT_DIR="${SCRIPT_DIR}/input"
readonly S3_BUCKET="s3://aperturedb-demos/benchmarks/data/descriptors_knn"

# Supported datasets configuration
declare -A DATASETS=(
    ["deepimage96"]="deep-image-96-angular.hdf5"
    ["yfcc100m"]="YFCC100M_hybridCNN_gmean_fc6_0.bin"
)

# Default values
readonly DEFAULT_SOURCE="deepimage96"

#######################################
# Print usage information
# Globals:
#   None
# Arguments:
#   None
#######################################
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Download vector database benchmark datasets from S3.

OPTIONS:
    -h, --help          Show this help message
    -s, --source SOURCE Specify dataset source (default: ${DEFAULT_SOURCE})
                        Supported: $(printf "%s " "${!DATASETS[@]}")

ENVIRONMENT VARIABLES:
    SOURCE              Dataset to download (overridden by -s option)

EXAMPLES:
    $0                          # Download default dataset (${DEFAULT_SOURCE})
    $0 -s yfcc100m             # Download yfcc100m dataset
    SOURCE=yfcc100m $0         # Download yfcc100m via environment variable

EOF
}

#######################################
# Log messages with timestamp
# Globals:
#   None
# Arguments:
#   $1 - Log level (INFO, ERROR, WARN)
#   $2 - Message
#######################################
log() {
    local level="$1"
    local message="$2"
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] [${level}] ${message}" >&2
}

#######################################
# Log info messages
# Arguments:
#   $1 - Message
#######################################
log_info() {
    log "INFO" "$1"
}

#######################################
# Log error messages
# Arguments:
#   $1 - Message
#######################################
log_error() {
    log "ERROR" "$1"
}

#######################################
# Check if required commands are available
# Globals:
#   None
# Arguments:
#   None
#######################################
check_dependencies() {
    local missing_deps=()

    if ! command -v aws &> /dev/null; then
        missing_deps+=("aws")
    fi

    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "Missing required dependencies: ${missing_deps[*]}"
        log_error "Please install the AWS CLI: https://aws.amazon.com/cli/"
        exit 1
    fi
}

#######################################
# Validate that the dataset source is supported
# Globals:
#   DATASETS
# Arguments:
#   $1 - Source to validate
#######################################
validate_source() {
    local source="$1"

    if [[ ! -v "DATASETS[$source]" ]]; then
        log_error "Unsupported dataset source: '$source'"
        log_error "Supported sources: $(printf "%s " "${!DATASETS[@]}")"
        return 1
    fi
}

#######################################
# Download dataset from S3
# Globals:
#   S3_BUCKET, INPUT_DIR, DATASETS
# Arguments:
#   $1 - Dataset source
#######################################
download_dataset() {
    local source="$1"
    local filename="${DATASETS[$source]}"

    log_info "Downloading ${source} dataset (${filename})..."

    # Check if file already exists and is not empty
    if [[ -f "${INPUT_DIR}/${filename}" && -s "${INPUT_DIR}/${filename}" ]]; then
        log_info "Dataset file already exists and is not empty: ${INPUT_DIR}/${filename}"
        log_info "Skipping download. Remove file to force re-download."
        return 0
    fi

    # Create input directory
    if ! mkdir -p "${INPUT_DIR}"; then
        log_error "Failed to create input directory: ${INPUT_DIR}"
        return 1
    fi

    # Download with aws s3 sync
    if aws s3 sync "${S3_BUCKET}/" "${INPUT_DIR}/" \
        --exclude='*' \
        --include="${filename}" \
        --no-progress; then

        log_info "Successfully downloaded: ${filename}"

        # Verify file was downloaded and is not empty
        if [[ -f "${INPUT_DIR}/${filename}" && -s "${INPUT_DIR}/${filename}" ]]; then
            local file_size
            file_size=$(du -h "${INPUT_DIR}/${filename}" | cut -f1)
            log_info "Downloaded file size: ${file_size}"
        else
            log_error "Download failed or file is empty: ${INPUT_DIR}/${filename}"
            return 1
        fi
    else
        log_error "Failed to download dataset from S3"
        return 1
    fi
}

#######################################
# Parse command line arguments
# Globals:
#   SOURCE
# Arguments:
#   Command line arguments
#######################################
parse_arguments() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -h|--help)
                usage
                exit 0
                ;;
            -s|--source)
                if [[ -n "${2:-}" ]]; then
                    SOURCE="$2"
                    shift 2
                else
                    log_error "Option --source requires an argument"
                    exit 1
                fi
                ;;
            -*)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                log_error "Unexpected argument: $1"
                usage
                exit 1
                ;;
        esac
    done
}

#######################################
# Main function
# Globals:
#   SOURCE, DEFAULT_SOURCE
# Arguments:
#   Command line arguments
#######################################
main() {
    # Parse command line arguments
    parse_arguments "$@"

    # Set default source if not provided
    SOURCE="${SOURCE:-${DEFAULT_SOURCE}}"

    # Convert to lowercase for consistency
    SOURCE="${SOURCE,,}"

    log_info "Starting data download script"
    log_info "Target dataset: ${SOURCE}"
    log_info "Input directory: ${INPUT_DIR}"

    # Check dependencies
    check_dependencies

    # Validate source
    if ! validate_source "${SOURCE}"; then
        exit 1
    fi

    # Download the dataset
    if download_dataset "${SOURCE}"; then
        log_info "Dataset download completed successfully"
    else
        log_error "Dataset download failed"
        exit 1
    fi
}

# Execute main function if script is run directly (not sourced)
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi
