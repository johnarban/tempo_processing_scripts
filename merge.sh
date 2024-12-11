#!/bin/bash

# Define source and destination directories
src="/Volumes/Extreme SSD/TEMPO_data/$1/images"
dest="/Users/jal194/github/tempo-data-holdings/released/images"
dryrun=false
delsrcpng=false

while getopts ':s:d:tx' opt; do
    case $opt in
        's')
            if [[ -n "$OPTARG" ]]; then
                src="$OPTARG"
            fi
        ;;

        'd') 
            if [[ -n "$OPTARG" ]]; then
                dest="$OPTARG"
            fi
        ;;
        't')
            dryrun=true
        ;;
        'x')
            delsrcpng=true
        ;;
    esac
done

echo "Merge source directory: $src"
echo "Merge destination dir: $dest"


# Function to check if a directory exists and print an error message if not
check_dir_exists() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        echo "${dir} does not exist"
        exit 1
    fi
}


check_dir_exists_and_make() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        # make directory and parents if necessary
        mkdir -p "$dir"
    fi
}


# Check if source and destination directories exist
check_dir_exists "$src"
check_dir_exists_and_make "$dest"

# Copy contents of the source directory to the destination directory
# The -R option ensures recursive copying, and -n prevents overwriting existing files
# The --parents option preserves the directory structure of the copied files
# if cp -R does not work, then find all files in src and run cp that file to dest
# gcp -R "$src/"* "$dest"
if [ $dryrun = false ]; then
    rsync -av "$src" "$dest"
    echo "merged"
else
    echo "rsync -av \"$src\" \"$dest\""
    echo "dry run"
fi

# Since cp with -Rn already handles subdirectories and prevents overwriting,
# there's no need for a separate loop to handle subdirectories explicitly.
