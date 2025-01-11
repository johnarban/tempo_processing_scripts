#!/usr/bin/env bash

# Directory where the original images are located is the argument
inputDir="$*"
# unescape spaces
inputDir="${inputDir//\\ / }"

echo "Subsetting *.nc files in directory: $inputDir"

# output directory is the input directory with "subset_images" appended
outputDir=$inputDir"/subsetted_netcdf"

echo "Output directory: $outputDir"

mkdir -p "$outputDir"  # Create the output directory if it doesn't exist


# Get the list of *.nc files in the input directory
files=("$inputDir"/*.nc)
lengthFiles=
# Count the total number of *.nc files in the input directory using wc -l
totalFiles=${#files[@]}
echo "Total number of files: $totalFiles"


# Iterate over each *.nc file in the input directory
for file in "${files[@]}"; do


  # Use subset_tempo_data.py to subset the file and save it in the output directory
  basename="${file##*/}"
  echo $file
  python ./subset_tempo_data.py -f "$inputDir/$basename" -o "$outputDir" -d

  # Clear the progress bar after updating it
  # printf "\033[2K\r"
done