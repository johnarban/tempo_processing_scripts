#!/bin/bash

# look in all folders. look for .nc files in the folder
# if you find a folder like that, run subset.sh on it

# get a list of top level folders in current directory
folders=$(ls -d */)
# exclude "__pycache__" folder
folders=$(echo $folders | sed 's/\_\_pycache\_\_\///')
# exclude "alldata" folder
folders=$(echo $folders | sed 's/alldata\///')
# exclude "colormap_test" folder
folders=$(echo $folders | sed 's/colormap\_test\///')
# exclude "live_data" folder
folders=$(echo $folders | sed 's/live\ data\///')
# exclude "test_images" folder
folders=$(echo $folders | sed 's/test\_images\///')
# exclude "early_release" folder
folders=$(echo $folders | sed 's/early\_release\///')
echo $folders


# loop through each folder
# for folder in $folders
# do
#     echo "Running subset.sh on $folder"
#     sh subset_files.sh $folder
# done