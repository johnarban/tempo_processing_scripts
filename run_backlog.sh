
#!/bin/bash


# Read the list of folders from the backlog file
backlog_file="backlog"
while IFS= read -r folder; do
    echo "Processing folder: $folder"
    python get_new_tempo_data.py --data-dir "$folder" --use-subset --skip-download --output-dir "all_reprocessed" --name "$folder"
    # break the loop
    # break
done < "$backlog_file"