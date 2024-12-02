
#!/bin/bash


# Read the list of folders from the backlog file
backlog_file="/Volumes/Extreme SSD/TEMPO_data/backlog"
while IFS= read -r folder; do
    echo "Processing folder: $folder"
    # python .get_new_tempo_data.py --data-dir "$folder" --use-subset
done < "$backlog_file"