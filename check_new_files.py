from pathlib import Path
from get_tempo_data_utils import fetch_granule_data

if __name__ == "__main__":
    noPath = Path('.')
    fetch_granule_data(None, None, noPath, noPath, noPath, noPath, skip_download=False, check_only=True)
