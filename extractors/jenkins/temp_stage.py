from pathlib import Path

def process(file_directory):

    if not file_directory.exists():
        return

    for file in file_directory.glob("*.csv"):
        archive_file = Path(str(file).replace(".csv", "_staging.csv"))
        file.rename(archive_file)

if __name__ == "__main__":
    directory = Path("./data/builds")
    process(directory)
