from pathlib import Path

def process(file_directory):

    if not file_directory.exists():
        return

    for file in file_directory.glob("*_staging.csv"):
        archive_file = Path(str(file).replace("_staging", ""))
        file.rename(archive_file)

if __name__ == "__main__":
    file_directory = Path("./data/builds")
    process(file_directory)