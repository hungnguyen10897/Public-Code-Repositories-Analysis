from pathlib import Path

def process(file_directory):

    if not file_directory.exists():
        return

    for file in file_directory.glob("*builds.csv"):
        archive_file = Path(str(file).replace("builds.csv", "builds_staging.csv"))
        file.rename(archive_file)

if __name__ == "__main__":
    file_directory = Path("./data/builds")
    process(file_directory)