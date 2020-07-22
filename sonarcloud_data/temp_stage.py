from pathlib import Path

def process(file_directory):

    if not file_directory.exists():
        return

    for file in file_directory.glob("*.csv"):
        archive_file = Path(str(file).replace(".csv", "_staging.csv"))
        file.rename(archive_file)

if __name__ == "__main__":
    analyses_directory = Path("./data/analyses")
    process(analyses_directory)
    issues_directory = Path("./data/issues")
    process(issues_directory)
    measures_directory = Path("./data/measures")
    process(measures_directory)
