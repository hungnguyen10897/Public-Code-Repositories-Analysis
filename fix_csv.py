from pathlib import Path
import sys

def process(file_directory):

    if not file_directory.exists():
        print("Directory not found")
        return

    for file in file_directory.glob("*.csv"):
        lines = []
        taken_lines = []
        with open(file, 'r') as f:
            lines = f.readlines()
            for i in range(len(lines)):
                line = lines[i]
                if i == 0:
                    taken_lines.append(line)
                if line not in ["job,build_number,result,duration,estimated_duration,revision_number,commit_id,commit_ts,test_pass_count,test_fail_count,test_skip_count,total_test_duration\n",
                "project,analysis_key,date,project_version,revision\n"]:
                    taken_lines.append(line)
        
        with open(file, 'w') as f:
            f.writelines(taken_lines)
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please provide only and at least a path")
    directory = Path(sys.argv[1])

    process(directory)
