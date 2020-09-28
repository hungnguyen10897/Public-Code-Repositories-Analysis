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
                "project,analysis_key,date,project_version,revision\n",
                "project,analysis_key,complexity,class_complexity,function_complexity,file_complexity,function_complexity_distribution,file_complexity_distribution,complexity_in_classes,complexity_in_functions,cognitive_complexity,test_errors,skipped_tests,test_failures,tests,test_execution_time,test_success_density,coverage,lines_to_cover,uncovered_lines,line_coverage,conditions_to_cover,uncovered_conditions,branch_coverage,new_coverage,new_lines_to_cover,new_uncovered_lines,new_line_coverage,new_conditions_to_cover,new_uncovered_conditions,new_branch_coverage,executable_lines_data,public_api,public_documented_api_density,public_undocumented_api,duplicated_lines,duplicated_lines_density,duplicated_blocks,duplicated_files,duplications_data,new_duplicated_lines,new_duplicated_blocks,new_duplicated_lines_density,quality_profiles,quality_gate_details,violations,blocker_violations,critical_violations,major_violations,minor_violations,info_violations,new_violations,new_blocker_violations,new_critical_violations,new_major_violations,new_minor_violations,new_info_violations,false_positive_issues,open_issues,reopened_issues,confirmed_issues,wont_fix_issues,sqale_index,sqale_rating,development_cost,new_technical_debt,sqale_debt_ratio,new_sqale_debt_ratio,code_smells,new_code_smells,effort_to_reach_maintainability_rating_a,new_maintainability_rating,new_development_cost,alert_status,bugs,new_bugs,reliability_remediation_effort,new_reliability_remediation_effort,reliability_rating,new_reliability_rating,last_commit_date,vulnerabilities,new_vulnerabilities,security_remediation_effort,new_security_remediation_effort,security_rating,new_security_rating,security_hotspots,new_security_hotspots,security_review_rating,classes,ncloc,functions,comment_lines,comment_lines_density,files,directories,lines,statements,generated_lines,generated_ncloc,ncloc_data,comment_lines_data,projects,ncloc_language_distribution,new_lines\n",
                "project,current_analysis_key,creation_analysis_key,issue_key,type,rule,severity,status,resolution,effort,debt,tags,creation_date,update_date,close_date\n"]:
                    taken_lines.append(line)
        
        with open(file, 'w') as f:
            f.writelines(taken_lines)
        

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Please provide only and at least a path")
    directory = Path(sys.argv[1])

    process(directory)
