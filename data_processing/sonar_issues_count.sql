SELECT 

coalesce(t1.project, t2.project) AS project,
coalesce(creation_analysis_key, current_analysis_key) AS analysis_key,
coalesce(creation_date, update_date) AS date,

coalesce(introduced_blocker_code_smell, 0) AS introduced_blocker_code_smell,
coalesce(introduced_critical_code_smell, 0) AS introduced_critical_code_smell,
coalesce(introduced_major_code_smell, 0) AS introduced_major_code_smell,
coalesce(introduced_minor_code_smell, 0) AS introduced_minor_code_smell,
coalesce(introduced_info_code_smell, 0) AS introduced_info_code_smell,
coalesce(introduced_null_severity_code_smell, 0) AS introduced_null_severity_code_smell,

coalesce(introduced_blocker_bug, 0) AS introduced_blocker_bug,
coalesce(introduced_critical_bug, 0) AS introduced_critical_bug,
coalesce(introduced_major_bug, 0) AS introduced_major_bug,
coalesce(introduced_minor_bug, 0) AS introduced_minor_bug,
coalesce(introduced_info_bug, 0) AS introduced_info_bug,
coalesce(introduced_null_severity_bug, 0) AS introduced_null_severity_bug,

coalesce(introduced_blocker_vulnerability, 0) AS introduced_blocker_vulnerability,
coalesce(introduced_critical_vulnerability, 0) AS introduced_critical_vulnerability,
coalesce(introduced_major_vulnerability, 0) AS introduced_major_vulnerability,
coalesce(introduced_minor_vulnerability, 0) AS introduced_minor_vulnerability,
coalesce(introduced_info_vulnerability, 0) AS introduced_info_vulnerability,
coalesce(introduced_null_severity_vulnerability, 0) AS introduced_null_severity_vulnerability,

coalesce(introduced_blocker_security_hotspot, 0) AS introduced_blocker_security_hotspot,
coalesce(introduced_critical_security_hotspot, 0) AS introduced_critical_security_hotspot,
coalesce(introduced_major_security_hotspot, 0) AS introduced_major_security_hotspot,
coalesce(introduced_minor_security_hotspot, 0) AS introduced_minor_security_hotspot,
coalesce(introduced_info_security_hotspot, 0) AS introduced_info_security_hotspot,
coalesce(introduced_null_severity_security_hotspot, 0) AS introduced_null_severity_security_hotspot,

coalesce(removed_blocker_code_smell, 0) AS removed_blocker_code_smell,
coalesce(removed_critical_code_smell, 0) AS removed_critical_code_smell,
coalesce(removed_major_code_smell, 0) AS removed_major_code_smell,
coalesce(removed_minor_code_smell, 0) AS removed_minor_code_smell,
coalesce(removed_info_code_smell, 0) AS removed_info_code_smell,
coalesce(removed_null_severity_code_smell, 0) AS removed_null_severity_code_smell,

coalesce(removed_blocker_bug, 0) AS removed_blocker_bug,
coalesce(removed_critical_bug, 0) AS removed_critical_bug,
coalesce(removed_major_bug, 0) AS removed_major_bug,
coalesce(removed_minor_bug, 0) AS removed_minor_bug,
coalesce(removed_info_bug, 0) AS removed_info_bug,
coalesce(removed_null_severity_bug, 0) AS removed_null_severity_bug,

coalesce(removed_blocker_vulnerability, 0) AS removed_blocker_vulnerability,
coalesce(removed_critical_vulnerability, 0) AS removed_critical_vulnerability,
coalesce(removed_major_vulnerability, 0) AS removed_major_vulnerability,
coalesce(removed_minor_vulnerability, 0) AS removed_minor_vulnerability,
coalesce(removed_info_vulnerability, 0) AS removed_info_vulnerability,
coalesce(removed_null_severity_vulnerability, 0) AS removed_null_severity_vulnerability,

coalesce(removed_blocker_security_hotspot, 0) AS removed_blocker_security_hotspot,
coalesce(removed_critical_security_hotspot, 0) AS removed_critical_security_hotspot,
coalesce(removed_major_security_hotspot, 0) AS removed_major_security_hotspot,
coalesce(removed_minor_security_hotspot, 0) AS removed_minor_security_hotspot,
coalesce(removed_info_security_hotspot, 0) AS removed_info_security_hotspot,
coalesce(removed_null_severity_security_hotspot, 0) AS removed_null_severity_security_hotspot

FROM
(
SELECT 
project,
creation_analysis_key,
MAX(creation_date) as creation_date,

SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS introduced_blocker_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS introduced_critical_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS introduced_major_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS introduced_minor_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'INFO' THEN 1 ELSE 0 END) AS introduced_info_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity IS NULL THEN 1 ELSE 0 END) AS introduced_null_severity_code_smell,

SUM(CASE WHEN type = 'BUG' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS introduced_blocker_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS introduced_critical_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS introduced_major_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS introduced_minor_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'INFO' THEN 1 ELSE 0 END) AS introduced_info_bug,
SUM(CASE WHEN type = 'BUG' AND severity IS NULL THEN 1 ELSE 0 END) AS introduced_null_severity_bug,

SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS introduced_blocker_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS introduced_critical_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS introduced_major_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS introduced_minor_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'INFO' THEN 1 ELSE 0 END) AS introduced_info_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity IS NULL THEN 1 ELSE 0 END) AS introduced_null_severity_vulnerability,

SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS introduced_blocker_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS introduced_critical_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS introduced_major_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS introduced_minor_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'INFO' THEN 1 ELSE 0 END) AS introduced_info_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity IS NULL THEN 1 ELSE 0 END) AS introduced_null_severity_security_hotspot

from sonar_issues 
WHERE status IN ('OPEN', 'REOPENED', 'CONFIRMED', 'TO_REVIEW')
GROUP BY project, creation_analysis_key
) t1

FULL OUTER JOIN

(
SELECT 
project,
current_analysis_key,
MAX(update_date) as update_date,

SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS removed_blocker_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'CRITICAL'  THEN 1 ELSE 0 END) AS removed_critical_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS removed_major_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS removed_minor_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity = 'INFO' THEN 1 ELSE 0 END) AS removed_info_code_smell,
SUM(CASE WHEN type = 'CODE_SMELL' AND severity IS NULL THEN 1 ELSE 0 END) AS removed_null_severity_code_smell,

SUM(CASE WHEN type = 'BUG' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS removed_blocker_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS removed_critical_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS removed_major_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS removed_minor_bug,
SUM(CASE WHEN type = 'BUG' AND severity = 'INFO' THEN 1 ELSE 0 END) AS removed_info_bug,
SUM(CASE WHEN type = 'BUG' AND severity IS NULL THEN 1 ELSE 0 END) AS removed_null_severity_bug,

SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS removed_blocker_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS removed_critical_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS removed_major_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS removed_minor_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity = 'INFO' THEN 1 ELSE 0 END) AS removed_info_vulnerability,
SUM(CASE WHEN type = 'VULNERABILITY' AND severity IS NULL THEN 1 ELSE 0 END) AS removed_null_severity_vulnerability,

SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'BLOCKER' THEN 1 ELSE 0 END) AS removed_blocker_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'CRITICAL' THEN 1 ELSE 0 END) AS removed_critical_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'MAJOR' THEN 1 ELSE 0 END) AS removed_major_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'MINOR' THEN 1 ELSE 0 END) AS removed_minor_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity = 'INFO' THEN 1 ELSE 0 END) AS removed_info_security_hotspot,
SUM(CASE WHEN type = 'SECURITY_HOTSPOT' AND severity IS NULL THEN 1 ELSE 0 END) AS removed_null_severity_security_hotspot

from sonar_issues 
WHERE status IN ('RESOLVED', 'CLOSED', 'REVIEWED')
GROUP BY project, current_analysis_key
) t2

ON t1.creation_analysis_key = t2.current_analysis_key


