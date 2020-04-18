SELECT *,

SUM((introduced_blocker_code_smell - removed_blocker_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_blocker_code_smell,
SUM((introduced_critical_code_smell - removed_critical_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_critical_code_smell,
SUM((introduced_major_code_smell - removed_major_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_major_code_smell,
SUM((introduced_minor_code_smell - removed_minor_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_minor_code_smell,
SUM((introduced_info_code_smell - removed_info_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_info_code_smell,
SUM((introduced_null_severity_code_smell - removed_null_severity_code_smell)) OVER (PARTITION BY project ORDER BY date) AS current_null_severity_code_smell,

SUM((introduced_blocker_bug - removed_blocker_bug)) OVER (PARTITION BY project ORDER BY date) AS current_blocker_bug,
SUM((introduced_critical_bug - removed_critical_bug)) OVER (PARTITION BY project ORDER BY date) AS current_critical_bug,
SUM((introduced_major_bug - removed_major_bug)) OVER (PARTITION BY project ORDER BY date) AS current_major_bug,
SUM((introduced_minor_bug - removed_minor_bug)) OVER (PARTITION BY project ORDER BY date) AS current_minor_bug,
SUM((introduced_info_bug - removed_info_bug)) OVER (PARTITION BY project ORDER BY date) AS current_info_bug,
SUM((introduced_null_severity_bug - removed_null_severity_bug)) OVER (PARTITION BY project ORDER BY date) AS current_null_severity_bug,

SUM((introduced_blocker_vulnerability - removed_blocker_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_blocker_vulnerability,
SUM((introduced_critical_vulnerability - removed_critical_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_critical_vulnerability,
SUM((introduced_major_vulnerability - removed_major_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_major_vulnerability,
SUM((introduced_minor_vulnerability - removed_minor_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_minor_vulnerability,
SUM((introduced_info_vulnerability - removed_info_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_info_vulnerability,
SUM((introduced_null_severity_vulnerability - removed_null_severity_vulnerability)) OVER (PARTITION BY project ORDER BY date) AS current_null_severity_vulnerability,

SUM((introduced_blocker_security_hotspot - removed_blocker_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_blocker_security_hotspot,
SUM((introduced_critical_security_hotspot - removed_critical_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_critical_security_hotspot,
SUM((introduced_major_security_hotspot - removed_major_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_major_security_hotspot,
SUM((introduced_minor_security_hotspot - removed_minor_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_minor_security_hotspot,
SUM((introduced_info_security_hotspot - removed_info_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_info_security_hotspot,
SUM((introduced_null_severity_security_hotspot - removed_null_severity_security_hotspot)) OVER (PARTITION BY project ORDER BY date) AS current_null_severity_security_hotspot

FROM sonar_issues_count