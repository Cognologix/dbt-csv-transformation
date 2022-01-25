WITH  final as (

SELECT
	ssa_b.resourceid,
	ssa_b.externalid,
	ssa_b.resourcetype,
	ssa_b.operation,
   	json_build_object(
		'entryDate', ssa_b.entrydate,
		'calendarReference', ssa_b.calendarReference,
		'classOfSchoolYearTypeReference', ssa_b.classOfSchoolYearTypeReference,
		'graduationPlanReference', ssa_b.graduationPlanReference,
		'schoolReference', ssa_b.schoolReference,
		'schoolYearTypeReference', ssa_b.schoolYearTypeReference,
		'studentReference', ssa_b.studentReference,
		'alternativeGraduationPlans', ssa_agpr.alternativeGraduationPlans,
		'educationPlans', ssa_ep.educationPlans,
		'employedWhileEnrolled', ssa_b.employedwhileenrolled,
		'entryGradeLevelDescriptor	', ssa_b.entrygradeleveldescriptor,
		'entryGradeLevelReasonDescriptor', ssa_b.entrygradelevelreasondescriptor,
		'entryTypeDescriptor', ssa_b.entrytypedescriptor,
		'exitWithdrawDate', ssa_b.exitwithdrawdate,
		'exitWithdrawTypeDescriptor', ssa_b.exitwithdrawtypedescriptor,
		'fullTimeEquivalency',ssa_b.fulltimeequivalency,
		'primarySchool', ssa_b.primaryschool,
		'repeatGradeIndicator', ssa_b.repeatgradeindicator,
		'residencyStatusDescriptor', ssa_b.residencystatusdescriptor,
		'schoolChoiceTransfer', ssa_b.schoolchoicetransfer,
		'termCompletionIndicator', ssa_b.termcompletionindicator

	) AS payload,
	status
FROM
	{{ ref('stg_ssa_base') }} AS ssa_b
LEFT OUTER JOIN
	{{ ref('stg_ssa_alternative_graduation_plan_reference') }} AS ssa_agpr
ON
	ssa_agpr.schoolid = ssa_b.schoolid
	ssa_agpr.studentuniqueid = ssa_b.studentuniqueid
LEFT OUTER JOIN
	{{ ref('stg_ssa_education_plans') }}  AS ssa_ep
ON
	ssa_ep.schoolid = ssa_b.schoolid
	ssa_ep.studentuniqueid = ssa_b.studentuniqueid

)

select * from final