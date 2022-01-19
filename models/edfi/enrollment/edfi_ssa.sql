WITH base AS (
	SELECT 
		uuid_generate_v4() AS resourceid, 
		json_build_object(
			'schoolid', ssb.schoolid,
			'studentuniqueid', ssb.studentuniqueid
		)AS externalid,
		'SSA' AS resourcetype,
		ssb.operation,
		0 AS status,
		ssb.schoolid,
		ssb.studentuniqueid,
		json_build_object(
			'calendarCode', ssb.calendarcode,
			'schoolId', calendarreference_schoolid,
			'schoolYear', ssb.calendarreference_schoolyear
		) AS calendarReference,
		ssb.entrydate,	
		json_build_object(
			'schoolYear', ssb.classofschoolyeartypereference_schoolyear
		)AS classOfSchoolYearTypeReference,
		json_build_object(
			'educationOrganizationId', ssb.graduationplanrefere__ucationorganizationid,
			'graduationPlanTypeDescriptor', ssb.graduationplanrefere__ionplantypedescriptor,
			'graduationSchoolYear', ssb.graduationplanrefere___graduationschoolyear
		) AS graduationPlanReference,
		json_build_object(
			'schoolId', ssb.schoolid
		) AS schoolReference,
		json_build_object(
			'schoolYear', ssb.schoolyeartypereference_schoolyear
		) AS schoolYearTypeReference,
		json_build_object(
			'studentUniqueId', ssb.studentuniqueid
		) AS studentReference,
		ssb.employedwhileenrolled,
		ssb.entrygradeleveldescriptor,
		ssb.entrygradelevelreasondescriptor,
		ssb.entrytypedescriptor,
		ssb.exitwithdrawdate,
		ssb.exitwithdrawtypedescriptor,
		ssb.fulltimeequivalency,
		ssb.primaryschool,
		ssb.repeatgradeindicator,
		ssb.residencystatusdescriptor,
		ssb.schoolchoicetransfer,
		ssb.termcompletionindicator
	FROM 
		public.ssa_base AS ssb
),
alternate_graduation_plan_reference AS (
	SELECT
		agpr.schoolid,
		agpr.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationOrganizationId', agpr.alternativegraduatio__ucationorganizationid,
				'graduationPlanTypeDescriptor', agpr."alternativeGraduatio__nPlanTypeDescriptor.1",
				'graduationSchoolYear', agpr.alternativegraduatio___graduationschoolyear
			)
		) AS alternativeGraduationPlans
	FROM
		public.ssa_alternate_graduation_plan_reference AS agpr
	GROUP BY
		agpr.schoolid, 
		agpr.studentuniqueid
),
education_plans AS (
	SELECT
		ep.schoolid,
		ep.studentuniqueid,
		jsonb_agg(json_build_object(
				'educationPlanDescriptor', ep.educationplans_educationplandescriptor
			)
		) AS educationPlans
	FROM 
		public.ssa_education_plans AS ep
	GROUP BY
		ep.schoolid , 
		ep.studentuniqueid
)

SELECT 
	resourceid,
	externalid,
	resourcetype,
	operation,
	json_build_object(
		'entryDate', ssb.entrydate,
		'calendarReference', ssb.calendarReference,
		'classOfSchoolYearTypeReference', ssb.classOfSchoolYearTypeReference,
		'graduationPlanReference', ssb.graduationPlanReference,
		'schoolReference', ssb.schoolReference,
		'schoolYearTypeReference', ssb.schoolYearTypeReference,
		'studentReference', ssb.studentReference,
		'alternativeGraduationPlans', agpr.alternativeGraduationPlans,
		'educationPlans', ep.educationPlans,
		'employedWhileEnrolled', ssb.employedwhileenrolled,
		'entryGradeLevelDescriptor', ssb.entrygradeleveldescriptor,
		'entryGradeLevelReasonDescriptor', ssb.entrygradelevelreasondescriptor,
		'entryTypeDescriptor', ssb.entrytypedescriptor,
		'exitWithdrawDate', ssb.exitwithdrawdate,
		'exitWithdrawTypeDescriptor', ssb.exitwithdrawtypedescriptor,
		'fullTimeEquivalency', ssb.fulltimeequivalency,
		'primarySchool', ssb.primaryschool,
		'repeatGradeIndicator', ssb.repeatgradeindicator,
		'residencyStatusDescriptor', ssb.residencystatusdescriptor,
		'schoolChoiceTransfer', ssb.schoolchoicetransfer,
		'termCompletionIndicator', ssb.termcompletionindicator
	) AS payload,
	status
FROM 
	base AS ssb
LEFT OUTER JOIN
	alternate_graduation_plan_reference AS agpr
ON
	agpr.schoolid = ssb.schoolid AND
	agpr.studentuniqueid = ssb.studentuniqueid
LEFT OUTER JOIN
	education_plans AS ep
ON
	ep.schoolid = ssb.schoolid AND
	ep.studentuniqueid = ssb.studentuniqueid
