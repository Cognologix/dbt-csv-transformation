-- Apply lookup and other business transformations at this stage
WITH s_b as (

    SELECT
    	distinct schoolid,
    	        studentuniqueid,
    	        operation,
		        entrydate,
		        employedwhileenrolled,
                entrygradeleveldescriptor,
                entrygradelevelreasondescriptor,
                entrytypedescriptor,
                exitwithdrawdate,
                exitwithdrawtypedescriptor,
                fulltimeequivalency,
                primaryschool,
                repeatgradeindicator,
                residencystatusdescriptor,
                schoolchoicetransfer,
                termcompletionindicator,
                tx_adaeligibilitydescriptor,
                tx_studentattributiondescriptor
    from
        {{ref('raw_ssa_base')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		s_b.schoolid,
		s_b.studentuniqueid,
		s_b.operation,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'schoolid', s_b.schoolid
			'studentuniqueid', s_b.studentuniqueid
		) AS externalid,
		'STUDENTSCHOOLASSOCIATION' AS resourcetype,
		0 AS status,
		s_b.entryDate,
		jsonb_agg(json_build_object(
			'calendarCode', s_b.calendarcode,
			'schoolId', s_b.calendarreference_schoolid,
			'schoolYear', s_b.calendarreference_schoolyear
		    ) AS calendarReference,
            json_build_object(
                'schoolYear', s_b.classofschoolyeartypereference_schoolyear
            )AS classOfSchoolYearTypeReference,
            json_build_object(
                'educationOrganizationId', s_b.graduationplanrefere__ucationorganizationid,
                'graduationPlanTypeDescriptor', s_b.graduationplanrefere__ionplantypedescriptor,
                'graduationSchoolYear', s_b.graduationplanrefere___graduationschoolyear
            ) AS graduationPlanReference,
            json_build_object(
                'schoolId', s_b.schoolid
            ) AS schoolReference,
            json_build_object(
                'schoolYear', s_b.schoolyeartypereference_schoolyear
            ) AS schoolYearTypeReference,
            json_build_object(
                'studentUniqueId', s_b.studentuniqueid
            ) AS studentReference
		)
        s_b.employedwhileenrolled,
        s_b.entrygradeleveldescriptor,
        s_b.entrygradelevelreasondescriptor,
        s_b.entrytypedescriptor,
        s_b.exitwithdrawdate,
        s_b.exitwithdrawtypedescriptor,
        s_b.fulltimeequivalency,
        s_b.primaryschool,
        s_b.repeatgradeindicator,
        s_b.residencystatusdescriptor,
        s_b.schoolchoicetransfer,
        s_b.termcompletionindicator,
        s_b.tx_adaeligibilitydescriptor,
        s_b.tx_studentattributiondescriptor

   FROM
	    s_b
   GROUP BY
		s_b.schoolid,
		s_b.studentuniqueid


)

select * from final


