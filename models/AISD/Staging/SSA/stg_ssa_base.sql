------------------------------------------------------------------------------
-- Fetch Entry Grade Level Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
WITH ssa_egld as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_entrygradeleveldescriptor')}} as src_egld
   on d.descriptorid = src_egld.gradeleveldescriptorid
),

------------------------------------------------------------------------------
-- Fetch Entry Grade Level Reason Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_eglrd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_entrygradelevelreasondescriptor')}} as src_eglrd
   on d.descriptorid = src_eglrd.entrygradelevelreasondescriptorid
),

------------------------------------------------------------------------------
-- Fetch Entry Type Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_etd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_entrytypedescriptor')}} as src_etd
   on d.descriptorid = src_etd.entrytypedescriptorid
),

------------------------------------------------------------------------------
-- Fetch Exit Withdraw Type Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_ewtd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_exitwithdrawtypedescriptor')}} as src_ewtd
   on d.descriptorid = src_ewtd.exitwithdrawtypedescriptorid
),

------------------------------------------------------------------------------
-- Fetch Residency Status Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_rsd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_residencystatusdescriptor')}} as src_rsd
   on d.descriptorid = src_rsd.residencystatusdescriptorid
),

------------------------------------------------------------------------------
-- Fetch TX_ada Eligibility Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_txaed as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_adaeligibilitydescriptor')}} as src_txaed
   on d.descriptorid = src_txaed.tx_adaeligibilitydescriptorid
),

------------------------------------------------------------------------------
-- Fetch TX_student Attribution Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
ssa_txsad as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_studentattributiondescriptor')}} as src_txsad
   on d.descriptorid = src_txsad.tx_studentattributiondescriptorid
),

------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
s_b as (
    select
        loadid,
        schoolid,
		studentuniqueid,
        operation,
        calendarcode,
        calendarreference_schoolid,
        calendarreference_schoolyear,
        classofschoolyeartypereference_schoolyear,
        graduationplanrefere__ucationorganizationid,
        graduationplanrefere__ionplantypedescriptor,
        graduationplanrefere___graduationschoolyear,
        schoolyeartypereference_schoolyear,
        entrydate,
        employedwhileenrolled,

        CASE
        -- When Entry Grade Level Descriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(entrygradeleveldescriptor),'') is null)
        THEN NULL

        -- When Entry Grade Level Descriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(entrygradeleveldescriptor),'') is not null and NULLIF(TRIM(ssa_egld.codevalue),'') is NULL)
        THEN ''

        -- Else matching record is found, so concatenate namespace and codevalue to create new Entry Grade Level Descriptor
        else concat(ssa_egld.namespace, '#', ssa_egld.codevalue)

        END as entrygradeleveldescriptor,
        CASE
        -- When Entry Grade Level Reason Descriptor Values in temp table for look-up validations contain blanks or null, process as is or set null
        when (NULLIF(TRIM(entrygradelevelreasondescriptor),'') is null)
        THEN NULL

        -- When -- Fetch Entry Grade Level Reason Descriptor Values in temp table for look-up validations is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(entrygradelevelreasondescriptor),'') is not null and NULLIF(TRIM(ssa_eglrd.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new -- Fetch Entry Grade Level Reason Descriptor Values in temp table for look-up validations

        else concat(ssa_eglrd.namespace, '#',ssa_eglrd.codevalue)

        END as entrygradelevelreasondescriptor,
        CASE
        -- When Entry Type Descriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(entrytypedescriptor),'') is null)
        THEN NULL

        -- When Entry Type Descriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(entrytypedescriptor),'') is not null and NULLIF(TRIM(ssa_etd.codevalue),'') is NULL)
        THEN 'Not Selected'

        -- Else matching record is found, so concatenate namespace and codevalue to create new Entry Type Descriptor
        else concat(ssa_etd.namespace, '#', ssa_etd.codevalue)

        END as entrytypedescriptor,
        ----------------------
        exitwithdrawdate,
        ----------------------
        CASE
        -- When Exit Withdraw Type Descriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(exitwithdrawtypedescriptor),'') is null)
        THEN NULL

        -- When Exit Withdraw Type Descriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(exitwithdrawtypedescriptor),'') is not null and NULLIF(TRIM(ssa_ewtd.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new Exit Withdraw Type Descriptor
        else concat(ssa_ewtd.namespace, '#', ssa_ewtd.codevalue)

        END as exitwithdrawtypedescriptor,
        ----------------------
        fulltimeequivalency,
        primaryschool,
        repeatgradeindicator,
        ----------------------
        CASE
        -- When residencystatusdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(residencystatusdescriptor),'') is null)
        THEN NULL

        -- When residencystatusdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(residencystatusdescriptor),'') is not null and NULLIF(TRIM(ssa_rsd.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new residencystatusdescriptor
        else concat(ssa_rsd.namespace, '#', ssa_rsd.codevalue)

        END as residencystatusdescriptor,
        ----------------------
        schoolchoicetransfer,
        termcompletionindicator,
        ----------------------
        CASE
        -- When tx_adaeligibilitydescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(tx_adaeligibilitydescriptor),'') is null)
        THEN NULL

        -- When tx_adaeligibilitydescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(tx_adaeligibilitydescriptor),'') is not null and NULLIF(TRIM(ssa_txaed.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new tx_adaeligibilitydescriptor
        else concat(ssa_txaed.namespace, '#', ssa_txaed.codevalue)

        END as tx_adaeligibilitydescriptor,
        CASE
        -- When tx_studentattributiondescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(tx_studentattributiondescriptor),'') is null)
        THEN NULL

        -- When tx_studentattributiondescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(tx_studentattributiondescriptor),'') is not null and NULLIF(TRIM(ssa_txsad.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new tx_studentattributiondescriptor
        else concat(ssa_txsad.namespace, '#', ssa_txsad.codevalue)

        END as tx_studentattributiondescriptor


    FROM
        {{ref('cl_ssa_base')}} sb
    left outer join ssa_egld
        on entrygradeleveldescriptor = ssa_egld.codevalue
    left outer join ssa_eglrd
        on entrygradelevelreasondescriptor = ssa_eglrd.codevalue
    left outer join ssa_etd
        on entrytypedescriptor = ssa_etd.codevalue
    left outer join ssa_ewtd
        on exitwithdrawtypedescriptor = ssa_ewtd.codevalue
    left outer join ssa_rsd
        on residencystatusdescriptor = ssa_rsd.codevalue
    left outer join ssa_txaed
        on tx_adaeligibilitydescriptor = ssa_txaed.codevalue
    left outer join ssa_txsad
        on tx_studentattributiondescriptor = ssa_txsad.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        s_b.loadid as LOADID,
		s_b.schoolid,
		s_b.studentuniqueid,
        s_b.operation AS operation,
        uuid_generate_v4() AS resourceid,
		json_build_object(
			'schoolid', s_b.schoolid,
			'studentuniqueid', s_b.studentuniqueid
		) AS externalid,
		'STUDENTSCHOOLASSOCIATION' AS resourcetype,
		0 AS status,

		json_build_object(
			'calendarCode', s_b.calendarcode,
			'schoolId', s_b.calendarreference_schoolid,
			'schoolYear', s_b.calendarreference_schoolyear
            )AS calendarReference,
        json_build_object(
                'schoolYear', s_b.classofschoolyeartypereference_schoolyear
            )AS classOfSchoolYearTypeReference,
        json_build_object(
            'educationOrganizationId', s_b.graduationplanrefere__ucationorganizationid,
            'graduationPlanTypeDescriptor', s_b.graduationplanrefere__ionplantypedescriptor,
            'graduationSchoolYear', s_b.graduationplanrefere___graduationschoolyear) AS graduationPlanReference,
        json_build_object(
            'schoolId', s_b.schoolid
        ) AS schoolReference,
        json_build_object(
            'schoolYear', s_b.schoolyeartypereference_schoolyear
        ) AS schoolYearTypeReference,
        json_build_object(
            'studentUniqueId', s_b.studentuniqueid
        ) AS studentReference,
        s_b.entrydate,
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

)

select * from final


