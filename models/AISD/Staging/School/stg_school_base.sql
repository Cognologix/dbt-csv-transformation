------------------------------------------------------------------------------
-- Fetch Administrative Funding Control Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
WITH sc_afcd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_administrativefundingcontroldescriptor')}} as src_afcd
   on d.descriptorid = src_afcd.administrativefundingcontroldescriptorid
),

------------------------------------------------------------------------------
-- Fetch Charter Status Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_csd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_charterstatusdescriptor')}} as src_csd
   on d.descriptorid = src_csd.charterstatusdescriptorid
),

------------------------------------------------------------------------------
-- Fetch Charter Approval Agency Type Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_caatd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_charterapprovalagencytypedescriptor')}} as src_caatd
   on d.descriptorid = src_caatd.charterapprovalagencytypedescriptorid
),

------------------------------------------------------------------------------
-- Fetch Internet Access Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_iad as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_internetaccessdescriptor')}} as src_iad
   on d.descriptorid = src_iad.internetaccessdescriptorid
),

------------------------------------------------------------------------------
-- Fetch Magnet Special Program Emphasis School Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_mspesd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_magnetspecialprogram__hasisschooldescriptor')}} as src_mspesd
   on d.descriptorid = src_mspesd.magnetspecialprogram__hasisschooldescriptorid
),

------------------------------------------------------------------------------
-- Fetch Operational Status Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_osd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_operationalstatusdescriptor')}} as src_osd
   on d.descriptorid = src_osd.operationalstatusdescriptorid
),

------------------------------------------------------------------------------
-- Fetch School Type Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_std as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_schooltypedescriptor')}} as src_std
   on d.descriptorid = src_std.schooltypedescriptorid
),
------------------------------------------------------------------------------
-- Fetch Title I Part  A School Designation Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
sc_tipsdd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_titleipartaschooldesignationdescriptor')}} as src_tipsdd
   on d.descriptorid = src_tipsdd.titleipartaschooldesignationdescriptorid
),

------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
s_b as (
    select
        loadid,
        schoolid,
        operation,
        charterapprovalschoolyeartypereference_schoolyear,
        localeducationagencyid,
        CASE
        -- When administrativefundingcontroldescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(administrativefundingcontroldescriptor),'') is null)
        THEN NULL

        -- When administrativefundingcontroldescriptor is not null but does not have matching record in descriptor, set as Not Applicable category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(administrativefundingcontroldescriptor),'') is not null and NULLIF(TRIM(sc_afcd.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new administrativefundingcontroldescriptor
        else concat(sc_afcd.namespace, '#', sc_afcd.codevalue)

        END as administrativefundingcontroldescriptor,
        CASE
        -- When charterstatusdescriptor Values in temp table for look-up validations contain blanks or null, process as is or set null
        when (NULLIF(TRIM(charterstatusdescriptor),'') is null)
        THEN NULL

        -- When -- Fetch Entry Grade Level Reason Descriptor Values in temp table for look-up validations is not null but does not have matching record in descriptor, set as Not Applicable category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(charterstatusdescriptor),'') is not null and NULLIF(TRIM(sc_csd.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new -- Fetch Entry Grade Level Reason Descriptor Values in temp table for look-up validations

        else concat(sc_csd.namespace, '#',sc_csd.codevalue)

        END as charterstatusdescriptor,
        CASE
        -- When charterapprovalagencytypedescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(charterapprovalagencytypedescriptor),'') is null)
        THEN NULL

        -- When Entry Type Descriptor is not null but does not have matching record in descriptor, set as Not Applicable category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(charterapprovalagencytypedescriptor),'') is not null and NULLIF(TRIM(sc_caatd.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new charterapprovalagencytypedescriptor
        else concat(sc_caatd.namespace, '#', sc_caatd.codevalue)

        END as charterapprovalagencytypedescriptor,
        CASE
        -- When internetaccessdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(internetaccessdescriptor),'') is null)
        THEN NULL

        -- When internetaccessdescriptor is not null but does not have matching record in descriptor, set as Not Applicable
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(internetaccessdescriptor),'') is not null and NULLIF(TRIM(sc_iad.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new internetaccessdescriptor
        else concat(sc_iad.namespace, '#', sc_iad.codevalue)

        END as internetaccessdescriptor,
        CASE
        -- When magnetspecialprogramemphasisschooldescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(magnetspecialprogram__hasisschooldescriptor),'') is null)
        THEN NULL

        -- When magnetspecialprogramemphasisschooldescriptor is not null but does not have matching record in descriptor, set as Not Applicable category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(magnetspecialprogram__hasisschooldescriptor),'') is not null and NULLIF(TRIM(sc_mspesd.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new magnetspecialprogramemphasisschooldescriptor
        else concat(sc_mspesd.namespace, '#', sc_mspesd.codevalue)

        END as magnetspecialprogram__hasisschooldescriptor,
        ----------------------
        nameofinstitution,
        ----------------------+
        CASE
        -- When operationalstatusdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(operationalstatusdescriptor),'') is null)
        THEN NULL

        -- When operationalstatusdescriptor is not null but does not have matching record in descriptor, set as Not Applicable category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(operationalstatusdescriptor),'') is not null and NULLIF(TRIM(sc_osd.codevalue),'') is NULL)
        THEN 'Not Applicable'

        -- Else matching record is found, so concatenate namespace and codevalue to create new operationalstatusdescriptor
        else concat(sc_osd.namespace, '#', sc_osd.codevalue)

        END as operationalstatusdescriptor,
        CASE
        -- When schooltypedescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(schooltypedescriptor),'') is null)
        THEN NULL

        -- When schooltypedescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(schooltypedescriptor),'') is not null and NULLIF(TRIM(sc_std.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new schooltypedescriptor
        else concat(sc_std.namespace, '#', sc_std.codevalue)

        END as schooltypedescriptor,
        ----------------------
        shortnameofinstitution,
        ----------------------
        CASE
        -- When titleipartaschooldesignationdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(titleipartaschooldesignationdescriptor),'') is null)
        THEN NULL

        -- When titleipartaschooldesignationdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(titleipartaschooldesignationdescriptor),'') is not null and NULLIF(TRIM(sc_tipsdd.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new titleipartaschooldesignationdescriptor
        else concat(sc_tipsdd.namespace, '#', sc_tipsdd.codevalue)

        END as titleipartaschooldesignationdescriptor,
        website,
        tx_pkfulldaywaiver,
        tx_additionaldaysprogram,
        tx_numberofbullyingincidents,
        tx_numberofcyberbullyingincidents


    FROM
        {{ref('cl_school_base')}} sb
    left outer join sc_afcd
        on administrativefundingcontroldescriptor = sc_afcd.codevalue
    left outer join sc_csd
        on charterstatusdescriptor = sc_csd.codevalue
    left outer join sc_caatd
        on charterapprovalagencytypedescriptor = sc_caatd.codevalue
    left outer join sc_iad
        on internetaccessdescriptor = sc_iad.codevalue
    left outer join sc_mspesd
        on magnetspecialprogram__hasisschooldescriptor = sc_mspesd.codevalue
    left outer join sc_osd
        on operationalstatusdescriptor = sc_osd.codevalue
    left outer join sc_std
        on schooltypedescriptor = sc_std.codevalue
    left outer join sc_tipsdd
        on titleipartaschooldesignationdescriptor = sc_tipsdd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        s_b.loadid as LOADID,
		s_b.schoolid,
        s_b.operation AS operation,
        uuid_generate_v4() AS resourceid,
		json_build_object(
			'schoolid', s_b.schoolid
		) AS externalid,
		'SCHOOL' AS resourcetype,
		0 AS status,
		json_build_object(
				'schoolYear', s_b.charterapprovalschoolyeartypereference_schoolyear

               )AS charterApprovalSchoolYearTypeReference,
	    json_build_object(
				'localEducationAgencyId', s_b.localeducationagencyid

			   )AS localEducationAgencyReference,
        s_b.administrativefundingcontroldescriptor,
        s_b.charterstatusdescriptor,
        s_b.charterapprovalagencytypedescriptor,
        s_b.internetaccessdescriptor,
        s_b.magnetspecialprogram__hasisschooldescriptor,
        s_b.nameofinstitution,
        s_b.operationalstatusdescriptor,
        s_b.schooltypedescriptor,
        s_b.shortnameofinstitution,
        s_b.titleipartaschooldesignationdescriptor,
        s_b.website,
        s_b.tx_pkfulldaywaiver,
        s_b.tx_additionaldaysprogram,
        s_b.tx_numberofbullyingincidents,
        s_b.tx_numberofcyberbullyingincidents

   FROM
	    s_b

)

select * from final