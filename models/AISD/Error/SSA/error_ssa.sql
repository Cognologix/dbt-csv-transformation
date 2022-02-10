{{ config(
    materialized='incremental'
    )
}}

----------------------------------------------------------------
-- Logging invalid records from Student School Association Base entity
----------------------------------------------------------------
with err_ssa_b as (
    SELECT studentuniqueid,
           schoolid,

        'Student School Association Base' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'entrydate', entrydate,
                    'entrygradeleveldescriptor',entrygradeleveldescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'ssa_base')}}
    WHERE
	    studentuniqueid IS NULL
	    OR schoolid IS NULL
	    OR entrydate IS NULL
	    OR NULLIF(TRIM(entrygradeleveldescriptor),'') IS NULL

    GROUP BY studentuniqueid,
             schoolid
),

------------------------------------------------------------------------------------------------
-- Logging invalid records from Student School Association Alternate Graduation Plan Reference
-------------------------------------------------------------------------------------------------
err_ssa_agpr as (
    SELECT studentuniqueid,
           schoolid,
        'Student School Association Alternate Graduation Plan Reference' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'alternativegraduatio__ucationorganizationid', alternativegraduatio__ucationorganizationid,
                    'alternativeGraduatio__nplantypedescriptor', alternativeGraduatio__nplantypedescriptor,
                    'alternativegraduatio___graduationschoolyear', alternativegraduatio___graduationschoolyear
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('public', 'ssa_alternate_graduation_plan_reference')}}
	WHERE
	    studentuniqueid IS NULL
	    OR schoolid IS NULL
	    OR NULLIF(TRIM(alternativegraduatio__ucationorganizationid),'') IS NULL
	    OR NULLIF(TRIM(alternativeGraduatio__nplantypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(alternativegraduatio___graduationschoolyear),'') IS NULL


    GROUP BY studentuniqueid,
             schoolid
),

-----------------------------------------------------------------------------
-- Logging invalid records from Student School Association Education Plans
------------------------------------------------------------------------------
err_ssa_ep as (
    SELECT studentuniqueid,
           schoolid,
        'Student School Association Education Plans' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'educationplans_educationplandescriptor', educationplans_educationplandescriptor
                    )) as Source_Record,
        now() as processed_at

    FROM
     {{ source('public', 'ssa_education_plans')}}
    WHERE
        studentuniqueid IS NULL
        OR schoolid IS NULL
        OR NULLIF(TRIM(educationplans_educationplandescriptor),'') IS NULL

    GROUP BY
        studentuniqueid,
        schoolid
),

----------------------------------------------------------------
-- Logging records from Student School Association Base entity where look-up is failed
----------------------------------------------------------------

err_lk_ssa_b_tmp as (
    SELECT
        studentuniqueid,
        schoolid,
        entrygradeleveldescriptor,
        entrygradelevelreasondescriptor,
        entrytypedescriptor,
        exitwithdrawtypedescriptor,
        residencystatusdescriptor,
        tx_adaeligibilitydescriptor,
        tx_studentattributiondescriptor

--        jsonb_array_elements(TexasExtensions)->>'tx_adaeligibilitydescriptor'
--        as tx_adaeligibilitydescriptor,
--        jsonb_array_elements(TexasExtensions)->>'tx_studentattributiondescriptor'
--        as tx_studentattributiondescriptor

    FROM
        {{ref('stg_ssa_base')}}
),

err_lk_ssa_b AS (
    SELECT studentuniqueid,
           schoolid,
        'Student School Association Base' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'entrygradeleveldescriptor', entrygradeleveldescriptor,
                    'entrygradelevelreasondescriptor', entrygradelevelreasondescriptor,
                    'entrytypedescriptor', entrytypedescriptor,
                    'exitwithdrawtypedescriptor', exitwithdrawtypedescriptor,
                    'residencystatusdescriptor', residencystatusdescriptor,
                    'tx_adaeligibilitydescriptor', tx_adaeligibilitydescriptor,
                    'tx_studentattributiondescriptor', tx_studentattributiondescriptor
                    --'graduationplanreference_graduationplantypedescriptor', graduationplanreference_graduationplantypedescriptor

                    )) as Source_Record,
        now() as processed_at

    FROM
        err_lk_ssa_b_tmp
	WHERE
	    studentuniqueid IS NULL
	    OR schoolid IS NULL
	    OR NULLIF(TRIM(entrygradeleveldescriptor),'') IS NULL
	    OR NULLIF(TRIM(entrygradelevelreasondescriptor),'') IS NULL
	    OR NULLIF(TRIM(entrytypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(exitwithdrawtypedescriptor),'') IS NULL
	    OR NULLIF(TRIM(residencystatusdescriptor),'') IS NULL
	    OR NULLIF(TRIM(tx_adaeligibilitydescriptor),'') IS NULL
        OR NULLIF(TRIM(tx_studentattributiondescriptor),'') IS NULL
        --OR NULLIF(TRIM(graduationplanreference_graduationplantypedescriptor),'') IS NULL



    GROUP BY studentuniqueid,
             schoolid
),

----------------------------------------------------------------
-- Logging records from Student School Association Alternate Graduation Plan Reference where lookup is failed
----------------------------------------------------------------
err_lk_ssa_agpr_tmp as (
    SELECT
        studentuniqueid,
        schoolid,
        jsonb_array_elements(alternativeGraduationPlans)->>'alternativeGraduatio__nplantypedescriptor'
        as alternativeGraduatio__nplantypedescriptor
	FROM
        {{ref('stg_ssa_alternative_graduation_plan_reference')}}
),

err_lk_ssa_agpr as (
    SELECT
        studentuniqueid,
        schoolid,
        'Student School Association Alternate Graduation Plan Reference' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'alternativeGraduatio__nplantypedescriptor', alternativeGraduatio__nplantypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_ssa_agpr_tmp
	WHERE
	    NULLIF(TRIM(alternativeGraduatio__nplantypedescriptor),'') IS NULL
    GROUP BY studentuniqueid,
             schoolid
),


----------------------------------------------------------------
-- Logging records from Student School Association Education Plans where lookup is failed
----------------------------------------------------------------
err_lk_ssa_ep_tmp as (
    SELECT
        studentuniqueid,
        schoolid,
        jsonb_array_elements(educationPlans)->>'educationplans_educationplandescriptor'
        as educationplans_educationplandescriptor
	FROM
        {{ref('stg_ssa_education_plans')}}
),

err_lk_ssa_ep as (
    SELECT studentuniqueid,
           schoolid,
        'Student School Association Education Plans' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'schoolid', schoolid,
                    'educationplans_educationplandescriptor', educationplans_educationplandescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        err_lk_ssa_ep_tmp
	WHERE
	    NULLIF(TRIM(educationplans_educationplandescriptor),'') IS NULL
    GROUP BY studentuniqueid,
             schoolid
),


----------------------------------------------------------------
-- Collecting all failures in error table
----------------------------------------------------------------
final as (
    SELECT * from err_ssa_b
    UNION
    SELECT * from err_ssa_agpr
    UNION
    SELECT * from err_ssa_ep
    UNION
    SELECT * from err_lk_ssa_b
    UNION
    SELECT * from err_lk_ssa_agpr
    UNION
    SELECT * from err_lk_ssa_ep
)

----------------------------------------------------------------
-- Incremental flag set for maintaining previous load's error data
----------------------------------------------------------------
select {{ var('LOADID',-1) }} as LOADID, * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})

{% endif %}