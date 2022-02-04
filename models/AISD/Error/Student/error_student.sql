{{ config(
    materialized='incremental'
    )
}}

----------------------------------------------------------------
-- Logging invalid records from Student Base entity
----------------------------------------------------------------
with err_sb as (
    SELECT studentuniqueid,
        'Student Base' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'studentuniqueid', studentuniqueid,
                    'birthdate',birthdate,
                    'firstname',firstname,
                    'lastsurname',lastsurname)) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_base')}}
    WHERE
	    studentuniqueid IS NULL
	    OR birthdate IS NULL
	    OR NULLIF(TRIM(firstname),'') IS NULL
	    OR NULLIF(TRIM(lastsurname),'') IS NULL

    GROUP BY studentuniqueid
),

----------------------------------------------------------------
-- Logging invalid records from Student Identification Documents entity
----------------------------------------------------------------
err_sid as (
    SELECT studentuniqueid,
        'Student Identification Documents' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'identificationdocumentusedescriptor',identificationdocumentusedescriptor,
                    'personalinformationverificationdescriptor',personalinformationverificationdescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_identification_documents')}}
	WHERE
	    studentuniqueid IS NULL
	    OR NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NULL
	    OR NULLIF(TRIM(personalinformationverificationdescriptor),'') IS NULL

    GROUP BY studentuniqueid
),

----------------------------------------------------------------
-- Logging invalid records from Student Other Names entity
----------------------------------------------------------------
err_son as (
    SELECT STUDENTUNIQUEID,
        'Student Other Names' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'OTHERNAMETYPEDESCRIPTOR',OTHERNAMETYPEDESCRIPTOR,
                    'FIRSTNAME',FIRSTNAME)) as Source_Record,
        CURRENT_TIME as modified_at

    FROM PUBLIC.STUDENT_OTHER_NAMES
    WHERE STUDENTUNIQUEID IS NULL
        OR NULLIF(TRIM(OTHERNAMETYPEDESCRIPTOR),'') IS NULL
        OR NULLIF(TRIM(firstname),'') IS NULL
    GROUP BY STUDENTUNIQUEID
),

----------------------------------------------------------------
-- Logging invalid records from Student Personal Identification Documents entity
----------------------------------------------------------------
err_spid as (
    SELECT studentuniqueid,
        'Student Personal Identification Documents' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'identificationdocumentusedescriptor', identificationdocumentusedescriptor,
                    'personalinformationverificationdescriptor', personalinformationverificationdescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_personal_identification_documents')}}
	WHERE
	    studentuniqueid IS NULL
	    OR NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NULL
	    OR NULLIF(TRIM(personalinformationverificationdescriptor),'') IS NULL

    GROUP BY studentuniqueid
),

----------------------------------------------------------------
-- Logging invalid records from Student Visas entity
----------------------------------------------------------------
err_sv as (
    SELECT studentuniqueid,
        'Student Visas' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'visadescriptor',visadescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_visas')}}
	WHERE
	    studentuniqueid IS NULL
	    OR NULLIF(TRIM(visadescriptor),'') IS NULL

    GROUP BY studentuniqueid
),

----------------------------------------------------------------
-- Logging invalid records from Student Crisis Events entity
----------------------------------------------------------------
err_sce as (
    SELECT studentuniqueid,
        'Student Student Crisis Events' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'tx_crisiseventdescriptor',tx_crisiseventdescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_crisis_events')}} AS sce
	WHERE
	    studentuniqueid IS NULL
	    OR NULLIF(TRIM(tx_crisiseventdescriptor),'') IS NULL

    GROUP BY studentuniqueid
),


----------------------------------------------------------------
-- Logging invalid records from Student Census Block Group entity
----------------------------------------------------------------
err_scbg as (
    SELECT studentuniqueid,
        'Student Census Block Group' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'tx_studentcensusblockgroup',tx_studentcensusblockgroup
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'student_census_block_groups')}}
	WHERE
	    studentuniqueid IS NULL
	    OR NULLIF(TRIM(tx_studentcensusblockgroup),'') IS NULL

    GROUP BY studentuniqueid
),


----------------------------------------------------------------
-- Logging records from Student Base entity where look-up is failed
----------------------------------------------------------------
err_lk_sb as (
    SELECT studentuniqueid,
        'Student Base' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'birthcountrydescriptor', birthcountrydescriptor,
                    'birthstateabbreviationdescriptor', birthstateabbreviationdescriptor,
                    'citizenshipstatusdescriptor', citizenshipstatusdescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

    FROM
        {{ref('stg_st_base')}}
	WHERE
	    studentuniqueid IS NULL
	    OR birthcountrydescriptor IS NULL
	    OR birthstateabbreviationdescriptor IS NULL
	    OR citizenshipstatusdescriptor IS NULL

    GROUP BY studentuniqueid
),

----------------------------------------------------------------
-- Logging records from Student Identification Documents entity where lookup is failed
----------------------------------------------------------------
err_lk_sid_tmp as (
    SELECT
        studentuniqueid,
        jsonb_array_elements(identificationdocuments)->>'identificationdocumentusedescriptor'
        as identificationdocumentusedescriptor
	FROM
        {{ref('stg_st_identification_documents')}}
),
err_lk_sid as (
    SELECT
        studentuniqueid,
        'Student Identification Documents' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'identificationdocumentusedescriptor', identificationdocumentusedescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
        err_lk_sid_tmp
	WHERE
	    NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NULL
    GROUP BY studentuniqueid
),


----------------------------------------------------------------
-- Logging records from Student Personal Identification Documents entity where lookup is failed
----------------------------------------------------------------
err_lk_spid_tmp as (
    SELECT
        studentuniqueid,
        jsonb_array_elements(personalidentificationdocuments)->>'identificationdocumentusedescriptor'
        as identificationdocumentusedescriptor
	FROM
        {{ref('stg_st_personal_identification_documents')}}
),

err_lk_spid as (
    SELECT studentuniqueid,
        'Student Personal Identification Documents' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'STUDENTUNIQUEID', STUDENTUNIQUEID,
                    'identificationdocumentusedescriptor', identificationdocumentusedescriptor
                    )) as Source_Record,
        CURRENT_TIME as modified_at

	FROM
        err_lk_spid_tmp
	WHERE
	    NULLIF(TRIM(identificationdocumentusedescriptor),'') IS NULL
    GROUP BY studentuniqueid
),


----------------------------------------------------------------
-- Collecting all failures in error table
----------------------------------------------------------------
final as (
    SELECT * from err_sb
    UNION
    SELECT * from err_sid
    UNION
    SELECT * from err_son
    UNION
    SELECT * from err_spid
    UNION
    SELECT * from err_sv
    UNION
    SELECT * from err_sce
    UNION
    SELECT * from err_scbg
    UNION
    SELECT * from err_lk_sb
    UNION
    SELECT * from err_lk_sid
    UNION
    SELECT * from err_lk_spid
)

----------------------------------------------------------------
-- Incremental flag set for maintaining previous load's error data
----------------------------------------------------------------
select * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where modified_at > (select max(modified_at) from {{ this }})

{% endif %}