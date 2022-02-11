{{ config(
    materialized='incremental'
    )
}}

----------------------------------------------------------------
-- Logging invalid records from Calendar Base entity
----------------------------------------------------------------
with err_cb as (
    SELECT calendarcode,
           schoolid,
           schoolyear,
        'Calendar Base' as Source_Entity,
        'ERROR: Mandatory Field Null' as Error_Message,
        jsonb_agg(json_build_object(
                    'calendarcode', calendarcode,
                    'schoolid', schoolid,
                    'schoolyear', schoolyear,
                    'calendartypedescriptor', calendartypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
		{{ source('raw_data', 'calendar_base')}}
    WHERE
	    calendarcode IS NULL
	    OR schoolid IS NULL
	    OR schoolyear IS NULL
	    OR NULLIF(TRIM(calendartypedescriptor),'') IS NULL

    GROUP BY calendarcode,
             schoolid,
             schoolyear
),
----------------------------------------------------------------
-- Logging records from Calender Base entity where look-up is failed
----------------------------------------------------------------
err_lk_cb as (
    SELECT calendarcode,
           schoolid,
           schoolyear,
        'Calender Base' as Source_Entity,
        'WARNING: LOVs lookup failed' as Error_Message,
        jsonb_agg(json_build_object(
                    'calendarcode', calendarcode,
                    'schoolid', schoolid,
                    'schoolyear', schoolyear,
                    'calendartypedescriptor', calendartypedescriptor
                    )) as Source_Record,
        now() as processed_at

	FROM
        {{ref('stg_cal_base')}}
	WHERE
	    NULLIF(TRIM(calendartypedescriptor),'') IS NULL
    GROUP BY calendarcode,
             schoolid,
             schoolyear
),

----------------------------------------------------------------
-- Collecting all failures in error table
----------------------------------------------------------------
final as (
    SELECT * from err_cb
    UNION
    SELECT * from err_lk_cb

)

----------------------------------------------------------------
-- Incremental flag set for maintaining previous load's error data
----------------------------------------------------------------
select {{ var('LOADID',-1) }} as LOADID, * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where processed_at > (select max(processed_at) from {{ this }})

{% endif %}