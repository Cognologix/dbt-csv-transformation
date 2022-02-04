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
        CURRENT_TIME as modified_at

	FROM
		{{ source('public', 'calendar_base')}}
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
        CURRENT_TIME as modified_at

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
select * from final
{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where modified_at > (select max(modified_at) from {{ this }})

{% endif %}