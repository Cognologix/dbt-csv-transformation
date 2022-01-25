-- Apply lookup and other business transformations at this stage
WITH cb as (

    SELECT
    	distinct calendarcode,
		         schoolid,
		         schoolyear,
		         calendartypedescriptor
    from
        {{ref('raw_cal_base')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
        uuid_generate_v4() AS resourceid,
        cb.calendarcode,
        json_build_object(
            'calendarCode', cast(cb.calendarcode as varchar),
            'schoolId', cb.schoolid,
            'schoolYear', cb.schoolyear
        )  AS externalid,
        'CALENDER' AS resourcetype,
        'CREATE' AS operation,
        0 AS status,
       jsonb_agg(json_build_object(
				    'schoolId', cb.schoolid
                )AS schoolReference,

                json_build_object(
				    'schoolYear', cb.schoolyear
                )AS schoolYearTypeReference

			)

    FROM
        cb
    GROUP BY
		cb.calendarcode,
		cb.schoolid,
		cb.schoolyear
)

select * from final