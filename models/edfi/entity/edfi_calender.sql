WITH calendar AS (
	SELECT
        'CREATE' AS Operation,
		json_build_object(
			'calendarCode', cb.calendarcode,
			'schoolId', cb.schoolid,
			'schoolYear', cb.schoolyear,
            'calendarTypeDescriptor', cb.calendartypedescriptor
		) AS calendar_base
	FROM 
		calendar_base AS cb
)

SELECT 
    * 
FROM 
    calendar
