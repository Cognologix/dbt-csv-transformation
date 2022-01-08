WITH calendar AS (
	SELECT
		uuid_generate_v4() AS resourceid,
        json_build_object(
            'calendarCode', cb.calendarcode,
            'schoolId', cb.schoolid,
            'schoolYear', cb.schoolyear
            )  AS externalid,
		'CALENDER' AS resourcetype,
        'CREATE' AS operation,
		0 AS status,
		json_build_object(
			'calendarCode', cb.calendarcode,
			'schoolId', cb.schoolid,
			'schoolYear', cb.schoolyear,
            'calendarTypeDescriptor', cb.calendartypedescriptor
		) AS payload
	FROM
		calendar_base AS cb
)
SELECT
    resourceid,
    externalid,
	resourcetype,
	operation,
	payload,
    status
FROM
    calendar
