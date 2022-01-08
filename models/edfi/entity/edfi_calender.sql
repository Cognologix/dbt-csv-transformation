CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

WITH calendar AS (
	SELECT
		uuid_generate_v4() AS uid, 
		LPAD(cb.calendarcode::text, 16, '0') ||
		LPAD(cb.schoolid::text, 16, '0') ||
		LPAD(cb.schoolyear::text, 6, '0') AS resourceid,
		0 AS status,
		'CALENDER' AS resourcetype,
        'CREATE' AS operation,
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
    uid, 
	resourceid,
	resourcetype,
	operation,
	payload
FROM 
    calendar
