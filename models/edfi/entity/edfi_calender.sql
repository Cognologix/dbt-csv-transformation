CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

WITH calendar AS (
	SELECT
		uuid_generate_v4() AS uniqueid, 
		LPAD(cb.calendarcode::text, 16, '0') ||
		LPAD(cb.schoolid::text, 16, '0') ||
		LPAD(cb.schoolyear::text, 6, '0') AS resourceid,
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
    uniqueid, 
	resourceid,
	resourcetype,
	operation,
	payload
FROM 
    calendar
