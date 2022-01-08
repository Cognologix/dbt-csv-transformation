CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

WITH calendar AS (
	SELECT
		uuid_generate_v4() AS uid, 
		LPAD(cb.calendarcode::text, 16, '0') ||
		LPAD(cb.schoolid::text, 16, '0') ||
		LPAD(cb.schoolyear::text, 6, '0') AS resourceid,
        'CREATE' AS Operation,
		0 AS status,
		'CALENDER' AS tabletype,
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
	operation,
	tabletype,
	payload
FROM 
    calendar
