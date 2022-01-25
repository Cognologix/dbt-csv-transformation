WITH calendar AS (

	SELECT
	    NULLIF(TRIM(operation),'') AS operation,
		calendarcode,
		schoolid,
		schoolyear,
        NULLIF(TRIM(calendartypedescriptor),'') AS calendartypedescriptor

	FROM
		public.calendar_base

	WHERE
	    calendarcode IS NOT NULL AND
	    schoolid IS NOT NULL AND
	    schoolyear IS NOT NULL AND
	    NULLIF(TRIM(calendartypedescriptor),'') IS NOT NULL

)

select * from calendar