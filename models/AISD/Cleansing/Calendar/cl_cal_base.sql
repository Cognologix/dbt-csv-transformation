WITH calendar AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
	    NULLIF(TRIM(operation),'') AS operation,
		calendarcode,
		schoolid,
		schoolyear,
        NULLIF(TRIM(calendartypedescriptor),'') AS calendartypedescriptor

	FROM
		{{ source('public', 'calendar_base')}}

	WHERE
	    calendarcode IS NOT NULL AND
	    schoolid IS NOT NULL AND
	    schoolyear IS NOT NULL AND
	    NULLIF(TRIM(calendartypedescriptor),'') IS NOT NULL

)

select * from calendar