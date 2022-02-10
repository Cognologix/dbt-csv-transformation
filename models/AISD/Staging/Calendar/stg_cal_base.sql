------------------------------------------------------------------------------
-- Fetch Calendar Type Descriptor Values in temp table for look-up validations
------------------------------------------------------------------------------
WITH c_ctd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_calendartypedescriptor')}} as src_ctd
   on d.descriptorid = src_ctd.calendartypedescriptorid
),
cb as (

    SELECT
        loadid,
    	calendarcode,
    	operation,
		schoolid,
		schoolyear,

		CASE
        -- When calendartypedescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(calendartypedescriptor),'') is null)
        THEN NULL

        -- When calendartypedescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(calendartypedescriptor),'') is not null and NULLIF(TRIM(c_ctd.codevalue),'') is NULL)
        THEN calendartypedescriptor

        -- Else matching record is found, so concatenate namespace and codevalue to create new calendartypedescriptor
        else concat(c_ctd.namespace, '#', c_ctd.codevalue)

        END as calendartypedescriptor

FROM
        {{ref('cl_cal_base')}} AS sb
    left outer join c_ctd
        on calendartypedescriptor = c_ctd.codevalue

),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------

final as (
   SELECT
        cb.loadid as LOADID,
        uuid_generate_v4() AS resourceid,
        cb.calendarcode,
        cb.schoolid,
        cb.schoolyear,
        cb.calendartypedescriptor,
        json_build_object(
            'calendarCode', cast(cb.calendarcode as varchar),
            'schoolId', cb.schoolid,
            'schoolYear', cb.schoolyear
        )  AS externalid,
        'CALENDER' AS resourcetype,
        'CREATE' AS operation,
        0 AS status,

       json_build_object(
				    'schoolId', cb.schoolid
				    )AS schoolReference,

       json_build_object(
				    'schoolYear', cb.schoolyear
                    )AS schoolYearTypeReference


    FROM
        cb

)

select * from final