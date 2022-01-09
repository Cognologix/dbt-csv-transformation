WITH calendar AS (
    SELECT
        uuid_generate_v4() AS resourceid,
        json_build_object(
            'calendarCode', cast(cb.calendarcode as varchar),
            'schoolId', cb.schoolid,
            'schoolYear', cb.schoolyear
        )  AS externalid,
        'CALENDER' AS resourcetype,
        'CREATE' AS operation,
        0 AS status,
        json_build_object(
            'calendarCode', cast(cb.calendarcode as varchar),
            'schoolReference', json_build_object(
                'schoolId', cb.schoolid
            ),
            'schoolYearTypeReference', json_build_object(
                'schoolYear', cb.schoolyear
            ),
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