WITH st_ext_new as (

    SELECT
        st_b.loadid,
        st_b.studentuniqueid,
        json_build_object(
				'txAdultPreviousAttendanceIndicator', st_b.tx_adultpreviousattendanceindicator,
				'txLocalStudentID', st_b.tx_localstudentid,
				'txStudentID', st_b.tx_studentid,
				'censusBlockGroups', st_cbg.studentCensusBlockGroup,
				'crisisEvents', st_ce.crisisEvents

		) AS TexasExtensions


    FROM
        {{ref('stg_st_base')}} as st_b
    LEFT JOIN
        {{ref('stg_st_student_census_block_group')}} AS st_cbg
    ON
        st_cbg.studentuniqueid = st_b.studentuniqueid

    LEFT JOIN
        {{ref('stg_st_crisis_events')}} AS st_ce
    ON
        st_ce.studentuniqueid = st_b.studentuniqueid


)
select * from st_ext_new