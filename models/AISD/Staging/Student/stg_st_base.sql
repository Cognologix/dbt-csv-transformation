-- Add lookup and other business validations at this stage
WITH ssdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_sourcesystemdescriptor')}} as ssd
   on d.descriptorid = ssd.sourcesystemdescriptorid
),

bcdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_countrydescriptor')}} as bcd
   on d.descriptorid = bcd.countrydescriptorid
),

bsdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_sexdescriptor')}} as bsd
   on d.descriptorid = bsd.sexdescriptorid
),

bsadm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_stateabbreviationdescriptor')}} as bsad
   on d.descriptorid = bsad.stateabbreviationdescriptorid
),

csdm as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_citizenshipstatusdescriptor')}} as csd
   on d.descriptorid = csd.citizenshipstatusdescriptorid
),

sb as (
    select
    -- * from {{ref('raw_st_base')}}
        studentuniqueid,
        operation,

        CASE
        -- When sourcesystemdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(sourcesystemdescriptor),'') is null)
        THEN NULL

        -- When sourcesystemdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(sourcesystemdescriptor),'') is not null and NULLIF(TRIM(ssdm.codevalue),'') is NULL)
        THEN sourcesystemdescriptor

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(ssdm.namespace, '#', ssdm.codevalue)

        END as sourcesystemdescriptor,

		birthcity,
        ----------------------
        CASE
        -- When birthcountrydescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(birthcountrydescriptor),'') is null)
        THEN NULL

        -- When birthcountrydescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(birthcountrydescriptor),'') is not null and NULLIF(TRIM(bcdm.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(bcdm.namespace, '#', bcdm.codevalue)

        END as birthcountrydescriptor,
        ----------------------
        ----------------------
		birthdate,
		birthinternationalprovince,
        ----------------------
        CASE
        -- When birthsexdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(birthsexdescriptor),'') is null)
        THEN NULL

        -- When birthsexdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(birthsexdescriptor),'') is not null and NULLIF(TRIM(bsdm.codevalue),'') is NULL)
        THEN 'Not Selected'

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(bsdm.namespace, '#', bsdm.codevalue)

        END as birthsexdescriptor,
        ----------------------
        ----------------------
        CASE
        -- When birthstateabbreviationdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(birthstateabbreviationdescriptor),'') is null)
        THEN NULL

        -- When birthstateabbreviationdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(birthstateabbreviationdescriptor),'') is not null and NULLIF(TRIM(bsadm.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(bsadm.namespace, '#', bsadm.codevalue)

        END as birthstateabbreviationdescriptor,
        ----------------------
        ----------------------
        CASE
        -- When citizenshipstatusdescriptor contain blanks or null, process as is or set null
        when (NULLIF(TRIM(citizenshipstatusdescriptor),'') is null)
        THEN NULL

        -- When citizenshipstatusdescriptor is not null but does not have matching record in descriptor, set as Other Name category
        -- revisit the transformation rule once business logic is confirmed
        when (NULLIF(TRIM(citizenshipstatusdescriptor),'') is not null and NULLIF(TRIM(csdm.codevalue),'') is NULL)
        THEN NULL

        -- Else matching record is found, so concatenate namespace and codevalue to create new other name type descrptor
        else concat(csdm.namespace, '#', csdm.codevalue)

        END as citizenshipstatusdescriptor,

		sb.dateenteredus,
		sb.firstname,
		sb.generationcodesuffix,
		sb.lastsurname,
		sb.maidenname,
		sb.middlename,
		sb.multiplebirthstatus,
		sb.personaltitleprefix,
		sb.tx_adultpreviousattendanceindicator,
		sb.tx_localstudentid,
		sb.tx_studentid

    FROM
        {{ref('raw_st_base')}} sb
    left outer join ssdm
        on sourcesystemdescriptor = ssdm.codevalue
    left outer join bcdm
        on birthcountrydescriptor = bcdm.codevalue
    left outer join bsdm
        on birthsexdescriptor = bsdm.codevalue
    left outer join bsadm
        on birthstateabbreviationdescriptor = bsadm.codevalue
    left outer join csdm
        on citizenshipstatusdescriptor = csdm.codevalue


),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sb.studentuniqueid,
		uuid_generate_v4() AS resourceid,
		json_build_object(
			'studentuniqueid', sb.studentuniqueid
		) AS externalid,
		'STUDENT' AS resourcetype,
		sb.operation AS operation,
		0 AS status,
		json_build_object(
			'personId', sb.studentuniqueid,
			'sourceSystemDescriptor', sb.sourcesystemdescriptor
		) AS identificationDocuments,
		sb.birthcity,
		sb.birthcountrydescriptor,
		sb.birthdate,
		sb.birthinternationalprovince,
		sb.birthsexdescriptor,
		sb.birthstateabbreviationdescriptor,
		sb.citizenshipstatusdescriptor,
		sb.dateenteredus,
		sb.firstname,
		sb.generationcodesuffix,
		sb.lastsurname,
		sb.maidenname,
		sb.middlename,
		sb.multiplebirthstatus,
		sb.personaltitleprefix,
		sb.tx_adultpreviousattendanceindicator,
		sb.tx_localstudentid,
		sb.tx_studentid

	FROM
		sb

)

select * from final
