WITH sc_international_addresses AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(CAST(addresstypedescriptor AS TEXT)),'') AS addresstypedescriptor,
		NULLIF(TRIM(CAST(countrydescriptor AS TEXT)),'') AS countrydescriptor,
		NULLIF(TRIM(CAST(addressline1 AS TEXT)),'') AS addressline1,
		NULLIF(TRIM(addressline2),'') AS addressline2,
		NULLIF(TRIM(CAST(addressline3 AS TEXT)),'') AS addressline3,
		NULLIF(TRIM(CAST(addressline4 AS TEXT)),'') AS addressline4,
		begindate,
		enddate,
		NULLIF(TRIM(CAST(latitude AS TEXT)),'') AS latitude,
		NULLIF(TRIM(CAST(longitude AS TEXT)),'') AS longitude

	FROM
		{{ source('raw_data', 'school_international_addresses')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(CAST(addresstypedescriptor AS TEXT)),'') IS NOT NULL AND
	    NULLIF(TRIM(CAST(countrydescriptor AS TEXT)),'') IS NOT NULL AND
	    NULLIF(TRIM(CAST(addressline1 AS TEXT)),'') IS NOT NULL

)

select * from sc_international_addresses
