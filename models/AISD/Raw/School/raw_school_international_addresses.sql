WITH sc_international_addresses AS (

	SELECT
		schoolid,
		NULLIF(TRIM(addresstypedescriptor),'') AS addresstypedescriptor,
		NULLIF(TRIM(countrydescriptor),'') AS countrydescriptor,
		NULLIF(TRIM(addressline1),'') AS addressline1,
		NULLIF(TRIM(addressline2),'') AS addressline2,
		NULLIF(TRIM(addressline3),'') AS addressline3,
		NULLIF(TRIM(addressline4),'') AS addressline4,
		begindate,
		enddate,
		NULLIF(TRIM(latitude),'') AS latitude,
		NULLIF(TRIM(longitude),'') AS longitude

	FROM
		{{ source('public', 'school_international_addresses')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(addresstypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(countrydescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(addressline1),'') IS NOT NULL

)

select * from sc_international_addresses
