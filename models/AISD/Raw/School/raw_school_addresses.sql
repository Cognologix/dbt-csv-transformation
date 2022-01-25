WITH sc_addresses AS (

	SELECT
		schoolid,
		NULLIF(TRIM(addresstypedescriptor),'') AS addresstypedescriptor,
		NULLIF(TRIM(stateabbreviationdescriptor),'') AS stateabbreviationdescriptor,
		NULLIF(TRIM(city),'') AS city,
		NULLIF(TRIM(postalcode),'') AS postalcode,
		NULLIF(TRIM(streetnumbername),'') AS streetnumbername,
		NULLIF(TRIM(localedescriptor),'') AS localedescriptor,
		NULLIF(TRIM(apartmentroomsuitenumber),'') AS apartmentroomsuitenumber,
		NULLIF(TRIM(buildingsitenumber),'') AS buildingsitenumber,
		NULLIF(TRIM(congressionaldistrict),'') AS congressionaldistrict,
		NULLIF(TRIM(countyfipscode),'') AS countyfipscode,
		NULLIF(TRIM(donotpublishindicator),'') AS donotpublishindicator,
		NULLIF(TRIM(latitude),'') AS latitude,
		NULLIF(TRIM(longitude),'') AS longitude,
		NULLIF(TRIM(nameofcounty),'') AS nameofcounty,
		begindate,
		enddate

	FROM
		{{ source('public', 'school_addresses')}}

	WHERE
	    schoolid IS NOT NULL AND
	    NULLIF(TRIM(addresstypedescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(stateabbreviationdescriptor),'') IS NOT NULL AND
	    NULLIF(TRIM(city),'') IS NOT NULL AND
	    NULLIF(TRIM(postalcode),'') IS NOT NULL AND
	    NULLIF(TRIM(streetnumbername),'') IS NOT NULL
)

select * from sc_addresses
