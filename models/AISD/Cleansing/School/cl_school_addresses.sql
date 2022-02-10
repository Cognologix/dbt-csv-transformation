WITH sc_addresses AS (

	SELECT
	    {{ var('LOADID',-1) }} as LOADID,
		schoolid,
		NULLIF(TRIM(addresstypedescriptor),'') AS addresstypedescriptor,
		NULLIF(TRIM(stateabbreviationdescriptor),'') AS stateabbreviationdescriptor,
		NULLIF(TRIM(city),'') AS city,
		NULLIF(TRIM(postalcode),'') AS postalcode,
		NULLIF(TRIM(streetnumbername),'') AS streetnumbername,
		NULLIF(TRIM(CAST(localedescriptor AS TEXT)),'') AS localedescriptor,
		NULLIF(TRIM(CAST(apartmentroomsuitenumber AS TEXT)),'') AS apartmentroomsuitenumber,
		NULLIF(TRIM(CAST(buildingsitenumber AS TEXT)),'') AS buildingsitenumber,
		NULLIF(TRIM(CAST(congressionaldistrict AS TEXT)),'') AS congressionaldistrict,
		NULLIF(TRIM(CAST(countyfipscode AS TEXT)),'') AS countyfipscode,
		donotpublishindicator,
		NULLIF(TRIM(CAST(latitude AS TEXT)),'') AS latitude,
		NULLIF(TRIM(CAST(longitude AS TEXT)),'') AS longitude,
		NULLIF(TRIM(CAST(nameofcounty AS TEXT)),'') AS nameofcounty,
		NULLIF(TRIM(begindate),'') AS begindate,
		NULLIF(TRIM(enddate),'') AS enddate

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
