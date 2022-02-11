WITH sc_atd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_addresstypedescriptor')}} as src_atd
   on d.descriptorid = src_atd.addresstypedescriptorid
),

sc_sad as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_stateabbreviationdescriptor')}} as src_sad
   on d.descriptorid = src_sad.stateabbreviationdescriptorid
),
sc_ld as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_localedescriptor')}} as src_ld
   on d.descriptorid = src_ld.localedescriptorid
),

------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------


sc_add as (

    SELECT
        -- * from {{ref('cl_school_addresses')}}
    loadid,
    schoolid,
    -- lookup Address Type Descriptor use descriptor
    CASE
		-- When Address Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sc_address.addresstypedescriptor),'') is null)
		THEN NULL

		-- When Address Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sc_address.addresstypedescriptor),'') is not null and NULLIF(TRIM(sc_atd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Address Type Descriptor
		else concat(sc_atd.namespace, '#', sc_atd.codevalue)

	END as addresstypedescriptor,

    -- lookup state abbreviation descriptor use descriptor
    CASE
		-- When state abbreviation descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sc_address.stateabbreviationdescriptor),'') is null)
		THEN NULL

		-- When state abbreviation descriptor is not null but does not have matching record in descriptor
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sc_address.stateabbreviationdescriptor),'') is not null and NULLIF(TRIM(sc_sad.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new stateabbreviationdescriptor
		else concat(sc_sad.namespace, '#', sc_sad.codevalue)

	END as stateabbreviationdescriptor,
	CASE
		-- When locale descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sc_address.localedescriptor),'') is null)
		THEN NULL

		-- When locale descriptor is not null but does not have matching record in descriptor, set as Not Applicable category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sc_address.localedescriptor),'') is not null and NULLIF(TRIM(sc_ld.codevalue),'') is NULL)
		THEN 'Not Applicable'

		-- Else matching record is found, so concatenate namespace and codevalue to create new locale descriptor
		else concat(sc_ld.namespace, '#', sc_ld.codevalue)

	END as localedescriptor,
    city,
    postalcode,
    streetnumbername,
    apartmentroomsuitenumber,
    buildingsitenumber,
    congressionaldistrict,
    countyfipscode,
    donotpublishindicator,
    latitude,
    longitude,
    nameofcounty,
    begindate,
    enddate


    from {{ref('cl_school_addresses')}} as cl_sc_address

    left outer join sc_atd
    on cl_sc_address.addresstypedescriptor = sc_atd.codevalue

    left outer join sc_sad
    on cl_sc_address.stateabbreviationdescriptor = sc_sad.codevalue

    left outer join sc_ld
    on cl_sc_address.localedescriptor = sc_ld.codevalue

),


------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
sc_addresses_periods AS (
	SELECT
		sa.schoolid,
		sa.addresstypedescriptor,
		sa.stateabbreviationdescriptor,
		sa.city,
		sa.postalcode,
		sa.streetnumbername,

		jsonb_agg(json_build_object(
				'beginDate', sa.begindate,
				'endDate', sa.enddate
			)
		) AS periods
	FROM
		sc_add AS sa
	WHERE begindate IS NOT NULL
	GROUP BY
	    sa.schoolid,
		sa.addresstypedescriptor,
		sa.stateabbreviationdescriptor,
		sa.city,
		sa.postalcode,
		sa.streetnumbername
),
final as (

   SELECT
        sc_add.loadid as LOADID,
		sc_add.schoolid,
		jsonb_agg(json_build_object(
				'addressTypeDescriptor', sc_add.addresstypedescriptor,
                'stateAbbreviationDescriptor', sc_add.stateabbreviationdescriptor,
				'city', sc_add.city,
				'postalCode', sc_add.postalcode,
				'streetNumberName', sc_add.streetnumbername,
				'localeDescriptor', sc_add.localedescriptor,
				'apartmentRoomSuiteNumber', sc_add.apartmentroomsuitenumber,
				'buildingSiteNumber', sc_add.buildingsitenumber,
				'congressionalDistrict', sc_add.congressionaldistrict,
				'countyFIPSCode', sc_add.countyfipscode,
				'doNotPublishIndicator', sc_add.donotpublishindicator,
				'latitude', sc_add.latitude,
				'longitude', sc_add.longitude,
			    'nameOfCounty', sc_add.nameofcounty,
                'periods', sap.periods

        ))AS addresses

   FROM
	    sc_add

   LEFT JOIN
		sc_addresses_periods AS sap
	ON
		sap.schoolid = sc_add.schoolid AND
		sap.city = sc_add.city AND
		sap.addresstypedescriptor = sc_add.addresstypedescriptor AND
		sap.stateabbreviationdescriptor = sc_add.stateabbreviationdescriptor AND
		sap.postalcode = sc_add.postalcode AND
		sap.streetnumbername = sc_add.streetnumbername


   GROUP BY
        sc_add.loadid,
		sc_add.schoolid

)

select * from final