WITH sc_ia_addr as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_ia_addresstypedescriptor')}} as src_ia_addr
   on d.descriptorid = src_ia_addr.addresstypedescriptorid
),

sc_cd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_countrydescriptor')}} as src_cd
   on d.descriptorid = src_cd.countrydescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_ia as (

    SELECT
        -- * from {{ref('cl_school_international_addresses')}}
    loadid,
    schoolid,
    -- lookup address Type Descriptor use descriptor
    CASE
		-- When Address Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sia.addresstypedescriptor),'') is null)
		THEN NULL

		-- When Address Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sia.addresstypedescriptor),'') is not null and NULLIF(TRIM(sc_ia_addr.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Address Type Descriptor
		else concat(sc_ia_addr.namespace, '#', sc_ia_addr.codevalue)

	END as addresstypedescriptor,

    -- lookup state abbreviation descriptor use descriptor
    CASE
		-- When Country Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_sia.countrydescriptor),'') is null)
		THEN NULL

		-- When Country Descriptor is not null but does not have matching record in descriptor
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_sia.countrydescriptor),'') is not null and NULLIF(TRIM(sc_cd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Country Descriptor
		else concat(sc_cd.namespace, '#', sc_cd.codevalue)

	END as countrydescriptor,

    addressline1,
    addressline2,
    addressline3,
    addressline4,
    begindate,
    enddate,
    latitude,
    longitude

    from {{ref('cl_school_international_addresses')}} as cl_sia

    left outer join sc_ia_addr
    on cl_sia.addresstypedescriptor = sc_ia_addr.codevalue

    left outer join sc_cd
    on cl_sia.countrydescriptor = sc_cd.codevalue

),


------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
        sc_ia.loadid as LOADID,
		sc_ia.schoolid,
		jsonb_agg(json_build_object(
				'addressTypeDescriptor', sc_ia.addresstypedescriptor,
				'countryDescriptor', sc_ia.countrydescriptor,
				'addressLine1', sc_ia.addressline1,
                'addressLine2', sc_ia.addressline2,
                'addressLine3', sc_ia.addressline3,
                'addressLine4', sc_ia.addressline4,
                'beginDate', sc_ia.begindate,
                'endDate', sc_ia.enddate,
                'latitude', sc_ia.latitude,
                'longitude', sc_ia.longitude
                )

			) AS internationalAddresses

   FROM
	    sc_ia
   GROUP BY
        sc_ia.loadid,
		sc_ia.schoolid

)

select * from final
	