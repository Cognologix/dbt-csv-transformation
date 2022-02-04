WITH sc_indcd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_indicatordescriptor')}} as src_indcd
   on d.descriptorid = src_indcd.indicatordescriptorid
),

sc_igd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_indicatorgroupdescriptor')}} as src_igd
   on d.descriptorid = src_igd.indicatorgroupdescriptorid
),
sc_ild as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_indicatorleveldescriptor')}} as src_ild
   on d.descriptorid = src_ild.indicatorleveldescriptorid
),

------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_ind as (

    SELECT
        -- * from {{ref('cl_school_addresses')}}
    schoolid,
    -- lookup Indicator Descriptor use descriptor
    CASE
		-- When Indicator Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(sc_si.indicatordescriptor),'') is null)
		THEN NULL

		-- When Indicator Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(sc_si.indicatordescriptor),'') is not null and NULLIF(TRIM(sc_indcd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Indicator Descriptor
		else concat(sc_indcd.namespace, '#', sc_indcd.codevalue)

	END as indicatordescriptor,

    -- lookup Indicator Group Descriptor use descriptor
    CASE
		-- When Indicator Group Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(sc_si.indicatorgroupdescriptor),'') is null)
		THEN NULL

		-- When Indicator Group Descriptor is not null but does not have matching record in descriptor
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(sc_si.indicatorgroupdescriptor),'') is not null and NULLIF(TRIM(sc_igd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Indicator Group Descriptor
		else concat(sc_igd.namespace, '#', sc_igd.codevalue)

	END as indicatorgroupdescriptor,
	CASE
		-- When Indicator Level Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(sc_si.indicatorleveldescriptor),'') is null)
		THEN NULL

		-- When Indicator Level Descriptor is not null but does not have matching record in descriptor, set as Other category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(sc_si.indicatorleveldescriptor),'') is not null and NULLIF(TRIM(sc_ild.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new Indicator Level Descriptor
		else concat(sc_ild.namespace, '#', sc_ild.codevalue)

	END as indicatorleveldescriptor,
    designatedby,
    indicatorvalue,
    begindate,
    enddate


    from {{ref('cl_school_indicators')}} as sc_si

    left outer join sc_indcd
    on sc_si.indicatordescriptor = sc_indcd.codevalue

    left outer join sc_igd
    on sc_si.indicatorgroupdescriptor = sc_igd.codevalue

    left outer join sc_ild
    on sc_si.indicatorleveldescriptor = sc_ild.codevalue

),


------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
		sc_ind.schoolid,

		jsonb_agg(json_build_object(
				'indicatorDescriptor', sc_ind.indicatordescriptor,
				'indicatorGroupDescriptor', sc_ind.indicatorgroupdescriptor,
				'indicatorLevelDescriptor', sc_ind.indicatorleveldescriptor,
				'designatedBy', sc_ind.designatedby,
                'indicatorValue', sc_ind.indicatorvalue,
                'beginDate', sc_ind.begindate,
		        'endDate', sc_ind.enddate
        ))AS indicators

   FROM
	    sc_ind
   GROUP BY
		sc_ind.schoolid

)

select * from final

