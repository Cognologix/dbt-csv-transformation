WITH sc_txcntd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_campusenrollmenttypedescriptor')}} as src_txcntd
   on d.descriptorid = src_txcntd.tx_campusenrollmenttypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_cet as (

    SELECT
        -- * from {{ref('cl_school_campus_enrollment_types')}}
    schoolid,
    -- lookup tx_campus enrollment type descriptor use descriptor
    CASE
		-- When tx_campus enrollment type descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_scet.tx_campusenrollmenttypedescriptor),'') is null)
		THEN NULL

		-- When tx_campus enrollment type descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_scet.tx_campusenrollmenttypedescriptor),'') is not null and NULLIF(TRIM(sc_txcntd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new tx_campus enrollment type descriptor
		else concat(sc_txcntd.namespace, '#', sc_txcntd.codevalue)

	END as tx_campusenrollmenttypedescriptor,
	tx_begindate,
	tx_enddate


    from {{ref('cl_school_campus_enrollment_types')}} as cl_scet

    left outer join sc_txcntd
    on cl_scet.tx_campusenrollmenttypedescriptor = sc_txcntd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
		sc_cet.schoolid,
		jsonb_agg(json_build_object(
				'txCampusEnrollmentTypeDescriptor', sc_cet.tx_campusenrollmenttypedescriptor,
				'txBeginDate', sc_cet.tx_begindate,
				'txEndDate', sc_cet.tx_enddate

			)) AS campusEnrollmentTypes
   FROM
	    sc_cet
   GROUP BY
		sc_cet.schoolid

)

select * from final
