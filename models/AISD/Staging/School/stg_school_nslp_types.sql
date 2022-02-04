WITH sc_txnslptd as (
   select namespace, codevalue
   from {{ref('src_descriptor')}} as d
   JOIN {{ref('src_tx_nslptypedescriptor')}} as src_txnslptd
   on d.descriptorid = src_txnslptd.tx_nslptypedescriptorid
),
------------------------------------------------------------------------------
-- Add lookup and other business validations at this stage
------------------------------------------------------------------------------
sc_nslp_ty as (

    SELECT
        -- * from {{ref('cl_school_nslp_types')}}
    schoolid,
    -- lookup TX_NSLP Type Descriptor use descriptor
    CASE
		-- When TX_NSLP Type Descriptor contain blanks or null, process as is or set null
		-- since records with these descriptors are being excluded in raw stage, this case is not likely. REVISIT
		when (NULLIF(TRIM(cl_nslpt.tx_nslptypedescriptor),'') is null)
		THEN NULL

		-- When TX_NSLP Type Descriptor is not null but does not have matching record in descriptor, set as BLANK/NULL category
		-- since records with these descriptors are being excluded in raw stage, this check is not required. REVISIT
		when (NULLIF(TRIM(cl_nslpt.tx_nslptypedescriptor),'') is not null and NULLIF(TRIM(sc_txnslptd.codevalue),'') is NULL)
		THEN ''

		-- Else matching record is found, so concatenate namespace and codevalue to create new TX_NSLP Type Descriptor
		else concat(sc_txnslptd.namespace, '#', sc_txnslptd.codevalue)

	END as tx_nslptypedescriptor,
	tx_begindate,
	tx_enddate


    from {{ref('cl_school_nslp_types')}} as cl_nslpt

    left outer join sc_txnslptd
    on cl_nslpt.tx_nslptypedescriptor = sc_txnslptd.codevalue
),

------------------------------------------------------------------------------
-- Final Json block to be created after all validations and transformations are done
------------------------------------------------------------------------------
final as (
   SELECT
		sc_nslp_ty.schoolid,
		jsonb_agg(json_build_object(
				'txNSLPTypeDescriptor', sc_nslp_ty.tx_nslptypedescriptor,
				'txBeginDate', sc_nslp_ty.tx_begindate,
				'txEndDate', sc_nslp_ty.tx_enddate
				)

			) AS nslpTypes

   FROM
	    sc_nslp_ty
   GROUP BY
		sc_nslp_ty.schoolid

)

select * from final
