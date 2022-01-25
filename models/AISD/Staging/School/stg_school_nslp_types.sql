-- Apply lookup and other business transformations at this stage
WITH sc_nslp_ty as (

    SELECT
    	distinct schoolid,
		        tx_nslptypedescriptor,
		        tx_begindate,
		        tx_enddate
    from
        {{ref('raw_school_nslp_types')}}

 ),

-- Final Json block to be created after all validations and transformations are done
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
