-- Apply lookup and other business transformations at this stage
WITH sc_ia as (

    SELECT
    	distinct schoolid,
		        addresstypedescriptor,
		        countrydescriptor,
		        addressline1,
		        addressline2,
		        addressline3,
		        addressline4,
		        begindate,
                enddate,
                latitude,
                longitude
    from
        {{ref('raw_school_international_addresses')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
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
		sc_ia.schoolid

)

select * from final
	