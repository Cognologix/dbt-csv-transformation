-- Apply lookup and other business transformations at this stage
WITH sc_add as (


    SELECT
    	distinct schoolid,
    	        addresstypedescriptor,
		        stateabbreviationdescriptor,
		        city,
		        postalcode,
		        streetnumbername,
		        localedescriptor,
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

    from
        {{ref('raw_school_addresses')}}

 ),

-- Final Json block to be created after all validations and transformations are done
final as (

   SELECT
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
			    'nameOfCounty', sc_add.nameofcounty

        ))AS addresses
        sc_add.begindate,
		sc_add.enddate
   FROM
	    sc_add
   GROUP BY
		sc_add.schoolid

)

select * from final