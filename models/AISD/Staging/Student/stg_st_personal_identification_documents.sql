-- Apply lookup and other business transformations at this stage
WITH spid as (
    select * from {{ref('raw_st_personal_identification_documents')}}

),

-- Final Json block to be created after all validations and transformations are done
final as (
   SELECT
		spid.studentuniqueid,
        jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', spid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', spid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', spid.issuercountrydescriptor,
				'documentExpirationDate', spid.documentexpirationdate,
				'documentTitle', spid.documenttitle,
				'issuerDocumentIdentificationCode', spid.issuerdocumentidentificationcode,
				'issuerName', spid.issuername
			)) AS personalIdentificationDocuments
	FROM
	    spid
	GROUP BY
		spid.studentuniqueid

)

select * from final