-- Add lookup and other business validations at this stage
WITH sid as (

    select * from {{ref('raw_st_identification_documents')}}

),

-- Final Json block to be created after all validations and transformations are done
final as (
    SELECT
		sid.studentuniqueid,
		jsonb_agg(json_build_object(
				'identificationDocumentUseDescriptor', sid.identificationdocumentusedescriptor,
				'personalInformationVerificationDescriptor', sid.personalinformationverificationdescriptor,
				'issuerCountryDescriptor', sid.issuercountrydescriptor,
				'documentExpirationDate', sid.documentexpirationdate,
				'documentTitle', sid.documenttitle,
				'issuerDocumentIdentificationCode', sid.issuerdocumentidentificationcode,
				'issuerName', sid.issuername
			)) AS identificationDocuments
	FROM
	  sid
	GROUP BY
		sid.studentuniqueid


)


select * from final