Test:  Two step dedup (soundex and NYSIIS)
Created by:  Karen Fishwick


Input file structure:
REC_ID,BIRTH_DATE,PREF_FIRST_GIVEN_NAME,PREF_SECOND_GIVEN_NAME,PREF_FAMILY_NAME,FIRST_GIVEN_NAME,SECOND_GIVEN_NAME,FAMILY_NAME,STREET_LINE_1,STREET_LINE_2,COMMUNITY_OR_LOCATION,PROVINCE_OR_STATE,COUNTRY,CANADIAN_POSTAL_CODE,INGESTION_ID

Dedup project :
Step 1:  Block match exact on "BIRTH_DATE", "FAMILY_NAME", "CANADIAN_POSTAL_CODE", "COMMUNITY_OR_LOCATION" (group flag set to true, so that linked matches will not be evaluated for the 2nd step)
 - linking algorithm soundex on given first name

Step 2;  Block match exact on "BIRTH_DATE", "CANADIAN_POSTAL_CODE
 - linking algorithm head match first 4 chars on PREF_SECOND_GIVEN_NAME


Test cases:

1. Block match should fail for step 1, but succeed for step 2, pref second given matches algorithm, records should link

REC-842804-ORG,19790813,CHRISTIANE,DOLORES,SCIBELLI ,CHRISTIANE,DOLORES, ,PO BOX 866 , ,KASLO ,BC, ,V0G1M0,20          
REC-842805-ORG,19790813,CHRISTIANN,DOLOHHTTTTT,SCIBELLI ,CHRISTIANE,DOLORES, ,PO BOX 866 , ,NELSON ,BC, ,V0G1M0,25         


2.  Block match step 1 should fail, soundex would succeed, then block match step 2, head match fail (records should not link)

REC-690042-ORG,19890827,GRACIE,TAYLOR ,SANTIAGO ,GRACIE,TAYLOR ,SANTIAGO ,5117 GARDEN CITY,RICHMOND,BC,,V6X4H7,1
REC-690043-ORG,19890827,GRAZIE,TARA ,SANTIAGO ,GRACIE,TAYLOR ,SANTIAGO ,6203 5117 GARDEN CITY,RICHMOND,BC,,V6X4H7,5

