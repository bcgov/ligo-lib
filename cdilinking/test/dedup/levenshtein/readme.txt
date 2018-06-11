Test:  Levenshtein deduplication
Created by:  Karen Fishwick


Input file structure:
BIRTH_DATE,PREF_FIRST_GIVEN_NAME,PREF_FAMILY_NAME,COMMUNITY_OR_LOCATION,CANADIAN_POSTAL_CODE,INGESTION_ID


Dedup project :
Block match on birth date, pref family name, community, and postal code, levenshtein max edits 2 on pref first given name

Test cases:

1.  Levenshtein should match (one edit difference)
19890827,GRACIE0,SANTIAGO,RICHMOND,V6X4H7, 1
19890827,GRACIE,SANTIAGO,RICHMOND,V6X4H7,2

2.  Levenshtein should match the first two, but not the third (one char diff first two, 3 for the 3rd)
19790813,CHRISTIANE0,SCIBELLI,KASLO,V0G1M0,3
19790813,CHRISTIANE1,SCIBELLI,KASLO,V0G1M0,4
19790813,CHRISTIANE123,SCIBELLI,KASLO,V0G1M0,5


3.  Levenshtein should match all three
19210721,0NEDRA,SZOCINSKI,VANDERHOOF,V0J3A0,6
19210721,NEDRA1,SZOCINSKI,VANDERHOOF,V0J3A0,7
19210721,NEDRA,SZOCINSKI,VANDERHOOF,V0J3A0,8

4.  Blocking variable should not match these two
19790813,LAURENA,PAINE,PORT HARDY,V0N2P0,9
19790814,LAURENA,PAINE,PORT HARDY,V0N2P0,10

5.  Null first name, should not match
19451002,,HUGGINS,SURREY,V3X1R5,11
19451002,BREE1,HUGGINS,SURREY,V3X1R5,12

