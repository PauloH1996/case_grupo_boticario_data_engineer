CREATE OR REPLACE TABLE `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_7`

AS(

SELECT *

FROM `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_6`

WHERE name LIKE '%Boticário%'

)