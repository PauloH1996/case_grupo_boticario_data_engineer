CREATE OR REPLACE TABLE `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_2`

AS(

SELECT 
  MARCA
, LINHA
, SUM(QTD_VENDA) AS QTD_VENDAS

FROM `b2w-bee-u-dados-e-insights-stg.Case_gb.table_base`

GROUP BY MARCA, LINHA



)