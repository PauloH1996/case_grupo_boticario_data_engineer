CREATE OR REPLACE TABLE `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_3`

AS(

SELECT 
  MARCA
,  EXTRACT(YEAR FROM DATA_VENDA) AS ANO 
, EXTRACT(MONTH FROM DATA_VENDA) AS MES 
, SUM(QTD_VENDA) AS QTD_VENDAS

FROM `b2w-bee-u-dados-e-insights-stg.Case_gb.table_base`

GROUP BY MARCA, ANO, MES
ORDER BY MARCA, ANO, MES


)