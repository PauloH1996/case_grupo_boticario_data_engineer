CREATE OR REPLACE TABLE `b2w-bee-u-dados-e-insights-stg.Case_gb.case_2_table_4`

AS(

SELECT 
  LINHA
,  EXTRACT(YEAR FROM DATA_VENDA) AS ANO 
, EXTRACT(MONTH FROM DATA_VENDA) AS MES 
, SUM(QTD_VENDA) AS QTD_VENDAS

FROM `b2w-bee-u-dados-e-insights-stg.Case_gb.table_base`

GROUP BY LINHA, ANO, MES
ORDER BY LINHA, ANO, MES


)