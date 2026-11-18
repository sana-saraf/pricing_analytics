-- pnfp_src: PNFP accounts, all months, with the study's rate class, product, product group and subproductid (the 33-product rule).
-- Used by 01_account_mapping (Step 2) and 10_truncation_cohorts, so both read the same rule. Edit the rule HERE, then re-run 01.
-- Table tokens resolve through sql(); __INCLUDE_INTERNAL__ is replaced with Y or N (config/study_params.json pnfp_include_internal).
CREATE OR REPLACE TEMP VIEW pnfp_src AS
WITH s AS (
  SELECT
    nmds.load_date                                            AS eopdate,
    CAST(CONCAT(nmds.account_type, nmds.account_number) AS STRING) AS accountid,
    CAST(nmds.service_charge_code AS STRING)                  AS acct_typ_code,
    nmds.ledger                                               AS eopbook,
    nmds.avgbal                                               AS avgbook,
    CAST(nmds.rate AS DOUBLE)                                 AS currcoupon,
    nmds.rate * nmds.ledger                                   AS sumprod_eoprate,
    nmds.override_rate_number                                 AS override_rate,
    nmds.prime_rate_num                                       AS prime_rate,
    nmds.closed                                               AS closed_flag,
    nmds.internal                                             AS internal_flag,
    nmds.ics                                                  AS ics_flag,
    nmds.index                                                AS index_flag,
    nmds.date7_opened                                         AS open_date,
    CASE WHEN nmds.override_rate_number = '787' THEN 'Y' ELSE 'N' END AS dnc_flag,
    CASE WHEN nmds.pf = '1' THEN 'Y' ELSE 'N' END             AS pf_flag,
    map.ServiceChargeDescription                              AS acct_typ_desc,
    -- rate class (first match wins)
    CASE
      WHEN nmds.override_rate_number = '787' THEN 'DNC'
      WHEN nmds.override_rate_number = '100' AND nmds.prime_rate_num <> '100' THEN 'ZeroRate'
      WHEN nmds.override_rate_description IN ('WSJ PRIME', 'Fed Funds Target', 'Fed Funds Effective', 'Pinnacle Index')
           OR nmds.prime_rate_num IN ('657', '659') THEN 'Indexed'
      WHEN nmds.service_charge_code IN ('AG', 'EG') THEN 'Standard'
      WHEN nmds.override_rate_number <> '0' AND nmds.override_rate_number <> nmds.prime_rate_num THEN 'Standard Plus'
      ELSE 'Standard'
    END                                                       AS rate_class,
    -- product type, group and segment (service charge code -> product grouping table)
    CASE
      WHEN nmds.internal = '1' THEN 'Internal'
      WHEN nmds.service_charge_code IN ('4G', '3K') THEN 'ICS'
      WHEN map.gl_group_code = '400' THEN 'IOLTA'
      WHEN map.gl_group_code IN ('170', '300') THEN 'Brokered'
      WHEN map.gl_group_code = '600' THEN 'Escrow'
      WHEN nmds.service_charge_code IN ('1T', '1E', '1S', '1F') THEN 'HSA'
      ELSE 'Traditional'
    END                                                       AS product_type,
    CASE
      WHEN map.gl_group_code = '350' THEN 'Repo'
      WHEN map.Dep_Port_Sale_Perform = 'NIB' THEN 'NIB DDA'
      WHEN nmds.service_charge_code IN ('3Y', 'AG', 'EG', '2H') THEN 'IB DDA Hybrid'
      WHEN map.prod_type = 'Trans' THEN 'IB DDA'
      WHEN map.prod_type = 'MMDA' THEN 'MMA'
      WHEN map.prod_type = 'SAV' THEN 'Savings'
      WHEN nmds.service_charge_code IN ('7V', '7N', '2G', 'CP') THEN 'Savings'
      ELSE 'UNMAPPED'
    END                                                       AS base_group,
    CASE
      WHEN nmds.internal = '1' THEN 'Commercial'
      WHEN nmds.pf = '1' THEN 'Public Funds'
      WHEN map.`Group` = 'Business' OR map.gl_group_code = '350' THEN 'Commercial'
      ELSE 'Consumer'
    END                                                       AS product_segment
  FROM {{src_pnfp_deposits}} nmds
  LEFT JOIN {{src_product_grouping}} map ON nmds.service_charge_code = map.Code
)
SELECT *,
  TRIM(COALESCE(
    CASE
      WHEN product_type = 'Internal' THEN 'Internal Deposits'
      WHEN product_type = 'ICS' THEN CONCAT('ICS ', base_group, ' ', product_segment)
      WHEN product_type = 'IOLTA' THEN CONCAT(base_group, ' IOLTA')
      WHEN product_type = 'Brokered' AND base_group = 'IB DDA' THEN 'Brokered DDA'
      WHEN product_type = 'Brokered' AND base_group = 'MMA' THEN 'Brokered MMA'
    END,
    CONCAT(base_group, ' ', product_segment, ' ',
      CASE
        WHEN pf_flag = 'Y' THEN ''
        WHEN base_group IN ('NIB DDA', 'Savings', 'Repo', 'IB DDA Hybrid') THEN ''
        WHEN rate_class = 'ZeroRate' THEN 'Standard'
        WHEN rate_class = 'DNC' THEN 'Exception'
        WHEN base_group = 'MMA' AND rate_class = 'Standard Plus' THEN 'Exception'
        WHEN base_group = 'IB DDA' AND product_segment = 'Consumer' AND rate_class = 'Standard Plus' THEN 'Exception'
        ELSE rate_class
      END)))                                                  AS product,
  CASE product_type WHEN 'ICS' THEN CONCAT('ICS ', base_group) WHEN 'Brokered' THEN 'Brokered' WHEN 'Internal' THEN 'Internal'
       ELSE base_group END                                    AS product_group,
  CASE WHEN internal_flag = '1' THEN CONCAT(acct_typ_code, '_INT')
       ELSE CONCAT(acct_typ_code, '_', CASE rate_class WHEN 'Indexed' THEN '000_01' WHEN 'Standard Plus' THEN '070_00'
                                                      WHEN 'DNC' THEN '081_00' ELSE '000_00' END) END AS subproductid,
  closed_flag <> '1' AND (internal_flag <> '1' OR '__INCLUDE_INTERNAL__' = 'Y') AS in_scope
FROM s
