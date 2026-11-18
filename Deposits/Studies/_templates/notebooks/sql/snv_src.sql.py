# Databricks notebook source
-- snv_src: SNV accounts, all months, with the study's product group, client group, rate class, product and subproductid.
-- Used by 01_account_mapping (Step 3) and 10_truncation_cohorts, so both read the same rule. Edit the rule HERE, then re-run 01.
-- Table tokens resolve through sql(); __BDSP__ and __EXCL__ are replaced by 01_account_mapping before calling sql().
CREATE OR REPLACE TEMP VIEW snv_src AS
WITH s AS (
  SELECT
    nmds.EOPDate                                                 AS eopdate,
    CAST(nmds.AccountID AS STRING)                               AS accountid,
    CAST(nmds.ACCT_TYP_CDE AS STRING)                            AS acct_typ_code,
    nmds.SubProductID                                            AS subproductid,
    nmds.EOPBook                                                 AS eopbook,
    nmds.AvgBook                                                 AS avgbook,
    nmds.CurrCoupon                                              AS currcoupon,
    nmds.CurrCoupon * nmds.EOPBook                               AS sumprod_eoprate,
    nmds.OpenDate                                                AS open_date,
    CASE WHEN CAST(nmds.AccountID AS STRING) IN (__BDSP__) THEN 'Y' ELSE 'N' END AS bdsp_flag,
    CASE WHEN nmds.SubProductID IN (__EXCL__) THEN 'Y' ELSE 'N' END              AS excluded_flag,
    -- product group (ACCT_TYP_CDE -> product group; 450 splits on SubProductID suffix)
    CASE
      WHEN nmds.ACCT_TYP_CDE IN (194, 196)                                           THEN 'Brokered DDA'
      WHEN nmds.ACCT_TYP_CDE = 199                                                   THEN 'Brokered MMA'
      WHEN nmds.ACCT_TYP_CDE = 450 AND SUBSTRING(nmds.SubProductID, 5) = '000_00'    THEN 'ICS NIB DDA'
      WHEN nmds.ACCT_TYP_CDE IN (450, 451, 453, 456)                                 THEN 'ICS IB DDA'
      WHEN nmds.ACCT_TYP_CDE = 78                                                    THEN 'IB DDA IOLTA'
      WHEN nmds.ACCT_TYP_CDE IN (35, 36, 81, 83, 163, 164, 222)                     THEN 'IB DDA Hybrid'
      WHEN nmds.ACCT_TYP_CDE IN (18, 23, 79, 80, 99, 101, 220, 270, 301, 320, 342, 770, 774, 775, 776, 777, 961) THEN 'IB DDA'
      WHEN nmds.ACCT_TYP_CDE IN (30, 31, 32, 34, 38, 86, 94, 95, 96, 97, 98, 119, 185, 330) THEN 'MMA'
      WHEN nmds.ACCT_TYP_CDE IN (40, 42, 43, 44, 45, 48, 85, 340)                   THEN 'Savings'
      ELSE 'NIB DDA'
    END                                                          AS product_group,
    -- client group / segment
    CASE
      WHEN nmds.ACCT_TYP_CDE IN (424, 800)                                           THEN 'Internal'
      WHEN nmds.ACCT_TYP_CDE IN (250, 270, 300, 301, 320, 330, 340, 342)             THEN 'Public Funds'
      WHEN nmds.ACCT_TYP_CDE IN (32, 35, 36, 38, 39, 75, 76, 78, 79, 80, 81, 83, 85, 86,
                                  94, 95, 96, 97, 98, 99, 100, 101, 114, 117, 119, 150, 151,
                                  156, 157, 158, 163, 164, 167, 168, 175, 185, 194, 196, 199,
                                  200, 220, 222, 424, 450, 451, 453, 456, 800, 961)  THEN 'Commercial'
      ELSE 'Consumer'
    END                                                          AS client_group,
    -- rate class (from SubProductID suffix: _082_ = Exception, _000_01 = Indexed, _000_00 = Standard)
    CASE
      WHEN CAST(nmds.AccountID AS STRING) IN (__BDSP__)           THEN 'BDSP'
      WHEN SUBSTRING(nmds.SubProductID, 5, 3) = '082'            THEN 'Exception'
      WHEN SUBSTRING(nmds.SubProductID, 9, 2) = '01'             THEN 'Indexed'
      ELSE 'Standard'
    END                                                          AS rate_class
  FROM {{src_lsnv_nmds}} nmds
)
SELECT *,
  -- product name: group + segment + rate class, with exceptions for specialised groups
  TRIM(CASE
    WHEN product_group LIKE 'Brokered%'    THEN product_group
    WHEN product_group LIKE 'ICS%'         THEN product_group
    WHEN product_group = 'IB DDA IOLTA'    THEN 'IB DDA IOLTA'
    WHEN client_group  = 'Internal'        THEN product_group || ' Internal'
    WHEN bdsp_flag     = 'Y'               THEN product_group || ' ' || client_group || ' BDSP'
    WHEN client_group  = 'Public Funds'    THEN product_group || ' Public Funds'
    WHEN product_group = 'NIB DDA'         THEN 'NIB DDA ' || client_group
    WHEN product_group = 'Savings'         THEN 'Savings ' || client_group
    WHEN product_group = 'IB DDA Hybrid'   THEN 'IB DDA Hybrid ' || client_group
    ELSE product_group || ' ' || client_group || ' ' || rate_class
  END)                                                           AS product,
  excluded_flag <> 'Y'                                           AS in_scope
FROM s
