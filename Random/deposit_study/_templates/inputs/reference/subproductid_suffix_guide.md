# Subproduct ID suffix guide (2027v8)

The `subproductid` has the format `<account type code>_<XXX>_<YY>`. This page covers the six right-most digits (`XXX_YY`): what each value means and which source columns decide it, for each legacy bank. The suffix and the account type code together are the `subproductid` that `notebooks/01_account_mapping` joins to the mapping.

## PNFP

Source table: `finance.ftp_validation.pnfp_deposits` (config key `src_pnfp_deposits`).
The study builds the ID itself in `01_account_mapping`, Step 2:
`subproductid = service_charge_code || '_' || suffix`.

### Prefix

| Part | Source column | Notes |
|---|---|---|
| Account type code (left of the first `_`) | `service_charge_code` | Two characters, e.g. `1B`, `4A`, `3K` |

### Suffix values

| Suffix | Meaning (rate class) | Rule (first match wins, in this order) | Source columns used |
|---|---|---|---|
| `INT` (replaces `XXX_YY`) | Internal account | `internal = '1'`. Checked before the rate class, so the ID is `<code>_INT` | `internal` |
| `081_00` | DNC (do not change) | `override_rate_number = '787'` | `override_rate_number` |
| `000_00` | ZeroRate (shown as Standard) | `override_rate_number = '100'` and `prime_rate_num <> '100'` | `override_rate_number`, `prime_rate_num` |
| `000_01` | Indexed | `override_rate_description` in ('WSJ PRIME', 'Fed Funds Target', 'Fed Funds Effective', 'Pinnacle Index') or `prime_rate_num` in ('657', '659') | `override_rate_description`, `prime_rate_num` |
| `000_00` | Standard | `service_charge_code` in ('AG', 'EG') | `service_charge_code` |
| `070_00` | Standard Plus | `override_rate_number <> '0'` and `override_rate_number <> prime_rate_num` | `override_rate_number`, `prime_rate_num` |
| `000_00` | Standard | Anything else | none |

### Notes

* Product names: depending on product group and segment, Standard Plus (`070_00`) and DNC (`081_00`) can show as "Exception", e.g. MMA and IB DDA Consumer. Public funds, NIB DDA, Savings, Repo and IB DDA Hybrid products have no rate label.
* Related columns that are not part of the suffix: `pf` (public-funds flag, part of the mapping join key), plus `closed` and `internal` with `pnfp_include_internal` in `config/study_params.json` (scope).
* Older rule (kept in the notebook for reference only): `000_01` Indexed, `000_00` Standard, `082_00` Exception. It used the same rate columns. The original `inputs/reference/dim_nmd.csv` still has PNFP keys ending in `082_00` from that rule.

## SNV

Source table: `finance.pricing_analytics.lsnv_nmds` (config key `src_lsnv_nmds`).
The study does **not** build the SNV ID. `01_account_mapping` Step 3 reads `SubProductID` as-is. The suffix is assigned upstream of this study, and `lsnv_nmds` has no rate or override columns that show how.

### Prefix

| Part | Source column | Notes |
|---|---|---|
| Account type code (left of the first `_`) | `ACCT_TYP_CDE`, padded to 3 digits | Matched on all 699,473 rows at 2026-06-30 |

### Suffix values

The meanings below come from the `rate_class` values in `inputs/reference/dim_nmd.csv` and the product names in the 2027v8 mapping.

| Suffix | Meaning (rate class) | Source column |
|---|---|---|
| `000_00` | Standard | `SubProductID` (pre-built) |
| `000_01` | Indexed (also used by the two BDSP accounts) | `SubProductID` (pre-built) |
| `082_00` | Exception | `SubProductID` (pre-built) |
| `099_00` | Special case: only `450_099_00` (ICS COMMERCIAL DDA, mapped to ICS IB DDA) | `SubProductID` (pre-built) |

### Notes

* Related columns that are not part of the suffix: `AccountID` is checked against `snv_bdsp_accounts` to set `bdsp_flag` (part of the join key), and `SubProductID` is checked against `snv_excluded_subproducts` to set scope.
* SNV has no public-funds flag in the source. It comes from the mapping (`dim_pf_flag`).

## Side-by-side

| Meaning | PNFP suffix | SNV suffix |
|---|---|---|
| Standard / ZeroRate | `000_00` | `000_00` |
| Indexed | `000_01` | `000_01` |
| Standard Plus | `070_00` | n/a |
| DNC / Exception | `081_00` | `082_00` |
| Internal | `INT` (no digits) | n/a (internal is a product, e.g. NIB DDA Internal) |
| Special | n/a | `099_00` |

_Source: `notebooks/01_account_mapping` (Steps 2 and 3), `config/study_params.json`, `inputs/reference/dim_nmd.csv`, and the snapshot 2026-06-30 of `lsnv_nmds`._
