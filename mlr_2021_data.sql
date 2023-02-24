select distinct 
    coalesce(payor_xwalk.payor_name_out,hpr.COMPANY_NAME) as payor_name
    , hpr.BUSINESS_STATE as state
    , mlr.* 
from analytics.core_reference.cms_mlr_summary_premium_claims as mlr
left join analytics.core_reference.cms_mlr_health_plan_reference hpr on mlr.HEALTH_PLAN_ID = hpr.HEALTH_PLAN_ID 
left join imports.reference.payor_name_crosswalk as payor_xwalk on hpr.company_name = payor_xwalk.payor_name_in
    and payor_xwalk.source='CMS MLR PUF'
where mlr.submission_year = 2021
    and hpr.submission_year = 2021
  and mlr.row_lookup_code in ('TOTAL_INCURRED_CLAIMS_PT1')
  and hpr.BUSINESS_STATE <> 'Grand Total'
order by 1,2
