    select distinct
        year
        , region
        , cat.intake_category
        , hcpcs
        , case when modifier_1 = 'RR' or modifier_2 = 'RR' then true else false end as is_rental
        , unit_rate_usd 
    from claims_import.rates.medicare_rates_unpivoted as cms
    left join analytics.core_reference.hcpcs_category_mapping as cat on cms.hcpcs = cat.hcpcs_code
    where is_rural_rate=false
    qualify 1 = row_number() over (partition by region, hcpcs, is_rental order by year desc) 
    order by 2,3,4,5
