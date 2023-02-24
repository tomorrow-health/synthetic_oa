with hcpcs_ratio as (
    select
        hcpcs
        , is_rental
        -- , line_of_business
        , sum(units) as total_units
        , total_units / sum(total_units) over (partition by hcpcs) as pct_hcpcs
    from claims_analytics.core.unified_claim_lines as unified_claim_lines
    where payor in ('Geisinger','Superior') 
    group by 1,2--,3
)

, payor_volume as (
    select
        definitive_claims.claim_year
        , coalesce(payor_xwalk.payor_name_out,definitive_claims.payor_name) as payor_name
        , definitive_claims.hcp_state as state
        -- , definitive_claims.payor_bucket as line_of_business
        , listagg(distinct cat.intake_category_name,';') as categories
        , case 
            when categories='' then 'Missing'
            when contains(categories,'Catheters & Urology Supplies') then 'Catheters & Urology Supplies'
            when contains(categories,'CPAP & BiPAP') then 'CPAP & BiPAP'
            when contains(categories,'Diabetic') then 'Diabetic'
            when contains(categories,'Respiratory') then 'Respiratory'
            when contains(categories,'Wound Care') then 'Wound Care'
            when contains(categories,'Prosthetics') then 'Prosthetics'
            when contains(categories,'Transcutaneous Electrical Nerve Stimulators (TENS)') then 'Transcutaneous Electrical Nerve Stimulators (TENS)'
            else categories
        end as category
        , definitive_claims.hcpcs_code
        , coalesce(hcpcs_ratio.is_rental,false) as hcpcs_is_rental
        , hcpcs_ratio.pct_hcpcs
        , sum(definitive_claims.claims_billed) as service_count
        , coalesce(service_count * hcpcs_ratio.pct_hcpcs,service_count) as adj_service_count
    from claims_import.definitive_healthcare.definitive_healthcare_dme_report_2022_05_10 as definitive_claims
    left join imports.reference.payor_name_crosswalk as payor_xwalk on definitive_claims.payor_name = payor_xwalk.payor_name_in
        and payor_xwalk.source='Definitive Health'
    left join hcpcs_ratio on definitive_claims.hcpcs_code = hcpcs_ratio.hcpcs
        and hcpcs_is_rental = hcpcs_ratio.is_rental
        -- and definitive_claims.payor_bucket = hcpcs_ratio.line_of_business
    left join analytics.core.hcpcs_subcategories as cat on definitive_claims.hcpcs_code = cat.hcpcs_code
    group by 1,2,3,6,7,8
    order by 2,1,3,5,6,7
)

select
    claim_year
    , payor_name
    , state
    -- , line_of_business
    , category
    , sum(adj_service_count) as total_service_count
from payor_volume
where 1=1
    and payor_name in ('GHP'
                        ,'Moda Health'
                        ,'UCare'
                        ,'SelectHealth'
                        ,'Molina CA'
                        ,'Medica'
                        ,'CDPHP'
                        ,'WellSense'
                        ,'L.A. Care'
                        ,'BCBSKS'
                        ,'EmblemHealth'
                        ,'Kaiser Permanente'
                        ,'Blue Shield of California'
                        ,'BCBSMA'
                        ,'BCBSNC'
                        ,'Empire BCBS'
                        ,'IBC'
                        ,'MetroPlus'
                        ,'Optima'
                        ,'BCBSIL (HCSC)'
                        ,'BCBSMT (HCSC)'
                        ,'BCBSNM (HCSC)'
                        ,'Higmark'
                        ,'Capital Blue Cross'
                        ,'AmeriHealth Caritas'
                        ,'UPMC Health Plans'
                        ,'Priority Health'
                        ,'CareSource'
                        ,'Community First'
                        ,'BCBSTX (HCSC)'
                        ,'Blue KC'
                        ,'BCBSLA'
                        ,'Premera'
                        ,'BCBS Michigan'
                        ,'BCBSOK (HCSC)'
                        ,'Healthfirst'
                        ,'BCBS Minnesota'
                        ,'Baylor Scott & White Health Plan (SWHP)'
                        ,'HealthPartners'
                        ,'Presbyterian Health Plan'
                        ,'AultCare Health Plans'
                        ,'Independent Health'
                        ,'Health Alliance Plan'
                        ,'Security Health Plan'
                        ,'Sanford Health Plan'
                        ,'Point32'
                        ,'Allina Health - Aenta'
                        ,'BCBS AZ'
                        ,'BCBS SC'
                      )
group by 1,2,3,4--,5
order by 2,1,3,4,5 desc
