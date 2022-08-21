with cte1 as
(
select *, row_number() over (order by month) as rn
from
(
select generate_series(date_trunc('month', current_date - interval '12 month'), date_trunc('month', current_date - interval '1 month'), '1 month'::interval) as month
) sub
)
,

funded as
(
select account_number, first_fund_date, id, region, rn as first_fund_rn
from
(
select min(settle_date) first_fund_date, account_number, region, id
from
(
select date_trunc('month',nta.settle_date) as settle_date, accounts.id, nta.account_number, 
case when country_of_tax_residence in ('USA') then 'USA'
     when country_of_tax_residence in ('MDG','TZA','GGY','NLD','AUT','HUN','RUS','IRL','ISR','GBR','EST','BEL','MNE','MAR','ZAF','MKD','TGO','BEN','NER','SMR','CZE','ERI','CHE','SDN','LBN','SVN',
        'LTU','BIH','MLT','ESP','TUN','GAH','BLR','ROU','OMN','GRC','HVN','ALA','ALB','BHR','NGA','MUS','LIE','BFA','PRT','ZWE','NOR','ZMB','MDA','DNK','SYC','IRQ','AND','MWI','SSD','LSO','UKR','DZA'
        ,'JOR','KWT','POL','EGY','ENG','TUR','CYP','SWE','DEU','MOZ','SLV','FRA','UGA','RWA','YEM','ITA','SWZ','CMR','NAM','GIN','JEY','MCO','SAU','KEN','SVK','FIN','GUY','BGR','LUX','SRB','LVA','ETH') then 'EMEA'
     when country_of_tax_residence in ('PRY','VEN','BMU','PAN','COL','CAN','AIA','VCT','GRD','GTM','CUB','URY','DMA','LCA',
         'ARG','BRA','MEX','PRI','HND','LCA','TTO','CHL','PER','ECU','TCA','JAM','BRB','BLZ','HTI','VIR','DOM','ARB','CRI','BHS') then 'AMER'
     when country_of_tax_residence in ('HKG','MMR','IRN','KAZ','BRN','KOZ','MNG','JPN','NPL','IDN','PAK','KOR','AFG','IND','VNM','KHM','ARM','PHL','SGP',
         'CHN','THA','TWN','MYS','TJK','MDV','AZE','GEO','BGD','LKA','QAT','UZB') then 'APAC'
     when country_of_tax_residence in ('UMI','FSM','VGB','VBI','TUV','NIA','NZL','AUS','WLF','AMS') then 'OCEANA' 
end as region

-- sum(nta.net_amount) as net_amount

from public.non_trade_activity nta
left join accounts
on accounts.account_number = nta.account_number
join account_owners 
on account_owners.account_id = accounts.id
join owner_details 
on account_owners.owner_id = owner_details.owner_id::uuid

where nta.settle_date >= date_trunc('month', current_date - interval '12 month')
and (nta.entry_type in ('CSD') and nta.status = 'executed')
and correspondent in ('LPCA','')

-- group by 1,2,3
) sub 
-- where net_amount > 5
group by 2,3,4
) subb
left join cte1
on cte1.month = subb.first_fund_date
)
,

withdrawals as
(
select *
from
(
select *, row_number() over (partition by account_number order by settle_date desc)
FROM
(
SELECT 
    account_number, 
    max(settle_date) settle_date, entry_type
FROM non_trade_activity
WHERE (entry_type in ('CSW','CSD') and status = 'executed')
and settle_date >= date_trunc('month', current_date - interval '12 month')
group by 1,3--, settle_date
-- order by account_number
) sub 
-- where entry_type = 'CSW'
) subb 
where row_number = 1 and entry_type = 'CSW'
)
,

churn as
(
select *, rn as churn_rn
from
(
select account_number, settle_date, equity 
from
(
select withdrawals.account_number, date_trunc('month',settle_date) as settle_date, daily_balances.equity
from withdrawals
join accounts
on withdrawals.account_number = accounts.account_number
join daily_balances
on accounts.id = daily_balances.account_id and daily_balances.asof = settle_date
where correspondent in ('LPCA','')
) sub
-- group by 1,2
) subb 
left join cte1
on cte1.month = settle_date
where equity < 6
)
,

main as
(
select funded.account_number, region, first_fund_date, settle_date, first_fund_rn, churn_rn, churn_rn - first_fund_rn as month_number
from funded
left join churn 
on funded.account_number = churn.account_number
where first_fund_rn is not null 
and region is not null
),

monthly_churn as
(
select first_fund_date as "First Deposit", region, count(account_number) as "# Funded Accounts",
greatest((sum(case when month_number = 0 then 1 end) *1.00 / count(account_number)),0) as month_0,
greatest((sum(case when month_number = 1 then 1 end) *1.00 / count(account_number)),0) as month_1,
greatest((sum(case when month_number = 2 then 1 end) *1.00 / count(account_number)),0) as month_2,
greatest((sum(case when month_number = 3 then 1 end) *1.00 / count(account_number)),0) as month_3,
greatest((sum(case when month_number = 4 then 1 end) *1.00 / count(account_number)),0) as month_4,
greatest((sum(case when month_number = 5 then 1 end) *1.00 / count(account_number)),0) as month_5,
greatest((sum(case when month_number = 6 then 1 end) *1.00 / count(account_number)),0) as month_6,
greatest((sum(case when month_number = 7 then 1 end) *1.00 / count(account_number)),0) as month_7,
greatest((sum(case when month_number = 8 then 1 end) *1.00 / count(account_number)),0) as month_8,
greatest((sum(case when month_number = 9 then 1 end) *1.00 / count(account_number)),0) as month_9,
greatest((sum(case when month_number = 10 then 1 end) *1.00 /count(account_number)),0) as month_10,
greatest((sum(case when month_number = 11 then 1 end) *1.00 /count(account_number)),0) as month_11
from main 
group by first_fund_date, region
),

unadjusted as 
(
select "First Deposit", region, "# Funded Accounts",
sum(month_0) as month_0,
sum(month_0 + month_1) as month_1,
sum(month_0 + month_1 + month_2) as month_2,
sum(month_0 + month_1 + month_2 + month_3) as month_3,
sum(month_0 + month_1 + month_2 + month_3 + month_4) as month_4,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5) as month_5,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6) as month_6,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6 + month_7) as month_7,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6 + month_7 + month_8) as month_8,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6 + month_7 + month_8 + month_9) as month_9,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6 + month_7 + month_8 + month_9 + month_10) as month_10,
sum(month_0 + month_1 + month_2 + month_3 + month_4 + month_5 + month_6 + month_7 + month_8 + month_9 + month_10 + month_11) as month_11
from monthly_churn
group by 1,2,3
)
,

adjustment as (
select *,
case when month_1 = month_0 then 0 end case1,
case when month_2 = month_1 then 0 end case2,
case when month_3 = month_2 then 0 end case3,
case when month_4 = month_3 then 0 end case4,
case when month_5 = month_4 then 0 end case5,
case when month_6 = month_5 then 0 end case6,
case when month_7 = month_6 then 0 end case7,
case when month_8 = month_7 then 0 end case8,
case when month_9 = month_8 then 0 end case9,
case when month_10 = month_9 then 0 end case10,
case when month_11 = month_10 then 0 end case11
from unadjusted
)


select 
    "First Deposit", region, "# Funded Accounts",
    case when month_0 = 0 then null else month_0 end as month_0,
    case when case1 is null then month_1 else null end month_1,
    case when case2 is null then month_2 else null end month_2,
    case when case3 is null then month_3 else null end month_3,
    case when case4 is null then month_4 else null end month_4,
    case when case5 is null then month_5 else null end month_5,
    case when case6 is null then month_6 else null end month_6,
    case when case7 is null then month_7 else null end month_7,
    case when case8 is null then month_8 else null end month_8,
    case when case9 is null then month_9 else null end month_9,
    case when case10 is null then month_10 else null end month_10,
    case when case11 is null then month_11 else null end month_11
from
    adjustment
order by region desc, "First Deposit"
