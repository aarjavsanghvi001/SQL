with date_list as (
select distinct
date_trunc('day',at) as date_cd

from stream_account_status
),

statuses as (
select

account_number,
at,
case when lag(at,-1) over (partition by account_number order by at) is null then '2199-12-31' else
lag(at,-1) over (partition by account_number order by at) end as testing,
status_from,
status_to

from stream_account_status
left join documents
on stream_account_status.account_id = documents.account_id

where correspondent in (select correspondent from correspondents where use_cip_api = 'false') or correspondent in ('LPCA','')
and documents.doc_type in ('address_verification','address_vertification','date_of_birth_verification','tax_id_verification','identity_verification')
)
,

action_req as
(
select
date_cd,
count(distinct account_number) as tot_action_req_accnts

from date_list A

join statuses B
on A.date_cd <= date_trunc('day',B.testing)
and A.date_cd >= date_trunc('day',B.at)
and B.status_to = 'ACTION_REQUIRED'

group by 1
order by 1
),

approved as
(
select
count(distinct(account_number)) as tot_approved_rejected_accnts,
date_cd

from date_list A
join statuses B
on A.date_cd = date_trunc('day',B.at)
and B.status_to in ('APPROVED','REJECTED')
and B.status_from in ('ACTION_REQUIRED','APPROVAL_PENDING')

group by 2
)

select action_req.date_cd, tot_action_req_accnts, tot_approved_rejected_accnts, (tot_approved_rejected_accnts*1.00/tot_action_req_accnts) as rate_of_approval
from action_req
join approved
on action_req.date_cd = approved.date_cd

where action_req.date_cd is not null and action_req.date_cd > '03/01/2021'
order by 1 