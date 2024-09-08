with cte as
    (select r.id
         , r.contract_issue_date
         , (to_timestamp(r.created_at) + '7h')::date as dt
         , case when contract_issue_date is not null then 1 else 0 end approved
         , amount_principal_accrued / 24850 as amount
         , case when (now()+'7h')::date - (first_repayment_date+'7h')::date <= 1 or first_repayment_date is null then null
             when coalesce(first_payment_date+'7h', now()+'7h')::date - (first_repayment_date+'7h')::date > 1 then 1
             else 0 end npl1
         , case when (now()+'7h')::date - (first_repayment_date+'7h')::date <= 5 or first_repayment_date is null then null
            when coalesce(first_payment_date+'7h', now()+'7h')::date - (first_repayment_date+'7h')::date > 5 then 1
            else 0 end npl5
         , case when (now()+'7h')::date - (first_repayment_date+'7h')::date <= 10 or first_repayment_date is null then null
             when coalesce(first_payment_date+'7h', now()+'7h')::date - (first_repayment_date+'7h')::date > 10 then 1
             else 0 end npl10
        --repaiments
        , case
                 when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 1 then
                     r.amount_principal_accrued end as amount1
        , case
                 when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 5 then
                     r.amount_principal_accrued end as amount5
        , case
                 when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 10 then
                     r.amount_principal_accrued end as amount10
        , case
                  when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 1 then
                          coalesce((select sum(fo.amount)
                                    from financial_operation fo
                                    where r.id = fo.request_id
                                      and fo.action_type = 2
                                      and (fo.date + '7h')::date - (r.first_repayment_date + '7h')::date <= 1), 0)  end as paid1
        , case
                  when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 5 then
                          coalesce((select sum(fo.amount)
                                    from financial_operation fo
                                    where r.id = fo.request_id
                                      and fo.action_type = 2
                                      and (fo.date + '7h')::date - (r.first_repayment_date + '7h')::date <= 5), 0) end as paid5
        , case
                  when (now() + '7h')::date - (r.first_repayment_date + '7h')::date > 10 then
                          coalesce((select sum(fo.amount)
                                    from financial_operation fo
                                    where r.id = fo.request_id
                                      and fo.action_type = 2
                                      and (fo.date + '7h')::date - (r.first_repayment_date + '7h')::date <= 10), 0) end as paid10
   FROM request r
    where 1=1
    and (to_timestamp(r.created_at) + '7h')::date > ((now() +'7h')::date - 56)
    and  (select count(r1.contract_close_date)
          from request r1
          where r1.contract_close_date < to_timestamp(r.created_at)
            and r1.client_id = r.client_id) = 0
    )
select
    --INITIAL NPLs
      ((now() +'7h')::date - 16) as dt_1
    , ((now() +'7h')::date - 20) as dt_5
    , ((now() +'7h')::date - 25) as dt_10
	, ((now() +'7h')::date - 1) as dt_last
	, ((now() +'7h')::date) as dt_now
    , (select round(avg(npl1::numeric) , 3) from cte where dt = ((now() +'7h')::date - 16)) as npl1_mean
    , (select round(avg(npl5::numeric) , 3) from cte where dt = ((now() +'7h')::date - 20)) as npl5_mean
    , (select round(avg(npl10::numeric), 3) from cte where dt = ((now() +'7h')::date - 25)) as npl10_mean
    , (select 1 - round(sum(paid1)/sum(amount1)::numeric  , 3) from cte where dt = ((now() +'7h')::date - 16)) as npl1_$_mean
    , (select 1 - round(sum(paid5)/sum(amount5)::numeric  , 3) from cte where dt = ((now() +'7h')::date - 20)) as npl5_$_mean
    , (select 1 - round(sum(paid10)/sum(amount10)::numeric, 3) from cte where dt = ((now() +'7h')::date - 25)) as npl10_$_mean
     --
    , (select round(avg(npl1::numeric), 3)
       from cte
       where dt <= ((now() +'7h')::date - 17) ) as npl1_mean_last_month
    , (select round(avg(npl5::numeric), 3)
       from cte
       where dt <= ((now() +'7h')::date - 21) ) as npl5_mean_last_month
    , (select round(avg(npl10::numeric), 3)
       from cte
       where dt <= ((now() +'7h')::date - 26) ) as npl10_mean_last_month
    , (select 1 - round(sum(paid1)/sum(amount1)::numeric, 3)
       from cte
       where dt <= ((now() +'7h')::date - 17) ) as npl1_$_mean_last_month
    , (select 1 - round(sum(paid5)/sum(amount5)::numeric, 3)
       from cte
       where dt <= ((now() +'7h')::date - 21) ) as npl5_$_mean_last_month
    , (select 1 - round(sum(paid10)/sum(amount10)::numeric, 3)
       from cte
       where dt <= ((now() +'7h')::date - 26) ) as npl10_$_mean_last_month
    -- INITIAL AR LAST DAY
    , (select round(avg(approved::numeric), 3)
       from cte
       where dt = ((now() +'7h')::date-1)) as AR_last_day
    , (select round(avg(amount::numeric), 1)
       from cte
       where dt = ((now() +'7h')::date-1)) as avg_check_last_day
    , (select count(*)
       from cte
       where dt = ((now() +'7h')::date-1)) as requests_last_day
    , (select count(contract_issue_date)
       from cte
       where dt = ((now() +'7h')::date-1)) as agreements_last_day
    -- INITIAL AR for TODAY
    , (select round(avg(approved::numeric), 3)
       from cte
       where dt = ((now() +'7h')::date)) as AR_today
    , (select round(avg(amount::numeric), 1)
       from cte
       where dt = ((now() +'7h')::date)) as avg_check_today
    , (select count(*)
       from cte
       where dt = ((now() +'7h')::date)) as requests_today
    , (select count(contract_issue_date)
       from cte
       where dt = ((now() +'7h')::date)) as agreements_today
