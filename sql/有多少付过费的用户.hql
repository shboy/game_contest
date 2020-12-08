-- 有多少付过费的用户
select count(*) as uid_distinct_cnt from
    (select uid
    from aiservice.m_c_pay_info
    where date>=20200601 and date<=20200831
    and source = "pay"
    and pay_fee_s>0
    group by uid)a