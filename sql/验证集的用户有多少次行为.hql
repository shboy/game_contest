-- 验证集的用户有多少次行为

select a.user_id as user_id, if (b.id is null, 0, b.cnt) as cnt from
    (select user_id
    from aiservice.m_c_recall_validation
    where date>=20200901 and date<=20200907
    group by user_id)a
left outer join
    (select id, sum(cnt) as cnt from
        (select uid as id, count(*) as cnt
        from aiservice.m_c_pay_info
        where date<=20200831
        group by uid
        union all
        select uid as id, count(*) as cnt
        from aiservice.m_c_user_do_action
        where date<=20200831
        group by uid
        )bb
    group by id)b
on a.user_id = b.id
