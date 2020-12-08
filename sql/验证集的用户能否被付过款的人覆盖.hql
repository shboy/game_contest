-- 验证集的用户能否被付过款的人覆盖

select t1.user_id as user_id, t2.id as user_id_tmp from
        (select user_id
        from aiservice.m_c_recall_validation 
        where date>=20200901 and date<=20200907)t1
    left outer join
        (select id
        from aiservice.m_c_userid_paid_info
        where date=20200831)t2
    on t1.user_id=t2.id
    where t2.id is null



select category, count(*) from
    (select a.user_id, if(b.id is null, "not paid", "paid") as category from
        (select user_id
        from aiservice.m_c_recall_validation
        where date>=20200901 and date<=20200907
        group by user_id)a
    left outer join
        (select id 
        from aiservice.m_c_userid_paid_info
        where date=20200831)b
        on a.user_id = b.id 
    )c
    group by category