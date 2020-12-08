-- 验证集的用户能否被有过行为的人覆盖

select category, count(*) from
    (select a.user_id, if(b.id is null, "not paid", "paid") as category from
        (select user_id
        from aiservice.m_c_recall_validation
        where date>=20200901 and date<=20200907
        group by user_id)a
    left outer join
        (select id from
            (select uid as id 
            from aiservice.m_c_pay_info
            where date<=20200831
            group by uid
            union all
            select uid as id 
            from aiservice.m_c_user_do_action
            where date<=20200831
            group by uid
            )bb
        group by id)b
        on a.user_id = b.id 
    )c
    group by category