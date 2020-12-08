-- 有行为的用户数

select count(*) from
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