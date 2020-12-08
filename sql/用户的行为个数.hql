-- 用户的行为个数

select click_cnt, count(*) as stat_cnt from 
    (select id, sum(cnt) as click_cnt from
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
    group by id)bbb
group by click_cnt