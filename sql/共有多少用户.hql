-- 共有多少用户

select count(*) from
    (select uid from
    aiservice.m_c_user_attrib
    where date=20200831
    and uid is not null
    group by uid)a