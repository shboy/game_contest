-- 验证集的游戏能否被付过费的游戏覆盖

select t1.game_id as game_id, t2.id as game_id_tmp from
        (select game_id from 
            (select user_id, real_satisfied_game_ids 
            from aiservice.m_c_recall_validation 
            where date>=20200901 and date<=20200907)a 
            LATERAL view explode(split(a.real_satisfied_game_ids, "\\|")) temp as game_id
        group by game_id)t1
    left outer join
        (select id
        from aiservice.m_c_game_paid_info
        where date=20200831)t2
    on t1.game_id=t2.id
    where t2.id is null


select category, count(*) from
    (select a.game_id, if(b.id is null, "not paid", "paid") as category from
        (select game_id from 
            (select user_id, real_satisfied_game_ids 
            from aiservice.m_c_recall_validation 
            where date>=20200901 and date<=20200907)a 
            LATERAL view explode(split(a.real_satisfied_game_ids, "\\|")) temp as game_id
        group by game_id)a
    left outer join
        (select id 
        from aiservice.m_c_game_paid_info
        where date=20200831)b
    on a.game_id = b.id 
    )c
    group by category

