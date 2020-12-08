-- 构造验证集
INSERT OVERWRITE
TABLE aiservice.m_c_recall_validation
PARTITION
(date=${date-3})
----------------------------------------------------
select uid as user_id, concat_ws('|', collect_list(game_id_s)) as real_satisfied_game_ids from
    (select a.uid as uid, a.game_id_s as game_id_s from 
        (select uid, game_id_s
        from aiservice.m_c_user_do_action
        where date=${date-3}
        and action='EVENT_ACTIVE')a 
    join
        (select uid, game_id_s
        from aiservice.m_c_pay_info
        where date>=${date-3}
        and date<=${date}
        and source='pay' 
        and pay_fee_s>0)b
    on a.uid = b.uid
    and a.game_id_s = b.game_id_s
    group by a.uid, a.game_id_s
    )c
    group by uid