-- 有多少被付过费的游戏
select count(*) as game_id_s_distinc_cnt from
    (select game_id_s
    from aiservice.m_c_pay_info
    where date>=20200601 and date<=20200831
    and source = "pay"
    and pay_fee_s>0
    group by game_id_s)a