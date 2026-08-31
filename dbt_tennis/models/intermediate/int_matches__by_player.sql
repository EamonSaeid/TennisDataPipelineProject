WITH MATCHES AS (
SELECT * FROM `tennisdataengproject.tml_staging.stg_tml__matches`
),

winners as (
  SELECT
    to_hex(md5(concat(match_sk,"|", winner_id))) match_player_sk,
    match_sk,
    tourney_id,
    tourney_name,
    tourney_date,
    winner_id player_id,
    winner_name player_name,
    winner_rank player_rank,
    tourney_round,
    minutes,
    true as won
    
  FROM
    MATCHES
  WHERE
    winner_id is not null
),

losers as (
  SELECT
    to_hex(md5(concat(match_sk,"|", loser_id))) match_player_sk,
    match_sk,
    tourney_id,
    tourney_name,
    tourney_date,
    loser_id player_id,
    loser_name player_name,
    loser_rank player_rank,
    tourney_round,
    minutes,
    false as won,
    
  FROM
    MATCHES
  WHERE
    loser_id is not null
)


SELECT * FROM winners
UNION ALL
SELECT * FROM losers