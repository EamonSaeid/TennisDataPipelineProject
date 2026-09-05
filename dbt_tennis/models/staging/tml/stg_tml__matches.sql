{{config(materialized='view')}}


WITH RAW_MATCHES as (
    SELECT * FROM {{source('tml_raw','matches')}}
),

renamed as (
SELECT 
  to_hex(md5(concat(tourney_id,"|",match_num))) match_sk,
  tourney_id,
  tourney_name,
  surface,
  safe_cast(draw_size as integer) draw_size,
  tourney_level,
  indoor,
  safe.parse_date('%Y%m%d',tourney_date) tourney_date,
  safe_cast(match_num as integer) match_num,
  winner_id,
  safe_cast(winner_seed as integer) winner_seed,
  winner_entry,
  winner_name,
  winner_hand,
  safe_cast(winner_ht as integer) winner_ht,
  winner_ioc,
  safe_cast(winner_age as float64) winner_age,
  safe_cast(winner_rank as integer) winner_rank,
  safe_cast(winner_rank_points as integer) winner_rank_points,
  loser_id,
  safe_cast(loser_seed as integer) loser_seed,
  loser_entry,
  loser_name,
  loser_hand,
  safe_cast(loser_ht as integer) loser_ht,
  loser_ioc,
  safe_cast(loser_age as float64) loser_age,
  safe_cast(loser_rank as integer) loser_rank,
  safe_cast(loser_rank_points as integer) loser_rank_points,
  score,
  safe_cast(best_of as integer) as best_of,
  `round` tourney_round,
  safe_cast(minutes as integer) as minutes,
  safe_cast(w_ace as integer) as w_ace,
  safe_cast(w_df as integer) as w_df,
  safe_cast(w_svpt as integer) as w_svpt,
  safe_cast(w_1stIn as integer) as w_1stIn,
  safe_cast(w_1stWon as integer) as w_1stWon,
  safe_cast(w_2ndWon as integer) as w_2ndWon,
  safe_cast(w_SvGms as integer) as w_SvGms,
  safe_cast(w_bpSaved as integer) as w_bpSaved,
  safe_cast(w_bpFaced as integer) as w_bpFaced,
  safe_cast(l_ace as integer) as l_ace,
  safe_cast(l_df as integer) as l_df,
  safe_cast(l_svpt as integer) as l_svpt,
  safe_cast(l_1stIn as integer) as l_1stIn,
  safe_cast(l_1stWon as integer) as l_1stWon,
  safe_cast(l_2ndWon as integer) as l_2ndWon,
  safe_cast(l_SvGms as integer) as l_SvGms,
  safe_cast(l_bpSaved as integer) as l_bpSaved,
  safe_cast(l_bpFaced as integer) as l_bpFaced,
  NOT(winner_id is not null and loser_id is not null and winner_id=loser_id) has_plausible_player_ids,
  _batch_id as batch_id,
  _ingested_at as ingested_at	
FROM
  RAW_MATCHES
)


SELECT 
    * 
FROM 
    RENAMED
QUALIFY
    ROW_NUMBER() OVER (PARTITION BY BATCH_ID,TOURNEY_ID,MATCH_NUM order by winner_id desc)=1