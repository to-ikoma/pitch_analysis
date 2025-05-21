select 
    dim_owner.owner_id, 
    dim_owner.email, 
    title,  
    date,
    year,
    month, 
    batter_name, 
    bat, 
    batter_team, 
    pitcher_name,
    throw,
    pitcher_team, 
    pitching_number, 
    pitch_x_position, 
    pitch_y_position, 
    fp.speed, 
    pitching_type_name, 
    result_english_name,
    pitching_result_name, 
    inning, 
    top_bottom,
    out_counts,
    runner_code, 
    runner_description 
from fact_pitching fp
join dim_batter on fp.batter_key = dim_batter.batter_key
join dim_pitcher on fp.pitcher_key = dim_pitcher.pitcher_key
join dim_game_day on fp.game_day_key = dim_game_day.game_day_key
join dim_owner on fp.owner_key = dim_owner.owner_key
join dim_inning on fp.inning_key = dim_inning.inning_key
join dim_out_counts on fp.out_count_key = dim_out_counts.out_count_key
join dim_runners on fp.runner_key = dim_runners.runner_key
join dim_pitching_types on fp.pitching_type_key = dim_pitching_types.pitching_type_key
join dim_pitching_results on fp.pitching_result_key = dim_pitching_results.pitching_result_key
join dim_pitching_numbers on fp.pitching_number_key = dim_pitching_numbers.pitching_number_key