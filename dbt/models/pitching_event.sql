SELECT 
  games.owner AS owner,
  situations.pitcher AS pitcher,
  situations.throw AS throw,
  games.firstAttackTeam AS first_attack_team,
  games.secondAttackTeam AS second_attack_team,
  games.date AS date,
  sequences.breakingBall AS breaking_ball,
  situations.title AS title,
  situations.inning AS inning,
  situations.topOrBottom AS top_or_bottom,
  sequences.strikes AS strikes,
  sequences.balls AS balls,
  situations.outCounts AS out_counts,
  situations.runner AS runner,
  situations.firstAttackTeamScore AS first_attack_team_score,
  situations.secondAttackTeamScore AS second_attack_team_score,
  sequences.pitchResult AS pitch_result,
  sequences.sequence AS sequence,
  situations.batter AS batter,
  sequences.speed AS speed,
  sequences.pitchXPosition AS pitch_x_position,
  sequences.pitchYPosition AS pitch_y_position,
  sequences.id AS id,
  sequences.updatedAt AS updated_at
 FROM default.raw_privategameforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS games
 JOIN default.raw_privatesituationforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS situations
 ON games.id = situations.gameId
 JOIN default.raw_privatesequenceforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS sequences
 ON situations.id = sequences.situationId