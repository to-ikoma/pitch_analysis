SELECT 
  games.owner AS owner,
  situations.pitcher AS pitcher,
  situations.throw AS throw,
  games.firstAttackTeam AS first_attack_team,
  games.secondAttackTeam AS second_attack_team,
  CASE
      WHEN situations.topOrBottom = 'top' THEN games.secondAttackTeam
      WHEN situations.topOrBottom = 'bottom' THEN games.firstAttackTeam
      ELSE ''
  END AS team_for_pitcher,
  games.date AS date,
  sequences.breakingBall AS breaking_ball,
  situations.title AS title,
  CAST(situations.inning AS INT) AS inning,
  situations.topOrBottom AS top_or_bottom,
  CAST(sequences.strikes AS INT) AS strikes,
  CAST(sequences.balls AS INT) AS balls,
  CAST(situations.outCounts AS INT) AS out_counts,
  CAST(situations.runner AS INT) AS runner,
  CAST(situations.firstAttackTeamScore AS INT) AS first_attack_team_score,
  CAST(situations.secondAttackTeamScore AS INT) AS second_attack_team_score,
  sequences.pitchResult AS pitch_result,
  CAST(sequences.sequence AS INT) AS sequence,
  situations.batter AS batter,
  situations.bat AS bat,
  CASE
      WHEN situations.topOrBottom = 'bottom' THEN games.secondAttackTeam
      WHEN situations.topOrBottom = 'top' THEN games.firstAttackTeam
      ELSE ''
  END AS team_for_batter,
  CAST(sequences.speed AS INT) AS speed,
  CAST(sequences.pitchXPosition AS INT) AS pitch_x_position,
  CAST(sequences.pitchYPosition AS INT) AS pitch_y_position,
  sequences.id AS id,
  sequences.updatedAt AS updated_at
 FROM default.raw_privategameforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS games
 JOIN default.raw_privatesituationforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS situations
 ON games.id = situations.gameId
 JOIN default.raw_privatesequenceforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS sequences
 ON situations.id = sequences.situationId