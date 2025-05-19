SELECT 
  games.owner AS owner,
  situations.pitcher AS pitcher,
  games.date AS date,
  sequences.breakingBall AS breakingBall,
  situations.title AS title,
  situations.inning AS inning,
  situations.topOrBottom AS topOrBottom,
  sequences.strikes AS strikes,
  sequences.balls AS balls,
  situations.outCounts AS outCounts,
  situations.runner AS runner,
  situations.firstAttackTeamScore AS firstAttackTeamScore,
  situations.secondAttackTeamScore AS secondAttackTeamScore,
  sequences.pitchResult AS pitchResult,
  sequences.sequence AS sequence,
  situations.batter AS batter,
  sequences.speed AS speed,
  sequences.pitchXPosition AS pitchXPosition,
  sequences.pitchYPosition AS pitchYPosition,
  sequences.id AS id
 FROM default.raw_privategameforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS games
 JOIN default.raw_privatesituationforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS situations
 ON games.id = situations.gameId
 JOIN default.raw_privatesequenceforgameformat_jqdkca4vcbf6nh7ojsquhjo4ai_staging AS sequences
 ON situations.id = sequences.situationId