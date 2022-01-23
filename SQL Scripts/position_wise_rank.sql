SELECT * FROM weighted_goals_ranked_full WHERE Position = "D" LIMIT 10;
SELECT * FROM weighted_goals_ranked_full WHERE Position = "D" ORDER BY Goals DESC LIMIT 100;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "D" LIMIT 10;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "D" ORDER BY Assists DESC LIMIT 100;
SELECT * FROM weighted_points_ranked_full WHERE Position = "D" LIMIT 10;
SELECT * FROM weighted_points_ranked_full WHERE Position = "D" ORDER BY Points DESC LIMIT 100;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "D" LIMIT 10;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "D" ORDER BY PlusMinus DESC LIMIT 100;


SELECT * FROM weighted_goals_ranked_full WHERE Position = "C" LIMIT 10;
SELECT * FROM weighted_goals_ranked_full WHERE Position = "C" ORDER BY Goals DESC LIMIT 100;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "C" LIMIT 10;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "C" ORDER BY Assists DESC LIMIT 100;
SELECT * FROM weighted_points_ranked_full WHERE Position = "C" LIMIT 10;
SELECT * FROM weighted_points_ranked_full WHERE Position = "C" ORDER BY Points DESC LIMIT 100;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "C" LIMIT 10;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "C" ORDER BY PlusMinus DESC LIMIT 100;


SELECT * FROM weighted_goals_ranked_full WHERE Position = "L" OR Position = "R" LIMIT 10;
SELECT * FROM weighted_goals_ranked_full WHERE Position = "L" OR Position = "R" ORDER BY Goals DESC LIMIT 100;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "L" OR Position = "R" LIMIT 10;
SELECT * FROM weighted_assists_ranked_full WHERE Position = "L" OR Position = "R" ORDER BY Assists DESC LIMIT 100;
SELECT * FROM weighted_points_ranked_full WHERE Position = "L" OR Position = "R" LIMIT 10;
SELECT * FROM weighted_points_ranked_full WHERE Position = "L" OR Position = "R" ORDER BY Points DESC LIMIT 100;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "L" OR Position = "R" LIMIT 10;
SELECT * FROM weighted_plusminus_ranked_full WHERE Position = "L" OR Position = "R" ORDER BY plusminus DESC LIMIT 100;
