SELECT *,
    away_team.team_name as away_team_name,
    home_team.team_name as home_team_name
FROM matches
    JOIN teams as away_team
        ON away_team.team_id = matches.away_team_id
    JOIN teams as home_team
        ON home_team.team_id = matches.home_team_id