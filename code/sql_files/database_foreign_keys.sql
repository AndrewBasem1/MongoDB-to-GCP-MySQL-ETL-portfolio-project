/*------------------------------------------------------------------------*/

-- Adding foreign keys relations to the tables

ALTER TABLE `competitions` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);

ALTER TABLE `seasons` ADD FOREIGN KEY (`competition_id`) REFERENCES `competitions` (`competition_id`);

ALTER TABLE `stadiums` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);

ALTER TABLE `managers_base_data` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);

ALTER TABLE `team_base_info` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);

ALTER TABLE `referees` ADD FOREIGN KEY (`country_id`) REFERENCES `countries` (`country_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`competition_id`) REFERENCES `competitions` (`competition_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`home_team_id`) REFERENCES `team_base_info` (`team_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`away_team_id`) REFERENCES `team_base_info` (`team_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`competition_stage_id`) REFERENCES `competition_stages` (`competition_stage_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`stadium_id`) REFERENCES `stadiums` (`stadium_id`);

ALTER TABLE `matches` ADD FOREIGN KEY (`referee_id`) REFERENCES `referees` (`referee_id`);

ALTER TABLE `team_managers_matches` ADD FOREIGN KEY (`team_id`) REFERENCES `team_base_info` (`team_id`);

ALTER TABLE `team_managers_matches` ADD FOREIGN KEY (`manager_id`) REFERENCES `managers_base_data` (`manager_id`);

ALTER TABLE `team_managers_matches` ADD FOREIGN KEY (`match_id`) REFERENCES `matches` (`match_id`);
