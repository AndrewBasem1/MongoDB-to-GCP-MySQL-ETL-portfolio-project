-- deletion of previous tables created (this is for testing purposes)
SET foreign_key_checks = 0;
DROP TABLE IF EXISTS `competitions`;
DROP TABLE IF EXISTS `seasons`;
DROP TABLE IF EXISTS `countries`;
DROP TABLE IF EXISTS `stadiums`;
DROP TABLE IF EXISTS `managers_base_data`;
DROP TABLE IF EXISTS `team_base_info`;
DROP TABLE IF EXISTS `referees`;
DROP TABLE IF EXISTS `competition_stages`;
DROP TABLE IF EXISTS `matches`;
DROP TABLE IF EXISTS `team_managers_matches`;
SET foreign_key_checks = 1;

-- creation of the database schema
CREATE TABLE `competitions` (
  `competition_id` int PRIMARY KEY,
  `country_id` int,
  `country_name` varchar(255) NOT NULL COMMENT 'this is left beacuse there are international competitions',
  `competition_name` varchar(255) NOT NULL,
  `competiton_gender` varchar(255),
  `competiton_is_youth` tinyint,
  `competition_is_international` tinyint
);

CREATE TABLE `seasons` (
  `competition_id` int NOT NULL,
  `season_id` int NOT NULL COMMENT 'this is not ideal, but will allow it for time constraints',
  `season_start_year` int NOT NULL,
  `season_end_year` int NOT NULL
);

CREATE TABLE `countries` (
  `country_id` int PRIMARY KEY,
  `country_name` varchar(255) UNIQUE NOT NULL
);

CREATE TABLE `stadiums` (
  `stadium_id` int PRIMARY KEY,
  `stadium_name` varchar(255) NOT NULL,
  `country_id` int
);

CREATE TABLE `managers_base_data` (
  `manager_id` int PRIMARY KEY,
  `manager_name` varchar(255) NOT NULL,
  `manager_nickname` varchar(255),
  `manager_dob` date,
  `country_id` int
);

CREATE TABLE `team_base_info` (
  `team_id` int PRIMARY KEY,
  `team_name` varchar(255) NOT NULL,
  `team_gender` varchar(255),
  `country_id` int
);

CREATE TABLE `referees` (
  `referee_id` int PRIMARY KEY,
  `referee_name` varchar(255) NOT NULL,
  `country_id` int
);

CREATE TABLE `competition_stages` (
  `competition_stage_id` int PRIMARY KEY,
  `competition_stage_name` varchar(255) NOT NULL
);

CREATE TABLE `matches` (
  `match_id` int PRIMARY KEY,
  `competition_id` int,
  `season_id` int COMMENT 'not ideal but will allow it for time constraints',
  `home_team_id` int NOT NULL,
  `away_team_id` int NOT NULL,
  `home_score` int,
  `away_score` int,
  `match_week` int,
  `match_datetime` datetime,
  `competition_stage_id` int,
  `stadium_id` int,
  `referee_id` int
);

CREATE TABLE `team_managers_matches` (
  `team_id` int,
  `manager_id` int,
  `match_id` int
);

/*------------------------------------------------------------------------*/

-- Adding comments to the tables
ALTER TABLE `competitions` COMMENT = 'this table contains data about all the competitions, including international ones';

ALTER TABLE `seasons` COMMENT = 'this table contains date range off all the seasons in each competition, including international ones';

ALTER TABLE `countries` COMMENT = 'this is a lookup table for all countries in the database';

ALTER TABLE `stadiums` COMMENT = 'this table is a lookup table for all stadiums in the database';

ALTER TABLE `managers_base_data` COMMENT = 'this table contains basic data about all the managers';

ALTER TABLE `team_base_info` COMMENT = 'this table contains basic data about all the teams';

ALTER TABLE `referees` COMMENT = 'this table contains basic data about all the referees';

ALTER TABLE `competition_stages` COMMENT = 'this table is a lookup table for the different stages of a competition, e.g. group stage, round of 16, etc.';

ALTER TABLE `matches` COMMENT = 'this table contains data about all the matches in the database';

ALTER TABLE `team_managers_matches` COMMENT = 'this is a lookup table to show which managers managed which teams in which matches';


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
