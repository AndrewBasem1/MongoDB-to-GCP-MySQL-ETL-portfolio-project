# Folders schema

```
data/
├── competitions.json
│
├── matches/
│    └── <competition_id>/
│        └── <season_id>.json
│  
├── lineups/
│    └── <match_id>.json
│  
└── events/
     └── <match_id>.json
```

# files explanation

1. `competitions.json`
    * This file contains basic data about the competitions as well as their seasons

2. `matches/<competition_id>/<season_id>.json`
    * This file contains basic data about all the matches in that season, it's important to note that some of this data is truncated because we're using the free verison of the data

3. `lineups/<match_id>.json`
    * This file contains the lineups for that match id
    * This will contain data about all players that actually played in the match
    * It will also show their position, and if they changed from one position to another during the match


4. `events/<match_id>.json`
    * This file contains the events for that match id
    * This is the bread and butter of this dataset, it contains each pass, each tackle, and every single event that happened in the match, and each event has related events
    * (PS: this may be my undoing because it's a LOT of data (JK), but let's see where this goes)