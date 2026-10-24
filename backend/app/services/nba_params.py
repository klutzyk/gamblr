# Default NBA API parameters for traditional player stats. Season is resolved
# at request time by NBAClient so this module never goes stale at league-year
# rollover.
TRADITIONAL_PARAMS = {
    "SeasonType": "Regular Season",
    "PerMode": "PerGame",
    "PlayerExperience": "",
    "PlayerPosition": "",
    "College": "",
    "Country": "",
    "DraftPick": "",
    "DraftYear": "",
    "GameScope": "",
    "GameSegment": "",
    "Height": "",
    "Month": "0",
    "OpponentTeamID": "0",
    "Outcome": "",
    "PORound": "0",
    "Period": "0",
    "ShotClockRange": "",
    "TeamID": "0",
    "VsConference": "",
    "VsDivision": "",
    "Weight": ""
}
