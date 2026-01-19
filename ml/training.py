import os
from dotenv import load_dotenv
import pandas as pd
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
import joblib
from sqlalchemy import create_engine

# load env variables
load_dotenv()
DATABASE_URL = os.getenv("SYNC_DATABASE_URL")

# SQLAlchemy engine
engine = create_engine(DATABASE_URL)

# get player stats
query = """
SELECT pg.player_id, pg.game_id, pg.game_date, pg.matchup, p.team_abbreviation,
       pg.minutes, pg.points, pg.assists, pg.rebounds, pg.steals, pg.blocks, pg.turnovers
FROM player_game_stats pg
JOIN players p ON pg.player_id = p.id
"""
df_raw = pd.read_sql(query, engine)

# copy for feature engineering
df_features = df_raw.copy()

# feature enginering
# adding indicator for home/away based on @/vs symbols
df_features["is_home"] = df_features["matchup"].apply(
    lambda x: 1 if "@" not in x else 0
)

# sort by player and game date
df_features = df_features.sort_values(["player_id", "game_date"])

# calc rolling averages for base stats in last 5 games.
# getitng rolling averages to get a sense of current form of the player
df_features["avg_points_last5"] = df_features.groupby("player_id")["points"].transform(
    lambda x: x.rolling(5, min_periods=1).mean().shift(1)
)
df_features["avg_assists_last5"] = df_features.groupby("player_id")[
    "assists"
].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))
df_features["avg_rebounds_last5"] = df_features.groupby("player_id")[
    "rebounds"
].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))

# teammate influence (average assists of other teammates last 5 games)
teammate_avg_assists = []
for idx, row in df_features.iterrows():
    team = row["team_abbreviation"]
    game_date = row["game_date"]
    player_id = row["player_id"]
    # get teammates last 5 games excluding current player
    teammates = df_features[
        (df_features["team_abbreviation"] == team)
        & (df_features["player_id"] != player_id)
        & (df_features["game_date"] < game_date)
    ]
    last5 = teammates.groupby("player_id").tail(5)
    avg_assist = last5["assists"].mean() if not last5.empty else 0
    teammate_avg_assists.append(avg_assist)
df_features["teammate_avg_assists_last5"] = teammate_avg_assists

# opponent strength (points allowed, blocks, steals, turnovers)
opponent_avg_points_allowed = []
opponent_avg_blocks = []
opponent_avg_steals = []
opponent_avg_turnovers = []

for idx, row in df_features.iterrows():
    matchup = row["matchup"]
    player_team = row["team_abbreviation"]
    if " vs. " in matchup:
        opponent_team = matchup.split(" vs. ")[1]
    else:
        opponent_team = matchup.split(" @ ")[1]

    opp_games = df_features[
        (df_features["team_abbreviation"] == opponent_team)
        & (df_features["game_date"] < row["game_date"])
    ]
    last5 = opp_games.tail(5)

    opponent_avg_points_allowed.append(last5["points"].mean() if not last5.empty else 0)
    opponent_avg_blocks.append(last5["blocks"].mean() if not last5.empty else 0)
    opponent_avg_steals.append(last5["steals"].mean() if not last5.empty else 0)
    opponent_avg_turnovers.append(last5["turnovers"].mean() if not last5.empty else 0)

df_features["opponent_avg_points_allowed_last5"] = opponent_avg_points_allowed
df_features["opponent_avg_blocks_last5"] = opponent_avg_blocks
df_features["opponent_avg_steals_last5"] = opponent_avg_steals
df_features["opponent_avg_turnovers_last5"] = opponent_avg_turnovers

# store the rolling average of minutes as well to avoid data leakage during prediction
# can pass the rolling avg during prediction as real minutes arent known yet
df_features["avg_minutes_last5"] = df_features.groupby("player_id")[
    "minutes"
].transform(lambda x: x.rolling(5, min_periods=1).mean().shift(1))

# drop first few games with NaN
df_features = df_features.dropna()


# features to feed model
features = [
    "avg_minutes_last5",
    "is_home",
    "avg_points_last5",
    "avg_assists_last5",
    "avg_rebounds_last5",
    "teammate_avg_assists_last5",
    "opponent_avg_points_allowed_last5",
    "opponent_avg_blocks_last5",
    "opponent_avg_steals_last5",
    "opponent_avg_turnovers_last5",
]
X = df_features[features]
y = df_features["points"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

# train xgboost model
model = XGBRegressor(n_estimators=500, learning_rate=0.05, max_depth=5, random_state=42)
model.fit(X_train, y_train)

# save the trained model
joblib.dump(model, "ml_models/models/xgb_points_model.pkl")
print("model trained and saved!")
