from datetime import datetime

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Text,
    UniqueConstraint,
)

from app.db.base import Base


class MlbSourcePull(Base):
    __tablename__ = "mlb_source_pulls"

    id = Column(Integer, primary_key=True)
    source = Column(Text, nullable=False)
    resource_type = Column(Text, nullable=False)
    request_url = Column(Text, nullable=False)
    request_params = Column(JSON, nullable=True)
    local_path = Column(Text, nullable=False)
    response_format = Column(Text, nullable=False)
    season = Column(Integer, nullable=True)
    game_pk = Column(BigInteger, nullable=True)
    start_date = Column(Date, nullable=True)
    end_date = Column(Date, nullable=True)
    row_count = Column(Integer, nullable=False, default=0)
    status = Column(Text, nullable=False, default="completed")
    notes = Column(Text, nullable=True)
    fetched_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )


class MlbTeam(Base):
    __tablename__ = "mlb_teams"

    id = Column(Integer, primary_key=True)
    abbreviation = Column(Text, nullable=False)
    name = Column(Text, nullable=False)
    team_name = Column(Text, nullable=True)
    location_name = Column(Text, nullable=True)
    franchise_name = Column(Text, nullable=True)
    club_name = Column(Text, nullable=True)
    league_id = Column(Integer, nullable=True)
    division_id = Column(Integer, nullable=True)
    venue_id = Column(Integer, nullable=True)
    first_year_of_play = Column(Text, nullable=True)
    active = Column(Boolean, nullable=False, default=True)

    __table_args__ = (UniqueConstraint("abbreviation", name="uq_mlb_teams_abbreviation"),)


class MlbVenue(Base):
    __tablename__ = "mlb_venues"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    active = Column(Boolean, nullable=False, default=True)
    city = Column(Text, nullable=True)
    state = Column(Text, nullable=True)
    country = Column(Text, nullable=True)
    timezone_id = Column(Text, nullable=True)
    timezone_offset = Column(Integer, nullable=True)
    latitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)
    elevation = Column(Integer, nullable=True)
    roof_type = Column(Text, nullable=True)
    turf_type = Column(Text, nullable=True)
    left_line = Column(Integer, nullable=True)
    left_center = Column(Integer, nullable=True)
    center = Column(Integer, nullable=True)
    right_center = Column(Integer, nullable=True)
    right_line = Column(Integer, nullable=True)
    capacity = Column(Integer, nullable=True)


class MlbPlayer(Base):
    __tablename__ = "mlb_players"

    id = Column(Integer, primary_key=True)
    full_name = Column(Text, nullable=False)
    first_name = Column(Text, nullable=True)
    last_name = Column(Text, nullable=True)
    use_name = Column(Text, nullable=True)
    use_last_name = Column(Text, nullable=True)
    birth_date = Column(Date, nullable=True)
    current_age = Column(Integer, nullable=True)
    bat_side = Column(Text, nullable=True)
    pitch_hand = Column(Text, nullable=True)
    primary_position_code = Column(Text, nullable=True)
    primary_position_name = Column(Text, nullable=True)
    primary_position_abbreviation = Column(Text, nullable=True)
    active = Column(Boolean, nullable=False, default=True)
    draft_year = Column(Integer, nullable=True)
    mlb_debut_date = Column(Date, nullable=True)
    last_played_date = Column(Date, nullable=True)


class MlbGame(Base):
    __tablename__ = "mlb_games"

    game_pk = Column(BigInteger, primary_key=True)
    official_date = Column(Date, nullable=False)
    start_time_utc = Column(TIMESTAMP(timezone=True), nullable=True)
    season = Column(Integer, nullable=False)
    game_type = Column(Text, nullable=True)
    double_header = Column(Text, nullable=True)
    game_number = Column(Integer, nullable=True)
    status_code = Column(Text, nullable=True)
    detailed_state = Column(Text, nullable=True)
    day_night = Column(Text, nullable=True)
    home_team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    away_team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    venue_id = Column(Integer, ForeignKey("mlb_venues.id"), nullable=True)
    home_score = Column(Integer, nullable=True)
    away_score = Column(Integer, nullable=True)
    probable_home_pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    probable_away_pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    weather_condition = Column(Text, nullable=True)
    temperature_f = Column(Float, nullable=True)
    wind_text = Column(Text, nullable=True)
    roof_type = Column(Text, nullable=True)
    last_ingested_at = Column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.utcnow,
    )


class MlbGameSnapshot(Base):
    __tablename__ = "mlb_game_snapshots"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)
    snapshot_type = Column(Text, nullable=False)
    captured_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    status_code = Column(Text, nullable=True)
    detailed_state = Column(Text, nullable=True)
    weather_condition = Column(Text, nullable=True)
    temperature_f = Column(Float, nullable=True)
    wind_text = Column(Text, nullable=True)
    roof_type = Column(Text, nullable=True)
    probable_home_pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    probable_away_pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    payload = Column(JSON, nullable=True)


class MlbUmpire(Base):
    __tablename__ = "mlb_umpires"

    id = Column(Integer, primary_key=True)
    full_name = Column(Text, nullable=False)
    first_name = Column(Text, nullable=True)
    last_name = Column(Text, nullable=True)
    jersey_number = Column(Text, nullable=True)
    job = Column(Text, nullable=True)
    job_id = Column(Text, nullable=True)
    title = Column(Text, nullable=True)
    active = Column(Boolean, nullable=False, default=True)


class MlbGameOfficialAssignment(Base):
    __tablename__ = "mlb_game_official_assignments"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("mlb_game_snapshots.id", ondelete="CASCADE"), nullable=False)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    umpire_id = Column(Integer, ForeignKey("mlb_umpires.id"), nullable=False)
    official_type = Column(Text, nullable=False)
    is_home_plate = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "umpire_id",
            "official_type",
            name="uq_mlb_game_official_assignment",
        ),
    )


class MlbLineupSnapshot(Base):
    __tablename__ = "mlb_lineup_snapshots"

    id = Column(Integer, primary_key=True)
    snapshot_id = Column(Integer, ForeignKey("mlb_game_snapshots.id", ondelete="CASCADE"), nullable=False)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    batting_order = Column(Integer, nullable=True)
    position_code = Column(Text, nullable=True)
    position_abbreviation = Column(Text, nullable=True)
    status_code = Column(Text, nullable=True)
    status_description = Column(Text, nullable=True)
    is_starter = Column(Boolean, nullable=False, default=False)
    is_bench = Column(Boolean, nullable=False, default=False)
    is_substitute = Column(Boolean, nullable=False, default=False)

    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "team_id",
            "player_id",
            name="uq_mlb_lineup_snapshot_player",
        ),
    )


class MlbRosterSnapshot(Base):
    __tablename__ = "mlb_roster_snapshots"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    roster_type = Column(Text, nullable=False)
    roster_date = Column(Date, nullable=False)
    season = Column(Integer, nullable=True)
    jersey_number = Column(Text, nullable=True)
    status_code = Column(Text, nullable=True)
    status_description = Column(Text, nullable=True)
    position_code = Column(Text, nullable=True)
    position_name = Column(Text, nullable=True)
    position_type = Column(Text, nullable=True)
    position_abbreviation = Column(Text, nullable=True)
    is_pitcher = Column(Boolean, nullable=False, default=False)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)
    captured_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "team_id",
            "player_id",
            "roster_type",
            "roster_date",
            name="uq_mlb_roster_snapshot_player",
        ),
    )


class MlbPlayerGameBatting(Base):
    __tablename__ = "mlb_player_game_batting"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    batting_order = Column(Integer, nullable=True)
    plate_appearances = Column(Integer, nullable=True)
    at_bats = Column(Integer, nullable=True)
    hits = Column(Integer, nullable=True)
    doubles = Column(Integer, nullable=True)
    triples = Column(Integer, nullable=True)
    home_runs = Column(Integer, nullable=True)
    total_bases = Column(Integer, nullable=True)
    runs = Column(Integer, nullable=True)
    rbi = Column(Integer, nullable=True)
    walks = Column(Integer, nullable=True)
    strikeouts = Column(Integer, nullable=True)
    hit_by_pitch = Column(Integer, nullable=True)
    stolen_bases = Column(Integer, nullable=True)
    caught_stealing = Column(Integer, nullable=True)
    left_on_base = Column(Integer, nullable=True)
    sac_bunts = Column(Integer, nullable=True)
    sac_flies = Column(Integer, nullable=True)
    summary = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "game_pk",
            "player_id",
            name="uq_mlb_player_game_batting",
        ),
    )


class MlbPlayerGamePitching(Base):
    __tablename__ = "mlb_player_game_pitching"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=False)
    is_starter = Column(Boolean, nullable=False, default=False)
    innings_pitched = Column(Text, nullable=True)
    outs_recorded = Column(Integer, nullable=True)
    batters_faced = Column(Integer, nullable=True)
    pitches_thrown = Column(Integer, nullable=True)
    strikes = Column(Integer, nullable=True)
    balls = Column(Integer, nullable=True)
    hits_allowed = Column(Integer, nullable=True)
    home_runs_allowed = Column(Integer, nullable=True)
    earned_runs = Column(Integer, nullable=True)
    walks = Column(Integer, nullable=True)
    strikeouts = Column(Integer, nullable=True)
    summary = Column(Text, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "game_pk",
            "player_id",
            name="uq_mlb_player_game_pitching",
        ),
    )


class MlbPitchEvent(Base):
    __tablename__ = "mlb_pitch_events"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    play_id = Column(Text, nullable=False)
    at_bat_index = Column(Integer, nullable=False)
    pitch_number = Column(Integer, nullable=True)
    inning = Column(Integer, nullable=True)
    half_inning = Column(Text, nullable=True)
    pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    batter_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    balls_before = Column(Integer, nullable=True)
    strikes_before = Column(Integer, nullable=True)
    outs_before = Column(Integer, nullable=True)
    balls_after = Column(Integer, nullable=True)
    strikes_after = Column(Integer, nullable=True)
    outs_after = Column(Integer, nullable=True)
    pitch_type_code = Column(Text, nullable=True)
    pitch_type_description = Column(Text, nullable=True)
    call_code = Column(Text, nullable=True)
    call_description = Column(Text, nullable=True)
    event_type = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    is_in_play = Column(Boolean, nullable=False, default=False)
    is_strike = Column(Boolean, nullable=False, default=False)
    is_ball = Column(Boolean, nullable=False, default=False)
    is_out = Column(Boolean, nullable=False, default=False)
    start_speed = Column(Float, nullable=True)
    end_speed = Column(Float, nullable=True)
    zone = Column(Integer, nullable=True)
    plate_time = Column(Float, nullable=True)
    extension = Column(Float, nullable=True)
    spin_rate = Column(Float, nullable=True)
    spin_direction = Column(Float, nullable=True)
    break_angle = Column(Float, nullable=True)
    break_length = Column(Float, nullable=True)
    break_y = Column(Float, nullable=True)
    break_vertical = Column(Float, nullable=True)
    break_vertical_induced = Column(Float, nullable=True)
    break_horizontal = Column(Float, nullable=True)
    pfx_x = Column(Float, nullable=True)
    pfx_z = Column(Float, nullable=True)
    plate_x = Column(Float, nullable=True)
    plate_z = Column(Float, nullable=True)
    release_pos_x = Column(Float, nullable=True)
    release_pos_y = Column(Float, nullable=True)
    release_pos_z = Column(Float, nullable=True)
    vx0 = Column(Float, nullable=True)
    vy0 = Column(Float, nullable=True)
    vz0 = Column(Float, nullable=True)
    ax = Column(Float, nullable=True)
    ay = Column(Float, nullable=True)
    az = Column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "game_pk",
            "play_id",
            name="uq_mlb_pitch_event_play",
        ),
    )


class MlbBattedBallEvent(Base):
    __tablename__ = "mlb_batted_ball_events"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    play_id = Column(Text, nullable=False)
    at_bat_index = Column(Integer, nullable=False)
    inning = Column(Integer, nullable=True)
    half_inning = Column(Text, nullable=True)
    pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    batter_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    event_type = Column(Text, nullable=True)
    description = Column(Text, nullable=True)
    launch_speed = Column(Float, nullable=True)
    launch_angle = Column(Float, nullable=True)
    total_distance = Column(Float, nullable=True)
    trajectory = Column(Text, nullable=True)
    hardness = Column(Text, nullable=True)
    location = Column(Text, nullable=True)
    coord_x = Column(Float, nullable=True)
    coord_y = Column(Float, nullable=True)
    is_hard_hit = Column(Boolean, nullable=False, default=False)
    is_sweet_spot = Column(Boolean, nullable=False, default=False)
    estimated_ba_using_speedangle = Column(Float, nullable=True)
    estimated_woba_using_speedangle = Column(Float, nullable=True)
    launch_speed_angle = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "game_pk",
            "play_id",
            name="uq_mlb_batted_ball_event_play",
        ),
    )


class MlbStatcastBatterSeason(Base):
    __tablename__ = "mlb_statcast_batter_season"

    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    player_name = Column(Text, nullable=False)
    team_abbreviation = Column(Text, nullable=True)
    plate_appearances = Column(Integer, nullable=True)
    at_bats = Column(Integer, nullable=True)
    hits = Column(Integer, nullable=True)
    home_runs = Column(Integer, nullable=True)
    strikeouts = Column(Integer, nullable=True)
    walks = Column(Integer, nullable=True)
    avg = Column(Float, nullable=True)
    obp = Column(Float, nullable=True)
    slg = Column(Float, nullable=True)
    ops = Column(Float, nullable=True)
    iso = Column(Float, nullable=True)
    babip = Column(Float, nullable=True)
    xba = Column(Float, nullable=True)
    xslg = Column(Float, nullable=True)
    xwoba = Column(Float, nullable=True)
    xobp = Column(Float, nullable=True)
    xiso = Column(Float, nullable=True)
    exit_velocity_avg = Column(Float, nullable=True)
    launch_angle_avg = Column(Float, nullable=True)
    barrel_batted_rate = Column(Float, nullable=True)
    hard_hit_percent = Column(Float, nullable=True)
    sweet_spot_percent = Column(Float, nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "season",
            "player_id",
            name="uq_mlb_statcast_batter_season",
        ),
    )


class MlbStatcastPitcherSeason(Base):
    __tablename__ = "mlb_statcast_pitcher_season"

    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    player_name = Column(Text, nullable=False)
    team_abbreviation = Column(Text, nullable=True)
    batters_faced = Column(Integer, nullable=True)
    strikeout_percent = Column(Float, nullable=True)
    walk_percent = Column(Float, nullable=True)
    xera = Column(Float, nullable=True)
    xba = Column(Float, nullable=True)
    xslg = Column(Float, nullable=True)
    xwoba = Column(Float, nullable=True)
    exit_velocity_avg = Column(Float, nullable=True)
    launch_angle_avg = Column(Float, nullable=True)
    barrel_batted_rate = Column(Float, nullable=True)
    hard_hit_percent = Column(Float, nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "season",
            "player_id",
            name="uq_mlb_statcast_pitcher_season",
        ),
    )


class MlbBatTrackingBatterSeason(Base):
    __tablename__ = "mlb_bat_tracking_batter_season"

    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    player_name = Column(Text, nullable=False)
    team_abbreviation = Column(Text, nullable=True)
    bat_speed = Column(Float, nullable=True)
    fast_swing_rate = Column(Float, nullable=True)
    squared_up_rate = Column(Float, nullable=True)
    blast_rate = Column(Float, nullable=True)
    blasts = Column(Float, nullable=True)
    swing_length = Column(Float, nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "season",
            "player_id",
            name="uq_mlb_bat_tracking_batter_season",
        ),
    )


class MlbSwingPathBatterSeason(Base):
    __tablename__ = "mlb_swing_path_batter_season"

    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    player_name = Column(Text, nullable=False)
    team_abbreviation = Column(Text, nullable=True)
    attack_angle = Column(Float, nullable=True)
    attack_direction = Column(Float, nullable=True)
    ideal_attack_angle_rate = Column(Float, nullable=True)
    swing_path_tilt = Column(Float, nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "season",
            "player_id",
            name="uq_mlb_swing_path_batter_season",
        ),
    )


class MlbParkFactor(Base):
    __tablename__ = "mlb_park_factors"

    id = Column(Integer, primary_key=True)
    season = Column(Integer, nullable=False)
    venue_id = Column(Integer, ForeignKey("mlb_venues.id"), nullable=True)
    venue_name = Column(Text, nullable=False)
    factor_type = Column(Text, nullable=False)
    stat_key = Column(Text, nullable=False)
    stat_value = Column(Float, nullable=True)
    bat_side = Column(Text, nullable=True)
    condition = Column(Text, nullable=True)
    rolling_value = Column(Text, nullable=True)
    tracking = Column(Text, nullable=True)
    speed_bucket = Column(Text, nullable=True)
    angle_bucket = Column(Text, nullable=True)
    temperature_factor = Column(Float, nullable=True)
    elevation_factor = Column(Float, nullable=True)
    roof_factor = Column(Float, nullable=True)
    environment_factor = Column(Float, nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "season",
            "venue_name",
            "factor_type",
            "stat_key",
            "bat_side",
            "condition",
            "rolling_value",
            "tracking",
            "speed_bucket",
            "angle_bucket",
            name="uq_mlb_park_factor_lookup",
        ),
    )


class MlbWeatherSnapshot(Base):
    __tablename__ = "mlb_weather_snapshots"

    id = Column(Integer, primary_key=True)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    venue_id = Column(Integer, ForeignKey("mlb_venues.id"), nullable=True)
    source_pull_id = Column(Integer, ForeignKey("mlb_source_pulls.id"), nullable=True)
    provider = Column(Text, nullable=False)
    dataset = Column(Text, nullable=False)
    pulled_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    target_time_utc = Column(DateTime(timezone=True), nullable=False)
    game_time_offset_hours = Column(Float, nullable=True)
    temperature_2m_c = Column(Float, nullable=True)
    relative_humidity_2m = Column(Float, nullable=True)
    dew_point_2m_c = Column(Float, nullable=True)
    surface_pressure_hpa = Column(Float, nullable=True)
    pressure_msl_hpa = Column(Float, nullable=True)
    wind_speed_10m_kph = Column(Float, nullable=True)
    wind_direction_10m_deg = Column(Float, nullable=True)
    wind_gusts_10m_kph = Column(Float, nullable=True)
    cloud_cover_percent = Column(Float, nullable=True)
    visibility_m = Column(Float, nullable=True)
    precipitation_probability = Column(Float, nullable=True)
    precipitation_mm = Column(Float, nullable=True)
    rain_mm = Column(Float, nullable=True)
    showers_mm = Column(Float, nullable=True)
    snowfall_cm = Column(Float, nullable=True)
    weather_code = Column(Integer, nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "game_pk",
            "provider",
            "dataset",
            "pulled_at",
            "target_time_utc",
            name="uq_mlb_weather_snapshot_lookup",
        ),
    )


class MlbPredictionLog(Base):
    __tablename__ = "mlb_prediction_logs"

    id = Column(Integer, primary_key=True)
    market = Column(Text, nullable=False)
    game_pk = Column(BigInteger, ForeignKey("mlb_games.game_pk", ondelete="CASCADE"), nullable=False)
    game_date = Column(Date, nullable=False)
    prediction_date = Column(Date, nullable=False)
    player_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=False)
    player_name = Column(Text, nullable=True)
    team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=True)
    team_abbreviation = Column(Text, nullable=True)
    opponent_team_id = Column(Integer, ForeignKey("mlb_teams.id"), nullable=True)
    opponent_team_abbreviation = Column(Text, nullable=True)
    is_home = Column(Boolean, nullable=True)
    batting_order = Column(Float, nullable=True)
    has_posted_lineup = Column(Boolean, nullable=True)
    starter_pitcher_id = Column(Integer, ForeignKey("mlb_players.id"), nullable=True)
    probability = Column(Float, nullable=True)
    prediction = Column(Float, nullable=True)
    model_path = Column(Text, nullable=True)
    actual_value = Column(Float, nullable=True)
    abs_error = Column(Float, nullable=True)
    payload = Column(JSON, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "market",
            "game_pk",
            "player_id",
            name="uq_mlb_prediction_log_market_game_player",
        ),
    )


class MlbPropOddsSnapshot(Base):
    __tablename__ = "mlb_prop_odds_snapshots"

    id = Column(Integer, primary_key=True)
    provider = Column(Text, nullable=False)
    sport = Column(Text, nullable=False)
    market = Column(Text, nullable=False)
    bookmaker = Column(Text, nullable=False)
    event_id = Column(Text, nullable=False)
    game_date = Column(Date, nullable=False)
    commence_time = Column(DateTime(timezone=True), nullable=True)
    home_team = Column(Text, nullable=True)
    away_team = Column(Text, nullable=True)
    player_name = Column(Text, nullable=False)
    normalized_player_name = Column(Text, nullable=False)
    line = Column(Float, nullable=True)
    american_odds = Column(Integer, nullable=False)
    decimal_odds = Column(Float, nullable=False)
    implied_probability = Column(Float, nullable=False)
    payload = Column(JSON, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "provider",
            "bookmaker",
            "market",
            "event_id",
            "normalized_player_name",
            "line",
            name="uq_mlb_prop_odds_snapshot_lookup",
        ),
    )


class MlbPropOddsFetchLog(Base):
    __tablename__ = "mlb_prop_odds_fetch_logs"

    id = Column(Integer, primary_key=True)
    provider = Column(Text, nullable=False)
    sport = Column(Text, nullable=False)
    market = Column(Text, nullable=False)
    bookmaker = Column(Text, nullable=False)
    game_date = Column(Date, nullable=False)
    status = Column(Text, nullable=False, default="completed")
    props_count = Column(Integer, nullable=False, default=0)
    events_count = Column(Integer, nullable=False, default=0)
    notes = Column(Text, nullable=True)
    fetched_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "provider",
            "bookmaker",
            "market",
            "game_date",
            name="uq_mlb_prop_odds_fetch_log_lookup",
        ),
    )
