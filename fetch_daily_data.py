"""
fetch_daily_data.py - Fetch daily challenge data for NBA Legacy

Fetches:
1. Season averages for 2025-26 via stats.nba.com/leagueleaders (multi-category merge)
2. Recent completed games via cdn.nba.com schedule
3. Box scores for each game via cdn.nba.com liveData

Outputs: Assets/StreamingAssets/Data/daily_challenge.json
"""

import json
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

# ── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = SCRIPT_DIR / "data" / "daily_challenge.json"
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

DATA_DIR.mkdir(parents=True, exist_ok=True)

# ── Config ───────────────────────────────────────────────────────────────────
SEASON = "2025-26"
RECENT_DAYS = 5
API_DELAY = 0.6
API_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_BASE_DELAY = 5

LEADER_CATEGORIES = ["PTS", "REB", "AST", "STL", "BLK", "EFF"]

STATS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}

CDN_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.nba.com/",
}


# ── HTTP helpers ─────────────────────────────────────────────────────────────

def stats_get(url):
    """GET from stats.nba.com with retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=STATS_HEADERS, timeout=API_TIMEOUT)
            r.raise_for_status()
            time.sleep(API_DELAY)
            return r
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = RETRY_BASE_DELAY * attempt
                print(f"  Retry {attempt}/{MAX_RETRIES}: {type(e).__name__}. Waiting {wait}s...")
                sys.stdout.flush()
                time.sleep(wait)
            else:
                print(f"  FAILED after {MAX_RETRIES} attempts: {e}")
                return None


def cdn_get(url):
    """GET from cdn.nba.com with retry."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=CDN_HEADERS, timeout=30)
            r.raise_for_status()
            time.sleep(0.3)  # CDN is fast, light delay
            return r
        except Exception as e:
            if attempt < MAX_RETRIES:
                time.sleep(2)
            else:
                return None


def sf(val):
    try:
        return float(val) if val is not None else 0.0
    except (ValueError, TypeError):
        return 0.0


def si(val):
    try:
        return int(val) if val is not None else 0
    except (ValueError, TypeError):
        return 0


# ── Step 1: Season Averages (stats.nba.com/leagueleaders) ───────────────────

def fetch_season_averages():
    print(f"\n--- Step 1: Season averages for {SEASON} ---")
    merged = {}

    for cat in LEADER_CATEGORIES:
        url = (f"https://stats.nba.com/stats/leagueleaders?"
               f"ActiveFlag=&LeagueID=00&PerMode=PerGame&Scope=S"
               f"&Season={SEASON}&SeasonType=Regular+Season&StatCategory={cat}")

        r = stats_get(url)
        if r is None:
            print(f"  {cat}: FAILED")
            continue

        data = r.json()
        rs = data.get("resultSet", {})
        hdrs = rs.get("headers", [])
        rows = rs.get("rowSet", [])

        cat_new = 0
        for row in rows:
            info = dict(zip(hdrs, row))
            pid = si(info.get("PLAYER_ID"))
            if pid <= 0 or pid in merged:
                continue
            merged[pid] = {
                "playerId": pid,
                "playerName": info.get("PLAYER", ""),
                "season": SEASON,
                "team": info.get("TEAM", ""),
                "gamesPlayed": si(info.get("GP")),
                "mpg": round(sf(info.get("MIN")), 1),
                "ppg": round(sf(info.get("PTS")), 1),
                "rpg": round(sf(info.get("REB")), 1),
                "apg": round(sf(info.get("AST")), 1),
                "spg": round(sf(info.get("STL")), 1),
                "bpg": round(sf(info.get("BLK")), 1),
                "tov": round(sf(info.get("TOV")), 1),
                "fgPct": round(sf(info.get("FG_PCT")), 3),
                "fgm": round(sf(info.get("FGM")), 1),
                "fga": round(sf(info.get("FGA")), 1),
                "fg3Pct": round(sf(info.get("FG3_PCT")), 3),
                "fg3m": round(sf(info.get("FG3M")), 1),
                "fg3a": round(sf(info.get("FG3A")), 1),
                "ftPct": round(sf(info.get("FT_PCT")), 3),
                "ftm": round(sf(info.get("FTM")), 1),
                "fta": round(sf(info.get("FTA")), 1),
                "oreb": round(sf(info.get("OREB")), 1),
                "dreb": round(sf(info.get("DREB")), 1),
                "pf": round(sf(info.get("PF")), 1),
            }
            cat_new += 1

        print(f"  {cat}: {cat_new} new players (total: {len(merged)})")

    print(f"  Total: {len(merged)} players with season averages")
    return list(merged.values())


# ── Step 2: Recent Game IDs (cdn.nba.com schedule) ──────────────────────────

def fetch_recent_game_ids():
    print(f"\n--- Step 2: Recent completed games (last {RECENT_DAYS} days) ---")

    url = "https://cdn.nba.com/static/json/staticData/scheduleLeagueV2.json"
    r = cdn_get(url)
    if r is None:
        print("  FAILED to fetch schedule")
        return []

    data = r.json()
    dates = data.get("leagueSchedule", {}).get("gameDates", [])
    print(f"  Schedule has {len(dates)} game dates")

    now = datetime.now(timezone.utc)
    game_ids = []

    for d in dates:
        date_str = d.get("gameDate", "")[:10]
        try:
            gd = datetime.strptime(date_str, "%m/%d/%Y").replace(tzinfo=timezone.utc)
        except ValueError:
            try:
                gd = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue

        days_ago = (now - gd).days
        if days_ago < 0 or days_ago > RECENT_DAYS:
            continue

        day_games = 0
        for g in d.get("games", []):
            if g.get("gameStatus") == 3:  # Completed
                gid = g.get("gameId", "")
                if gid:
                    game_ids.append(gid)
                    day_games += 1

        if day_games > 0:
            print(f"  {date_str}: {day_games} completed games")

    print(f"  Total: {len(game_ids)} completed games")
    return game_ids


# ── Step 3: Box Scores (cdn.nba.com liveData) ───────────────────────────────

def parse_minutes(minute_str):
    if not minute_str:
        return 0.0
    s = str(minute_str)
    if s.startswith("PT"):
        s = s[2:]
        mins, secs = 0.0, 0.0
        if "M" in s:
            parts = s.split("M")
            try: mins = float(parts[0])
            except ValueError: pass
            s = parts[1] if len(parts) > 1 else ""
        if "S" in s:
            try: secs = float(s.replace("S", ""))
            except ValueError: pass
        return mins + secs / 60.0
    if ":" in s:
        parts = s.split(":")
        try: return float(parts[0]) + float(parts[1]) / 60.0
        except (ValueError, IndexError): return 0.0
    try: return float(s)
    except ValueError: return 0.0


def fetch_box_scores(game_ids):
    print(f"\n--- Step 3: Box scores for {len(game_ids)} games (CDN) ---")

    player_games = {}  # playerId -> list of game stat dicts
    failed = 0

    for i, game_id in enumerate(game_ids):
        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

        r = cdn_get(url)
        if r is None or r.status_code != 200:
            failed += 1
            if (i + 1) % 10 == 0 or i == 0:
                print(f"  Game {i+1}/{len(game_ids)} ({game_id}): FAILED")
            continue

        data = r.json()
        game = data.get("game", {})
        players_parsed = 0

        for team_key in ("homeTeam", "awayTeam"):
            team = game.get(team_key, {})
            for p in team.get("players", []):
                stats = p.get("statistics", {})
                if not stats:
                    continue

                pid = si(p.get("personId"))
                if pid <= 0:
                    continue

                minutes = parse_minutes(stats.get("minutes", "0"))
                if minutes <= 0:
                    continue

                game_stats = {
                    "minutes": minutes,
                    "points": si(stats.get("points")),
                    "rebounds": si(stats.get("reboundsTotal")),
                    "assists": si(stats.get("assists")),
                    "steals": si(stats.get("steals")),
                    "blocks": si(stats.get("blocks")),
                    "turnovers": si(stats.get("turnovers")),
                    "fgm": si(stats.get("fieldGoalsMade")),
                    "fga": si(stats.get("fieldGoalsAttempted")),
                    "fg3m": si(stats.get("threePointersMade")),
                    "fg3a": si(stats.get("threePointersAttempted")),
                    "ftm": si(stats.get("freeThrowsMade")),
                    "fta": si(stats.get("freeThrowsAttempted")),
                    "oreb": si(stats.get("reboundsOffensive")),
                    "dreb": si(stats.get("reboundsDefensive")),
                    "pf": si(stats.get("foulsPersonal")),
                }

                if pid not in player_games:
                    player_games[pid] = []
                player_games[pid].append(game_stats)
                players_parsed += 1

        if (i + 1) % 10 == 0 or i == len(game_ids) - 1:
            print(f"  Game {i+1}/{len(game_ids)}: {players_parsed} players "
                  f"(total unique: {len(player_games)})")

    if failed:
        print(f"  ({failed} games failed to fetch)")

    # Average each player's games
    print(f"\n  Averaging stats for {len(player_games)} players...")
    recent_players = []
    for pid, games in player_games.items():
        n = len(games)
        totals = {}
        for key in games[0]:
            totals[key] = sum(g[key] for g in games)

        total_fga = totals["fga"]
        total_fg3a = totals["fg3a"]
        total_fta = totals["fta"]

        recent_players.append({
            "playerId": pid,
            "gamesInWindow": n,
            "mpg": round(totals["minutes"] / n, 1),
            "ppg": round(totals["points"] / n, 1),
            "rpg": round(totals["rebounds"] / n, 1),
            "apg": round(totals["assists"] / n, 1),
            "spg": round(totals["steals"] / n, 1),
            "bpg": round(totals["blocks"] / n, 1),
            "tov": round(totals["turnovers"] / n, 1),
            "fgm": round(totals["fgm"] / n, 1),
            "fga": round(totals["fga"] / n, 1),
            "fg3m": round(totals["fg3m"] / n, 1),
            "fg3a": round(totals["fg3a"] / n, 1),
            "ftm": round(totals["ftm"] / n, 1),
            "fta": round(totals["fta"] / n, 1),
            "oreb": round(totals["oreb"] / n, 1),
            "dreb": round(totals["dreb"] / n, 1),
            "pf": round(totals["pf"] / n, 1),
            "fgPct": round(totals["fgm"] / total_fga, 3) if total_fga > 0 else 0.0,
            "fg3Pct": round(totals["fg3m"] / total_fg3a, 3) if total_fg3a > 0 else 0.0,
            "ftPct": round(totals["ftm"] / total_fta, 3) if total_fta > 0 else 0.0,
        })

    print(f"  Done: {len(recent_players)} players with recent stats")
    return recent_players


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("NBA Daily Challenge Data Fetcher")
    print(f"Season: {SEASON} | Recent days: {RECENT_DAYS}")
    print(f"Output: {OUTPUT_FILE}")
    print("=" * 60)
    sys.stdout.flush()

    # Step 1: Season averages (stats.nba.com)
    season_stats = fetch_season_averages()
    if not season_stats:
        print("\nERROR: No season data fetched. Aborting.")
        sys.exit(1)

    # Step 2: Recent game IDs (cdn.nba.com schedule)
    game_ids = fetch_recent_game_ids()

    # Step 3: Box scores (cdn.nba.com liveData)
    recent_players = []
    if game_ids:
        recent_players = fetch_box_scores(game_ids)
    else:
        print("\nNo recent games found. Saving season-only data.")

    # Step 4: Save
    now = datetime.now(timezone.utc)
    result = {
        "date": now.strftime("%Y-%m-%d"),
        "fetchedAtUtc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seasonStats": season_stats,
        "recentGames": recent_players,
    }

    with open(OUTPUT_FILE, "w") as f:
        json.dump(result, f, indent=2)

    print(f"\n{'=' * 60}")
    print(f"DONE: {len(season_stats)} season, {len(recent_players)} recent")
    print(f"File: {OUTPUT_FILE}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
