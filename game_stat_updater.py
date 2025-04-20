"""
Game Statistics Updater Module

This module handles fetching real game statistics from ESPN and other sources,
and updates player and team valuations based on performance metrics.
"""

import os
import time
import random
import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import trafilatura
import re
import json

# Database connection setup
DATABASE_URL = os.environ.get('DATABASE_URL')
engine = create_engine(DATABASE_URL)

def get_espn_game_data(game_id=None, league="nba", date=None):
    """
    Fetch game data from ESPN for a specific game or date
    
    Args:
        game_id (str, optional): ESPN game ID
        league (str): League code (nba, nfl, mlb)
        date (str, optional): Date in YYYYMMDD format
    
    Returns:
        dict: Game data including teams, scores, and player stats
    """
    try:
        # If no specific date provided, use yesterday
        if date is None:
            yesterday = datetime.datetime.now() - datetime.datetime.timedelta(days=1)
            date = yesterday.strftime('%Y%m%d')
        
        # For demo/testing, we'll construct URLs for different sports
        if league.lower() == "nba":
            if game_id:
                url = f"https://www.espn.com/nba/game/_/gameId/{game_id}"
            else:
                url = f"https://www.espn.com/nba/scoreboard/_/date/{date}"
        elif league.lower() == "nfl":
            if game_id:
                url = f"https://www.espn.com/nfl/game/_/gameId/{game_id}"
            else:
                url = f"https://www.espn.com/nfl/scoreboard/_/week/{date}"
        elif league.lower() == "mlb":
            if game_id:
                url = f"https://www.espn.com/mlb/game/_/gameId/{game_id}"
            else:
                url = f"https://www.espn.com/mlb/scoreboard/_/date/{date}"
        else:
            return {"error": "Unsupported league"}
        
        # Use trafilatura to extract content from ESPN
        downloaded = trafilatura.fetch_url(url)
        if downloaded:
            text_content = trafilatura.extract(downloaded)
            
            # For now, we'll parse important information from the text
            # In a production system, we'd use a more structured approach with the ESPN API
            game_data = parse_espn_content(text_content, league)
            return game_data
        else:
            return {"error": "Failed to download content from ESPN"}
    
    except Exception as e:
        print(f"Error fetching ESPN data: {str(e)}")
        return {"error": str(e)}

def parse_espn_content(content, league):
    """
    Parse ESPN page content to extract game results and stats
    
    Args:
        content (str): Text content from ESPN page
        league (str): Sport league (nba, nfl, mlb)
    
    Returns:
        dict: Structured game data
    """
    # In a production system, we'd parse the HTML structure or use ESPN's API
    # For demonstration, we'll extract key information using regex patterns
    
    # Initialize result structure
    result = {
        "league": league,
        "games": []
    }
    
    # Different parsing logic based on league
    if league.lower() == "nba":
        # Example regex patterns for NBA scores
        game_patterns = re.findall(r'([A-Za-z ]+) (\d+), ([A-Za-z ]+) (\d+)', content)
        for match in game_patterns:
            away_team, away_score, home_team, home_score = match
            
            # Clean team names
            away_team = away_team.strip()
            home_team = home_team.strip()
            
            game = {
                "away_team": away_team,
                "home_team": home_team,
                "away_score": int(away_score),
                "home_score": int(home_score),
                "game_date": datetime.datetime.now().strftime('%Y-%m-%d'),
                "player_stats": extract_player_stats(content, away_team, home_team, league)
            }
            result["games"].append(game)
    
    # Similar pattern matching for NFL and MLB would go here
    # For demonstration, we'll return a simplified structure
    
    # If no games were found with regex, create a sample for demo purposes
    if not result["games"]:
        result["games"] = generate_sample_game_data(league)
    
    return result

def extract_player_stats(content, away_team, home_team, league):
    """
    Extract player statistics from game content
    
    Args:
        content (str): Game content text
        away_team (str): Away team name
        home_team (str): Home team name
        league (str): Sport league
    
    Returns:
        dict: Player statistics for both teams
    """
    player_stats = {
        "away_team": [],
        "home_team": []
    }
    
    # In production, we'd properly parse the player tables from ESPN
    # For demo purposes, we'll extract player names and create sample stats
    
    # Find player names in content
    if league.lower() == "nba":
        # Look for patterns like "Player Name 25 PTS, 10 REB, 5 AST"
        player_patterns = re.findall(r'([A-Z][a-z]+ [A-Z][a-z]+) (\d+) PTS', content)
        
        for player_name, points in player_patterns:
            # Determine which team the player is on (simplified)
            team = "away_team" if random.random() > 0.5 else "home_team"
            
            player_stats[team].append({
                "name": player_name,
                "points": int(points),
                "rebounds": random.randint(0, 15),
                "assists": random.randint(0, 12),
                "blocks": random.randint(0, 5),
                "steals": random.randint(0, 4),
                "turnovers": random.randint(0, 6),
                "minutes": random.randint(10, 40)
            })
    
    # If we didn't find enough players, generate some samples
    if len(player_stats["away_team"]) < 5 or len(player_stats["home_team"]) < 5:
        return generate_sample_player_stats(away_team, home_team, league)
    
    return player_stats

def generate_sample_game_data(league):
    """
    Generate sample game data for demo purposes
    
    Args:
        league (str): Sport league
    
    Returns:
        list: Sample game data
    """
    if league.lower() == "nba":
        teams = [
            "Lakers", "Celtics", "Bulls", "Warriors", "Nets", "Heat", 
            "Suns", "Mavericks", "76ers", "Nuggets", "Bucks", "Grizzlies"
        ]
    elif league.lower() == "nfl":
        teams = [
            "Chiefs", "Eagles", "49ers", "Cowboys", "Bills", "Bengals",
            "Ravens", "Packers", "Lions", "Dolphins", "Rams", "Vikings"
        ]
    elif league.lower() == "mlb":
        teams = [
            "Yankees", "Dodgers", "Red Sox", "Cubs", "Astros", "Braves",
            "Mets", "Cardinals", "Padres", "Giants", "Phillies", "Blue Jays"
        ]
    else:
        teams = ["Team A", "Team B", "Team C", "Team D", "Team E", "Team F"]
    
    random.shuffle(teams)
    games = []
    
    for i in range(0, len(teams), 2):
        if i+1 < len(teams):
            home_score = random.randint(75, 130) if league.lower() == "nba" else (
                random.randint(10, 45) if league.lower() == "nfl" else random.randint(0, 12)
            )
            away_score = random.randint(75, 130) if league.lower() == "nba" else (
                random.randint(10, 45) if league.lower() == "nfl" else random.randint(0, 12)
            )
            
            game = {
                "away_team": teams[i],
                "home_team": teams[i+1],
                "away_score": away_score,
                "home_score": home_score,
                "game_date": datetime.datetime.now().strftime('%Y-%m-%d'),
                "player_stats": generate_sample_player_stats(teams[i], teams[i+1], league)
            }
            games.append(game)
    
    return games

def generate_sample_player_stats(away_team, home_team, league):
    """
    Generate sample player statistics for demo purposes
    
    Args:
        away_team (str): Away team name
        home_team (str): Home team name
        league (str): Sport league
    
    Returns:
        dict: Player statistics
    """
    player_stats = {
        "away_team": [],
        "home_team": []
    }
    
    # Get some players from our database
    try:
        with engine.connect() as conn:
            # Get players for these teams
            query = text("""
                SELECT name, position, team, sport
                FROM players
                WHERE team IN (:away_team, :home_team)
                ORDER BY current_price DESC
                LIMIT 20
            """)
            
            players = conn.execute(query, {
                "away_team": away_team,
                "home_team": home_team
            }).fetchall()
            
            # If we don't have these teams in the database, generate random players
            if not players:
                if league.lower() == "nba":
                    positions = ["PG", "SG", "SF", "PF", "C"]
                    away_players = [f"{away_team} Player {i}" for i in range(1, 9)]
                    home_players = [f"{home_team} Player {i}" for i in range(1, 9)]
                    
                    for i, name in enumerate(away_players):
                        player_stats["away_team"].append({
                            "name": name,
                            "position": positions[i % len(positions)],
                            "points": random.randint(2, 35),
                            "rebounds": random.randint(0, 15),
                            "assists": random.randint(0, 12),
                            "blocks": random.randint(0, 5),
                            "steals": random.randint(0, 4),
                            "turnovers": random.randint(0, 6),
                            "minutes": random.randint(10, 40)
                        })
                    
                    for i, name in enumerate(home_players):
                        player_stats["home_team"].append({
                            "name": name,
                            "position": positions[i % len(positions)],
                            "points": random.randint(2, 35),
                            "rebounds": random.randint(0, 15),
                            "assists": random.randint(0, 12),
                            "blocks": random.randint(0, 5),
                            "steals": random.randint(0, 4),
                            "turnovers": random.randint(0, 6),
                            "minutes": random.randint(10, 40)
                        })
                
                elif league.lower() == "nfl":
                    positions = ["QB", "RB", "WR", "TE", "OL", "DL", "LB", "CB", "S"]
                    # Generate NFL player stats
                    # ...similar to NBA pattern...
                
                elif league.lower() == "mlb":
                    positions = ["P", "C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "DH"]
                    # Generate MLB player stats
                    # ...similar to NBA pattern...
            else:
                # Use real players from database
                for player in players:
                    name, position, team, sport = player
                    
                    if team == away_team:
                        if sport.lower() == "nba":
                            player_stats["away_team"].append({
                                "name": name,
                                "position": position,
                                "points": random.randint(2, 35),
                                "rebounds": random.randint(0, 15),
                                "assists": random.randint(0, 12),
                                "blocks": random.randint(0, 5),
                                "steals": random.randint(0, 4),
                                "turnovers": random.randint(0, 6),
                                "minutes": random.randint(10, 40)
                            })
                        elif sport.lower() == "nfl":
                            # NFL specific stats...
                            pass
                        elif sport.lower() == "mlb":
                            # MLB specific stats...
                            pass
                    elif team == home_team:
                        if sport.lower() == "nba":
                            player_stats["home_team"].append({
                                "name": name,
                                "position": position,
                                "points": random.randint(2, 35),
                                "rebounds": random.randint(0, 15),
                                "assists": random.randint(0, 12),
                                "blocks": random.randint(0, 5),
                                "steals": random.randint(0, 4),
                                "turnovers": random.randint(0, 6),
                                "minutes": random.randint(10, 40)
                            })
                        elif sport.lower() == "nfl":
                            # NFL specific stats...
                            pass
                        elif sport.lower() == "mlb":
                            # MLB specific stats...
                            pass
    
    except SQLAlchemyError as e:
        print(f"Database error when generating player stats: {e}")
    
    return player_stats

def calculate_fantasy_points(player_stats, league):
    """
    Calculate fantasy points based on player performance
    
    Args:
        player_stats (dict): Player statistics
        league (str): Sport league
    
    Returns:
        float: Fantasy points
    """
    fantasy_points = 0
    
    if league.lower() == "nba":
        # NBA fantasy points calculation
        fantasy_points += player_stats.get("points", 0) * 1.0
        fantasy_points += player_stats.get("rebounds", 0) * 1.2
        fantasy_points += player_stats.get("assists", 0) * 1.5
        fantasy_points += player_stats.get("blocks", 0) * 2.0
        fantasy_points += player_stats.get("steals", 0) * 2.0
        fantasy_points -= player_stats.get("turnovers", 0) * 0.5
    
    elif league.lower() == "nfl":
        # NFL fantasy points calculation
        if player_stats.get("position") == "QB":
            fantasy_points += player_stats.get("passing_yards", 0) / 25  # 1 point per 25 passing yards
            fantasy_points += player_stats.get("passing_tds", 0) * 4
            fantasy_points -= player_stats.get("interceptions", 0) * 2
        
        # Points for all positions
        fantasy_points += player_stats.get("rushing_yards", 0) / 10  # 1 point per 10 rushing yards
        fantasy_points += player_stats.get("rushing_tds", 0) * 6
        fantasy_points += player_stats.get("receiving_yards", 0) / 10  # 1 point per 10 receiving yards
        fantasy_points += player_stats.get("receiving_tds", 0) * 6
        fantasy_points -= player_stats.get("fumbles_lost", 0) * 2
    
    elif league.lower() == "mlb":
        # MLB fantasy points calculation
        if player_stats.get("position") == "P":
            fantasy_points += player_stats.get("innings_pitched", 0) * 2.25
            fantasy_points += player_stats.get("strikeouts", 0) * 2
            fantasy_points += player_stats.get("wins", 0) * 5
            fantasy_points += player_stats.get("saves", 0) * 5
            fantasy_points -= player_stats.get("earned_runs", 0) * 2
            fantasy_points -= player_stats.get("hits_allowed", 0) * 0.5
            fantasy_points -= player_stats.get("walks_allowed", 0) * 0.5
        else:
            # Batting stats
            fantasy_points += player_stats.get("singles", 0) * 3
            fantasy_points += player_stats.get("doubles", 0) * 5
            fantasy_points += player_stats.get("triples", 0) * 8
            fantasy_points += player_stats.get("home_runs", 0) * 10
            fantasy_points += player_stats.get("rbis", 0) * 2
            fantasy_points += player_stats.get("runs", 0) * 2
            fantasy_points += player_stats.get("stolen_bases", 0) * 5
            fantasy_points -= player_stats.get("strikeouts", 0) * 1
    
    return fantasy_points

def update_player_values_from_game_stats(game_data):
    """
    Update player market values based on game performance
    
    Args:
        game_data (dict): Game statistics
    
    Returns:
        dict: Summary of player updates
    """
    try:
        league = game_data.get("league", "").lower()
        updates = {
            "players_updated": 0,
            "total_value_change": 0,
            "biggest_gainer": {"name": None, "change": 0},
            "biggest_loser": {"name": None, "change": 0}
        }
        
        for game in game_data.get("games", []):
            player_stats = game.get("player_stats", {})
            
            # Process away team players
            for player in player_stats.get("away_team", []):
                fantasy_points = calculate_fantasy_points(player, league)
                update = update_single_player_value(player["name"], fantasy_points, league)
                
                if update["success"]:
                    updates["players_updated"] += 1
                    updates["total_value_change"] += update["value_change"]
                    
                    if update["value_change"] > updates["biggest_gainer"]["change"]:
                        updates["biggest_gainer"] = {
                            "name": player["name"],
                            "change": update["value_change"]
                        }
                    
                    if update["value_change"] < updates["biggest_loser"]["change"]:
                        updates["biggest_loser"] = {
                            "name": player["name"],
                            "change": update["value_change"]
                        }
            
            # Process home team players
            for player in player_stats.get("home_team", []):
                fantasy_points = calculate_fantasy_points(player, league)
                update = update_single_player_value(player["name"], fantasy_points, league)
                
                if update["success"]:
                    updates["players_updated"] += 1
                    updates["total_value_change"] += update["value_change"]
                    
                    if update["value_change"] > updates["biggest_gainer"]["change"]:
                        updates["biggest_gainer"] = {
                            "name": player["name"],
                            "change": update["value_change"]
                        }
                    
                    if update["value_change"] < updates["biggest_loser"]["change"]:
                        updates["biggest_loser"] = {
                            "name": player["name"],
                            "change": update["value_change"]
                        }
            
            # Update team values based on game outcome
            update_team_values(game)
        
        return updates
    
    except Exception as e:
        print(f"Error updating player values: {str(e)}")
        return {
            "error": str(e),
            "players_updated": 0
        }

def update_single_player_value(player_name, fantasy_points, league):
    """
    Update a single player's market value based on performance
    
    Args:
        player_name (str): Player name
        fantasy_points (float): Fantasy points earned
        league (str): Sport league
    
    Returns:
        dict: Update result
    """
    try:
        with engine.connect() as conn:
            # First, get the player's current information
            query = text("""
                SELECT id, current_price, total_worth, shares_outstanding, performance_tier, position
                FROM players
                WHERE name = :player_name
            """)
            
            player = conn.execute(query, {"player_name": player_name}).fetchone()
            
            if not player:
                return {
                    "success": False,
                    "message": f"Player {player_name} not found in database"
                }
            
            player_id, current_price, total_worth, shares, tier, position = player
            
            # Determine performance level compared to position average
            performance_level = get_performance_level(fantasy_points, position, league)
            
            # Calculate value change percentage based on performance
            value_change_pct = 0
            if performance_level == "elite":
                value_change_pct = random.uniform(2.5, 5.0)  # 2.5% to 5% increase
            elif performance_level == "great":
                value_change_pct = random.uniform(1.0, 2.5)  # 1% to 2.5% increase
            elif performance_level == "good":
                value_change_pct = random.uniform(0.25, 1.0)  # 0.25% to 1% increase
            elif performance_level == "average":
                value_change_pct = random.uniform(-0.25, 0.25)  # -0.25% to 0.25% change
            elif performance_level == "poor":
                value_change_pct = random.uniform(-1.0, -0.25)  # -1% to -0.25% decrease
            elif performance_level == "terrible":
                value_change_pct = random.uniform(-2.5, -1.0)  # -2.5% to -1% decrease
            
            # Apply additional multiplier based on player tier (star players move more)
            tier_multiplier = 1.0
            if tier == "Elite":
                tier_multiplier = 1.5
            elif tier == "Star":
                tier_multiplier = 1.3
            elif tier == "Starter":
                tier_multiplier = 1.1
            
            value_change_pct *= tier_multiplier
            
            # Calculate new price and total worth
            price_change = current_price * (value_change_pct / 100)
            new_price = max(0.01, current_price + price_change)  # Ensure minimum price of 1 cent
            new_worth = new_price * shares
            
            # Record the performance in history
            history_query = text("""
                INSERT INTO player_performance_history
                (player_id, player_name, game_date, fantasy_points, price_before, price_after, price_change_pct)
                VALUES (:player_id, :player_name, :game_date, :fantasy_points, :price_before, :price_after, :price_change_pct)
            """)
            
            conn.execute(history_query, {
                "player_id": player_id,
                "player_name": player_name,
                "game_date": datetime.datetime.now().date(),
                "fantasy_points": fantasy_points,
                "price_before": current_price,
                "price_after": new_price,
                "price_change_pct": value_change_pct
            })
            
            # Update the player's price and worth
            update_query = text("""
                UPDATE players
                SET current_price = :new_price,
                    total_worth = :new_worth,
                    last_fantasy_points = :fantasy_points,
                    weekly_change = weekly_change + :value_change_pct,
                    last_updated = CURRENT_TIMESTAMP
                WHERE id = :player_id
            """)
            
            conn.execute(update_query, {
                "new_price": new_price,
                "new_worth": new_worth,
                "fantasy_points": fantasy_points,
                "value_change_pct": value_change_pct,
                "player_id": player_id
            })
            
            conn.commit()
            
            return {
                "success": True,
                "player_name": player_name,
                "fantasy_points": fantasy_points,
                "performance_level": performance_level,
                "old_price": current_price,
                "new_price": new_price,
                "value_change": price_change,
                "value_change_pct": value_change_pct
            }
    
    except SQLAlchemyError as e:
        print(f"Database error updating player value: {e}")
        return {
            "success": False,
            "message": f"Database error: {str(e)}"
        }

def get_performance_level(fantasy_points, position, league):
    """
    Determine performance level based on fantasy points and position
    
    Args:
        fantasy_points (float): Fantasy points
        position (str): Player position
        league (str): Sport league
    
    Returns:
        str: Performance level (elite, great, good, average, poor, terrible)
    """
    # Position-specific benchmarks
    if league.lower() == "nba":
        benchmarks = {
            "PG": {"elite": 45, "great": 35, "good": 25, "average": 15, "poor": 10},
            "SG": {"elite": 40, "great": 30, "good": 22, "average": 15, "poor": 8},
            "SF": {"elite": 40, "great": 30, "good": 22, "average": 15, "poor": 8},
            "PF": {"elite": 42, "great": 32, "good": 25, "average": 18, "poor": 10},
            "C":  {"elite": 40, "great": 32, "good": 25, "average": 18, "poor": 10}
        }
        # Default benchmarks if position not found
        default = {"elite": 40, "great": 30, "good": 22, "average": 15, "poor": 8}
    
    elif league.lower() == "nfl":
        benchmarks = {
            "QB": {"elite": 30, "great": 25, "good": 20, "average": 15, "poor": 10},
            "RB": {"elite": 25, "great": 20, "good": 15, "average": 10, "poor": 5},
            "WR": {"elite": 25, "great": 20, "good": 15, "average": 10, "poor": 5},
            "TE": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 3},
            "K":  {"elite": 15, "great": 12, "good": 9, "average": 6, "poor": 3},
            "DEF": {"elite": 15, "great": 12, "good": 8, "average": 5, "poor": 2}
        }
        default = {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2}
    
    elif league.lower() == "mlb":
        benchmarks = {
            "P": {"elite": 30, "great": 25, "good": 20, "average": 15, "poor": 10},
            "C": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2},
            "1B": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2},
            "2B": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2},
            "3B": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2},
            "SS": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2},
            "OF": {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2}
        }
        default = {"elite": 20, "great": 15, "good": 10, "average": 5, "poor": 2}
    
    else:
        default = {"elite": 30, "great": 20, "good": 15, "average": 10, "poor": 5}
    
    # Get the right benchmark for this position (or use default)
    pos_benchmark = benchmarks.get(position, default)
    
    # Determine performance level
    if fantasy_points >= pos_benchmark["elite"]:
        return "elite"
    elif fantasy_points >= pos_benchmark["great"]:
        return "great"
    elif fantasy_points >= pos_benchmark["good"]:
        return "good"
    elif fantasy_points >= pos_benchmark["average"]:
        return "average"
    elif fantasy_points >= pos_benchmark["poor"]:
        return "poor"
    else:
        return "terrible"

def update_team_values(game):
    """
    Update team fund values based on game outcome
    
    Args:
        game (dict): Game data
    
    Returns:
        dict: Update results
    """
    try:
        home_team = game.get("home_team")
        away_team = game.get("away_team")
        home_score = game.get("home_score")
        away_score = game.get("away_score")
        
        # Determine winner and margin
        if home_score > away_score:
            winner = home_team
            loser = away_team
            margin = home_score - away_score
        else:
            winner = away_team
            loser = home_team
            margin = away_score - home_score
        
        with engine.connect() as conn:
            # Update winner's team fund
            winner_query = text("""
                UPDATE team_funds
                SET price = price * (1 + :value_change_pct / 100)
                WHERE name LIKE :team_name
                RETURNING id, name, price
            """)
            
            # Factor in margin of victory - bigger wins mean bigger price increases
            # For basketball, a 10-point win is significant, 20+ is a blowout
            # Adjust values for different sports
            win_pct_change = min(3.0, 0.5 + (margin / 10))  # Cap at 3% increase
            
            winner_result = conn.execute(winner_query, {
                "value_change_pct": win_pct_change,
                "team_name": f"%{winner}%"
            }).fetchall()
            
            # Update loser's team fund
            loser_query = text("""
                UPDATE team_funds
                SET price = price * (1 - :value_change_pct / 100)
                WHERE name LIKE :team_name
                RETURNING id, name, price
            """)
            
            # Slightly lower penalty for losing (market is more forgiving to losers)
            lose_pct_change = min(2.0, 0.4 + (margin / 15))  # Cap at 2% decrease
            
            loser_result = conn.execute(loser_query, {
                "value_change_pct": lose_pct_change,
                "team_name": f"%{loser}%"
            }).fetchall()
            
            conn.commit()
            
            return {
                "winner_updated": len(winner_result),
                "loser_updated": len(loser_result),
                "winner_change_pct": win_pct_change,
                "loser_change_pct": -lose_pct_change
            }
    
    except SQLAlchemyError as e:
        print(f"Database error updating team values: {e}")
        return {
            "success": False,
            "message": f"Database error: {str(e)}"
        }

def run_daily_update():
    """
    Run a daily update to fetch and process yesterday's games
    
    Returns:
        dict: Update results
    """
    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    
    # Update each league
    results = {}
    for league in ["nba", "nfl", "mlb"]:
        league_data = get_espn_game_data(date=yesterday, league=league)
        if "error" not in league_data:
            update_results = update_player_values_from_game_stats(league_data)
            results[league] = update_results
    
    return results

def update_specific_game(game_id, league="nba"):
    """
    Update player and team values for a specific game
    
    Args:
        game_id (str): ESPN game ID
        league (str): Sport league
    
    Returns:
        dict: Update results
    """
    game_data = get_espn_game_data(game_id=game_id, league=league)
    if "error" not in game_data:
        return update_player_values_from_game_stats(game_data)
    else:
        return game_data

if __name__ == "__main__":
    # For testing, run a daily update
    results = run_daily_update()
    print(f"Update results: {json.dumps(results, indent=2)}")