"""
Game updater for automatic game results, summaries, and market impact.
This module handles updating game results and generating detailed summaries across all sports.
"""

import os
import random
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine, text

def detect_sport_from_team(team_name):
    """
    Detect the sport based on team name
    """
    nba_teams = ["Celtics", "Heat", "Knicks", "76ers", "Nuggets", "Lakers", "Thunder", "Pelicans", "Timberwolves", "Suns",
                "Bucks", "Cavaliers", "Clippers", "Warriors", "Mavericks", "Kings", "Grizzlies", "Raptors", "Hawks", "Bulls"]
    
    mlb_teams = ["Yankees", "Red Sox", "Dodgers", "Giants", "Astros", "Rangers", "Cubs", "Cardinals", "Braves", "Mets",
                "Phillies", "Blue Jays", "Angels", "Padres", "Mariners", "Nationals", "Diamondbacks", "Rockies", "Brewers", "Rays"]
    
    nfl_teams = ["Bills", "Cardinals", "Buccaneers", "Seahawks", "49ers", "Patriots", "Lions", "Chargers", "Broncos", 
                "Cowboys", "Packers", "Eagles", "Bears", "Vikings", "Titans", "Steelers", "Chiefs", "Raiders", "Saints", "Falcons"]
    
    # Check if team name contains any of the team identifiers
    for nba_team in nba_teams:
        if nba_team in team_name:
            return "NBA"
    
    for mlb_team in mlb_teams:
        if mlb_team in team_name:
            return "MLB"
    
    for nfl_team in nfl_teams:
        if nfl_team in team_name:
            return "NFL"
    
    # Default to NBA for playoffs
    return "NBA"

def update_game_and_generate_summary(game_id):
    """
    Update game result and generate a detailed summary of what happened based on the sport
    
    Parameters:
    - game_id: ID of the game to update
    
    Returns:
    - success: Boolean indicating if update was successful
    - message: Status message
    - summary: Detailed game summary if successful
    """
    try:
        engine = create_engine(os.environ.get('DATABASE_URL'))
        with engine.connect() as conn:
            # First check if the game exists and get its data
            game_query = text("""
                SELECT id, home_team, away_team, game_date, status, home_score, away_score 
                FROM upcoming_games WHERE id = :game_id
            """)
            game = conn.execute(game_query, {"game_id": game_id}).fetchone()
            
            if not game:
                return False, "Game not found", None
            
            if game.status == 'completed':
                # Game already completed, just return the existing summary
                summary_query = text("""
                    SELECT summary FROM game_summaries WHERE game_id = :game_id
                """)
                summary_result = conn.execute(summary_query, {"game_id": game_id}).fetchone()
                
                if summary_result:
                    return True, "Game already completed", summary_result[0]
                else:
                    # Generate a summary for completed game without one
                    sport = detect_sport_from_team(game.home_team)
                    summary = generate_game_summary(game._mapping, sport)
                    
                    # Check if game_summaries table exists, create if not
                    conn.execute(text("""
                        CREATE TABLE IF NOT EXISTS game_summaries (
                            id SERIAL PRIMARY KEY,
                            game_id INTEGER NOT NULL,
                            summary TEXT NOT NULL,
                            created_at TIMESTAMP NOT NULL DEFAULT NOW()
                        )
                    """))
                    
                    # Store the summary
                    store_query = text("""
                        INSERT INTO game_summaries (game_id, summary, created_at)
                        VALUES (:game_id, :summary, NOW())
                    """)
                    conn.execute(store_query, {"game_id": game_id, "summary": summary})
                    
                    return True, "Game already completed, summary generated", summary
            
            # Game is scheduled, let's simulate it
            sport = detect_sport_from_team(game.home_team)
            
            # Simulate scores based on sport
            if sport == "NBA":
                # NBA scores typically between 90-130
                home_score = random.randint(90, 130)
                away_score = random.randint(90, 130)
            elif sport == "MLB":
                # MLB scores typically between 0-10
                home_score = random.randint(0, 10)
                away_score = random.randint(0, 10)
            elif sport == "NFL":
                # NFL scores typically in multiples of 3 and 7
                home_score = random.choice([0, 3, 6, 7, 10, 13, 14, 17, 20, 21, 24, 27, 28, 31, 34, 35, 38])
                away_score = random.choice([0, 3, 6, 7, 10, 13, 14, 17, 20, 21, 24, 27, 28, 31, 34, 35, 38])
            else:
                # Default random scores
                home_score = random.randint(10, 38)
                away_score = random.randint(7, 35)
            
            # Update game with simulated scores
            with conn.begin() as trans:
                update_query = text("""
                    UPDATE upcoming_games
                    SET status = 'completed', home_score = :home_score, away_score = :away_score, updated_at = NOW()
                    WHERE id = :game_id
                """)
                conn.execute(update_query, {
                    "game_id": game_id,
                    "home_score": home_score,
                    "away_score": away_score
                })
                
                # Generate detailed summary
                game_data = {
                    "home_team": game.home_team,
                    "away_team": game.away_team,
                    "home_score": home_score,
                    "away_score": away_score,
                    "game_date": game.game_date
                }
                summary = generate_game_summary(game_data, sport)
                
                # Check if game_summaries table exists, create if not
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS game_summaries (
                        id SERIAL PRIMARY KEY,
                        game_id INTEGER NOT NULL,
                        summary TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """))
                
                # Store the summary
                store_query = text("""
                    INSERT INTO game_summaries (game_id, summary, created_at)
                    VALUES (:game_id, :summary, NOW())
                """)
                conn.execute(store_query, {"game_id": game_id, "summary": summary})
                
                # Update player props for this game
                update_props_query = text("""
                    UPDATE player_props SET is_active = false WHERE game_id = :game_id
                """)
                conn.execute(update_props_query, {"game_id": game_id})
                
                # Process bets for this game - use the existing function
                try:
                    from db import simulate_game_result
                    success, bet_message = simulate_game_result(game_id)
                    if not success:
                        trans.rollback()
                        return False, f"Error processing bets: {bet_message}", None
                except Exception as e:
                    print(f"Error calling simulate_game_result: {e}")
                    # If we can't use the existing function, process bets directly
                    process_bets_for_game(conn, game_id, home_score, away_score)
                
                # Update player performance data to reflect game results
                update_player_performance_from_game(conn, game_id, sport, home_team, away_team, home_score, away_score)
                
                trans.commit()
                
                return True, f"Game updated successfully: {game.home_team} {home_score} - {away_score} {game.away_team}", summary
                
    except Exception as e:
        print(f"Error updating game: {e}")
        return False, f"Error updating game: {str(e)}", None

def generate_game_summary(game, sport):
    """
    Generate a detailed game summary based on the sport and final score
    """
    home_team = game["home_team"]
    away_team = game["away_team"]
    home_score = game["home_score"]
    away_score = game["away_score"]
    
    # Determine winner
    if home_score > away_score:
        winner = home_team
        loser = away_team
        win_score = home_score
        lose_score = away_score
    else:
        winner = away_team
        loser = home_team
        win_score = away_score
        lose_score = home_score
    
    # Generate sport-specific summary
    if sport == "NBA":
        # Get lead players from database or use defaults
        home_players = get_team_top_players(home_team, "NBA") or ["Jayson Tatum", "Jaylen Brown", "Derrick White"]
        away_players = get_team_top_players(away_team, "NBA") or ["LeBron James", "Anthony Davis", "D'Angelo Russell"]
        
        # Generate player stats
        home_stats = {}
        for player in home_players[:3]:  # Top 3 players
            home_stats[player] = {
                "points": random.randint(15, 35),
                "rebounds": random.randint(3, 12),
                "assists": random.randint(2, 10),
                "blocks": random.randint(0, 3),
                "steals": random.randint(0, 3)
            }
        
        away_stats = {}
        for player in away_players[:3]:  # Top 3 players
            away_stats[player] = {
                "points": random.randint(15, 35),
                "rebounds": random.randint(3, 12),
                "assists": random.randint(2, 10),
                "blocks": random.randint(0, 3),
                "steals": random.randint(0, 3)
            }
        
        # Start with basic summary
        summary = f"{winner} defeats {loser} {win_score}-{lose_score} in an exciting NBA playoff matchup.\n\n"
        
        # Add quarter-by-quarter scoring
        quarters = []
        home_quarters = []
        away_quarters = []
        
        # Generate plausible quarter scores that add up to final
        for i in range(3):  # First 3 quarters
            home_q = random.randint(15, 35)
            away_q = random.randint(15, 35)
            home_quarters.append(home_q)
            away_quarters.append(away_q)
        
        # Last quarter makes up the difference
        home_quarters.append(home_score - sum(home_quarters))
        away_quarters.append(away_score - sum(away_quarters))
        
        summary += "Quarter-by-Quarter Scoring:\n"
        summary += f"{'Team':<15} {'Q1':>5} {'Q2':>5} {'Q3':>5} {'Q4':>5} {'Total':>7}\n"
        summary += f"{home_team:<15} {home_quarters[0]:>5} {home_quarters[1]:>5} {home_quarters[2]:>5} {home_quarters[3]:>5} {home_score:>7}\n"
        summary += f"{away_team:<15} {away_quarters[0]:>5} {away_quarters[1]:>5} {away_quarters[2]:>5} {away_quarters[3]:>5} {away_score:>7}\n\n"
        
        # Add leading performers
        summary += "Leading Performers:\n\n"
        
        # Home team top performers
        summary += f"{home_team} Leaders:\n"
        for player, stats in home_stats.items():
            summary += f"- {player}: {stats['points']} pts, {stats['rebounds']} reb, {stats['assists']} ast\n"
        
        summary += f"\n{away_team} Leaders:\n"
        for player, stats in away_stats.items():
            summary += f"- {player}: {stats['points']} pts, {stats['rebounds']} reb, {stats['assists']} ast\n"
        
        # Add game narrative
        point_diff = abs(win_score - lose_score)
        if point_diff <= 5:
            narrative = f"In a nail-biting finish, {winner} edged {loser} by just {point_diff} points. "
        elif point_diff <= 15:
            narrative = f"{winner} maintained control throughout the game to secure a solid {point_diff}-point victory over {loser}. "
        else:
            narrative = f"In a dominant performance, {winner} crushed {loser} by {point_diff} points. "
        
        # Add a key moment
        key_moments = [
            f"{random.choice(list(home_stats.keys()))} hit a crucial three-pointer late in the fourth quarter.",
            f"{random.choice(list(away_stats.keys()))} fouled out with 5 minutes remaining in the game.",
            "A controversial referee decision led to a technical foul that shifted momentum.",
            f"{random.choice(list(home_stats.keys()))} recorded a double-double with {random.randint(15, 30)} points and {random.randint(10, 15)} rebounds.",
            "The game featured 12 lead changes and 8 ties, keeping fans on the edge of their seats."
        ]
        
        summary += f"\nGame Summary:\n{narrative}{random.choice(key_moments)}\n"
        
    elif sport == "MLB":
        # Generate innings data
        innings = 9
        home_innings = [0] * innings
        away_innings = [0] * innings
        
        # Distribute runs across innings
        remaining_home = home_score
        remaining_away = away_score
        
        for i in range(innings - 1):  # All but last inning
            if remaining_home > 0:
                runs = min(random.randint(0, 2), remaining_home)
                home_innings[i] = runs
                remaining_home -= runs
            
            if remaining_away > 0:
                runs = min(random.randint(0, 2), remaining_away)
                away_innings[i] = runs
                remaining_away -= runs
        
        # Assign remaining runs to last inning
        home_innings[innings - 1] = remaining_home
        away_innings[innings - 1] = remaining_away
        
        # Create summary
        summary = f"Final Score: {away_team} {away_score}, {home_team} {home_score}\n\n"
        
        # Add inning-by-inning breakdown
        summary += "Inning-by-Inning:\n"
        summary += f"{'Team':<15} "
        for i in range(1, innings + 1):
            summary += f"{i:>3} "
        summary += f"{'R':>3} {'H':>3} {'E':>3}\n"
        
        # Home and away scores by inning
        summary += f"{away_team:<15} "
        for score in away_innings:
            summary += f"{score:>3} "
        away_hits = random.randint(away_score, away_score + 7)
        away_errors = random.randint(0, 2)
        summary += f"{away_score:>3} {away_hits:>3} {away_errors:>3}\n"
        
        summary += f"{home_team:<15} "
        for score in home_innings:
            summary += f"{score:>3} "
        home_hits = random.randint(home_score, home_score + 7)
        home_errors = random.randint(0, 2)
        summary += f"{home_score:>3} {home_hits:>3} {home_errors:>3}\n\n"
        
        # Player performances
        home_pitcher = f"{random.choice(['A.', 'J.', 'M.', 'T.', 'D.'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Garcia'])}"
        away_pitcher = f"{random.choice(['A.', 'J.', 'M.', 'T.', 'D.'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Garcia'])}"
        
        home_ip = random.randint(5, 9)
        home_k = random.randint(3, 10)
        home_bb = random.randint(0, 4)
        home_er = away_score if random.random() < 0.7 else min(away_score - 1, 0)
        
        away_ip = random.randint(5, 9)
        away_k = random.randint(3, 10)
        away_bb = random.randint(0, 4)
        away_er = home_score if random.random() < 0.7 else min(home_score - 1, 0)
        
        summary += "Pitching:\n"
        summary += f"{home_team}: {home_pitcher} - {home_ip} IP, {home_k} K, {home_bb} BB, {home_er} ER\n"
        summary += f"{away_team}: {away_pitcher} - {away_ip} IP, {away_k} K, {away_bb} BB, {away_er} ER\n\n"
        
        # Hitting highlights
        summary += "Hitting Highlights:\n"
        
        # Generate some hitting highlights
        for team, hits in [(home_team, home_hits), (away_team, away_hits)]:
            if hits >= 10:
                summary += f"{team} collected {hits} hits in the game.\n"
            
            # Add home runs
            hr_count = random.randint(0, min(3, hits // 3))
            if hr_count > 0:
                hr_players = []
                for _ in range(hr_count):
                    hr_players.append(f"{random.choice(['A.', 'J.', 'M.', 'T.', 'D.'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Garcia'])}")
                
                if hr_count == 1:
                    summary += f"{hr_players[0]} homered for {team}.\n"
                else:
                    summary += f"{', '.join(hr_players[:-1])} and {hr_players[-1]} hit home runs for {team}.\n"
        
        # Game narrative
        narratives = [
            f"{winner} broke a tie in the {random.choice(['7th', '8th', '9th'])} inning to secure the win.",
            f"A {random.choice(['spectacular', 'routine', 'diving'])} catch in the outfield saved at least one run for {winner}.",
            f"{winner}'s bullpen held strong, not allowing any runs in the final three innings.",
            f"{loser} threatened in the 9th with runners in scoring position, but couldn't capitalize.",
            f"A total of {random.randint(1, 4)} double plays were turned in this defensive battle."
        ]
        
        summary += f"\nGame Summary:\n{random.choice(narratives)}\n"
        
    elif sport == "NFL":
        # Generate quarter scores
        quarters = 4
        home_quarters = [0] * quarters
        away_quarters = [0] * quarters
        
        # Distribute points across quarters
        remaining_home = home_score
        remaining_away = away_score
        
        # Common football scores (TDs, FGs, etc.)
        score_options = [0, 3, 6, 7, 8]
        
        for i in range(quarters - 1):  # First 3 quarters
            if remaining_home > 0:
                possible_scores = [s for s in score_options if s <= remaining_home]
                if possible_scores:
                    score = random.choice(possible_scores)
                    home_quarters[i] = score
                    remaining_home -= score
            
            if remaining_away > 0:
                possible_scores = [s for s in score_options if s <= remaining_away]
                if possible_scores:
                    score = random.choice(possible_scores)
                    away_quarters[i] = score
                    remaining_away -= score
        
        # Assign remaining points to 4th quarter
        home_quarters[quarters - 1] = remaining_home
        away_quarters[quarters - 1] = remaining_away
        
        # Summary
        summary = f"Final Score: {away_team} {away_score}, {home_team} {home_score}\n\n"
        
        # Quarter-by-quarter breakdown
        summary += "Scoring by Quarter:\n"
        summary += f"{'Team':<15} {'1st':>5} {'2nd':>5} {'3rd':>5} {'4th':>5} {'Final':>7}\n"
        summary += f"{away_team:<15} {away_quarters[0]:>5} {away_quarters[1]:>5} {away_quarters[2]:>5} {away_quarters[3]:>5} {away_score:>7}\n"
        summary += f"{home_team:<15} {home_quarters[0]:>5} {home_quarters[1]:>5} {home_quarters[2]:>5} {home_quarters[3]:>5} {home_score:>7}\n\n"
        
        # Generate key stats
        home_qb = f"{random.choice(['A.', 'J.', 'M.', 'T.', 'D.'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Garcia'])}"
        away_qb = f"{random.choice(['A.', 'J.', 'M.', 'T.', 'D.'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Jones', 'Garcia'])}"
        
        home_passing = random.randint(150, 400)
        home_comp = random.randint(15, 30)
        home_att = home_comp + random.randint(5, 15)
        home_td_passes = random.randint(0, 3)
        home_ints = random.randint(0, 2)
        
        away_passing = random.randint(150, 400)
        away_comp = random.randint(15, 30)
        away_att = away_comp + random.randint(5, 15)
        away_td_passes = random.randint(0, 3)
        away_ints = random.randint(0, 2)
        
        # Add passing stats
        summary += "Passing:\n"
        summary += f"{home_team}: {home_qb} - {home_comp}/{home_att}, {home_passing} yards, {home_td_passes} TD, {home_ints} INT\n"
        summary += f"{away_team}: {away_qb} - {away_comp}/{away_att}, {away_passing} yards, {away_td_passes} TD, {away_ints} INT\n\n"
        
        # Add rushing stats
        home_rushes = random.randint(20, 35)
        home_rush_yards = random.randint(50, 180)
        home_rush_tds = random.randint(0, 2)
        
        away_rushes = random.randint(20, 35)
        away_rush_yards = random.randint(50, 180)
        away_rush_tds = random.randint(0, 2)
        
        summary += "Rushing:\n"
        summary += f"{home_team}: {home_rushes} attempts, {home_rush_yards} yards, {home_rush_tds} TD\n"
        summary += f"{away_team}: {away_rushes} attempts, {away_rush_yards} yards, {away_rush_tds} TD\n\n"
        
        # Game narrative
        point_diff = abs(win_score - lose_score)
        if point_diff <= 3:
            narrative = f"{winner} pulled off a nail-biting victory with a last-minute {random.choice(['field goal', 'touchdown', 'defensive stand'])}. "
        elif point_diff <= 10:
            narrative = f"{winner} controlled the tempo in a hard-fought battle against {loser}. "
        else:
            narrative = f"{winner} dominated from start to finish in a one-sided affair. "
        
        # Add key play
        key_plays = [
            f"A {random.randint(30, 70)}-yard touchdown pass broke the game open.",
            f"A crucial interception in the red zone prevented {loser} from taking the lead.",
            f"{winner} converted a critical 4th down in the final quarter to maintain possession.",
            f"A controversial pass interference call gave {winner} new life on their final drive.",
            f"{winner}'s defense forced {random.randint(2, 4)} turnovers that proved to be the difference."
        ]
        
        summary += f"Game Summary:\n{narrative}{random.choice(key_plays)}\n"
    
    else:
        # Generic summary for other sports
        summary = f"Final Score: {away_team} {away_score}, {home_team} {home_score}\n\n"
        summary += f"{winner} defeated {loser} by a score of {win_score}-{lose_score}.\n"
        
    return summary

def process_bets_for_game(conn, game_id, home_score, away_score):
    """
    Process all bets for a given game
    """
    try:
        # Get game info
        game_query = text("""
            SELECT home_team, away_team, spread, over_under FROM upcoming_games
            WHERE id = :game_id
        """)
        game = conn.execute(game_query, {"game_id": game_id}).fetchone()
        
        if not game:
            return False, "Game not found"
        
        # Determine winners
        home_covered = (home_score + game.spread) > away_score
        away_covered = (home_score + game.spread) < away_score
        push = (home_score + game.spread) == away_score
        
        total_score = home_score + away_score
        over = total_score > game.over_under
        under = total_score < game.over_under
        total_push = total_score == game.over_under
        
        moneyline_winner = "home" if home_score > away_score else "away"
        
        # Update single bets
        update_single_bets(conn, game_id, moneyline_winner, home_covered, away_covered, push, over, under, total_push)
        
        # Update parlay bets that include this game
        update_parlay_bets(conn, game_id, moneyline_winner, home_covered, away_covered, push, over, under, total_push)
        
        # Update player props
        update_player_props_results(game_id, None, conn)
        
        return True, "Bets processed successfully"
        
    except Exception as e:
        print(f"Error processing bets: {e}")
        return False, f"Error processing bets: {str(e)}"

def update_single_bets(conn, game_id, moneyline_winner, home_covered, away_covered, push, over, under, total_push):
    """
    Update single bets for a specific game
    """
    # Get all single bets for this game
    bets_query = text("""
        SELECT id, user_id, amount, bet_type, bet_pick, odds, potential_payout
        FROM bets WHERE game_id = :game_id AND status = 'pending'
    """)
    bets = conn.execute(bets_query, {"game_id": game_id}).fetchall()
    
    for bet in bets:
        bet_won = False
        
        # Check bet result based on type and pick
        if bet.bet_type == 'moneyline':
            bet_won = (bet.bet_pick == moneyline_winner)
        elif bet.bet_type == 'spread':
            if push:
                # Push - refund bet
                update_query = text("""
                    UPDATE bets SET status = 'push' WHERE id = :bet_id
                """)
                conn.execute(update_query, {"bet_id": bet.id})
                
                # Refund the bet amount
                refund_query = text("""
                    UPDATE users SET wallet_balance = wallet_balance + :amount
                    WHERE id = :user_id
                """)
                conn.execute(refund_query, {"user_id": bet.user_id, "amount": bet.amount})
                continue
            
            bet_won = (bet.bet_pick == 'home' and home_covered) or (bet.bet_pick == 'away' and away_covered)
        elif bet.bet_type == 'over_under':
            if total_push:
                # Push - refund bet
                update_query = text("""
                    UPDATE bets SET status = 'push' WHERE id = :bet_id
                """)
                conn.execute(update_query, {"bet_id": bet.id})
                
                # Refund the bet amount
                refund_query = text("""
                    UPDATE users SET wallet_balance = wallet_balance + :amount
                    WHERE id = :user_id
                """)
                conn.execute(refund_query, {"user_id": bet.user_id, "amount": bet.amount})
                continue
            
            bet_won = (bet.bet_pick == 'over' and over) or (bet.bet_pick == 'under' and under)
        
        # Update bet status
        status = 'won' if bet_won else 'lost'
        update_query = text("""
            UPDATE bets SET status = :status WHERE id = :bet_id
        """)
        conn.execute(update_query, {"bet_id": bet.id, "status": status})
        
        # If bet won, add payout to user's wallet
        if bet_won:
            payout_query = text("""
                UPDATE users SET wallet_balance = wallet_balance + :payout
                WHERE id = :user_id
            """)
            conn.execute(payout_query, {"user_id": bet.user_id, "payout": bet.potential_payout})

def update_parlay_bets(conn, game_id, moneyline_winner, home_covered, away_covered, push, over, under, total_push):
    """
    Update all parlay legs related to this game
    """
    # Get all parlay bets that include this game
    parlay_bets_query = text("""
        SELECT pb.id, pb.parlay_id, pb.user_id, pb.bet_type, pb.bet_pick, pb.potential_payout
        FROM parlay_bets pb
        WHERE pb.game_id = :game_id AND pb.status = 'pending'
    """)
    parlay_bets = conn.execute(parlay_bets_query, {"game_id": game_id}).fetchall()
    
    parlay_results = {}
    
    for pb in parlay_bets:
        bet_won = False
        is_push = False
        
        # Check bet result based on type and pick
        if pb.bet_type == 'moneyline':
            bet_won = (pb.bet_pick == moneyline_winner)
        elif pb.bet_type == 'spread':
            if push:
                is_push = True
            else:
                bet_won = (pb.bet_pick == 'home' and home_covered) or (pb.bet_pick == 'away' and away_covered)
        elif pb.bet_type == 'over_under':
            if total_push:
                is_push = True
            else:
                bet_won = (pb.bet_pick == 'over' and over) or (pb.bet_pick == 'under' and under)
        
        # Handle pushes in parlays (typically removes this leg and recalculates)
        if is_push:
            # Get original parlay odds
            parlay_query = text("SELECT odds FROM parlays WHERE id = :parlay_id")
            parlay = conn.execute(parlay_query, {"parlay_id": pb.parlay_id}).fetchone()
            
            if parlay:
                # Remove this leg from the parlay by setting status to 'push'
                conn.execute(
                    text("UPDATE parlay_bets SET status = 'push' WHERE id = :bet_id"),
                    {"bet_id": pb.id}
                )
                
                continue  # Skip further processing for this leg
        
        # Update leg status
        status = 'won' if bet_won else 'lost'
        conn.execute(
            text("UPDATE parlay_bets SET status = :status WHERE id = :bet_id"),
            {"bet_id": pb.id, "status": status}
        )
        
        # If any leg lost, the entire parlay loses
        if not bet_won:
            if pb.parlay_id not in parlay_results:
                parlay_results[pb.parlay_id] = {
                    'user_id': pb.user_id,
                    'potential_payout': pb.potential_payout,
                    'status': 'lost'
                }
            else:
                parlay_results[pb.parlay_id]['status'] = 'lost'
        else:
            if pb.parlay_id not in parlay_results:
                parlay_results[pb.parlay_id] = {
                    'user_id': pb.user_id,
                    'potential_payout': pb.potential_payout,
                    'status': 'pending'  # Still need to check other legs
                }
    
    # Check if any parlays are complete
    for parlay_id, result in parlay_results.items():
        # Check if all legs have been decided
        remaining_query = text("""
            SELECT COUNT(*) FROM parlay_bets
            WHERE parlay_id = :parlay_id AND status = 'pending'
        """)
        remaining = conn.execute(remaining_query, {"parlay_id": parlay_id}).fetchone()[0]
        
        if remaining == 0:
            # Check if all legs won
            lost_query = text("""
                SELECT COUNT(*) FROM parlay_bets
                WHERE parlay_id = :parlay_id AND status = 'lost'
            """)
            lost_count = conn.execute(lost_query, {"parlay_id": parlay_id}).fetchone()[0]
            
            if lost_count == 0:
                # All legs won, parlay wins
                conn.execute(
                    text("UPDATE parlays SET status = 'won' WHERE id = :parlay_id"),
                    {"parlay_id": parlay_id}
                )
                
                # Add payout to user's wallet
                conn.execute(
                    text("UPDATE users SET wallet_balance = wallet_balance + :payout WHERE id = :user_id"),
                    {"user_id": result['user_id'], "payout": result['potential_payout']}
                )
            else:
                # At least one leg lost, parlay loses
                conn.execute(
                    text("UPDATE parlays SET status = 'lost' WHERE id = :parlay_id"),
                    {"parlay_id": parlay_id}
                )

def update_player_props_results(game_id, sport, conn):
    """
    Update player prop results for a game
    """
    try:
        # Get all player props for this game
        props_query = text("""
            SELECT id, player_name, prop_type, line_value
            FROM player_props WHERE game_id = :game_id
        """)
        props = conn.execute(props_query, {"game_id": game_id}).fetchall()
        
        for prop in props:
            # Generate random result based on prop type
            actual_value = None
            
            if prop.prop_type == "points":
                actual_value = round(random.uniform(prop.line_value - 10, prop.line_value + 10), 1)
            elif prop.prop_type == "rebounds":
                actual_value = round(random.uniform(prop.line_value - 5, prop.line_value + 5), 1)
            elif prop.prop_type == "assists":
                actual_value = round(random.uniform(prop.line_value - 4, prop.line_value + 4), 1)
            elif prop.prop_type == "home_runs":
                actual_value = random.randint(0, 1) if prop.line_value <= 0.5 else 0
            elif prop.prop_type == "hits":
                actual_value = random.randint(0, 3)
            elif prop.prop_type == "strikeouts":
                actual_value = round(random.uniform(prop.line_value - 3, prop.line_value + 3), 1)
            else:
                actual_value = round(random.uniform(prop.line_value - 2, prop.line_value + 2), 1)
            
            # Determine result
            over_result = actual_value > prop.line_value
            
            # Update player prop with result
            result_query = text("""
                UPDATE player_props
                SET actual_value = :actual_value, over_result = :over_result
                WHERE id = :prop_id
            """)
            
            try:
                conn.execute(result_query, {
                    "prop_id": prop.id,
                    "actual_value": actual_value,
                    "over_result": over_result
                })
            except:
                # If actual_value column doesn't exist, create it
                try:
                    conn.execute(text("""
                        ALTER TABLE player_props
                        ADD COLUMN IF NOT EXISTS actual_value DOUBLE PRECISION,
                        ADD COLUMN IF NOT EXISTS over_result BOOLEAN
                    """))
                    
                    # Try again
                    conn.execute(result_query, {
                        "prop_id": prop.id,
                        "actual_value": actual_value,
                        "over_result": over_result
                    })
                except Exception as e:
                    print(f"Error adding columns: {e}")
            
            # Update player performance in players table
            if prop.player_name:
                # First check if player exists
                player_query = text("""
                    SELECT id FROM player_data
                    WHERE name = :player_name LIMIT 1
                """)
                player = conn.execute(player_query, {"player_name": prop.player_name}).fetchone()
                
                if player:
                    # Calculate fantasy points based on performance
                    fantasy_points = 0
                    
                    if prop.prop_type == "points":
                        fantasy_points = actual_value * 1.0
                    elif prop.prop_type == "rebounds":
                        fantasy_points = actual_value * 1.2
                    elif prop.prop_type == "assists":
                        fantasy_points = actual_value * 1.5
                    elif prop.prop_type == "home_runs":
                        fantasy_points = actual_value * 4.0
                    elif prop.prop_type == "hits":
                        fantasy_points = actual_value * 1.0
                    elif prop.prop_type == "strikeouts":
                        fantasy_points = actual_value * 0.5
                    
                    # Update player fantasy points
                    try:
                        update_query = text("""
                            UPDATE player_data
                            SET last_fantasy_points = :fantasy_points
                            WHERE id = :player_id
                        """)
                        conn.execute(update_query, {
                            "player_id": player.id,
                            "fantasy_points": fantasy_points
                        })
                    except Exception as e:
                        print(f"Error updating player fantasy points: {e}")
        
        return True, "Player props updated successfully"
    
    except Exception as e:
        print(f"Error updating player props: {e}")
        return False, f"Error updating player props: {str(e)}"

def update_player_performance_from_game(conn, game_id, sport, home_team, away_team, home_score, away_score):
    """
    Update player performance data based on game results
    """
    try:
        # Determine winning and losing teams
        winning_team = home_team if home_score > away_score else away_team
        losing_team = away_team if home_score > away_score else home_team
        
        # Update players from the winning team
        winning_query = text("""
            UPDATE player_data
            SET last_fantasy_points = 
                CASE
                    WHEN position = 'QB' THEN RANDOM() * 30 + 10
                    WHEN position IN ('RB', 'WR', 'TE') THEN RANDOM() * 20 + 5
                    WHEN position = 'K' THEN RANDOM() * 10 + 3
                    WHEN position = 'DEF' THEN RANDOM() * 15 + 5
                    WHEN position IN ('PG', 'SG', 'SF', 'PF', 'C') THEN RANDOM() * 40 + 10
                    WHEN position IN ('P', 'SP', 'RP') THEN RANDOM() * 25 + 5
                    WHEN position IN ('1B', '2B', '3B', 'SS', 'C', 'OF', 'DH') THEN RANDOM() * 15 + 3
                    ELSE RANDOM() * 10 + 5
                END,
                weekly_change = RANDOM() * 0.10 + 0.01
            WHERE team ILIKE :team_pattern AND sport = :sport
        """)
        
        conn.execute(winning_query, {
            "team_pattern": f"%{winning_team}%",
            "sport": sport
        })
        
        # Update players from the losing team
        losing_query = text("""
            UPDATE player_data
            SET last_fantasy_points = 
                CASE
                    WHEN position = 'QB' THEN RANDOM() * 15 + 5
                    WHEN position IN ('RB', 'WR', 'TE') THEN RANDOM() * 15 + 3
                    WHEN position = 'K' THEN RANDOM() * 7 + 1
                    WHEN position = 'DEF' THEN RANDOM() * 10 + 2
                    WHEN position IN ('PG', 'SG', 'SF', 'PF', 'C') THEN RANDOM() * 30 + 5
                    WHEN position IN ('P', 'SP', 'RP') THEN RANDOM() * 20 + 2
                    WHEN position IN ('1B', '2B', '3B', 'SS', 'C', 'OF', 'DH') THEN RANDOM() * 10 + 1
                    ELSE RANDOM() * 8 + 2
                END,
                weekly_change = RANDOM() * -0.08 - 0.01
            WHERE team ILIKE :team_pattern AND sport = :sport
        """)
        
        conn.execute(losing_query, {
            "team_pattern": f"%{losing_team}%",
            "sport": sport
        })
        
        # If it's MLB, apply strikeout and fielding error penalties
        if sport == "MLB":
            # Apply penalties for losing team (more errors, more strikeouts)
            losing_strikeouts_query = text("""
                UPDATE player_data
                SET last_fantasy_points = last_fantasy_points - 2
                WHERE team ILIKE :team_pattern AND sport = 'MLB' 
                AND position IN ('1B', '2B', '3B', 'SS', 'C', 'OF', 'DH') 
                AND RANDOM() < 0.6
            """)
            
            conn.execute(losing_strikeouts_query, {
                "team_pattern": f"%{losing_team}%"
            })
            
            # Apply fielding error penalties
            losing_error_query = text("""
                UPDATE player_data
                SET last_fantasy_points = last_fantasy_points - 2
                WHERE team ILIKE :team_pattern AND sport = 'MLB' 
                AND position IN ('1B', '2B', '3B', 'SS') 
                AND RANDOM() < 0.3
            """)
            
            conn.execute(losing_error_query, {
                "team_pattern": f"%{losing_team}%"
            })
        
        # Create game result news for top players
        add_game_result_news(conn, sport, winning_team, losing_team, home_score, away_score)
        
        return True, "Player performance updated successfully"
    
    except Exception as e:
        print(f"Error updating player performance: {e}")
        return False, f"Error updating player performance: {str(e)}"

def add_game_result_news(conn, sport, winning_team, losing_team, home_score, away_score):
    """
    Add news entries about game results for top players
    """
    try:
        # Get top 2 players from winning team
        winning_players_query = text("""
            SELECT id, name, position 
            FROM player_data 
            WHERE team ILIKE :team_pattern AND sport = :sport
            ORDER BY current_price DESC
            LIMIT 2
        """)
        
        winning_players = conn.execute(winning_players_query, {
            "team_pattern": f"%{winning_team}%",
            "sport": sport
        }).fetchall()
        
        # Get top player from losing team
        losing_player_query = text("""
            SELECT id, name, position 
            FROM player_data 
            WHERE team ILIKE :team_pattern AND sport = :sport
            ORDER BY current_price DESC
            LIMIT 1
        """)
        
        losing_player = conn.execute(losing_player_query, {
            "team_pattern": f"%{losing_team}%",
            "sport": sport
        }).fetchone()
        
        # Create news table if it doesn't exist
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS player_news (
                id SERIAL PRIMARY KEY,
                player_id INTEGER NOT NULL,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                impact VARCHAR(10) NOT NULL,
                published_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """))
        
        # Add news for winning players
        for player in winning_players:
            news_title = ""
            news_content = ""
            news_impact = "positive"
            
            if sport == "NBA":
                if player.position in ['PG', 'SG']:
                    news_title = f"{player.name} shines in {winning_team}'s victory"
                    news_content = f"{player.name} had an outstanding game, leading {winning_team} to a {home_score}-{away_score} win over {losing_team}. His shooting and playmaking were key factors in the victory."
                elif player.position in ['SF', 'PF', 'C']:
                    news_title = f"{player.name} dominates in {winning_team} win"
                    news_content = f"{player.name} dominated on both ends of the floor as {winning_team} defeated {losing_team} {home_score}-{away_score}. His inside presence was a difference-maker."
            elif sport == "MLB":
                if player.position in ['P', 'SP', 'RP']:
                    news_title = f"{player.name} delivers stellar pitching performance"
                    news_content = f"{player.name} pitched a gem as {winning_team} defeated {losing_team} {home_score}-{away_score}. His command was excellent throughout the game."
                else:
                    news_title = f"{player.name} leads {winning_team} offense in win"
                    news_content = f"{player.name} powered the {winning_team} offense in their {home_score}-{away_score} victory over {losing_team}. His timely hitting was crucial to the win."
            elif sport == "NFL":
                if player.position == 'QB':
                    news_title = f"{player.name} throws for multiple TDs in victory"
                    news_content = f"{player.name} delivered an efficient performance, leading {winning_team} to a {home_score}-{away_score} win over {losing_team}. His decision-making was exceptional throughout the game."
                elif player.position in ['RB', 'WR', 'TE']:
                    news_title = f"{player.name} has big game in {winning_team} win"
                    news_content = f"{player.name} was a key contributor as {winning_team} defeated {losing_team} {home_score}-{away_score}. He made several impact plays that helped secure the victory."
            else:
                news_title = f"{player.name} stars in {winning_team} win"
                news_content = f"{player.name} played a pivotal role in {winning_team}'s {home_score}-{away_score} victory over {losing_team}."
            
            # Insert news
            news_query = text("""
                INSERT INTO player_news (player_id, title, content, impact, published_at)
                VALUES (:player_id, :title, :content, :impact, NOW())
            """)
            
            conn.execute(news_query, {
                "player_id": player.id,
                "title": news_title,
                "content": news_content,
                "impact": news_impact
            })
        
        # Add news for losing player
        if losing_player:
            news_title = ""
            news_content = ""
            news_impact = "negative"
            
            if sport == "NBA":
                news_title = f"{losing_player.name} struggles in {losing_team}'s loss"
                news_content = f"Despite his efforts, {losing_player.name} couldn't help {losing_team} avoid a {home_score}-{away_score} defeat to {winning_team}. The team will look to bounce back in their next game."
            elif sport == "MLB":
                if losing_player.position in ['P', 'SP', 'RP']:
                    news_title = f"{losing_player.name} takes the loss against {winning_team}"
                    news_content = f"{losing_player.name} and {losing_team} fell short in a {home_score}-{away_score} defeat to {winning_team}. They'll look to rebound in their next outing."
                else:
                    news_title = f"{losing_player.name} and {losing_team} fall to {winning_team}"
                    news_content = f"{losing_player.name} couldn't help {losing_team} avoid a {home_score}-{away_score} loss to {winning_team}. The offense struggled to generate consistent production."
            elif sport == "NFL":
                news_title = f"{losing_player.name} and {losing_team} come up short"
                news_content = f"{losing_player.name} and {losing_team} suffered a {home_score}-{away_score} defeat to {winning_team}. The team will need to address several issues before their next game."
            else:
                news_title = f"{losing_player.name} can't prevent {losing_team} loss"
                news_content = f"{losing_player.name} and {losing_team} fell to {winning_team} {home_score}-{away_score}. They'll look to bounce back in their next matchup."
            
            # Insert news
            news_query = text("""
                INSERT INTO player_news (player_id, title, content, impact, published_at)
                VALUES (:player_id, :title, :content, :impact, NOW())
            """)
            
            conn.execute(news_query, {
                "player_id": losing_player.id,
                "title": news_title,
                "content": news_content,
                "impact": news_impact
            })
        
        return True, "Game news added successfully"
    
    except Exception as e:
        print(f"Error adding game news: {e}")
        return False, f"Error adding game news: {str(e)}"

def get_team_top_players(team_name, sport):
    """
    Get top players for a team from the database
    """
    try:
        engine = create_engine(os.environ.get('DATABASE_URL'))
        with engine.connect() as conn:
            # Try to find players matching the team in the database
            team_query = text("""
                SELECT name FROM player_data
                WHERE team ILIKE :team_pattern AND sport = :sport
                ORDER BY current_price DESC LIMIT 3
            """)
            
            players = conn.execute(team_query, {
                "team_pattern": f"%{team_name}%",
                "sport": sport
            }).fetchall()
            
            if players:
                return [p[0] for p in players]
            return None
    
    except Exception as e:
        print(f"Error getting team players: {e}")
        return None