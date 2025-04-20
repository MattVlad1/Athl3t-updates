"""
Update script to ensure all sports have proper market values and fantasy stats
"""

import pandas as pd
import random
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import os
import decimal
from decimal import Decimal
import json

# Connect to database
DATABASE_URL = os.environ.get('DATABASE_URL')
engine = create_engine(DATABASE_URL)

def update_mlb_nba_player_stats():
    """Update MLB and NBA player statistics to match NFL player structure"""
    
    try:
        with engine.connect() as conn:
            # Get MLB and NBA players that don't have fantasy points
            query = text("""
                SELECT id, name, team, position, sport, current_price, tier
                FROM players
                WHERE sport IN ('MLB', 'NBA') 
                LIMIT 25 -- Process just a subset of players for efficiency
            """)
            
            players = conn.execute(query).fetchall()
            count = 0
            
            for player in players:
                player_id = player.id
                sport = player.sport
                position = player.position
                
                # Generate realistic fantasy points based on position and sport
                fantasy_points = 0
                performance_tier = "Average"
                
                if sport == 'NBA':
                    if position in ['PG', 'SG']:
                        fantasy_points = random.uniform(15.0, 25.0)  # Point guards, shooting guards
                    elif position in ['SF', 'PF']:
                        fantasy_points = random.uniform(16.0, 26.0)  # Forwards
                    elif position == 'C':
                        fantasy_points = random.uniform(17.0, 27.0)  # Centers
                    else:
                        fantasy_points = random.uniform(15.0, 25.0)  # Default
                
                elif sport == 'MLB':
                    if position == 'P':
                        fantasy_points = random.uniform(15.0, 25.0)  # Pitchers
                    elif position == 'C':
                        fantasy_points = random.uniform(12.0, 20.0)  # Catchers
                    elif position in ['1B', '3B']:
                        fantasy_points = random.uniform(14.0, 22.0)  # Corner infielders
                    elif position in ['2B', 'SS']:
                        fantasy_points = random.uniform(13.0, 21.0)  # Middle infielders
                    elif position in ['OF', 'RF', 'LF', 'CF']:
                        fantasy_points = random.uniform(14.0, 22.0)  # Outfielders
                    elif position == 'DH':
                        fantasy_points = random.uniform(15.0, 23.0)  # Designated hitters
                    else:
                        fantasy_points = random.uniform(14.0, 22.0)  # Default
                
                # Determine weekly price change (similar to NFL players)
                weekly_change = random.uniform(-10.0, 15.0)
                
                # Determine performance tier based on weekly change
                if weekly_change > 10:
                    performance_tier = "Excellent"
                elif weekly_change > 5:
                    performance_tier = "Very Good"
                elif weekly_change > 0:
                    performance_tier = "Good"
                elif weekly_change > -5:
                    performance_tier = "Below Average"
                else:
                    performance_tier = "Poor"
                
                # Update the player record with fantasy stats and performance metrics
                update_query = text("""
                    UPDATE players
                    SET 
                        last_fantasy_points = :fantasy_points,
                        weekly_change = :weekly_change,
                        performance_tier = :performance_tier,
                        last_updated = CURRENT_TIMESTAMP
                    WHERE id = :player_id
                """)
                
                conn.execute(update_query, {
                    "fantasy_points": fantasy_points,
                    "weekly_change": weekly_change,
                    "performance_tier": performance_tier,
                    "player_id": player_id
                })
                
                count += 1
            
            conn.commit()
            return count
                
    except Exception as e:
        print(f"Error updating MLB/NBA players: {str(e)}")
        return 0

def add_performance_history():
    """Add performance history entries for MLB and NBA players"""
    
    try:
        with engine.connect() as conn:
            # Get MLB and NBA players
            query = text("""
                SELECT id, name, current_price, sport, team
                FROM players
                WHERE sport IN ('MLB', 'NBA')
                LIMIT 25 -- Process just a subset of players for efficiency
            """)
            
            players = conn.execute(query).fetchall()
            count = 0
            
            for player in players:
                player_id = player.id
                player_name = player.name
                current_price = player.current_price
                sport = player.sport
                team = player.team
                
                # Check if player already has history entries
                check_query = text("""
                    SELECT COUNT(*) as count 
                    FROM player_performance_history
                    WHERE player_name = :player_name
                """)
                
                result = conn.execute(check_query, {"player_name": player_name}).fetchone()
                
                # If player doesn't have history, add some entries
                if result and result.count < 5:
                    # Create just 7 days of price history for now to speed up processing
                    today = datetime.now().date()
                    
                    for i in range(1, 8):
                        date = today - timedelta(days=i)
                        
                        # Random price fluctuation (within reasonable bounds)
                        price_change_pct = random.uniform(-3.0, 3.0)
                        
                        # Calculate previous price based on current price and random change
                        # This creates a somewhat realistic price history
                        modifier = 1.0 + (price_change_pct / 100.0)
                        # Convert to float first to avoid decimal.Decimal errors
                        current_price_float = float(current_price)
                        previous_price = current_price_float / modifier
                        
                        # Ensure minimum price
                        previous_price = max(0.01, previous_price)
                        
                        # Random fantasy points
                        fantasy_points = random.uniform(10.0, 30.0)
                        
                        # Generate opponent team name
                        if sport == 'NBA':
                            opponents = ["Lakers", "Celtics", "Bulls", "Warriors", "Nets", "Heat", 
                                        "Suns", "Mavericks", "76ers", "Nuggets", "Bucks", "Grizzlies"]
                        elif sport == 'MLB':
                            opponents = ["Yankees", "Dodgers", "Red Sox", "Cubs", "Astros", "Braves",
                                        "Mets", "Cardinals", "Padres", "Giants", "Phillies", "Blue Jays"]
                        else:
                            opponents = ["Team A", "Team B", "Team C"]
                            
                        # Make sure opponent is not the player's team
                        opponents = [opp for opp in opponents if opp != team]
                        opponent = random.choice(opponents) if opponents else "Another Team"
                        
                        # Create performance stats JSON based on sport
                        performance_stats = {}
                        if sport == 'NBA':
                            performance_stats = {
                                "points": round(random.uniform(5, 30)),
                                "rebounds": round(random.uniform(1, 15)),
                                "assists": round(random.uniform(1, 12)),
                                "steals": round(random.uniform(0, 5)),
                                "blocks": round(random.uniform(0, 4))
                            }
                        elif sport == 'MLB':
                            if random.random() > 0.7:  # Pitcher
                                performance_stats = {
                                    "innings_pitched": round(random.uniform(3, 9), 1),
                                    "strikeouts": round(random.uniform(2, 12)),
                                    "earned_runs": round(random.uniform(0, 6)),
                                    "walks": round(random.uniform(0, 5))
                                }
                            else:  # Batter
                                performance_stats = {
                                    "hits": round(random.uniform(0, 4)),
                                    "runs": round(random.uniform(0, 3)),
                                    "rbis": round(random.uniform(0, 4)),
                                    "home_runs": round(random.uniform(0, 2))
                                }
                        
                        # Add to history
                        history_query = text("""
                            INSERT INTO player_performance_history
                            (player_name, game_date, opponent, fantasy_points, performance_stats, price_before, price_after, price_change_pct)
                            VALUES 
                            (:player_name, :game_date, :opponent, :fantasy_points, :performance_stats, :price_before, :price_after, :price_change_pct)
                        """)
                        
                        conn.execute(history_query, {
                            "player_name": player_name,
                            "game_date": date,
                            "opponent": opponent,
                            "fantasy_points": fantasy_points,
                            "performance_stats": json.dumps(performance_stats),  # Convert dict to JSON string
                            "price_before": previous_price,
                            "price_after": float(current_price),  # Convert Decimal to float
                            "price_change_pct": price_change_pct
                        })
                        
                        # Update current price for next iteration to create a chain of prices
                        current_price = previous_price
                        
                        count += 1
            
            conn.commit()
            return count
                
    except Exception as e:
        print(f"Error adding performance history: {str(e)}")
        return 0

if __name__ == "__main__":
    updated_count = update_mlb_nba_player_stats()
    history_count = add_performance_history()
    print(f"Updated {updated_count} MLB and NBA players with fantasy stats")
    print(f"Added {history_count} performance history entries")