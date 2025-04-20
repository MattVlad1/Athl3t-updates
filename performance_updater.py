"""
Performance updater for player share prices based on fantasy sports metrics.
This module automatically adjusts player market values based on their performance.
"""

import pandas as pd
from datetime import datetime, timedelta
import random  # For demo purposes only, will be replaced with real stats API
from sqlalchemy import create_engine, text
import os
from db import engine

# Fantasy scoring metrics by position and sport
FANTASY_METRICS = {
    # NFL scoring
    'QB': {
        'passing_yards': 0.04,  # 1 point per 25 passing yards
        'passing_td': 4,        # 4 points per passing TD
        'interception': -1,     # -1 point per interception
        'rushing_yards': 0.1,   # 1 point per 10 rushing yards
        'rushing_td': 6,        # 6 points per rushing TD
        'fumble_lost': -2       # -2 points per fumble lost
    },
    'RB': {
        'rushing_yards': 0.1,   # 1 point per 10 rushing yards
        'rushing_td': 6,        # 6 points per rushing TD
        'receiving_yards': 0.1, # 1 point per 10 receiving yards
        'receiving_td': 6,      # 6 points per receiving TD
        'fumble_lost': -2       # -2 points per fumble lost
    },
    'WR': {
        'receiving_yards': 0.1, # 1 point per 10 receiving yards
        'receiving_td': 6,      # 6 points per receiving TD
        'rushing_yards': 0.1,   # 1 point per 10 rushing yards (on reverses, etc.)
        'rushing_td': 6,        # 6 points per rushing TD
        'fumble_lost': -2       # -2 points per fumble lost
    },
    'TE': {
        'receiving_yards': 0.1, # 1 point per 10 receiving yards
        'receiving_td': 6,      # 6 points per receiving TD
        'fumble_lost': -2       # -2 points per fumble lost
    },
    'K': {
        'fg_0_39': 3,           # 3 points for FGs 0-39 yards
        'fg_40_49': 4,          # 4 points for FGs 40-49 yards
        'fg_50_plus': 5,        # 5 points for FGs 50+ yards
        'pat': 1,               # 1 point for PATs
        'fg_missed': -1         # -1 point for missed FGs
    },
    'DEF': {
        'sack': 1,              # 1 point per sack
        'interception': 2,      # 2 points per interception
        'fumble_recovery': 2,   # 2 points per fumble recovery
        'td': 6,                # 6 points per defensive TD
        'safety': 2,            # 2 points per safety
        'points_allowed_0': 10, # 10 points for shutout
        'points_allowed_1_6': 7, # 7 points for 1-6 points allowed
        'points_allowed_7_13': 4, # 4 points for 7-13 points allowed
        'points_allowed_14_20': 1, # 1 point for 14-20 points allowed
        'points_allowed_21_27': 0, # 0 points for 21-27 points allowed
        'points_allowed_28_34': -1, # -1 point for 28-34 points allowed
        'points_allowed_35_plus': -4 # -4 points for 35+ points allowed
    },
    
    # MLB scoring
    'P': {
        'inning_pitched': 2.25,  # 2.25 points per inning pitched
        'strikeout': 1,          # 1 point per strikeout
        'win': 4,                # 4 points for a win
        'save': 5,               # 5 points for a save
        'earned_run': -2,        # -2 points per earned run
        'hit_allowed': -0.6,     # -0.6 points per hit allowed
        'walk_allowed': -0.6,    # -0.6 points per walk allowed
        'hit_batsman': -0.6,     # -0.6 points per hit batsman
        'complete_game': 2.5,    # 2.5 bonus points for complete game
        'complete_game_shutout': 5  # 5 bonus points for complete game shutout
    },
    'C': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    '1B': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    '2B': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    '3B': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    'SS': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    'OF': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    'DH': {
        'run': 1.5,              # 1.5 points per run
        'hit': 2,                # 2 points per hit
        'home_run': 4,           # 4 points per home run
        'rbi': 2,                # 2 points per RBI
        'walk': 1,               # 1 point per walk
        'stolen_base': 5,        # 5 points per stolen base
        'caught_stealing': -1    # -1 point per caught stealing
    },
    
    # NBA/WNBA scoring
    'PG': {
        'point': 1,              # 1 point per point scored
        'rebound': 1.2,          # 1.2 points per rebound
        'assist': 1.5,           # 1.5 points per assist
        'steal': 2,              # 2 points per steal
        'block': 2,              # 2 points per block
        'turnover': -1,          # -1 point per turnover
        'three_pointer': 0.5,    # 0.5 bonus points per 3-pointer made
        'double_double': 1.5,    # 1.5 bonus points for double-double
        'triple_double': 3       # 3 bonus points for triple-double
    },
    'SG': {
        'point': 1,              # 1 point per point scored
        'rebound': 1.2,          # 1.2 points per rebound
        'assist': 1.5,           # 1.5 points per assist
        'steal': 2,              # 2 points per steal
        'block': 2,              # 2 points per block
        'turnover': -1,          # -1 point per turnover
        'three_pointer': 0.5,    # 0.5 bonus points per 3-pointer made
        'double_double': 1.5,    # 1.5 bonus points for double-double
        'triple_double': 3       # 3 bonus points for triple-double
    },
    'SF': {
        'point': 1,              # 1 point per point scored
        'rebound': 1.2,          # 1.2 points per rebound
        'assist': 1.5,           # 1.5 points per assist
        'steal': 2,              # 2 points per steal
        'block': 2,              # 2 points per block
        'turnover': -1,          # -1 point per turnover
        'three_pointer': 0.5,    # 0.5 bonus points per 3-pointer made
        'double_double': 1.5,    # 1.5 bonus points for double-double
        'triple_double': 3       # 3 bonus points for triple-double
    },
    'PF': {
        'point': 1,              # 1 point per point scored
        'rebound': 1.2,          # 1.2 points per rebound
        'assist': 1.5,           # 1.5 points per assist
        'steal': 2,              # 2 points per steal
        'block': 2,              # 2 points per block
        'turnover': -1,          # -1 point per turnover
        'three_pointer': 0.5,    # 0.5 bonus points per 3-pointer made
        'double_double': 1.5,    # 1.5 bonus points for double-double
        'triple_double': 3       # 3 bonus points for triple-double
    },
    'C': {
        'point': 1,              # 1 point per point scored
        'rebound': 1.2,          # 1.2 points per rebound
        'assist': 1.5,           # 1.5 points per assist
        'steal': 2,              # 2 points per steal
        'block': 2,              # 2 points per block
        'turnover': -1,          # -1 point per turnover
        'three_pointer': 0.5,    # 0.5 bonus points per 3-pointer made
        'double_double': 1.5,    # 1.5 bonus points for double-double
        'triple_double': 3       # 3 bonus points for triple-double
    }
}

# Performance percentile tiers for price adjustments
PERFORMANCE_TIERS = {
    'exceptional': {'percentile': 95, 'adjustment': 0.15},  # Top 5% -> +15%
    'excellent': {'percentile': 90, 'adjustment': 0.10},    # Top 10% -> +10%
    'very_good': {'percentile': 80, 'adjustment': 0.07},    # Top 20% -> +7%
    'good': {'percentile': 70, 'adjustment': 0.05},         # Top 30% -> +5%
    'above_average': {'percentile': 60, 'adjustment': 0.03},# Top 40% -> +3%
    'average': {'percentile': 50, 'adjustment': 0.01},      # Middle 20% -> +1%
    'below_average': {'percentile': 40, 'adjustment': -0.01},# Bottom 40% -> -1%
    'poor': {'percentile': 30, 'adjustment': -0.03},        # Bottom 30% -> -3%
    'very_poor': {'percentile': 20, 'adjustment': -0.05},   # Bottom 20% -> -5%
    'terrible': {'percentile': 10, 'adjustment': -0.10},    # Bottom 10% -> -10%
    'disastrous': {'percentile': 5, 'adjustment': -0.15}    # Bottom 5% -> -15%
}

def get_player_stats_for_period(player_name, position, sport, start_date, end_date):
    """
    Get player statistics for a given period.
    In a real implementation, this would fetch data from a sports statistics API.
    For this demo, we'll generate random representative stats.
    
    Args:
        player_name (str): Name of the player
        position (str): Player's position
        sport (str): Sport (NFL, MLB, NBA, etc.)
        start_date (datetime): Start date for stats
        end_date (datetime): End date for stats
    
    Returns:
        dict: Dictionary of player statistics
    """
    # For demo purposes only - would be replaced with actual API calls
    
    # Determine position category for appropriate stat generation
    if position in FANTASY_METRICS:
        pos_category = position
    elif position in ['WR1', 'WR2', 'WR3', 'Slot']:
        pos_category = 'WR'
    elif position in ['LF', 'CF', 'RF']:
        pos_category = 'OF'
    elif sport == 'NFL':
        pos_category = 'QB'  # Default for NFL
    elif sport == 'MLB':
        pos_category = '1B'  # Default for MLB
    elif sport in ['NBA', 'WNBA']:
        pos_category = 'PG'  # Default for basketball
    else:
        pos_category = 'QB'  # General default
    
    # Generate appropriate random stats based on position
    stats = {}
    
    # NFL stats generation
    if sport == 'NFL':
        if pos_category == 'QB':
            stats = {
                'passing_yards': random.randint(150, 400),
                'passing_td': random.randint(0, 4),
                'interception': random.randint(0, 2),
                'rushing_yards': random.randint(0, 50),
                'rushing_td': random.randint(0, 1),
                'fumble_lost': random.randint(0, 1)
            }
        elif pos_category in ['RB', 'WR', 'TE']:
            receiving_yards = 0
            rushing_yards = 0
            
            if pos_category == 'RB':
                rushing_yards = random.randint(30, 120)
                receiving_yards = random.randint(0, 50)
            elif pos_category == 'WR':
                receiving_yards = random.randint(40, 150)
                rushing_yards = random.randint(0, 15)
            elif pos_category == 'TE':
                receiving_yards = random.randint(20, 80)
            
            stats = {
                'rushing_yards': rushing_yards,
                'rushing_td': random.randint(0, 1 if pos_category == 'RB' else 0),
                'receiving_yards': receiving_yards,
                'receiving_td': random.randint(0, 2 if pos_category == 'WR' else 1),
                'fumble_lost': random.randint(0, 1)
            }
        elif pos_category == 'K':
            stats = {
                'fg_0_39': random.randint(0, 2),
                'fg_40_49': random.randint(0, 2),
                'fg_50_plus': random.randint(0, 1),
                'pat': random.randint(1, 5),
                'fg_missed': random.randint(0, 1)
            }
        elif pos_category == 'DEF':
            points_allowed = random.randint(0, 35)
            points_allowed_category = 'points_allowed_0'
            
            if points_allowed > 0 and points_allowed <= 6:
                points_allowed_category = 'points_allowed_1_6'
            elif points_allowed > 6 and points_allowed <= 13:
                points_allowed_category = 'points_allowed_7_13'
            elif points_allowed > 13 and points_allowed <= 20:
                points_allowed_category = 'points_allowed_14_20'
            elif points_allowed > 20 and points_allowed <= 27:
                points_allowed_category = 'points_allowed_21_27'
            elif points_allowed > 27 and points_allowed <= 34:
                points_allowed_category = 'points_allowed_28_34'
            elif points_allowed > 34:
                points_allowed_category = 'points_allowed_35_plus'
            
            stats = {
                'sack': random.randint(1, 5),
                'interception': random.randint(0, 2),
                'fumble_recovery': random.randint(0, 2),
                'td': random.randint(0, 1),
                'safety': random.randint(0, 1),
                points_allowed_category: 1  # Flag for the appropriate points allowed category
            }
    
    # MLB stats generation
    elif sport in ['MLB', 'College Baseball', 'Softball']:
        if pos_category == 'P':
            innings = random.randint(3, 7)
            complete_game = 1 if innings >= 9 else 0
            earned_runs = random.randint(0, 5)
            complete_game_shutout = 1 if complete_game and earned_runs == 0 else 0
            
            # Include potential fielding errors for pitchers
            fielding_errors = 1 if random.random() < 0.10 else 0  # 10% chance of error
            
            stats = {
                'inning_pitched': innings,
                'strikeout': random.randint(2, 10),
                'win': random.randint(0, 1),
                'save': random.randint(0, 1) if innings <= 3 else 0,
                'earned_run': earned_runs,
                'hit_allowed': random.randint(2, 8),
                'walk_allowed': random.randint(0, 4),
                'hit_batsman': random.randint(0, 1),
                'complete_game': complete_game,
                'complete_game_shutout': complete_game_shutout,
                'errors': fielding_errors,
                'wild_pitch': 1 if random.random() < 0.15 else 0,  # 15% chance of wild pitch
                'balk': 1 if random.random() < 0.05 else 0  # 5% chance of balk
            }
        else:  # All batting positions
            at_bats = random.randint(3, 5)
            hits = random.randint(0, min(at_bats, 3))
            home_runs = random.randint(0, min(hits, 1))
            
            # Include strikeouts and errors for MLB batters
            strikeouts = random.randint(0, 3)  # 0-3 strikeouts per game
            fielding_errors = 1 if random.random() < 0.15 else 0  # 15% chance of error
            
            stats = {
                'run': random.randint(0, 2),
                'hit': hits,
                'home_run': home_runs,
                'rbi': random.randint(0, 3),
                'walk': random.randint(0, 2),
                'stolen_base': random.randint(0, 1),
                'caught_stealing': random.randint(0, 1) if random.random() < 0.2 else 0,
                'strikeout': strikeouts,
                'errors': fielding_errors,
                'gidp': 1 if random.random() < 0.1 else 0  # 10% chance of grounding into double play
            }
    
    # NBA/WNBA stats generation
    elif sport in ['NBA', 'WNBA', 'Men\'s College Basketball', 'Women\'s College Basketball']:
        points = random.randint(4, 25)
        rebounds = random.randint(1, 10)
        assists = random.randint(1, 8)
        
        # Adjust based on typical position stats
        if pos_category in ['PG', 'SG']:
            assists = random.randint(3, 10)
            points = random.randint(8, 25)
            rebounds = random.randint(1, 6)
        elif pos_category in ['SF', 'PF']:
            points = random.randint(8, 22)
            rebounds = random.randint(4, 10)
        elif pos_category == 'C':
            rebounds = random.randint(5, 12)
            blocks = random.randint(1, 4)
        
        # Calculate double-double and triple-double
        stats_over_10 = sum(1 for s in [points, rebounds, assists] if s >= 10)
        double_double = 1 if stats_over_10 >= 2 else 0
        triple_double = 1 if stats_over_10 >= 3 else 0
        
        stats = {
            'point': points,
            'rebound': rebounds,
            'assist': assists,
            'steal': random.randint(0, 3),
            'block': random.randint(0, 3),
            'turnover': random.randint(0, 4),
            'three_pointer': random.randint(0, 5),
            'double_double': double_double,
            'triple_double': triple_double
        }
    
    return stats

def calculate_fantasy_points(stats, position, sport='NFL'):
    """
    Calculate fantasy points based on player stats, position, and sport
    
    Args:
        stats (dict): Player statistics
        position (str): Player's position
        sport (str): Sport (NFL, NBA, MLB, etc.)
    
    Returns:
        float: Total fantasy points
    """
    if position not in FANTASY_METRICS:
        # Handle alternative position names
        if position in ['WR1', 'WR2', 'WR3', 'Slot']:
            position = 'WR'
        elif position in ['LF', 'CF', 'RF']:
            position = 'OF'
        # Add more mappings as needed
    
    if position not in FANTASY_METRICS:
        # Default to a reasonable position if unknown
        if 'passing_yards' in stats:
            position = 'QB'
        elif 'rushing_yards' in stats:
            position = 'RB'
        elif 'receiving_yards' in stats:
            position = 'WR'
        elif 'inning_pitched' in stats:
            position = 'P'
        elif 'hit' in stats:
            position = '1B'  # Generic batter
        elif 'point' in stats:
            position = 'PG'  # Generic basketball player
        else:
            return 0  # No recognizable stats
    
    # Calculate fantasy points
    points = 0
    
    # Special handling for MLB to include strikeout and error penalties
    if sport == 'MLB':
        # Get standard points using metrics
        metrics = FANTASY_METRICS.get(position, {})
        for stat, value in stats.items():
            if stat in metrics:
                points += value * metrics[stat]
        
        # Apply specific MLB penalties
        # -2 points for strikeouts (batters)
        if 'strikeout' in stats and position not in ['P', 'SP', 'RP']:
            points -= stats['strikeout'] * 2.0
            
        # -2 points for fielding errors (all positions)
        if 'errors' in stats:
            points -= stats['errors'] * 2.0
            
        # -1 point for each wild pitch and balk (pitchers)
        if position in ['P', 'SP', 'RP']:
            if 'wild_pitch' in stats:
                points -= stats['wild_pitch'] * 1.0
            if 'balk' in stats:
                points -= stats['balk'] * 1.0
    else:
        # Standard calculation for other sports
        metrics = FANTASY_METRICS.get(position, {})
        for stat, value in stats.items():
            if stat in metrics:
                points += value * metrics[stat]
    
    return points

def get_player_performance_tier(fantasy_points, position, sport, period='weekly'):
    """
    Determine performance tier based on fantasy points comparison
    
    Args:
        fantasy_points (float): Player's fantasy points
        position (str): Player's position
        sport (str): Sport (NFL, MLB, NBA, etc.)
        period (str): Period of comparison (daily, weekly, etc.)
    
    Returns:
        dict: Performance tier with adjustment percentage
    """
    # In a real implementation, we would fetch actual fantasy point distributions
    # For this demo, we'll use a statistical approximation
    
    # First, determine average and standard deviation for the position/sport
    avg_points = {
        'NFL': {
            'QB': 18.0,
            'RB': 12.0,
            'WR': 10.0,
            'TE': 8.0,
            'K': 7.5,
            'DEF': 7.0
        },
        'MLB': {
            'P': 15.0,
            'C': 7.0,
            '1B': 8.0,
            '2B': 7.5,
            '3B': 7.5,
            'SS': 7.5,
            'OF': 8.0,
            'DH': 8.5
        },
        'NBA': {
            'PG': 32.0,
            'SG': 30.0,
            'SF': 29.0,
            'PF': 28.0,
            'C': 27.0
        },
        'WNBA': {
            'PG': 28.0,
            'SG': 26.0,
            'SF': 25.0,
            'PF': 24.0,
            'C': 23.0
        }
    }
    
    std_dev = {
        'NFL': {
            'QB': 7.0,
            'RB': 6.0,
            'WR': 5.5,
            'TE': 4.0,
            'K': 3.0,
            'DEF': 4.0
        },
        'MLB': {
            'P': 8.0,
            'C': 3.5,
            '1B': 4.0,
            '2B': 3.5,
            '3B': 3.5,
            'SS': 3.5,
            'OF': 4.0,
            'DH': 4.0
        },
        'NBA': {
            'PG': 9.0,
            'SG': 8.5,
            'SF': 8.0,
            'PF': 7.5,
            'C': 7.5
        },
        'WNBA': {
            'PG': 8.0,
            'SG': 7.5,
            'SF': 7.0,
            'PF': 6.5,
            'C': 6.5
        }
    }
    
    # Handle alternative position names and defaults
    if position not in avg_points.get(sport, {}):
        if position in ['WR1', 'WR2', 'WR3', 'Slot']:
            position = 'WR'
        elif position in ['LF', 'CF', 'RF']:
            position = 'OF'
        # Add more mappings as needed
    
    # Get average and standard deviation, with reasonable defaults
    avg = avg_points.get(sport, {}).get(position, 10.0)
    sd = std_dev.get(sport, {}).get(position, 5.0)
    
    # Calculate z-score
    z_score = (fantasy_points - avg) / sd
    
    # Convert z-score to percentile (approximate)
    # Using the cumulative distribution function of a standard normal distribution
    import math
    percentile = (1 + math.erf(z_score / math.sqrt(2))) / 2 * 100
    
    # Determine tier based on percentile
    for tier, data in PERFORMANCE_TIERS.items():
        if percentile >= data['percentile']:
            return {
                'tier': tier,
                'percentile': percentile,
                'adjustment': data['adjustment']
            }
    
    # Fallback to lowest tier
    return {
        'tier': 'disastrous',
        'percentile': percentile,
        'adjustment': PERFORMANCE_TIERS['disastrous']['adjustment']
    }

def update_player_prices_based_on_performance():
    """
    Update player prices based on their recent performance
    Returns the number of players updated
    """
    try:
        # Get current date and time
        now = datetime.now()
        
        # Define the period for performance evaluation
        # For NFL, we'll use weekly (typical game frequency)
        # For MLB/NBA, we could use daily or 3-day windows
        start_date = now - timedelta(days=7)  # One week for demo
        
        # Get all players from database
        with engine.connect() as conn:
            players_query = text("""
                SELECT id, name, position, sport, current_price, shares_outstanding, total_worth,
                       category, last_updated
                FROM player_data
            """)
            players = conn.execute(players_query).fetchall()
            
            update_count = 0
            
            for player in players:
                # Skip update if player was updated recently (e.g., in the last day)
                if player.last_updated and (now - player.last_updated).days < 1:
                    continue
                
                # Get player stats (in a real system, this would use a sports API)
                stats = get_player_stats_for_period(
                    player.name, 
                    player.position, 
                    player.sport, 
                    start_date, 
                    now
                )
                
                # Calculate fantasy points
                fantasy_points = calculate_fantasy_points(stats, player.position, player.sport)
                
                # Check for negative news events from player_news table
                # This allows news events to affect player pricing
                news_adjustment = 0
                try:
                    news_query = text("""
                        SELECT news_type, impact 
                        FROM player_news 
                        WHERE player_name = :player_name
                        AND published_at > :since_date
                        ORDER BY published_at DESC
                        LIMIT 3
                    """)
                    
                    recent_news = conn.execute(news_query, {
                        "player_name": player.name,
                        "since_date": (now - timedelta(days=14)).isoformat()
                    }).fetchall()
                    
                    # Apply adjustments based on news
                    for news in recent_news:
                        if news.impact == 'negative':
                            # Add negative adjustments based on news type
                            if news.news_type == 'injury':
                                news_adjustment -= 0.10  # -10% for injuries
                            elif news.news_type == 'suspension':
                                news_adjustment -= 0.15  # -15% for suspensions
                            elif news.news_type == 'benched':
                                news_adjustment -= 0.08  # -8% for being benched
                            elif news.news_type == 'trade':
                                news_adjustment -= 0.05  # -5% for potentially negative trades
                            elif news.news_type == 'off_field_issue':
                                news_adjustment -= 0.12  # -12% for off-field troubles
                            else:
                                news_adjustment -= 0.03  # -3% for other negative news
                        
                        elif news.impact == 'positive':
                            # Add positive adjustments based on news type
                            if news.news_type == 'return_from_injury':
                                news_adjustment += 0.08  # +8% for injury returns
                            elif news.news_type == 'promotion':
                                news_adjustment += 0.05  # +5% for depth chart promotions
                            elif news.news_type == 'trade':
                                news_adjustment += 0.07  # +7% for potentially positive trades
                            elif news.news_type == 'hot_streak':
                                news_adjustment += 0.10  # +10% for hot streaks
                            else:
                                news_adjustment += 0.03  # +3% for other positive news
                except Exception as news_err:
                    print(f"Error checking player news: {str(news_err)}")
                
                # Determine performance tier and price adjustment
                performance = get_player_performance_tier(
                    fantasy_points, 
                    player.position, 
                    player.sport
                )
                
                # Apply price adjustment (performance + news)
                adjustment_pct = performance['adjustment'] + news_adjustment
                
                # Cap adjustments to reasonable limits (-25% to +25% max per update)
                adjustment_pct = max(min(adjustment_pct, 0.25), -0.25)
                
                new_total_worth = player.total_worth * (1 + adjustment_pct)
                new_price = new_total_worth / player.shares_outstanding
                
                # Calculate weekly change percentage
                weekly_change_pct = adjustment_pct * 100  # Convert to percentage
                
                # Update the player's price in the database
                update_query = text("""
                    UPDATE player_data 
                    SET current_price = :new_price,
                        total_worth = :new_total_worth,
                        last_updated = CURRENT_TIMESTAMP,
                        last_fantasy_points = :fantasy_points,
                        weekly_change = :weekly_change
                    WHERE id = :player_id
                """)
                
                conn.execute(update_query, {
                    'new_price': new_price,
                    'new_total_worth': new_total_worth,
                    'fantasy_points': fantasy_points,
                    'weekly_change': weekly_change_pct,
                    'player_id': player.id
                })
                
                # Record this performance in the history table
                try:
                    history_query = text("""
                        INSERT INTO player_performance_history
                        (player_name, game_date, opponent, fantasy_points, 
                         performance_stats, price_before, price_after, price_change_pct)
                        VALUES
                        (:player_name, :game_date, :opponent, :fantasy_points,
                         :performance_stats, :price_before, :price_after, :price_change_pct)
                    """)
                    
                    # Format stats as JSON
                    import json
                    stats_json = json.dumps(stats)
                    
                    conn.execute(history_query, {
                        'player_name': player.name,
                        'game_date': now.date().isoformat(),
                        'opponent': stats.get('opponent', 'Multiple'),
                        'fantasy_points': fantasy_points,
                        'performance_stats': stats_json,
                        'price_before': player.current_price,
                        'price_after': new_price,
                        'price_change_pct': weekly_change_pct
                    })
                except Exception as hist_err:
                    print(f"Error recording performance history: {str(hist_err)}")
                
                update_count += 1
                
            # Make sure changes are committed
            conn.commit()
            
            return update_count
    
    except Exception as e:
        print(f"Error updating player prices: {str(e)}")
        return 0

def add_fantasy_points_column():
    """
    Add last_fantasy_points column to player_data table if it doesn't exist
    """
    try:
        with engine.connect() as conn:
            # Check if the column already exists
            check_query = text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'player_data' AND column_name = 'last_fantasy_points'
            """)
            
            result = conn.execute(check_query).fetchone()
            
            if not result:
                # Add the column if it doesn't exist
                alter_query = text("""
                    ALTER TABLE player_data
                    ADD COLUMN last_fantasy_points NUMERIC DEFAULT 0
                """)
                
                conn.execute(alter_query)
                conn.commit()
                return True
            
            return False
            
    except Exception as e:
        print(f"Error adding fantasy points column: {str(e)}")
        return False

def simulate_performance_update():
    """
    Simulate a performance update cycle for demonstration purposes
    """
    # First, make sure we have the right columns
    add_fantasy_points_column()
    
    # Update player prices
    updated_count = update_player_prices_based_on_performance()
    
    return f"Updated prices for {updated_count} players based on simulated performance."

if __name__ == "__main__":
    # When run directly, perform a simulation update
    result = simulate_performance_update()
    print(result)