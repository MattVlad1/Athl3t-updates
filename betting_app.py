import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import random
import os
import hashlib
import decimal
from sqlalchemy import text
from db import (
    load_data, engine, is_user_verified_adult, verify_user_age,
    get_upcoming_games, place_bet, create_parlay_bet, get_user_bets,
    simulate_game_result
)

# Import real-time sports data module - direct integration
try:
    from real_time_sports import get_upcoming_games as get_real_time_games
    USE_REAL_TIME_DATA = True
except ImportError:
    USE_REAL_TIME_DATA = False

# Helper function to convert decimal.Decimal to float
def to_float(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value

# Page configuration
st.set_page_config(page_title="ATHL3T Sports Betting", layout="wide")

# Initialize session state for authentication and other app state
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_id' not in st.session_state:
    st.session_state.user_id = None
if 'username' not in st.session_state:
    st.session_state.username = None
if 'wallet_balance' not in st.session_state:
    st.session_state.wallet_balance = 0

# App title
st.title("ATHL3T Sports Betting (21+ Only)")

# Authentication system (simplified for demo)
if not st.session_state.logged_in:
    # Login or demo account
    st.sidebar.header("Welcome to ATHL3T Sports Betting")
    st.sidebar.subheader("Login")
    
    if st.sidebar.button("Demo Account (Auto Login)"):
        # Create or use demo account
        with engine.connect() as conn:
            # Check if demo account exists
            check_query = text("SELECT id, wallet_balance FROM users WHERE username = 'DemoUser'")
            existing_user = conn.execute(check_query).fetchone()
            
            if existing_user:
                user_id = existing_user[0]
                wallet_balance = existing_user[1]
            else:
                # Create demo user
                user_id = f"demo_user_{int(datetime.now().timestamp())}"
                wallet_balance = 1000.00  # Higher balance for demo
                
                # Create new user with birthdate that makes them 21+
                insert_query = text("""
                    INSERT INTO users 
                    (id, username, email, password, wallet_balance, birthdate, is_verified_adult)
                    VALUES (:id, :username, :email, :password, :wallet_balance, :birthdate, TRUE)
                """)
                conn.execute(insert_query, {
                    "id": user_id,
                    "username": "DemoUser",
                    "email": "demo@example.com",
                    "password": "demo123",
                    "wallet_balance": wallet_balance,
                    "birthdate": "1990-01-01"  # Making the user 21+
                })
                conn.commit()
            
            # Log in the demo user
            st.session_state.logged_in = True
            st.session_state.user_id = user_id
            st.session_state.username = "DemoUser"
            st.session_state.wallet_balance = wallet_balance
            st.rerun()
    
    # Display welcome information
    st.markdown("""
    ## Welcome to ATHL3T Sports Betting
    
    Place bets on upcoming games and win big! 
    
    **Features:**
    - Moneyline, spread, and over/under betting
    - Parlay betting for bigger payouts
    - Live game tracking
    - Real-time odds updates
    
    **Note:** You must be 21 or older to place bets.
    """)

else:
    # User is logged in, show the betting platform
    # Sidebar with user info
    st.sidebar.header(f"Wallet: ${st.session_state.wallet_balance:.2f}")
    st.sidebar.write(f"User: {st.session_state.username}")
    
    # Add Funds button
    if st.sidebar.button("Add $100 Funds"):
        with engine.connect() as conn:
            query = text("""
                UPDATE users 
                SET wallet_balance = wallet_balance + 100
                WHERE id = :user_id
                RETURNING wallet_balance
            """)
            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
            conn.commit()
            
            if result:
                st.session_state.wallet_balance = result[0]
                st.sidebar.success("Added $100 to your wallet!")
                st.rerun()
    
    # Logout button
    if st.sidebar.button("Logout"):
        st.session_state.logged_in = False
        st.session_state.user_id = None
        st.session_state.username = None
        st.session_state.wallet_balance = 0
        st.rerun()
    
    # Check if user has verified age
    is_adult = is_user_verified_adult(st.session_state.user_id)
    
    if not is_adult:
        st.warning("⚠️ You must be 21 or older to access sports betting features.")
        
        # Age verification form
        st.subheader("Age Verification")
        st.write("Please enter your date of birth to verify your age.")
        
        birthdate = st.date_input("Date of Birth", 
                                 min_value=datetime(1900, 1, 1).date(),
                                 max_value=datetime.now().date())
        
        if st.button("Verify Age"):
            # Format date as YYYY-MM-DD string
            birthdate_str = birthdate.strftime("%Y-%m-%d")
            
            try:
                # Try to verify directly via SQL for better reliability
                birth_date = birthdate
                today = datetime.now().date()
                age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
                
                with engine.connect() as conn:
                    # Update user's birthdate
                    trans = conn.begin()
                    try:
                        conn.execute(
                            text("UPDATE users SET birthdate = :birthdate WHERE id = :user_id"),
                            {"user_id": st.session_state.user_id, "birthdate": birthdate_str}
                        )
                        
                        # Set verification status based on age
                        if age >= 21:
                            conn.execute(
                                text("UPDATE users SET is_verified_adult = TRUE WHERE id = :user_id"),
                                {"user_id": st.session_state.user_id}
                            )
                            trans.commit()
                            st.success("Age verification successful! You are 21 or older and can access betting features.")
                            st.rerun()  # Refresh to show betting interface
                        else:
                            conn.execute(
                                text("UPDATE users SET is_verified_adult = FALSE WHERE id = :user_id"),
                                {"user_id": st.session_state.user_id}
                            )
                            trans.commit()
                            st.error(f"You must be 21 or older to access betting features. Your current age is {age}.")
                    except Exception as e:
                        trans.rollback()
                        st.error(f"Error verifying age: {str(e)}")
                        
            except Exception as e:
                # Fallback to standard function if direct attempt fails
                success, message = verify_user_age(st.session_state.user_id, birthdate_str)
                
                if success:
                    st.success(message)
                    st.rerun()  # Refresh to show betting interface
                else:
                    st.error(message)
    else:
        # User is verified as 21+, show betting interface
        betting_tabs = st.tabs(["Available Games", "My Bets", "Parlays"])
        
        with betting_tabs[0]:
            col1, col2 = st.columns([3, 1])
            with col1:
                st.subheader("Upcoming Games")
            with col2:
                if USE_REAL_TIME_DATA:
                    st.success("Real-time data enabled ✅")
                else:
                    st.info("Using database games")
            
            # Get upcoming games - try to use real-time data if available
            if USE_REAL_TIME_DATA:
                try:
                    # First try to get real-time data directly
                    upcoming_games = get_real_time_games(limit=10)
                    if not upcoming_games:
                        # Fall back to database if no real-time games available
                        upcoming_games = get_upcoming_games()
                except Exception as e:
                    st.error(f"Error fetching real-time games: {e}")
                    # Fall back to database games
                    upcoming_games = get_upcoming_games()
            else:
                # Use database games if real-time module not available
                upcoming_games = get_upcoming_games()
            
            if not upcoming_games:
                st.info("No upcoming games available for betting at this time.")
            else:
                # Display each game with betting options
                for game in upcoming_games:
                    # Create a container for each game
                    game_container = st.container()
                    
                    with game_container:
                        # Format the date
                        game_date = game['game_date'].strftime("%a, %b %d - %I:%M %p")
                        
                        # Display game info with sport label
                        sport = game.get('sport', 'Unknown')
                        sport_emoji = "🏈" if sport == "NFL" else "⚾" if sport == "MLB" else "🏀" if sport == "NBA" else "🏒" if sport == "NHL" else "🏆"
                        st.subheader(f"{sport_emoji} {sport}: {game['home_team']} vs {game['away_team']}")
                        st.caption(f"Game Time: {game_date}")
                        
                        # Create tabs for different bet types
                        bet_tabs = st.tabs(["Moneyline", "Spread", "Over/Under"])
                        
                        with bet_tabs[0]:
                            # Moneyline betting
                            st.write("Moneyline: Pick the winner")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"Home: {game['home_team']}")
                                st.write(f"Odds: {to_float(game['home_odds']):.2f}")
                                
                                # Betting amount for home team
                                home_bet_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"home_ml_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                home_payout = home_bet_amount * to_float(game['home_odds'])
                                st.write(f"Potential Payout: ${home_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Home Team", key=f"home_ml_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'moneyline',
                                        'home',
                                        home_bet_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                            
                            with col2:
                                st.write(f"Away: {game['away_team']}")
                                st.write(f"Odds: {to_float(game['away_odds']):.2f}")
                                
                                # Betting amount for away team
                                away_bet_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"away_ml_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                away_payout = away_bet_amount * to_float(game['away_odds'])
                                st.write(f"Potential Payout: ${away_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Away Team", key=f"away_ml_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'moneyline',
                                        'away',
                                        away_bet_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                        
                        with bet_tabs[1]:
                            # Spread betting
                            spread = game['spread']
                            favored = "home" if spread > 0 else "away"
                            abs_spread = abs(spread)
                            
                            if favored == "home":
                                st.write(f"Spread: {game['home_team']} -{abs_spread} | {game['away_team']} +{abs_spread}")
                            else:
                                st.write(f"Spread: {game['home_team']} +{abs_spread} | {game['away_team']} -{abs_spread}")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"Home: {game['home_team']} {'-' if favored == 'home' else '+'}{abs_spread}")
                                st.write("Odds: 1.91")  # Standard -110 odds for spread bets
                                
                                # Betting amount
                                home_spread_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"home_spread_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                home_spread_payout = home_spread_amount * 1.91
                                st.write(f"Potential Payout: ${home_spread_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Home Spread", key=f"home_spread_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'spread',
                                        'home',
                                        home_spread_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                            
                            with col2:
                                st.write(f"Away: {game['away_team']} {'-' if favored == 'away' else '+'}{abs_spread}")
                                st.write("Odds: 1.91")  # Standard -110 odds for spread bets
                                
                                # Betting amount
                                away_spread_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"away_spread_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                away_spread_payout = away_spread_amount * 1.91
                                st.write(f"Potential Payout: ${away_spread_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Away Spread", key=f"away_spread_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'spread',
                                        'away',
                                        away_spread_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                        
                        with bet_tabs[2]:
                            # Over/Under betting
                            st.write(f"Total Points Over/Under: {game['over_under']}")
                            
                            col1, col2 = st.columns(2)
                            with col1:
                                st.write(f"Over {game['over_under']}")
                                st.write("Odds: 1.91")  # Standard -110 odds
                                
                                # Betting amount
                                over_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"over_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                over_payout = over_amount * 1.91
                                st.write(f"Potential Payout: ${over_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Over", key=f"over_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'over_under',
                                        'over',
                                        over_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                            
                            with col2:
                                st.write(f"Under {game['over_under']}")
                                st.write("Odds: 1.91")  # Standard -110 odds
                                
                                # Betting amount
                                under_amount = st.number_input(
                                    f"Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                                    min_value=5.0,
                                    max_value=to_float(st.session_state.wallet_balance),
                                    step=5.0,
                                    key=f"under_{game['id']}"
                                )
                                
                                # Calculate potential payout
                                under_payout = under_amount * 1.91
                                st.write(f"Potential Payout: ${under_payout:.2f}")
                                
                                # Place bet button
                                if st.button("Place Bet on Under", key=f"under_bet_{game['id']}"):
                                    success, message, bet_id = place_bet(
                                        st.session_state.user_id,
                                        game['id'],
                                        'over_under',
                                        'under',
                                        under_amount
                                    )
                                    
                                    if success:
                                        st.success(message)
                                        # Update wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = result[0]
                                        st.rerun()
                                    else:
                                        st.error(message)
                        
                        # Add a game simulation option for testing
                        if st.button("Simulate Game Result (Testing Only)", key=f"sim_{game['id']}"):
                            success, message = simulate_game_result(game['id'])
                            if success:
                                st.success(message)
                                # Update wallet balance after simulation
                                with engine.connect() as conn:
                                    query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                    result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                    if result:
                                        st.session_state.wallet_balance = result[0]
                                st.rerun()  # Refresh page to update bet statuses
                            else:
                                st.error(message)
                        
                        st.markdown("---")
        
        with betting_tabs[1]:
            st.subheader("My Active Bets")
            
            # Get user's bets
            single_bets, _ = get_user_bets(st.session_state.user_id)
            
            if not single_bets:
                st.info("You don't have any active bets. Head to the Available Games tab to place a bet!")
            else:
                # Display each bet
                for bet in single_bets:
                    # Create a container for each bet
                    bet_container = st.container()
                    
                    with bet_container:
                        # Format the date
                        game_date = bet['game_date'].strftime("%a, %b %d - %I:%M %p") if 'game_date' in bet else "Date unknown"
                        
                        # Create columns for bet details
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            # Display game info
                            st.markdown(f"**{bet['home_team']} vs {bet['away_team']}**")
                            st.caption(f"Game Time: {game_date}")
                            
                            # Display bet details
                            bet_type_display = {
                                'moneyline': 'Moneyline',
                                'spread': 'Point Spread',
                                'over_under': 'Over/Under'
                            }.get(bet['bet_type'], bet['bet_type'])
                            
                            bet_pick_display = {
                                'home': f"Home ({bet['home_team']})",
                                'away': f"Away ({bet['away_team']})",
                                'over': f"Over {bet.get('over_under', '')}",
                                'under': f"Under {bet.get('over_under', '')}"
                            }.get(bet['bet_pick'], bet['bet_pick'])
                            
                            st.write(f"Bet Type: {bet_type_display}")
                            st.write(f"Pick: {bet_pick_display}")
                            st.write(f"Odds: {to_float(bet['odds']):.2f}")
                            
                        with col2:
                            st.write(f"Amount: ${to_float(bet['amount']):.2f}")
                            st.write(f"Potential Payout: ${to_float(bet['potential_payout']):.2f}")
                            
                            # Display bet status with appropriate styling
                            status = bet['status']
                            if status == 'pending':
                                st.info("Status: Pending")
                            elif status == 'won':
                                st.success(f"Status: Won (+${to_float(bet['potential_payout']):.2f})")
                            elif status == 'lost':
                                st.error(f"Status: Lost (-${to_float(bet['amount']):.2f})")
                            else:
                                st.write(f"Status: {status.capitalize()}")
                        
                        st.markdown("---")
        
        with betting_tabs[2]:
            st.subheader("Parlay Betting")
            
            # Get user's parlays
            _, parlays = get_user_bets(st.session_state.user_id)
            
            # Create tabs for creating and viewing parlays
            parlay_tabs = st.tabs(["Create Parlay", "My Parlays"])
            
            with parlay_tabs[0]:
                st.write("Create a Parlay by selecting multiple bets. All selections must win for the parlay to pay out.")
                st.write("The more selections you add, the higher the potential payout!")
                
                # Get upcoming games for parlay selection
                upcoming_games = get_upcoming_games()
                
                if not upcoming_games:
                    st.info("No upcoming games available for betting at this time.")
                else:
                    # Create a form for the parlay
                    parlay_selections = []
                    
                    # Display available games with checkboxes for selection
                    for i, game in enumerate(upcoming_games):
                        st.subheader(f"{game['home_team']} vs {game['away_team']}")
                        game_date = game['game_date'].strftime("%a, %b %d - %I:%M %p")
                        st.caption(f"Game Time: {game_date}")
                        
                        # Create columns for different bet types
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.write("Moneyline")
                            home_ml = st.checkbox(f"{game['home_team']} ({game['home_odds']:.2f})", key=f"parlay_home_ml_{game['id']}")
                            away_ml = st.checkbox(f"{game['away_team']} ({game['away_odds']:.2f})", key=f"parlay_away_ml_{game['id']}")
                            
                            if home_ml:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'moneyline',
                                    'bet_pick': 'home'
                                })
                            if away_ml:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'moneyline',
                                    'bet_pick': 'away'
                                })
                        
                        with col2:
                            st.write("Spread")
                            spread = game['spread']
                            favored = "home" if spread > 0 else "away"
                            abs_spread = abs(spread)
                            
                            home_spread_txt = f"{game['home_team']} {'-' if favored == 'home' else '+'}{abs_spread} (1.91)"
                            away_spread_txt = f"{game['away_team']} {'-' if favored == 'away' else '+'}{abs_spread} (1.91)"
                            
                            home_spread = st.checkbox(home_spread_txt, key=f"parlay_home_spread_{game['id']}")
                            away_spread = st.checkbox(away_spread_txt, key=f"parlay_away_spread_{game['id']}")
                            
                            if home_spread:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'spread',
                                    'bet_pick': 'home'
                                })
                            if away_spread:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'spread',
                                    'bet_pick': 'away'
                                })
                        
                        with col3:
                            st.write("Over/Under")
                            over_txt = f"Over {game['over_under']} (1.91)"
                            under_txt = f"Under {game['over_under']} (1.91)"
                            
                            over = st.checkbox(over_txt, key=f"parlay_over_{game['id']}")
                            under = st.checkbox(under_txt, key=f"parlay_under_{game['id']}")
                            
                            if over:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'over_under',
                                    'bet_pick': 'over'
                                })
                            if under:
                                parlay_selections.append({
                                    'game_id': game['id'],
                                    'bet_type': 'over_under',
                                    'bet_pick': 'under'
                                })
                        
                        st.markdown("---")
                    
                    # Display parlay summary
                    if parlay_selections:
                        st.subheader("Parlay Summary")
                        st.write(f"Selections: {len(parlay_selections)}")
                        
                        # Input parlay amount
                        parlay_amount = st.number_input(
                            f"Parlay Bet Amount (${to_float(st.session_state.wallet_balance):.2f} available)",
                            min_value=5.0,
                            max_value=to_float(st.session_state.wallet_balance),
                            step=5.0,
                            key="parlay_amount"
                        )
                        
                        # Place parlay button
                        if st.button("Place Parlay Bet"):
                            if len(parlay_selections) < 2:
                                st.error("A parlay must include at least 2 selections.")
                            else:
                                success, message, parlay_id = create_parlay_bet(
                                    st.session_state.user_id,
                                    parlay_selections,
                                    parlay_amount
                                )
                                
                                if success:
                                    st.success(message)
                                    # Update wallet balance
                                    with engine.connect() as conn:
                                        query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                        result = conn.execute(query, {"user_id": st.session_state.user_id}).fetchone()
                                        if result:
                                            st.session_state.wallet_balance = result[0]
                                    st.rerun()  # Refresh page to update wallet balance and show new parlay
                                else:
                                    st.error(message)
                    else:
                        st.info("Select at least 2 bets to create a parlay.")
            
            with parlay_tabs[1]:
                st.subheader("My Parlay Bets")
                
                if not parlays:
                    st.info("You don't have any parlay bets. Create one in the Create Parlay tab!")
                else:
                    # Display each parlay
                    for parlay in parlays:
                        # Create a container for each parlay
                        parlay_container = st.container()
                        
                        with parlay_container:
                            # Create columns for parlay details
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.markdown(f"**Parlay #{parlay['id']}**")
                                st.caption(f"Created: {parlay['created_at'].strftime('%b %d, %Y')}")
                                st.write(f"Selections: {parlay['leg_count']}")
                            
                            with col2:
                                st.write(f"Amount: ${parlay['amount']:.2f}")
                                st.write(f"Potential Payout: ${parlay['potential_payout']:.2f}")
                                
                                # Display parlay status with appropriate styling
                                status = parlay['status']
                                if status == 'pending':
                                    st.info("Status: Pending")
                                elif status == 'won':
                                    st.success(f"Status: Won (+${parlay['potential_payout']:.2f})")
                                elif status == 'lost':
                                    st.error(f"Status: Lost (-${parlay['amount']:.2f})")
                                else:
                                    st.write(f"Status: {status.capitalize()}")
                            
                            st.markdown("---")