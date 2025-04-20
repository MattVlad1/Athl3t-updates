import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime, timedelta
import os
import hashlib
import random
from sqlalchemy import text
from sports_news import get_live_games, get_upcoming_games, get_sports_news, update_sports_news_from_real_sources

# Helper functions for calculations
def calculate_potential_win(amount, odds):
    """Calculate potential winnings based on American odds"""
    odds = float(odds)
    if odds > 0:
        return amount * (odds / 100)
    else:
        return amount * (100 / abs(odds))

def safe_division(numerator, denominator, default=0):
    """Safely perform division, returning default value if denominator is zero or None"""
    try:
        if denominator is None or denominator == 0:
            return default
        return numerator / denominator
    except (TypeError, ZeroDivisionError):
        return default
from db import (
    load_data, save_data, execute_transaction, get_transaction_history, 
    get_performance_summary, engine, is_user_verified_adult, verify_user_age,
    get_upcoming_games, place_bet, create_parlay_bet, get_user_bets,
    simulate_game_result
)
from scraper import update_player_data_in_database

# Page configuration
st.set_page_config(page_title="ATHL3T Trades", layout="wide")

# Initialize session state for authentication and other app state
if 'logged_in' not in st.session_state:
    st.session_state.logged_in = False
if 'user_id' not in st.session_state:
    st.session_state.user_id = None
if 'username' not in st.session_state:
    st.session_state.username = None
if 'wallet_balance' not in st.session_state:
    st.session_state.wallet_balance = 0
if 'show_signup' not in st.session_state:
    st.session_state.show_signup = False
if 'page' not in st.session_state:
    st.session_state.page = "market"
if 'selected_player' not in st.session_state:
    st.session_state.selected_player = None

# Update player data in the database - only uncommment when needed
# update_player_data_in_database(engine)

# Cache functions for better performance
@st.cache_data(ttl=600)  # Cache for 10 minutes
def get_trending_players():
    try:
        players_data, _, _, _ = load_data()
        return players_data.sort_values(by="Current Price", ascending=False).head(5)
    except Exception:
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_data():
    """Load all data from the database with caching for performance"""
    try:
        return load_data()
    except Exception:
        return None, None, None, None
        
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_cached_live_games():
    """Get live games data with caching"""
    try:
        # Use our real-time sports module instead
        from real_time_sports import get_live_games
        return get_live_games()
    except Exception as e:
        print(f"Error fetching live games: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_cached_upcoming_games(limit=10):
    """Get upcoming games data with caching"""
    try:
        # Use our real-time sports module instead
        from real_time_sports import get_upcoming_games
        return get_upcoming_games(limit=limit)
    except Exception as e:
        print(f"Error fetching upcoming games: {e}")
        return []

@st.cache_data(ttl=120)  # Cache for 2 minutes
def get_cached_sports_news(limit=10):
    """Get sports news with caching"""
    try:
        return get_sports_news(limit=limit)
    except Exception as e:
        print(f"Error fetching sports news: {e}")
        return []

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_transaction_history_cached(user_id):
    """Get cached transaction history with computed value"""
    try:
        transactions = get_transaction_history(user_id)
        if transactions is not None and not transactions.empty:
            # Add a value column for each transaction (price * quantity)
            transactions['value'] = transactions['price'] * transactions['quantity']
        return transactions
    except Exception:
        return pd.DataFrame()  # Return empty DataFrame instead of None for consistency

# Helper functions for authentication
def hash_password(password):
    """Create a SHA-256 hash of the password"""
    return hashlib.sha256(password.encode()).hexdigest()

def verify_password(stored_password, provided_password):
    """Verify the hashed password against the provided password"""
    return stored_password == hash_password(provided_password)

def authenticate_user(username, password):
    """Authenticate a user by checking username and password"""
    with engine.connect() as conn:
        query = text("""
            SELECT id, username, wallet_balance, password 
            FROM users 
            WHERE username = :username
        """)
        result = conn.execute(query, {"username": username}).fetchone()
        
        if result and result.password == password:  # For simplicity, using plain text for now
            return {
                "User ID": result.id,
                "Username": result.username,
                "Wallet Balance": result.wallet_balance
            }
    return None

def create_user(username, email, password, birthdate=None):
    """
    Create a new user with the given credentials
    
    Parameters:
    - username: Username for the new account
    - email: Email address
    - password: Password
    - birthdate: Optional birthdate string in YYYY-MM-DD format
    
    Returns:
    - success: Boolean indicating if creation was successful
    - message: Message about the creation
    """
    user_id = f"user_{int(datetime.now().timestamp())}"
    
    # If birthdate is provided, check if user is 21 or older
    is_adult = False
    if birthdate:
        try:
            birth_date = datetime.strptime(birthdate, "%Y-%m-%d").date()
            today = datetime.now().date()
            age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
            is_adult = age >= 21
        except Exception:
            # Invalid date format, will default to not verified
            pass
    
    with engine.connect() as conn:
        # Check if username already exists
        check_query = text("SELECT id FROM users WHERE username = :username")
        existing_user = conn.execute(check_query, {"username": username}).fetchone()
        
        if existing_user:
            return False, "Username already exists"
        
        # Create new user
        if birthdate:
            query = text("""
                INSERT INTO users (id, username, email, password, wallet_balance, birthdate, is_verified_adult)
                VALUES (:id, :username, :email, :password, :wallet_balance, :birthdate, :is_verified_adult)
                RETURNING id
            """)
        else:
            query = text("""
                INSERT INTO users (id, username, email, password, wallet_balance)
                VALUES (:id, :username, :email, :password, :wallet_balance)
                RETURNING id
            """)
        
        params = {
            "id": user_id,
            "username": username,
            "email": email,
            "password": password,  # For simplicity, using plain text for now
            "wallet_balance": 150.00  # Initial starting balance for new users
        }
        
        if birthdate:
            params["birthdate"] = birthdate
            params["is_verified_adult"] = is_adult
            
        result = conn.execute(query, params)
        conn.commit()
        
        if result:
            # When birthdate is provided, give feedback about age verification
            if birthdate and is_adult:
                return True, f"Account created! You're verified as 21+ and can access betting features."
            elif birthdate:
                return True, f"Account created! You need to be 21+ to access betting features."
            else:
                return True, f"Account created! Please log in."
        else:
            return False, "Error creating user account"

def add_funds(user_id, amount):
    """Add funds to user wallet"""
    # Convert amount to a Python float to avoid NumPy types in SQL
    amount_float = float(amount)
    
    with engine.connect() as conn:
        query = text("""
            UPDATE users 
            SET wallet_balance = wallet_balance + :amount
            WHERE id = :user_id
            RETURNING wallet_balance
        """)
        result = conn.execute(query, {"amount": amount_float, "user_id": user_id}).fetchone()
        conn.commit()
        
        if result:
            return True, float(result.wallet_balance)  # Convert to Python float
        else:
            return False, "Error adding funds"

# App title
st.title("ATHL3T Trades - Fantasy Sports Market")

# Authentication system
if not st.session_state.logged_in:
    # Login or Signup page
    st.sidebar.header("Welcome to ATHL3T Trades")
    
    if st.session_state.show_signup:
        # Signup form
        st.sidebar.subheader("Create an Account")
        signup_username = st.sidebar.text_input("Username", key="signup_username")
        signup_email = st.sidebar.text_input("Email", key="signup_email")
        signup_password = st.sidebar.text_input("Password", key="signup_password", type="password")
        confirm_password = st.sidebar.text_input("Confirm Password", key="confirm_password", type="password")
        
        # Add birthdate field with date picker
        st.sidebar.markdown("**Date of Birth** (required for betting features)")
        birthdate = st.sidebar.date_input(
            "Select your date of birth",
            value=None,
            min_value=datetime(1900, 1, 1),
            max_value=datetime.now(),
            key="birthdate"
        )
        
        # Convert birthdate to string format if selected
        birthdate_str = None
        if birthdate is not None:
            birthdate_str = birthdate.strftime("%Y-%m-%d")
            
        # Age verification note
        st.sidebar.info("You must be 21+ to access betting features.")
        
        if st.sidebar.button("Create Account"):
            if signup_password != confirm_password:
                st.sidebar.error("Passwords do not match")
            elif birthdate is None:
                st.sidebar.error("Please enter your date of birth")
            else:
                success, message = create_user(signup_username, signup_email, signup_password, birthdate_str)
                if success:
                    st.session_state.show_signup = False
                    st.sidebar.success(message)
                    st.rerun()
                else:
                    st.sidebar.error(message)
        
        if st.sidebar.button("Back to Login"):
            st.session_state.show_signup = False
            st.rerun()
            
        # Demo account options
        st.sidebar.markdown("---")
    
    else:
        # Login form
        st.sidebar.subheader("Login")
        username = st.sidebar.text_input("Username")
        password = st.sidebar.text_input("Password", type="password")
        
        # Standard login buttons
        col1, col2 = st.sidebar.columns(2)
        
        with col1:
            if st.button("Login"):
                user = authenticate_user(username, password)
                if user:
                    st.session_state.logged_in = True
                    st.session_state.user_id = user["User ID"]
                    st.session_state.username = user["Username"]
                    st.session_state.wallet_balance = user["Wallet Balance"]
                    st.rerun()
                else:
                    st.sidebar.error("Invalid username or password")
        
        with col2:
            if st.button("Sign Up"):
                st.session_state.show_signup = True
                st.rerun()
        
        # Demo login option
        st.sidebar.markdown("---")
        st.sidebar.subheader("Quick Access:")
        
        if st.sidebar.button("🚀 Demo Login (No Password Required)"):
            # Create a demo user for testing purposes
            user_id = "demo_user_001"
            
            # Create or update demo user in the database
            try:
                with engine.connect() as conn:
                    # Check if demo user exists
                    check_query = text("SELECT id, wallet_balance FROM users WHERE id = :user_id")
                    existing_user = conn.execute(check_query, {"user_id": user_id}).fetchone()
                    
                    if not existing_user:
                        # Create demo user with initial funds
                        insert_query = text("""
                            INSERT INTO users 
                            (id, username, email, password, wallet_balance, birthdate, is_verified_adult)
                            VALUES (:id, :username, :email, :password, :wallet_balance, :birthdate, :is_verified_adult)
                        """)
                        conn.execute(insert_query, {
                            "id": user_id,
                            "username": "Demo User",
                            "email": "demo@example.com",
                            "password": "demo123",
                            "wallet_balance": 300.00,
                            "birthdate": "1990-01-01",
                            "is_verified_adult": True
                        })
                        conn.commit()
                        wallet_balance = 300.00
                    else:
                        # Get current wallet balance (or reset it to 300 if needed)
                        wallet_balance = float(existing_user[1])
                        if wallet_balance < 50.0:  # If balance is too low, reset it
                            update_query = text("""
                                UPDATE users SET wallet_balance = 300.00 WHERE id = :user_id
                                RETURNING wallet_balance
                            """)
                            result = conn.execute(update_query, {"user_id": user_id}).fetchone()
                            conn.commit()
                            wallet_balance = 300.00
            except Exception as e:
                st.sidebar.error(f"Database connection error: {str(e)}")
                wallet_balance = 300.00  # Default if DB fails
            
            # Set session state
            st.session_state.logged_in = True
            st.session_state.user_id = user_id
            st.session_state.username = "Demo User"
            st.session_state.wallet_balance = wallet_balance
            st.rerun()
    
    # Display some information for visitors
    st.markdown("""
    ## Welcome to ATHL3T Trades - Fantasy Sports Trading Platform
    
    Please log in or create an account to start trading!
    
    Key Features:
    - Buy and sell player shares and team funds
    - Track your portfolio performance
    - Trade with other users in the marketplace
    - Realistic pricing based on market cap and performance
    """)
    
    # Maybe show some trending players for visitors
    st.subheader("Trending Players")
    trending_players = get_trending_players()
    if trending_players is not None:
        for _, row in trending_players.iterrows():
            st.markdown(f"**{row['Player Name']}** ({row['Team']}) - ${row['Current Price']:.2f} per share")
    else:
        st.info("Trending players data not available. Please check back later.")

else:
    # User is logged in, show the main application
    # Load data once for all pages
    try:
        # Use cached data for better performance
        players, funds, users, holdings = get_cached_data()
        
        # Check if data is None (cache miss or error), fallback to direct loading
        if players is None or funds is None or users is None or holdings is None:
            players, funds, users, holdings = load_data()
        
        # Get current user data
        current_user_id = st.session_state.user_id
        user_wallet = st.session_state.wallet_balance
        
        # Get user holdings if data is loaded
        user_holdings = holdings[holdings["User ID"] == current_user_id]
        player_holdings = user_holdings[user_holdings["asset_type"] == "Player"]
        fund_holdings = user_holdings[user_holdings["asset_type"] == "Team Fund"]
        
        # Check if a player is selected to show details modal
        if st.session_state.selected_player is not None:
            player_name = st.session_state.selected_player
            
            # Create a modal for player details
            with st.container():
                modal_col1, modal_col2 = st.columns([1, 10])
                with modal_col1:
                    if st.button("✖️", key="close_modal"):
                        st.session_state.selected_player = None
                        st.rerun()
                
                with modal_col2:
                    st.subheader(f"{player_name} Details")
                
                # Get player info
                player_info = players[players["Player Name"] == player_name].iloc[0] if player_name in players["Player Name"].values else None
                
                if player_info is not None:
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"**Team:** {player_info['Team']}")
                        st.markdown(f"**Position:** {player_info['Position']}")
                        if 'sport' in player_info:
                            st.markdown(f"**Sport:** {player_info['sport']}")
                        if 'Tier' in player_info:
                            st.markdown(f"**Tier:** {player_info['Tier']}")
                            
                        st.markdown("---")
                        st.markdown(f"**Current Price:** ${player_info['Current Price']:.2f}")
                        
                        # Calculate price change
                        price_change = player_info['Current Price'] - player_info['Initial Price']
                        price_change_pct = (price_change / player_info['Initial Price']) * 100
                        
                        if price_change >= 0:
                            st.markdown(f"**Change:** <span style='color:green'>↑ ${price_change:.2f} ({price_change_pct:.1f}%)</span>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"**Change:** <span style='color:red'>↓ ${abs(price_change):.2f} ({price_change_pct:.1f}%)</span>", unsafe_allow_html=True)
                            
                        # Get shares owned
                        qty_owned = 0
                        if not player_holdings.empty:
                            player_rows = player_holdings[player_holdings["Asset Name"] == player_name]
                            if not player_rows.empty:
                                qty_owned = player_rows.iloc[0]["Quantity"]
                                
                        st.markdown(f"**Shares Owned:** {qty_owned}")
                        st.markdown(f"**Position Value:** ${qty_owned * player_info['Current Price']:.2f}")
                        
                    with col2:
                        # Get historical price data
                        try:
                            # Use our new function to get price history
                            from db import get_player_price_history
                            history = get_player_price_history(player_name)
                            
                            if not history.empty:
                                # Prepare data for the stock-like graph
                                # Create a continuous timeline from first game to now
                                start_date = history['game_date'].min()
                                end_date = datetime.now().date()
                                
                                # Generate price points (starting with first known price)
                                prices = []
                                dates = []
                                last_price = player_info['Initial Price']  # Default if no history
                                
                                if len(history) > 0:
                                    # Start with initial price
                                    prices.append(history.iloc[0]['price_before'])
                                    dates.append(history.iloc[0]['game_date'])
                                    
                                    # Add all performance points
                                    for _, row in history.iterrows():
                                        prices.append(row['price_after'])
                                        dates.append(row['game_date'])
                                        last_price = row['price_after']
                                
                                # Add current price as final point if needed
                                if len(dates) > 0 and dates[-1] < end_date:
                                    prices.append(player_info['Current Price'])
                                    dates.append(end_date)
                                
                                # Create dataframe for the chart
                                chart_data = pd.DataFrame({
                                    'Date': dates,
                                    'Price': prices
                                })
                                
                                # Create a stock-like chart
                                try:
                                    import plotly.express as px
                                    
                                    fig = px.line(
                                        chart_data,
                                        x='Date', 
                                        y='Price',
                                        title=f"{player_name} Price History",
                                        labels={'Price': 'Share Price ($)', 'Date': 'Date'},
                                        markers=True
                                    )
                                    
                                    # Add styling to make it look more like a stock chart
                                    fig.update_layout(
                                        xaxis_title="Date",
                                        yaxis_title="Price ($)",
                                        hovermode="x unified",
                                        font=dict(size=12),
                                        height=500,
                                        xaxis=dict(
                                            showgrid=True,
                                            gridcolor='rgba(230, 230, 230, 0.8)'
                                        ),
                                        yaxis=dict(
                                            showgrid=True,
                                            gridcolor='rgba(230, 230, 230, 0.8)',
                                            tickprefix='$'
                                        ),
                                        plot_bgcolor='white'
                                    )
                                    
                                    # Add range slider
                                    fig.update_layout(
                                        xaxis=dict(
                                            rangeslider=dict(visible=True),
                                            type="date"
                                        )
                                    )
                                    
                                    # Determine line color based on trend
                                    if prices[0] < prices[-1]:
                                        line_color = 'green'
                                    else:
                                        line_color = 'red'
                                        
                                    fig.update_traces(line_color=line_color)
                                    
                                    # Add volume bars at the bottom (simulated)
                                    if len(prices) > 1:
                                        # Create simulated trading volume data
                                        volumes = [abs(prices[i] - prices[i-1])*100 for i in range(1, len(prices))]
                                        volumes.insert(0, volumes[0] if volumes else 100)
                                        
                                        volume_data = pd.DataFrame({
                                            'Date': dates,
                                            'Volume': volumes
                                        })
                                        
                                        # Add a second y-axis for volume
                                        fig.add_bar(
                                            x=volume_data['Date'],
                                            y=volume_data['Volume'],
                                            name='Volume',
                                            marker_color='rgba(0, 0, 255, 0.3)',
                                            opacity=0.3,
                                            yaxis='y2'
                                        )
                                        
                                        # Configure the second y-axis
                                        fig.update_layout(
                                            yaxis2=dict(
                                                title='Volume',
                                                overlaying='y',
                                                side='right',
                                                showgrid=False
                                            )
                                        )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error creating price chart: {str(e)}")
                            else:
                                st.info(f"No historical price data available for {player_name}. This will be populated as they play games.")
                        except Exception as e:
                            st.error(f"Error retrieving historical price data: {str(e)}")
                    
                    # Add game history
                    st.subheader("Recent Game Performances")
                    try:
                        with engine.connect() as conn:
                            # Get game performances
                            games_query = text("""
                                SELECT player_name, game_date, opponent, fantasy_points,
                                       performance_stats, price_before, price_after, price_change_pct
                                FROM player_performance_history
                                WHERE player_name = :player_name
                                ORDER BY game_date DESC
                                LIMIT 5
                            """)
                            games = pd.read_sql(games_query, conn, params={"player_name": player_name})
                            
                            if not games.empty:
                                for _, game in games.iterrows():
                                    with st.expander(f"{game['game_date'].strftime('%Y-%m-%d')} vs. {game['opponent']}"):
                                        left, right = st.columns(2)
                                        
                                        with left:
                                            st.markdown(f"**Fantasy Points:** {game['fantasy_points']:.1f}")
                                            
                                            # Display detailed performance stats
                                            if game['performance_stats']:
                                                st.markdown("**Performance Stats:**")
                                                # Parse JSON stats
                                                try:
                                                    stats = game['performance_stats']
                                                    if isinstance(stats, str):
                                                        import json
                                                        stats = json.loads(stats)
                                                        
                                                    for stat, value in stats.items():
                                                        st.write(f"- {stat.replace('_', ' ').title()}: {value}")
                                                except:
                                                    st.write("Stats data format error")
                                        
                                        with right:
                                            # Price information with color coding
                                            price_before = game['price_before']
                                            price_after = game['price_after']
                                            price_change = game['price_change_pct']
                                            
                                            st.markdown("**Price Impact:**")
                                            st.write(f"Price Before: ${price_before:.2f}")
                                            st.write(f"Price After: ${price_after:.2f}")
                                            
                                            if price_change > 0:
                                                st.markdown(f"<span style='color:green'>↑ +{price_change:.1f}%</span>", unsafe_allow_html=True)
                                            elif price_change < 0:
                                                st.markdown(f"<span style='color:red'>↓ {price_change:.1f}%</span>", unsafe_allow_html=True)
                                            else:
                                                st.write("No change (0%)")
                            else:
                                st.info(f"No game performance data available for {player_name} yet.")
                    except Exception as e:
                        st.error(f"Error retrieving game data: {str(e)}")
                    
                    # Quick trade buttons
                    st.subheader("Quick Trade")
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        buy_qty = st.number_input("Buy Shares", min_value=1, max_value=100, value=1, step=1)
                        total_cost = buy_qty * player_info['Current Price']
                        st.caption(f"Total Cost: ${total_cost:.2f}")
                        
                        if st.button("Buy Now"):
                            if total_cost > user_wallet:
                                st.error(f"Insufficient funds. Need ${total_cost:.2f}, but you have ${user_wallet:.2f}")
                            else:
                                try:
                                    # Execute transaction for each share
                                    success = True
                                    error_message = ""
                                    
                                    for i in range(buy_qty):
                                        success_one, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Player",
                                            asset_name=player_name,
                                            transaction_type="buy",
                                            price=player_info['Current Price'],
                                            users=users,
                                            holdings=holdings
                                        )
                                        if not success_one:
                                            success = False
                                            error_message = message
                                            break
                                    
                                    if success:
                                        st.success(f"Successfully purchased {buy_qty} shares of {player_name}")
                                        
                                        # Update session state wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": current_user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = float(result[0])
                                        
                                        st.rerun()
                                    else:
                                        st.error(f"Transaction failed: {error_message}")
                                except Exception as e:
                                    st.error(f"Error processing transaction: {str(e)}")
                    
                    with col2:
                        if qty_owned > 0:
                            sell_qty = st.number_input("Sell Shares", min_value=1, max_value=qty_owned, value=min(1, qty_owned), step=1)
                            total_return = sell_qty * player_info['Current Price']
                            st.caption(f"Total Return: ${total_return:.2f}")
                            
                            if st.button("Sell Now"):
                                try:
                                    # Loop through the number of shares to sell
                                    success = True
                                    for _ in range(sell_qty):
                                        success_one, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Player",
                                            asset_name=player_name,
                                            transaction_type="sell",
                                            price=player_info['Current Price'],
                                            users=users,
                                            holdings=holdings
                                        )
                                        if not success_one:
                                            success = False
                                            st.error(message)
                                            break
                                    
                                    if success:
                                        st.success(f"Successfully sold {sell_qty} shares of {player_name}")
                                        st.rerun()
                                except Exception as e:
                                    st.error(f"Error processing transaction: {str(e)}")
                        else:
                            st.info("You don't own any shares of this player to sell.")
                
                st.markdown("---")
        
        # Update the session state with the latest wallet balance
        try:
            with engine.connect() as conn:
                query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                result = conn.execute(query, {"user_id": current_user_id}).fetchone()
                if result:
                    st.session_state.wallet_balance = result.wallet_balance
                    user_wallet = result.wallet_balance
        except Exception as e:
            st.error(f"Error updating wallet balance: {str(e)}")
        
        # Sidebar with user info and navigation
        st.sidebar.header(f"Wallet Balance: ${user_wallet:.2f}")
        st.sidebar.write(f"User: {st.session_state.username}")
        
        # Add Funds button in sidebar
        if st.sidebar.button("Add Funds"):
            st.session_state.page = "add_funds"
            st.rerun()
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.logged_in = False
            st.session_state.user_id = None
            st.session_state.username = None
            st.session_state.wallet_balance = 0
            st.rerun()
        
        # Check if user is verified adult for betting access
        try:
            with engine.connect() as conn:
                query = text("SELECT is_verified_adult FROM users WHERE id = :user_id")
                result = conn.execute(query, {"user_id": current_user_id}).fetchone()
                is_adult_verified = result[0] if result else False
        except Exception as e:
            st.sidebar.error(f"Error checking age verification: {str(e)}")
            is_adult_verified = False

        # Auto-verify all users for testing purposes (remove in production)
        try:
            with engine.connect() as conn:
                update_query = text("""
                    UPDATE users 
                    SET birthdate = '1990-01-01', is_verified_adult = TRUE 
                    WHERE id = :user_id
                """)
                conn.execute(update_query, {"user_id": current_user_id})
                conn.commit()
                is_adult_verified = True
        except Exception as e:
            st.sidebar.error(f"Error verifying user: {str(e)}")
            
        # Navigation
        if st.session_state.page == "add_funds":
            page = "Add Funds"
        else:
            # Show different navigation options based on age verification
            if is_adult_verified:
                page = st.sidebar.radio("Navigation", [
                    "Dashboard", "Market", "Portfolio", "Sports News", "Live Games",
                    "Friends", "Competitions", "Peer Trading", "Transaction History", 
                    "Sports Betting", "Player Insights", "How It Works", "Add Funds", "Admin"
                ])
            else:
                page = st.sidebar.radio("Navigation", [
                    "Dashboard", "Market", "Portfolio", "Sports News", "Live Games",
                    "Friends", "Competitions", "Peer Trading", "Transaction History", 
                    "Player Insights", "How It Works", "Add Funds", "Admin"
                ])
        
        # Page content based on selection
        if page == "Dashboard":
            st.title("Personal Dashboard")
            st.subheader(f"Welcome back, {st.session_state.username}!")
            
            # Get performance data
            performance_summary = get_performance_summary(current_user_id)
            transaction_history = get_transaction_history_cached(current_user_id)
            
            # Create dashboard layout with cards
            st.markdown("""
            <style>
            .dashboard-card {
                padding: 20px;
                border-radius: 5px;
                background-color: #f8f9fa;
                margin-bottom: 20px;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
            }
            .metric-value {
                font-size: 24px;
                font-weight: bold;
                margin-bottom: 5px;
            }
            .metric-label {
                font-size: 14px;
                color: #6c757d;
            }
            .metric-positive {
                color: #28a745;
            }
            .metric-negative {
                color: #dc3545;
            }
            </style>
            """, unsafe_allow_html=True)
            
            # Top row with key metrics
            col1, col2, col3, col4 = st.columns(4)
            
            # Current portfolio value
            with col1:
                # Get current holdings value
                current_holdings_value = 0
                
                try:
                    with engine.connect() as conn:
                        holdings_query = text("""
                            SELECT h.asset_name, h.asset_type, h.quantity, 
                                   CASE 
                                     WHEN h.asset_type = 'Player' THEN p.current_price
                                     WHEN h.asset_type = 'Team Fund' THEN tf.price
                                     ELSE 0
                                   END as current_price
                            FROM holdings h
                            LEFT JOIN players p ON h.asset_name = p.name AND h.asset_type = 'Player'
                            LEFT JOIN team_funds tf ON h.asset_name = tf.name AND h.asset_type = 'Team Fund'
                            WHERE h.user_id = :user_id
                        """)
                        holdings_result = conn.execute(holdings_query, {"user_id": current_user_id}).fetchall()
                        
                        for holding in holdings_result:
                            asset_name, asset_type, quantity, current_price = holding
                            if current_price:
                                current_holdings_value += quantity * float(current_price)
                    
                except Exception as e:
                    st.error(f"Error fetching holdings: {str(e)}")
                
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value">${current_holdings_value:.2f}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">Portfolio Value</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Account balance
            with col2:
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value">${user_wallet:.2f}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">Account Balance</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Total profit/loss
            with col3:
                profit_loss = performance_summary.get('total_profit_loss', 0)
                profit_loss_class = "metric-positive" if profit_loss >= 0 else "metric-negative"
                profit_loss_prefix = "+" if profit_loss > 0 else ""
                
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value {profit_loss_class}">{profit_loss_prefix}${profit_loss:.2f}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">Total Profit/Loss</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Total transactions
            with col4:
                total_transactions = performance_summary.get('buy_count', 0) + performance_summary.get('sell_count', 0)
                
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown(f'<div class="metric-value">{total_transactions}</div>', unsafe_allow_html=True)
                st.markdown('<div class="metric-label">Total Transactions</div>', unsafe_allow_html=True)
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Middle row with charts and visualizations
            st.subheader("Portfolio Analysis")
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown("#### Portfolio Composition")
                
                try:
                    # Get asset distribution for pie chart
                    holdings_by_type = {}
                    
                    for holding in holdings_result:
                        asset_name, asset_type, quantity, current_price = holding
                        if current_price:
                            value = quantity * float(current_price)
                            
                            if asset_type == 'Player':
                                # Get player sport if available
                                try:
                                    # Detect sport using team name
                                    from db import detect_sport_from_team
                                    
                                    team_query = text("""
                                        SELECT team FROM players 
                                        WHERE name = :player_name
                                    """)
                                    team_result = conn.execute(team_query, {"player_name": asset_name}).fetchone()
                                    
                                    if team_result and team_result[0]:
                                        sport = detect_sport_from_team(team_result[0])
                                        category = f"{sport} Players"
                                    else:
                                        category = "Players"
                                except:
                                    category = "Players"
                            else:
                                category = asset_type
                                
                            if category in holdings_by_type:
                                holdings_by_type[category] += value
                            else:
                                holdings_by_type[category] = value
                    
                    if holdings_by_type:
                        # Create pie chart
                        fig = px.pie(
                            values=list(holdings_by_type.values()),
                            names=list(holdings_by_type.keys()),
                            title="Asset Distribution",
                            color_discrete_sequence=px.colors.qualitative.Bold
                        )
                        fig.update_traces(textposition='inside', textinfo='percent+label')
                        fig.update_layout(margin=dict(t=30, b=0, l=0, r=0), height=300)
                        st.plotly_chart(fig, use_container_width=True)
                    else:
                        st.info("No holdings data available. Start building your portfolio!")
                        
                except Exception as e:
                    st.error(f"Error creating portfolio composition chart: {str(e)}")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown("#### Transaction History")
                
                if not transaction_history.empty:
                    # Make sure timestamp is datetime type for grouping by date
                    try:
                        # Convert timestamp back to datetime if it's a string
                        if isinstance(transaction_history['timestamp'].iloc[0], str):
                            transaction_history['timestamp'] = pd.to_datetime(transaction_history['timestamp'])
                        
                        # Extract date from timestamp for grouping
                        transaction_history['date'] = transaction_history['timestamp'].dt.date
                        
                        # Look for transaction_type or type column
                        type_column = 'transaction_type' if 'transaction_type' in transaction_history.columns else 'type'
                        
                        # Group transactions by date for the chart
                        daily_totals = transaction_history.groupby('date').agg(
                            buy_value=pd.NamedAgg(column='value', aggfunc=lambda x: x.where(transaction_history[type_column] == 'Buy').sum()),
                            sell_value=pd.NamedAgg(column='value', aggfunc=lambda x: x.where(transaction_history[type_column] == 'Sell').sum())
                        ).reset_index()
                    except Exception as e:
                        st.error(f"Error processing transaction history: {str(e)}")
                        daily_totals = pd.DataFrame(columns=['date', 'buy_value', 'sell_value'])
                    
                    # Create transaction history chart
                    transactions_fig = px.bar(
                        daily_totals,
                        x='date',
                        y=['buy_value', 'sell_value'],
                        labels={'value': 'Transaction Value ($)', 'date': 'Date', 'variable': 'Type'},
                        title="Daily Transaction Activity",
                        color_discrete_map={'buy_value': '#4CAF50', 'sell_value': '#F44336'},
                        barmode='group'
                    )
                    transactions_fig.update_layout(margin=dict(t=30, b=0, l=0, r=0), height=300)
                    st.plotly_chart(transactions_fig, use_container_width=True)
                else:
                    st.info("No transaction history available yet.")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Add performance metrics rows
            st.subheader("Performance Metrics")
            
            # Time-based Performance
            st.markdown('<div class="dashboard-card" style="padding: 15px;">', unsafe_allow_html=True)
            st.markdown("#### Time-Based Performance")
            
            # Display weekly and monthly performance metrics side by side
            cols = st.columns(2)
            
            # Weekly performance
            with cols[0]:
                weekly_pl = performance_summary.get('weekly_profit_loss', 0)
                weekly_pl_class = "metric-positive" if weekly_pl >= 0 else "metric-negative"
                weekly_pl_sign = "+" if weekly_pl > 0 else ""
                
                st.markdown(f"""
                <div style="text-align:center;">
                    <div style="font-size:14px;color:#6c757d;">Last 7 Days</div>
                    <div style="font-size:22px;font-weight:bold;color:{'#28a745' if weekly_pl >= 0 else '#dc3545'};">
                        {weekly_pl_sign}${weekly_pl:.2f}
                    </div>
                    <div style="font-size:14px;color:#6c757d;">
                        {performance_summary.get('weekly_transaction_count', 0)} transactions
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Monthly performance
            with cols[1]:
                monthly_pl = performance_summary.get('monthly_profit_loss', 0)
                monthly_pl_class = "metric-positive" if monthly_pl >= 0 else "metric-negative"
                monthly_pl_sign = "+" if monthly_pl > 0 else ""
                
                st.markdown(f"""
                <div style="text-align:center;">
                    <div style="font-size:14px;color:#6c757d;">Last 30 Days</div>
                    <div style="font-size:22px;font-weight:bold;color:{'#28a745' if monthly_pl >= 0 else '#dc3545'};">
                        {monthly_pl_sign}${monthly_pl:.2f}
                    </div>
                    <div style="font-size:14px;color:#6c757d;">
                        {performance_summary.get('monthly_transaction_count', 0)} transactions
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Asset Performance
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown("#### Top Performing Assets")
                
                try:
                    # Get top assets by profit
                    with engine.connect() as conn:
                        top_assets_query = text("""
                            SELECT 
                                asset_name, 
                                asset_type,
                                SUM(CASE WHEN transaction_type = 'Sell' THEN profit_loss ELSE 0 END) as total_profit,
                                SUM(CASE WHEN transaction_type = 'Buy' THEN price * quantity ELSE 0 END) as total_invested,
                                SUM(CASE WHEN transaction_type = 'Sell' THEN price * quantity ELSE 0 END) as total_sold
                            FROM transactions
                            WHERE user_id = :user_id
                            GROUP BY asset_name, asset_type
                            HAVING SUM(CASE WHEN transaction_type = 'Sell' THEN profit_loss ELSE 0 END) <> 0
                            ORDER BY total_profit DESC
                            LIMIT 5
                        """)
                        top_assets = conn.execute(top_assets_query, {"user_id": current_user_id}).fetchall()
                        
                        if top_assets:
                            top_assets_data = []
                            for asset in top_assets:
                                name, type_val, profit, invested, sold = asset
                                roi = 0 if invested == 0 else (profit / invested) * 100
                                top_assets_data.append({
                                    "Asset": name,
                                    "Type": type_val,
                                    "Profit/Loss": profit,
                                    "ROI": f"{roi:.1f}%"
                                })
                            
                            top_assets_df = pd.DataFrame(top_assets_data)
                            st.dataframe(
                                top_assets_df,
                                column_config={
                                    "Asset": st.column_config.TextColumn("Asset"),
                                    "Type": st.column_config.TextColumn("Type"),
                                    "Profit/Loss": st.column_config.NumberColumn(
                                        "Profit/Loss", 
                                        format="$%.2f",
                                        help="Total profit or loss from all transactions"
                                    ),
                                    "ROI": st.column_config.TextColumn("ROI")
                                },
                                hide_index=True,
                                use_container_width=True
                            )
                        else:
                            st.info("No asset performance data available yet.")
                    
                except Exception as e:
                    st.error(f"Error fetching top assets: {str(e)}")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="dashboard-card">', unsafe_allow_html=True)
                st.markdown("#### Recent Activity")
                
                if not transaction_history.empty:
                    try:
                        # Look for transaction_type or type column
                        type_column = 'transaction_type' if 'transaction_type' in transaction_history.columns else 'type'
                        asset_column = 'asset_name' if 'asset_name' in transaction_history.columns else 'asset'
                        
                        # Ensure timestamp is datetime for sorting
                        if isinstance(transaction_history['timestamp'].iloc[0], str):
                            transaction_history['timestamp'] = pd.to_datetime(transaction_history['timestamp'])
                            
                        recent_transactions = transaction_history.sort_values('timestamp', ascending=False).head(5)
                        
                        for _, tx in recent_transactions.iterrows():
                            tx_type = tx[type_column]
                            asset = tx[asset_column]
                            
                            tx_type_color = "#4CAF50" if tx_type == 'Buy' else "#F44336"
                            tx_emoji = "🔼" if tx_type == 'Buy' else "🔽"
                            
                            # Format timestamp
                            timestamp_str = pd.to_datetime(tx['timestamp']).strftime('%Y-%m-%d %H:%M')
                            
                            st.markdown(f"""
                            <div style="margin-bottom:10px;">
                                <span style="color:{tx_type_color};font-weight:bold;">{tx_emoji} {tx_type}</span>
                                <span style="font-weight:bold;"> {asset}</span>
                                <div style="display:flex;justify-content:space-between;">
                                    <span>{tx['quantity']} @ ${tx['price']:.2f}</span>
                                    <span style="color:#6c757d;">{timestamp_str}</span>
                                </div>
                            </div>
                            """, unsafe_allow_html=True)
                    except Exception as e:
                        st.error(f"Error displaying recent transactions: {str(e)}")
                else:
                    st.info("No recent activity to display.")
                
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Portfolio Statistics
            st.subheader("Portfolio Statistics")
            
            st.markdown('<div class="dashboard-card" style="padding: 15px;">', unsafe_allow_html=True)
            # Display asset statistics in a clean layout
            
            # Get additional portfolio stats from the performance summary
            distinct_assets = performance_summary.get('distinct_assets_count', 0)
            asset_type_breakdown = performance_summary.get('asset_type_breakdown', {})
            
            # Create metrics display for portfolio
            metrics_cols = st.columns(4)
            
            with metrics_cols[0]:
                st.metric(
                    "Total Assets", 
                    distinct_assets, 
                    help="Number of distinct assets in your portfolio"
                )
                
            with metrics_cols[1]:
                # Calculate portfolio diversity score (0-100)
                # More asset types and more balanced distribution = higher score
                diversity_score = 0
                if distinct_assets > 0:
                    # Start with base score based on number of assets
                    diversity_score = min(50, distinct_assets * 5)
                    
                    # Add points for multiple asset types
                    asset_types_count = len(asset_type_breakdown)
                    diversity_score += min(25, asset_types_count * 10)
                    
                    # Add points for balance between asset types (if applicable)
                    if asset_types_count > 1:
                        # Simple variance calculation - lower variance = higher score
                        total_value = performance_summary.get('current_portfolio_value', 0)
                        if total_value > 0:
                            values = [data.get('profit_loss', 0) for data in asset_type_breakdown.values()]
                            avg = sum(values) / len(values)
                            variance = sum((x - avg) ** 2 for x in values) / len(values)
                            balance_score = 25 * (1 - min(1, variance / (avg**2 + 0.001)))
                            diversity_score += balance_score
                
                st.metric(
                    "Diversity Score", 
                    f"{int(diversity_score)}/100", 
                    help="Higher score means more diversified portfolio across asset types and sports"
                )
                
            with metrics_cols[2]:
                roi = 0
                if performance_summary.get('total_invested', 0) > 0:
                    roi = (performance_summary.get('total_profit_loss', 0) / 
                           performance_summary.get('total_invested', 0)) * 100
                
                roi_display = f"{roi:.1f}%"
                roi_delta = None
                if roi != 0:
                    roi_delta = roi_display
                    
                st.metric(
                    "Overall ROI", 
                    roi_display,
                    delta=roi_delta,
                    delta_color="normal",
                    help="Return on Investment across all transactions"
                )
                
            with metrics_cols[3]:
                # Calculate portfolio health score
                health_score = 50  # Start at neutral
                
                # Boost score for positive ROI, reduce for negative
                if roi > 0:
                    health_score += min(25, roi * 2)
                else:
                    health_score -= min(25, abs(roi) * 2)
                
                # Boost score for diversity
                health_score += diversity_score * 0.25
                
                # Adjust for recent performance
                recent_pl = performance_summary.get('weekly_profit_loss', 0)
                if recent_pl > 0:
                    health_score += 5
                elif recent_pl < 0:
                    health_score -= 5
                
                # Ensure in range 0-100
                health_score = max(0, min(100, health_score))
                
                # Determine color and icon based on score
                if health_score >= 70:
                    health_color = "#28a745"
                    health_icon = "💪"
                elif health_score >= 40:
                    health_color = "#ffc107"
                    health_icon = "👍"
                else:
                    health_color = "#dc3545"
                    health_icon = "⚠️"
                
                st.markdown(f"""
                <div style="text-align: center;">
                    <div style="font-size: 14px; color: #6c757d;">Portfolio Health</div>
                    <div style="font-size: 24px; font-weight: bold; color: {health_color};">
                        {health_icon} {int(health_score)}/100
                    </div>
                </div>
                """, unsafe_allow_html=True)
            
            # Display asset type breakdown if available
            if asset_type_breakdown:
                st.markdown("#### Asset Type Performance")
                
                asset_cols = st.columns(len(asset_type_breakdown) if len(asset_type_breakdown) <= 4 else 4)
                
                for i, (asset_type, data) in enumerate(asset_type_breakdown.items()):
                    col_idx = i % 4  # Wrap to 4 columns
                    with asset_cols[col_idx]:
                        profit = data.get('profit_loss', 0)
                        txn_count = data.get('transaction_count', 0)
                        
                        profit_color = "#28a745" if profit >= 0 else "#dc3545"
                        profit_prefix = "+" if profit > 0 else ""
                        
                        st.markdown(f"""
                        <div style="border: 1px solid #e0e0e0; border-radius: 5px; padding: 10px; text-align: center; margin-bottom: 10px;">
                            <div style="font-weight: bold;">{asset_type}</div>
                            <div style="color: {profit_color}; font-weight: bold;">{profit_prefix}${profit:.2f}</div>
                            <div style="font-size: 12px; color: #6c757d;">{txn_count} transactions</div>
                        </div>
                        """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Removed quick action buttons as requested
        elif page == "Market":
            # Market View
            tab1, tab2, tab3, tab4 = st.tabs(["Player Market", "Team Funds", "Performance Trends", "Search"])
            
            with tab1:
                st.header("Player Market")
                
                # Add search bar for players
                search_query = st.text_input("🔍 Search Players by Name or Team", "")
                
                # Add filtering options for player categories
                col1, col2 = st.columns(2)
                
                with col1:
                    # Add sorting options
                    sort_by = st.selectbox("Sort by", ["Player Name", "Team", "Position", "Current Price", "Tier"])
                    sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True)
                
                with col2:
                    # Add sport filter (new feature)
                    sport_options = ["All"]
                    if "sport" in players.columns and not players["sport"].isna().all():
                        sport_options += list(players["sport"].dropna().unique())
                    
                    sport_filter = st.multiselect("Filter by Sport", 
                                                 options=sport_options,
                                                 default=["All"])
                    
                    # Add filtering by position
                    position_options = ["All"]
                    if "Position" in players.columns:
                        position_options += list(players["Position"].unique())
                        
                    position_filter = st.multiselect("Filter by Position", 
                                                    options=position_options,
                                                    default=["All"])
                    
                    # Safely check if Tier column exists and has values
                    tier_options = ["All"]
                    if "Tier" in players.columns and not players["Tier"].isna().all():
                        tier_options += list(players["Tier"].dropna().unique())
                        
                    tier_filter = st.multiselect("Filter by Category", 
                                                options=tier_options,
                                                default=["All"])
                
                # Apply sorting
                ascending = sort_order == "Ascending"
                sorted_players = players.sort_values(by=sort_by, ascending=ascending)
                
                # Apply sport filter (new feature)
                if "sport" in players.columns and "All" not in sport_filter:
                    sorted_players = sorted_players[sorted_players["sport"].isin(sport_filter)]
                
                # Apply position filter
                if "All" not in position_filter:
                    sorted_players = sorted_players[sorted_players["Position"].isin(position_filter)]
                
                # Apply tier filter
                if "Tier" in players.columns and "All" not in tier_filter:
                    sorted_players = sorted_players[sorted_players["Tier"].isin(tier_filter)]
                    
                # Apply search filter if provided
                if search_query:
                    # Search in player name and team columns (case-insensitive)
                    sorted_players = sorted_players[
                        sorted_players["Player Name"].str.contains(search_query, case=False) | 
                        sorted_players["Team"].str.contains(search_query, case=False) |
                        (sorted_players["Position"].str.contains(search_query, case=False))
                    ]
                
                # Display players in a more organized way
                for _, row in sorted_players.iterrows():
                    player_name = row['Player Name']
                    
                    # Get quantity owned
                    qty_owned = 0
                    if not player_holdings.empty:
                        player_rows = player_holdings[player_holdings["Asset Name"] == player_name]
                        if not player_rows.empty:
                            qty_owned = player_rows.iloc[0]["Quantity"]
                    
                    # Calculate price change and percentage
                    price_change = row['Current Price'] - row['Initial Price']
                    price_change_pct = (price_change / row['Initial Price']) * 100
                    
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                    with col1:
                        # Make player name clickable to open details
                        player_button_key = f"player_btn_{player_name.replace(' ', '_')}"
                        if st.button(f"{player_name} ({row['Team']}) - {row['Position']}", key=player_button_key):
                            # Store selected player in session state to display popup
                            st.session_state.selected_player = player_name
                            st.rerun()
                        
                        # Display sport if available
                        if "sport" in row:
                            sport_text = row['sport']
                            st.caption(f"Sport: {sport_text}")
                            
                        # Display tier if available
                        if "Tier" in row:
                            st.caption(f"{row['Tier']} Tier - Week 1: {row['Week 1 Yards']} yards, {row['Week 1 TDs']} TDs")
                        else:
                            st.caption(f"Week 1: {row['Week 1 Yards']} yards, {row['Week 1 TDs']} TDs")
                        
                        # Display market cap info if available
                        if 'total_worth' in row and 'shares_outstanding' in row:
                            st.caption(f"Market Cap: ${row['total_worth']:,.0f} | Shares: {row['shares_outstanding']:,}")
                    
                    with col2:
                        st.write(f"${row['Current Price']:.2f} per share")
                        # Display price change with color
                        if price_change >= 0:
                            st.caption(f"↑ ${price_change:.2f} ({price_change_pct:.1f}%)")
                        else:
                            st.caption(f"↓ ${abs(price_change):.2f} ({price_change_pct:.1f}%)")
                        
                        # Display fantasy performance metrics if available
                        if 'weekly_change' in row and not pd.isna(row['weekly_change']):
                            weekly_change = row['weekly_change']
                            if weekly_change > 0:
                                performance_text = f"🔥 +{weekly_change:.1f}% this week"
                            elif weekly_change < 0:
                                performance_text = f"❄️ {weekly_change:.1f}% this week"
                            else:
                                performance_text = "No change this week"
                            st.caption(performance_text)
                        
                        if 'last_fantasy_points' in row and not pd.isna(row['last_fantasy_points']) and row['last_fantasy_points'] > 0:
                            st.caption(f"Last Perf: {row['last_fantasy_points']:.1f} fantasy pts")
                            
                        st.write(f"Owned: {qty_owned} shares")
                    
                    with col3:
                        # Buy shares with multiple options
                        share_quantity = st.selectbox(f"Shares to buy", [1, 5, 10], key=f"buy_qty_{player_name}")
                        
                        if st.button(f"Buy {share_quantity}", key=f"buy_{player_name}"):
                            # First check if user has enough funds directly
                            total_cost = share_quantity * row['Current Price']
                            if total_cost > user_wallet:
                                st.error(f"Insufficient funds. Need ${total_cost:.2f}, but you have ${user_wallet:.2f}")
                            else:
                                try:
                                    # Loop through the number of shares to buy
                                    success = True
                                    error_message = ""
                                    
                                    for i in range(share_quantity):
                                        success_one, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Player",
                                            asset_name=player_name,
                                            transaction_type="buy",
                                            price=row['Current Price'],
                                            users=users,
                                            holdings=holdings
                                        )
                                        if not success_one:
                                            success = False
                                            error_message = message
                                            break
                                    
                                    if success:
                                        st.success(f"Successfully purchased {share_quantity} shares of {player_name}")
                                        
                                        # Update session state wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": current_user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = float(result[0])
                                        
                                        st.rerun()
                                    else:
                                        st.error(f"Transaction failed: {error_message}")
                                except Exception as e:
                                    st.error(f"Error processing transaction: {str(e)}")
                    
                    with col4:
                        # Only show sell button if user owns shares
                        if qty_owned > 0:
                            # Sell shares with multiple options
                            sell_quantity = st.selectbox(f"Shares to sell", 
                                                      [1, min(5, qty_owned), min(10, qty_owned)], 
                                                      key=f"sell_qty_{player_name}")
                            
                            if st.button(f"Sell {sell_quantity}", key=f"sell_{player_name}"):
                                # Loop through the number of shares to sell
                                success = True
                                for _ in range(sell_quantity):
                                    success_one, message, users, holdings = execute_transaction(
                                        user_id=current_user_id,
                                        asset_type="Player",
                                        asset_name=player_name,
                                        transaction_type="sell",
                                        price=row['Current Price'],
                                        users=users,
                                        holdings=holdings
                                    )
                                    if not success_one:
                                        success = False
                                        st.error(message)
                                        break
                                
                                if success:
                                    st.success(f"Successfully sold {sell_quantity} shares of {player_name}")
                                    st.rerun()
                        else:
                            st.write("No shares to sell")
                    
                    # Add horizontal line between players
                    st.markdown("---")
            
            with tab2:
                st.header("Team Funds")
                
                # Add search bar for funds
                search_funds_query = st.text_input("🔍 Search Team Funds", "")
                
                # Add filtering options for fund categories
                col1, col2 = st.columns(2)
                
                with col1:
                    # Add sorting options
                    fund_sort_by = st.selectbox("Sort by", ["Fund Name", "Fund Price"], key="fund_sort")
                    fund_sort_order = st.radio("Order", ["Ascending", "Descending"], horizontal=True, key="fund_order")
                
                with col2:
                    # Add filtering by fund type
                    fund_type_options = ["All"]
                    if "Type" in funds.columns and not funds["Type"].isna().all():
                        fund_type_options += list(funds["Type"].dropna().unique())
                        
                    fund_type_filter = st.multiselect("Filter by Type", 
                                                    options=fund_type_options,
                                                    default=["All"])
                
                # Apply sorting
                ascending = fund_sort_order == "Ascending"
                sorted_funds = funds.sort_values(by=fund_sort_by, ascending=ascending)
                
                # Apply type filter
                if "Type" in funds.columns and "All" not in fund_type_filter:
                    sorted_funds = sorted_funds[sorted_funds["Type"].isin(fund_type_filter)]
                    
                # Apply search filter if provided
                if search_funds_query:
                    # Search in fund name and players included (case-insensitive)
                    sorted_funds = sorted_funds[
                        sorted_funds["Fund Name"].str.contains(search_funds_query, case=False) | 
                        sorted_funds["Players Included"].str.contains(search_funds_query, case=False)
                    ]
                
                for _, row in sorted_funds.iterrows():
                    fund_name = row['Fund Name']
                    
                    # Get quantity owned
                    qty_owned = 0
                    if not fund_holdings.empty:
                        fund_rows = fund_holdings[fund_holdings["Asset Name"] == fund_name]
                        if not fund_rows.empty:
                            qty_owned = fund_rows.iloc[0]["Quantity"]
                    
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
                    with col1:
                        st.markdown(f"**{fund_name}**")
                        st.caption(f"Includes: {row['Players Included']}")
                        
                        # Display fund type if available
                        if "Type" in row:
                            st.caption(f"Fund Type: {row['Type']}")
                    
                    with col2:
                        st.write(f"${row['Fund Price']:.2f} per share")
                        st.write(f"Owned: {qty_owned} shares")
                        # Add premium fund note
                        st.caption("Team Funds have premium pricing")
                    
                    with col3:
                        # Buy shares with multiple options
                        share_quantity = st.selectbox(f"Shares to buy", [1, 5, 10], key=f"buy_qty_{fund_name}")
                        
                        if st.button(f"Buy {share_quantity}", key=f"buy_{fund_name}"):
                            # First check if user has enough funds directly
                            total_cost = share_quantity * row['Fund Price']
                            if total_cost > user_wallet:
                                st.error(f"Insufficient funds. Need ${total_cost:.2f}, but you have ${user_wallet:.2f}")
                            else:
                                try:
                                    # Loop through the number of shares to buy
                                    success = True
                                    error_message = ""
                                    
                                    for i in range(share_quantity):
                                        success_one, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Team Fund",
                                            asset_name=fund_name,
                                            transaction_type="buy",
                                            price=row['Fund Price'],
                                            users=users,
                                            holdings=holdings
                                        )
                                        if not success_one:
                                            success = False
                                            error_message = message
                                            break
                                    
                                    if success:
                                        st.success(f"Successfully purchased {share_quantity} shares of {fund_name}")
                                        
                                        # Update session state wallet balance
                                        with engine.connect() as conn:
                                            query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                                            result = conn.execute(query, {"user_id": current_user_id}).fetchone()
                                            if result:
                                                st.session_state.wallet_balance = float(result[0])
                                        
                                        st.rerun()
                                    else:
                                        st.error(f"Transaction failed: {error_message}")
                                except Exception as e:
                                    st.error(f"Error processing transaction: {str(e)}")
                    
                    with col4:
                        # Only show sell button if user owns shares
                        if qty_owned > 0:
                            # Sell shares with multiple options
                            sell_quantity = st.selectbox(f"Shares to sell", 
                                                      [1, min(5, qty_owned), min(10, qty_owned)], 
                                                      key=f"sell_qty_{fund_name}")
                            
                            if st.button(f"Sell {sell_quantity}", key=f"sell_{fund_name}"):
                                # Loop through the number of shares to sell
                                success = True
                                for _ in range(sell_quantity):
                                    success_one, message, users, holdings = execute_transaction(
                                        user_id=current_user_id,
                                        asset_type="Team Fund",
                                        asset_name=fund_name,
                                        transaction_type="sell",
                                        price=row['Fund Price'],
                                        users=users,
                                        holdings=holdings
                                    )
                                    if not success_one:
                                        success = False
                                        st.error(message)
                                        break
                                
                                if success:
                                    st.success(f"Successfully sold {sell_quantity} shares of {fund_name}")
                                    st.rerun()
                        else:
                            st.write("No shares to sell")
                    
                    # Add horizontal line between funds
                    st.markdown("---")
            
            with tab3:
                st.header("Performance Trends")
                st.write("Track how player values change based on their real-world performance using fantasy sports metrics.")
                
                # Create tabs for different performance views
                perf_tabs = st.tabs(["Top Gainers", "Top Losers", "Recent Performance"])
                
                # Helper function to add trend indicators
                def get_trend_indicator(change):
                    if pd.isna(change) or change == 0:
                        return "➖"  # No change
                    elif change > 0:
                        if change >= 10:
                            return "🚀 +{:.1f}%".format(change)  # Rocket for big gains
                        elif change >= 5:
                            return "⬆️ +{:.1f}%".format(change)  # Up arrow for moderate gains
                        else:
                            return "↗️ +{:.1f}%".format(change)  # Up-right arrow for small gains
                    else:  # change < 0
                        if change <= -10:
                            return "📉 {:.1f}%".format(change)  # Chart down for big losses
                        elif change <= -5:
                            return "⬇️ {:.1f}%".format(change)  # Down arrow for moderate losses
                        else:
                            return "↘️ {:.1f}%".format(change)  # Down-right for small losses
                
                with perf_tabs[0]:
                    st.subheader("Top Price Gainers")
                    
                    # Try to get the weekly_change column, if it exists
                    if 'weekly_change' in players.columns:
                        # Sort by weekly_change descending
                        top_gainers = players.sort_values(by='weekly_change', ascending=False).head(10)
                        
                        # Create a display dataframe with selected columns
                        if not top_gainers.empty:
                            display_cols = ['Player Name', 'Position', 'Team', 'Current Price', 'weekly_change', 'last_fantasy_points']
                            display_cols = [col for col in display_cols if col in top_gainers.columns]
                            
                            display_df = top_gainers[display_cols].copy()
                            
                            # Add trend indicators
                            if 'weekly_change' in display_df.columns:
                                display_df['Trend'] = display_df['weekly_change'].apply(get_trend_indicator)
                                
                            # Rename columns for better display
                            display_df = display_df.rename(columns={
                                'Player Name': 'Player',
                                'weekly_change': 'Change (%)',
                                'last_fantasy_points': 'Fantasy Pts'
                            })
                            
                            # Display the dataframe
                            st.dataframe(display_df)
                            
                            # Create a bar chart of top gainers
                            try:
                                import plotly.express as px
                                
                                fig = px.bar(
                                    top_gainers,
                                    y='Player Name',
                                    x='weekly_change',
                                    orientation='h',
                                    title='Top Price Gainers (%)',
                                    labels={'weekly_change': 'Price Change (%)', 'Player Name': ''},
                                    color='weekly_change',
                                    color_continuous_scale=[(0, "green"), (1, "darkgreen")],
                                    height=400
                                )
                                
                                fig.update_layout(
                                    xaxis_title='Price Change (%)',
                                    yaxis_title='',
                                    coloraxis_showscale=False
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error creating chart: {str(e)}")
                        else:
                            st.info("No performance data available yet. Check back after player performances have been updated.")
                    else:
                        st.info("Performance tracking data is not yet available. Check back after games have been played.")
                
                with perf_tabs[1]:
                    st.subheader("Top Price Losers")
                    
                    # Try to get the weekly_change column, if it exists
                    if 'weekly_change' in players.columns:
                        # Sort by weekly_change ascending
                        top_losers = players.sort_values(by='weekly_change', ascending=True).head(10)
                        
                        # Create a display dataframe with selected columns
                        if not top_losers.empty:
                            display_cols = ['Player Name', 'Position', 'Team', 'Current Price', 'weekly_change', 'last_fantasy_points']
                            display_cols = [col for col in display_cols if col in top_losers.columns]
                            
                            display_df = top_losers[display_cols].copy()
                            
                            # Add trend indicators
                            if 'weekly_change' in display_df.columns:
                                display_df['Trend'] = display_df['weekly_change'].apply(get_trend_indicator)
                                
                            # Rename columns for better display
                            display_df = display_df.rename(columns={
                                'Player Name': 'Player',
                                'weekly_change': 'Change (%)',
                                'last_fantasy_points': 'Fantasy Pts'
                            })
                            
                            # Display the dataframe
                            st.dataframe(display_df)
                            
                            # Create a bar chart of top losers
                            try:
                                import plotly.express as px
                                
                                fig = px.bar(
                                    top_losers,
                                    y='Player Name',
                                    x='weekly_change',
                                    orientation='h',
                                    title='Top Price Losers (%)',
                                    labels={'weekly_change': 'Price Change (%)', 'Player Name': ''},
                                    color='weekly_change',
                                    color_continuous_scale=[(0, "darkred"), (1, "red")],
                                    height=400
                                )
                                
                                fig.update_layout(
                                    xaxis_title='Price Change (%)',
                                    yaxis_title='',
                                    coloraxis_showscale=False
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error creating chart: {str(e)}")
                        else:
                            st.info("No performance data available yet. Check back after player performances have been updated.")
                    else:
                        st.info("Performance tracking data is not yet available. Check back after games have been played.")
                
                with perf_tabs[2]:
                    st.subheader("Recent Player Performance")
                    
                    # Try to get fantasy points data
                    if 'last_fantasy_points' in players.columns:
                        # Sort by fantasy points descending
                        top_performers = players.sort_values(by='last_fantasy_points', ascending=False).head(10)
                        
                        if not top_performers.empty:
                            # Create a display dataframe
                            display_cols = ['Player Name', 'Position', 'Team', 'Current Price', 'last_fantasy_points', 'weekly_change']
                            display_cols = [col for col in display_cols if col in top_performers.columns]
                            
                            display_df = top_performers[display_cols].copy()
                            
                            # Add trend indicators if weekly_change exists
                            if 'weekly_change' in display_df.columns:
                                display_df['Trend'] = display_df['weekly_change'].apply(get_trend_indicator)
                            
                            # Rename columns for better display
                            display_df = display_df.rename(columns={
                                'Player Name': 'Player',
                                'weekly_change': 'Price Change (%)',
                                'last_fantasy_points': 'Fantasy Pts'
                            })
                            
                            # Display the dataframe
                            st.dataframe(display_df)
                            
                            # Create a bar chart of top fantasy performers
                            try:
                                import plotly.express as px
                                
                                fig = px.bar(
                                    top_performers,
                                    y='Player Name',
                                    x='last_fantasy_points',
                                    orientation='h',
                                    title='Top Fantasy Performers',
                                    labels={'last_fantasy_points': 'Fantasy Points', 'Player Name': ''},
                                    color='Position',
                                    color_discrete_sequence=px.colors.qualitative.Safe,
                                    height=400
                                )
                                
                                fig.update_layout(
                                    xaxis_title='Fantasy Points',
                                    yaxis_title='',
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error creating chart: {str(e)}")
                        else:
                            st.info("No fantasy point data available yet. Check back after games have been played.")
                    else:
                        st.info("Fantasy point tracking is not yet available. Check back after games have been played.")
                        
                # Add a section explaining how performance affects price
                with st.expander("How Performance Affects Player Price"):
                    st.markdown("""
                    ### Fantasy Sports Scoring
                    Player prices automatically adjust based on their real-world performance using standardized fantasy sports metrics. Each position in each sport has specific scoring criteria:
                    
                    **NFL Example:**
                    - QBs: Passing yards, touchdowns, interceptions
                    - RBs: Rushing yards, touchdowns, receiving yards
                    - WRs: Receiving yards, touchdowns, catches
                    
                    **NBA Example:**
                    - Points scored, rebounds, assists, steals, blocks
                    - Bonus for double-doubles and triple-doubles
                    
                    **MLB Example:**
                    - Batters: Hits, runs, RBIs, home runs
                    - Pitchers: Strikeouts, innings pitched, earned runs
                    
                    ### Price Adjustment Tiers
                    Player prices change based on their percentile performance compared to others at their position:
                    
                    - **Exceptional (95th+ percentile)**: +15% price increase 🚀
                    - **Excellent (90th+ percentile)**: +10% price increase ⬆️
                    - **Very Good (80th+ percentile)**: +7% price increase ↗️
                    - **Good (70th+ percentile)**: +5% price increase
                    - **Above Average (60th+ percentile)**: +3% price increase
                    - **Average (50th+ percentile)**: +1% price increase
                    - **Below Average (40th+ percentile)**: -1% price decrease
                    - **Poor (30th+ percentile)**: -3% price decrease
                    - **Very Poor (20th+ percentile)**: -5% price decrease ↘️
                    - **Terrible (10th+ percentile)**: -10% price decrease ⬇️
                    - **Disastrous (Bottom 5%)**: -15% price decrease 📉
                    """)
            
            with tab4:
                st.header("Market Search")
                
                # Comprehensive search functionality
                st.write("Search for players and funds by name, team, position, or sport")
                
                search_all_query = st.text_input("🔍 Global Search", "")
                
                if search_all_query:
                    # Search in players
                    player_results = players[
                        players["Player Name"].str.contains(search_all_query, case=False) | 
                        players["Team"].str.contains(search_all_query, case=False) |
                        (players["Position"].str.contains(search_all_query, case=False))
                    ]
                    
                    # Search in funds
                    fund_results = funds[
                        funds["Fund Name"].str.contains(search_all_query, case=False) | 
                        funds["Players Included"].str.contains(search_all_query, case=False) |
                        (funds["Type"].str.contains(search_all_query, case=False) if "Type" in funds.columns else False)
                    ]
                    
                    # Display results
                    if not player_results.empty:
                        st.subheader(f"Player Results ({len(player_results)} found)")
                        st.dataframe(player_results)
                    
                    if not fund_results.empty:
                        st.subheader(f"Fund Results ({len(fund_results)} found)")
                        st.dataframe(fund_results)
                        
                    if player_results.empty and fund_results.empty:
                        st.info(f"No results found for '{search_all_query}'")
                else:
                    st.info("Enter a search term to find players and funds")
        
        elif page == "Portfolio":
            st.header("My Portfolio")
            
            # Check if user has any holdings
            if user_holdings.empty:
                st.info("You don't have any holdings yet. Visit the Market to start trading!")
            else:
                # Calculate portfolio value
                portfolio_value = 0
                player_value = 0
                fund_value = 0
                
                # Add player holdings value
                if not player_holdings.empty:
                    for _, holding in player_holdings.iterrows():
                        player_name = holding["Asset Name"]
                        quantity = holding["Quantity"]
                        
                        # Get current price
                        player_row = players[players["Player Name"] == player_name]
                        if not player_row.empty:
                            current_price = player_row.iloc[0]["Current Price"]
                            value = quantity * current_price
                            player_value += value
                
                # Add fund holdings value
                if not fund_holdings.empty:
                    for _, holding in fund_holdings.iterrows():
                        fund_name = holding["Asset Name"]
                        quantity = holding["Quantity"]
                        
                        # Get current price
                        fund_row = funds[funds["Fund Name"] == fund_name]
                        if not fund_row.empty:
                            current_price = fund_row.iloc[0]["Fund Price"]
                            value = quantity * current_price
                            fund_value += value
                
                portfolio_value = player_value + fund_value
                
                # Display portfolio summary
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Portfolio Value", f"${portfolio_value:.2f}")
                with col2:
                    st.metric("Player Holdings", f"${player_value:.2f}")
                with col3:
                    st.metric("Fund Holdings", f"${fund_value:.2f}")
                
                # Display holdings by tabs
                portfolio_tabs = st.tabs(["Player Holdings", "Fund Holdings"])
                
                with portfolio_tabs[0]:
                    st.subheader("Player Holdings")
                    
                    if player_holdings.empty:
                        st.info("You don't own any player shares yet.")
                    else:
                        # Enhanced player holdings table
                        for _, holding in player_holdings.iterrows():
                            player_name = holding["Asset Name"]
                            quantity = holding["Quantity"]
                            # Use current price as purchase price since the column is missing
                            purchase_price = 0.0  # Default value
                            
                            # Get current market data
                            player_row = players[players["Player Name"] == player_name]
                            if not player_row.empty:
                                player_data = player_row.iloc[0]
                                current_price = player_data["Current Price"]
                                
                                # Calculate holding metrics
                                total_value = quantity * current_price
                                profit_loss = (current_price - purchase_price) * quantity
                                profit_loss_pct = safe_division(current_price - purchase_price, purchase_price, 0) * 100
                                
                                # Display holding info
                                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                                
                                with col1:
                                    st.markdown(f"**{player_name}** ({player_data['Team']}) - {player_data['Position']}")
                                    st.caption(f"Purchased at: ${purchase_price:.2f} per share")
                                
                                with col2:
                                    st.write(f"Shares Owned: {quantity}")
                                    st.write(f"Current Price: ${current_price:.2f}")
                                
                                with col3:
                                    st.write(f"Total Value: ${total_value:.2f}")
                                    # Show profit/loss with appropriate color
                                    if profit_loss >= 0:
                                        st.write(f"Profit: ${profit_loss:.2f} (↑ {profit_loss_pct:.1f}%)")
                                    else:
                                        st.write(f"Loss: ${abs(profit_loss):.2f} (↓ {abs(profit_loss_pct):.1f}%)")
                                
                                with col4:
                                    if st.button("Sell", key=f"portfolio_sell_{player_name}"):
                                        success, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Player",
                                            asset_name=player_name,
                                            transaction_type="sell",
                                            price=current_price,
                                            users=users,
                                            holdings=holdings
                                        )
                                        
                                        if success:
                                            st.success(f"Successfully sold 1 share of {player_name}")
                                            st.rerun()
                                        else:
                                            st.error(message)
                            
                            st.markdown("---")
                
                with portfolio_tabs[1]:
                    st.subheader("Fund Holdings")
                    
                    if fund_holdings.empty:
                        st.info("You don't own any fund shares yet.")
                    else:
                        # Enhanced fund holdings table
                        for _, holding in fund_holdings.iterrows():
                            fund_name = holding["Asset Name"]
                            quantity = holding["Quantity"]
                            # Use current price as purchase price since the column is missing
                            purchase_price = 0.0  # Default value
                            
                            # Get current market data
                            fund_row = funds[funds["Fund Name"] == fund_name]
                            if not fund_row.empty:
                                fund_data = fund_row.iloc[0]
                                current_price = fund_data["Fund Price"]
                                
                                # Calculate holding metrics
                                total_value = quantity * current_price
                                profit_loss = (current_price - purchase_price) * quantity
                                profit_loss_pct = safe_division(current_price - purchase_price, purchase_price, 0) * 100
                                
                                # Display holding info
                                col1, col2, col3, col4 = st.columns([3, 2, 2, 1])
                                
                                with col1:
                                    st.markdown(f"**{fund_name}**")
                                    st.caption(f"Purchased at: ${purchase_price:.2f} per share")
                                
                                with col2:
                                    st.write(f"Shares Owned: {quantity}")
                                    st.write(f"Current Price: ${current_price:.2f}")
                                
                                with col3:
                                    st.write(f"Total Value: ${total_value:.2f}")
                                    # Show profit/loss with appropriate color
                                    if profit_loss >= 0:
                                        st.write(f"Profit: ${profit_loss:.2f} (↑ {profit_loss_pct:.1f}%)")
                                    else:
                                        st.write(f"Loss: ${abs(profit_loss):.2f} (↓ {abs(profit_loss_pct):.1f}%)")
                                
                                with col4:
                                    if st.button("Sell", key=f"portfolio_sell_{fund_name}"):
                                        success, message, users, holdings = execute_transaction(
                                            user_id=current_user_id,
                                            asset_type="Team Fund",
                                            asset_name=fund_name,
                                            transaction_type="sell",
                                            price=current_price,
                                            users=users,
                                            holdings=holdings
                                        )
                                        
                                        if success:
                                            st.success(f"Successfully sold 1 share of {fund_name}")
                                            st.rerun()
                                        else:
                                            st.error(message)
                            
                            st.markdown("---")
                
        elif page == "Transaction History":
            st.header("Transaction History")
            
            try:
                # Get transaction history from database
                transactions = get_transaction_history(current_user_id)
                
                if transactions is None or transactions.empty:
                    st.info("No transaction history found. Start trading to build your history!")
                else:
                    # Display transactions in a table
                    st.dataframe(transactions, use_container_width=True)
                    
                    # Get performance summary
                    summary = get_performance_summary(current_user_id)
                    
                    if summary:
                        # Show performance metrics
                        st.subheader("Performance Summary")
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.metric("Total Invested", f"${summary['total_invested']:.2f}")
                        
                        with col2:
                            st.metric("Total Profits/Losses", f"${summary['total_pl']:.2f}")
                        
                        with col3:
                            st.metric("Return on Investment", f"{summary['roi']:.2f}%")
                        
                        # Create a plot of transaction history
                        if 'date' in transactions.columns and 'value' in transactions.columns:
                            st.subheader("Transaction Value Over Time")
                            
                            # Prepare data for plotting
                            transactions['date'] = pd.to_datetime(transactions['date'])
                            transactions = transactions.sort_values('date')
                            
                            # Create a line chart of transaction values over time
                            fig = px.line(
                                transactions, 
                                x='date', 
                                y='value',
                                color='transaction_type',
                                title='Transaction Values Over Time'
                            )
                            
                            st.plotly_chart(fig, use_container_width=True)
            except Exception as e:
                st.error(f"An error occurred loading transaction history: {str(e)}")
                st.info("Start trading to build your transaction history!")
                
        elif page == "Peer Trading":
            st.header("Peer-to-Peer Trading Marketplace")
            
            # Create tabs for browsing offers and creating new offers
            trade_tabs = st.tabs(["Browse Offers", "Create Offer", "My Offers", "Player-for-Player"])
            
            with trade_tabs[0]:
                st.subheader("Available Trading Offers")
                
                # Get all active trade offers
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT 
                                t.id, 
                                u.username as seller, 
                                t.asset_type, 
                                t.asset_name, 
                                t.quantity, 
                                t.price_per_share, 
                                t.total_price,
                                t.created_at
                            FROM trade_offers t
                            JOIN users u ON t.seller_id = u.id
                            WHERE t.status = 'active' AND t.seller_id != :current_user_id
                            ORDER BY t.created_at DESC
                        """)
                        trade_offers = pd.read_sql(query, conn, params={"current_user_id": current_user_id})
                    
                    if trade_offers.empty:
                        st.info("No trading offers available right now.")
                    else:
                        # Add search and filter options
                        search_offer = st.text_input("🔍 Search by Asset Name or Seller", "")
                        
                        # Filter by asset type
                        asset_type_filter = st.multiselect(
                            "Filter by Asset Type", 
                            options=["All"] + list(trade_offers["asset_type"].unique()),
                            default=["All"]
                        )
                        
                        # Apply filters
                        filtered_offers = trade_offers.copy()
                        
                        if search_offer:
                            filtered_offers = filtered_offers[
                                filtered_offers["asset_name"].str.contains(search_offer, case=False) | 
                                filtered_offers["seller"].str.contains(search_offer, case=False)
                            ]
                        
                        if "All" not in asset_type_filter:
                            filtered_offers = filtered_offers[filtered_offers["asset_type"].isin(asset_type_filter)]
                        
                        # Display offers
                        for _, offer in filtered_offers.iterrows():
                            offer_id = offer["id"]
                            
                            col1, col2, col3 = st.columns([2, 2, 1])
                            
                            with col1:
                                st.markdown(f"**{offer['asset_name']}** ({offer['asset_type']})")
                                st.caption(f"Seller: {offer['seller']}")
                                st.caption(f"Created: {offer['created_at']}")
                            
                            with col2:
                                st.write(f"Quantity: {offer['quantity']} shares")
                                st.write(f"Price: ${offer['price_per_share']:.2f} per share")
                                st.caption(f"Total: ${offer['total_price']:.2f}")
                            
                            with col3:
                                if st.button("Buy Now", key=f"buy_offer_{offer_id}"):
                                    # Check if user has enough funds
                                    if user_wallet < offer['total_price']:
                                        st.error("Insufficient funds for this purchase.")
                                    else:
                                        # Execute the purchase
                                        try:
                                            with engine.connect() as conn:
                                                # Start transaction
                                                transaction = conn.begin()
                                                
                                                try:
                                                    # 1. Update offer status
                                                    update_offer = text("""
                                                        UPDATE trade_offers
                                                        SET status = 'completed'
                                                        WHERE id = :offer_id
                                                        RETURNING seller_id
                                                    """)
                                                    seller_result = conn.execute(update_offer, {"offer_id": offer_id}).fetchone()
                                                    seller_id = seller_result.seller_id
                                                    
                                                    # 2. Transfer funds from buyer to seller
                                                    update_buyer = text("""
                                                        UPDATE users
                                                        SET wallet_balance = wallet_balance - :amount
                                                        WHERE id = :buyer_id
                                                    """)
                                                    conn.execute(update_buyer, {
                                                        "amount": offer['total_price'],
                                                        "buyer_id": current_user_id
                                                    })
                                                    
                                                    update_seller = text("""
                                                        UPDATE users
                                                        SET wallet_balance = wallet_balance + :amount
                                                        WHERE id = :seller_id
                                                    """)
                                                    conn.execute(update_seller, {
                                                        "amount": offer['total_price'],
                                                        "seller_id": seller_id
                                                    })
                                                    
                                                    # 3. Transfer asset ownership
                                                    # First check if buyer already owns some of this asset
                                                    check_holding = text("""
                                                        SELECT id, quantity FROM holdings
                                                        WHERE user_id = :user_id AND asset_name = :asset_name AND asset_type = :asset_type
                                                    """)
                                                    existing = conn.execute(check_holding, {
                                                        "user_id": current_user_id,
                                                        "asset_name": offer['asset_name'],
                                                        "asset_type": offer['asset_type']
                                                    }).fetchone()
                                                    
                                                    if existing:
                                                        # Update existing holding
                                                        update_holding = text("""
                                                            UPDATE holdings
                                                            SET quantity = quantity + :quantity
                                                            WHERE id = :holding_id
                                                        """)
                                                        conn.execute(update_holding, {
                                                            "quantity": offer['quantity'],
                                                            "holding_id": existing.id
                                                        })
                                                    else:
                                                        # Create new holding
                                                        insert_holding = text("""
                                                            INSERT INTO holdings (user_id, type, asset_name, quantity, purchase_price)
                                                            VALUES (:user_id, :asset_type, :asset_name, :quantity, :price)
                                                        """)
                                                        conn.execute(insert_holding, {
                                                            "user_id": current_user_id,
                                                            "asset_type": offer['asset_type'],
                                                            "asset_name": offer['asset_name'],
                                                            "quantity": offer['quantity'],
                                                            "price": offer['price_per_share']
                                                        })
                                                    
                                                    # 4. Remove from seller's holdings
                                                    update_seller_holding = text("""
                                                        UPDATE holdings
                                                        SET quantity = quantity - :quantity
                                                        WHERE user_id = :seller_id AND asset_name = :asset_name AND asset_type = :asset_type
                                                    """)
                                                    conn.execute(update_seller_holding, {
                                                        "quantity": offer['quantity'],
                                                        "seller_id": seller_id,
                                                        "asset_name": offer['asset_name'],
                                                        "asset_type": offer['asset_type']
                                                    })
                                                    
                                                    # 5. Record the transaction in the transactions table
                                                    insert_transaction = text("""
                                                        INSERT INTO transactions 
                                                        (user_id, transaction_type, asset_type, asset_name, price, quantity, value)
                                                        VALUES (:user_id, 'Buy P2P', :asset_type, :asset_name, :price, :quantity, :value)
                                                    """)
                                                    conn.execute(insert_transaction, {
                                                        "user_id": current_user_id,
                                                        "asset_type": offer['asset_type'],
                                                        "asset_name": offer['asset_name'],
                                                        "price": offer['price_per_share'],
                                                        "quantity": offer['quantity'],
                                                        "value": offer['total_price']
                                                    })
                                                    
                                                    # Commit the transaction
                                                    transaction.commit()
                                                    
                                                    # Update session state with new balance
                                                    st.session_state.wallet_balance -= offer['total_price']
                                                    user_wallet = st.session_state.wallet_balance
                                                    
                                                    st.success(f"Successfully purchased {offer['quantity']} shares of {offer['asset_name']}")
                                                    st.rerun()
                                                    
                                                except Exception as e:
                                                    # Rollback in case of error
                                                    transaction.rollback()
                                                    st.error(f"Transaction failed: {str(e)}")
                                        
                                        except Exception as e:
                                            st.error(f"Error purchasing offer: {str(e)}")
                            
                            st.markdown("---")
                except Exception as e:
                    st.error(f"Error loading trade offers: {str(e)}")
            
            with trade_tabs[1]:
                st.subheader("Create a New Trading Offer")
                
                # Select asset to sell
                asset_type = st.selectbox("Asset Type", ["Player", "Team Fund"])
                
                if asset_type == "Player":
                    # Get player holdings
                    available_players = []
                    if player_holdings is not None and not player_holdings.empty:
                        available_players = player_holdings["Asset Name"].tolist()
                    
                    if not available_players:
                        st.info("You don't have any player shares to sell. Purchase some in the Market first.")
                    else:
                        asset_name = st.selectbox("Select Player", available_players)
                        
                        # Get current holding data
                        holding_row = player_holdings[player_holdings["Asset Name"] == asset_name].iloc[0]
                        current_holding = holding_row["Quantity"]
                        
                        # Get current market price
                        market_price = 0
                        if not players.empty:
                            player_matches = players[players["Player Name"] == asset_name]
                            if not player_matches.empty:
                                market_price = player_matches.iloc[0]["Current Price"]
                        
                        # Form for creating offer
                        st.write(f"You currently own {current_holding} shares")
                        st.write(f"Current market price: ${market_price:.2f} per share")
                        
                        quantity = st.number_input("Quantity to Sell", min_value=1, max_value=current_holding, value=1)
                        price_per_share = st.number_input("Price per Share ($)", min_value=0.01, value=market_price, step=0.01)
                        
                        total_price = quantity * price_per_share
                        st.write(f"Total Price: ${total_price:.2f}")
                        
                        if st.button("Create Offer"):
                            # Create the trade offer
                            with engine.connect() as conn:
                                query = text("""
                                    INSERT INTO trade_offers 
                                    (seller_id, asset_type, asset_name, quantity, price_per_share, total_price) 
                                    VALUES (:seller_id, :asset_type, :asset_name, :quantity, :price_per_share, :total_price)
                                    RETURNING id
                                """)
                                
                                result = conn.execute(query, {
                                    "seller_id": current_user_id,
                                    "asset_type": "Player",
                                    "asset_name": asset_name,
                                    "quantity": quantity,
                                    "price_per_share": price_per_share,
                                    "total_price": total_price
                                })
                                conn.commit()
                                
                                if result:
                                    st.success(f"Offer created successfully! {quantity} shares of {asset_name} are now listed for sale.")
                                    st.rerun()
                                else:
                                    st.error("Error creating trade offer")
                
                else:  # Team Fund
                    # Get fund holdings
                    available_funds = []
                    if fund_holdings is not None and not fund_holdings.empty:
                        available_funds = fund_holdings["Asset Name"].tolist()
                    
                    if not available_funds:
                        st.info("You don't have any fund shares to sell. Purchase some in the Market first.")
                    else:
                        asset_name = st.selectbox("Select Fund", available_funds)
                        
                        # Get current holding data
                        holding_row = fund_holdings[fund_holdings["Asset Name"] == asset_name].iloc[0]
                        current_holding = holding_row["Quantity"]
                        
                        # Get current market price
                        market_price = 0
                        if not funds.empty:
                            fund_matches = funds[funds["Fund Name"] == asset_name]
                            if not fund_matches.empty:
                                market_price = fund_matches.iloc[0]["Fund Price"]
                        
                        # Form for creating offer
                        st.write(f"You currently own {current_holding} shares")
                        st.write(f"Current market price: ${market_price:.2f} per share")
                        
                        quantity = st.number_input("Quantity to Sell", min_value=1, max_value=current_holding, value=1)
                        price_per_share = st.number_input("Price per Share ($)", min_value=0.01, value=market_price, step=0.01)
                        
                        total_price = quantity * price_per_share
                        st.write(f"Total Price: ${total_price:.2f}")
                        
                        if st.button("Create Offer"):
                            # Create the trade offer
                            with engine.connect() as conn:
                                query = text("""
                                    INSERT INTO trade_offers 
                                    (seller_id, asset_type, asset_name, quantity, price_per_share, total_price) 
                                    VALUES (:seller_id, :asset_type, :asset_name, :quantity, :price_per_share, :total_price)
                                    RETURNING id
                                """)
                                
                                result = conn.execute(query, {
                                    "seller_id": current_user_id,
                                    "asset_type": "Team Fund",
                                    "asset_name": asset_name,
                                    "quantity": quantity,
                                    "price_per_share": price_per_share,
                                    "total_price": total_price
                                })
                                conn.commit()
                                
                                if result:
                                    st.success(f"Offer created successfully! {quantity} shares of {asset_name} are now listed for sale.")
                                    st.rerun()
                                else:
                                    st.error("Error creating trade offer")
            
            with trade_tabs[2]:
                st.subheader("My Active Offers")
                
                # Get user's active trade offers
                try:
                    with engine.connect() as conn:
                        query = text("""
                            SELECT 
                                id, 
                                asset_type, 
                                asset_name, 
                                quantity, 
                                price_per_share, 
                                total_price,
                                created_at,
                                status
                            FROM trade_offers
                            WHERE seller_id = :current_user_id
                            ORDER BY created_at DESC
                        """)
                        my_offers = pd.read_sql(query, conn, params={"current_user_id": current_user_id})
                    
                    if my_offers.empty:
                        st.info("You don't have any active offers.")
                    else:
                        # Group by status
                        active_offers = my_offers[my_offers["status"] == "active"]
                        completed_offers = my_offers[my_offers["status"] == "completed"]
                        
                        # Show active offers
                        st.write("Active Offers:")
                        if active_offers.empty:
                            st.info("No active offers.")
                        else:
                            for _, offer in active_offers.iterrows():
                                offer_id = offer["id"]
                                
                                col1, col2, col3 = st.columns([2, 2, 1])
                                
                                with col1:
                                    st.markdown(f"**{offer['asset_name']}** ({offer['asset_type']})")
                                    st.caption(f"Created: {offer['created_at']}")
                                
                                with col2:
                                    st.write(f"Quantity: {offer['quantity']} shares")
                                    st.write(f"Price: ${offer['price_per_share']:.2f} per share")
                                    st.caption(f"Total: ${offer['total_price']:.2f}")
                                
                                with col3:
                                    if st.button("Cancel", key=f"cancel_offer_{offer_id}"):
                                        # Cancel the offer
                                        with engine.connect() as conn:
                                            query = text("""
                                                UPDATE trade_offers
                                                SET status = 'cancelled'
                                                WHERE id = :offer_id
                                            """)
                                            conn.execute(query, {"offer_id": offer_id})
                                            conn.commit()
                                            
                                            st.success("Offer cancelled successfully")
                                            st.rerun()
                                
                                st.markdown("---")
                        
                        # Show completed offers
                        with st.expander("View Completed Offers"):
                            if completed_offers.empty:
                                st.info("No completed offers.")
                            else:
                                for _, offer in completed_offers.iterrows():
                                    st.markdown(f"**{offer['asset_name']}** ({offer['asset_type']})")
                                    st.write(f"Quantity: {offer['quantity']} shares at ${offer['price_per_share']:.2f} each")
                                    st.caption(f"Total: ${offer['total_price']:.2f} | Completed: {offer['created_at']}")
                                    st.markdown("---")
                except Exception as e:
                    st.error(f"Error loading your offers: {str(e)}")
            
            # Player-for-Player trading tab
            with trade_tabs[3]:
                st.subheader("Player-for-Player Trading")
                
                # Create subtabs for different functions
                p2p_tab1, p2p_tab2, p2p_tab3 = st.tabs(["Available Trades", "Create Trade", "My Trade Offers"])
                
                with p2p_tab1:
                    st.write("Browse available player-for-player trades")
                    
                    # Get all active player-for-player trade offers
                    try:
                        with engine.connect() as conn:
                            query = text("""
                                SELECT 
                                    to.id,
                                    u.username as creator_name,
                                    to.status,
                                    to.created_at,
                                    to.description
                                FROM trading_offers to
                                JOIN users u ON to.creator_id = u.id
                                WHERE to.status = 'pending' AND to.creator_id != :user_id
                                ORDER BY to.created_at DESC
                            """)
                            available_trades = pd.read_sql(query, conn, params={"user_id": current_user_id})
                        
                        if available_trades.empty:
                            st.info("No player-for-player trades available right now.")
                        else:
                            for _, trade in available_trades.iterrows():
                                trade_id = trade['id']
                                
                                # Get the offered and requested assets for this trade
                                with engine.connect() as conn:
                                    offered_query = text("""
                                        SELECT asset_name, asset_type, quantity
                                        FROM trading_offer_assets
                                        WHERE trade_id = :trade_id AND is_offered = True
                                    """)
                                    offered_assets = pd.read_sql(offered_query, conn, params={"trade_id": trade_id})
                                    
                                    requested_query = text("""
                                        SELECT asset_name, asset_type, quantity
                                        FROM trading_offer_assets
                                        WHERE trade_id = :trade_id AND is_offered = False
                                    """)
                                    requested_assets = pd.read_sql(requested_query, conn, params={"trade_id": trade_id})
                                
                                # Display trade offer
                                col1, col2, col3 = st.columns([2, 2, 1])
                                
                                with col1:
                                    st.markdown(f"**Trade from {trade['creator_name']}**")
                                    st.caption(f"Created: {trade['created_at']}")
                                    if trade['description']:
                                        st.caption(f"Message: {trade['description']}")
                                    
                                    st.markdown("**Offering:**")
                                    for _, asset in offered_assets.iterrows():
                                        st.write(f"• {asset['quantity']} shares of {asset['asset_name']} ({asset['asset_type']})")
                                
                                with col2:
                                    st.markdown("**Requesting:**")
                                    for _, asset in requested_assets.iterrows():
                                        st.write(f"• {asset['quantity']} shares of {asset['asset_name']} ({asset['asset_type']})")
                                
                                with col3:
                                    # Check if user has the requested assets
                                    can_accept = True
                                    missing_assets = []
                                    
                                    with engine.connect() as conn:
                                        for _, asset in requested_assets.iterrows():
                                            check_query = text("""
                                                SELECT quantity FROM holdings
                                                WHERE user_id = :user_id AND asset_name = :asset_name AND asset_type = :asset_type
                                            """)
                                            result = conn.execute(check_query, {
                                                "user_id": current_user_id,
                                                "asset_name": asset['asset_name'],
                                                "asset_type": asset['asset_type']
                                            }).fetchone()
                                            
                                            if not result or result[0] < asset['quantity']:
                                                can_accept = False
                                                missing_assets.append(asset['asset_name'])
                                    
                                    if can_accept:
                                        if st.button("Accept Trade", key=f"accept_p2p_trade_{trade_id}"):
                                            from db import respond_to_trade_offer
                                            success, message = respond_to_trade_offer(trade_id, current_user_id, "accept")
                                            if success:
                                                st.success(message)
                                                st.rerun()
                                            else:
                                                st.error(message)
                                    else:
                                        st.warning("Missing required assets")
                                        missing_str = ", ".join(missing_assets)
                                        st.caption(f"You need: {missing_str}")
                                
                                st.markdown("---")
                    except Exception as e:
                        st.error(f"Error loading trade offers: {str(e)}")
                
                with p2p_tab2:
                    st.write("Create a new player-for-player trade offer")
                    
                    # Get user's holdings for selection
                    try:
                        with engine.connect() as conn:
                            query = text("""
                                SELECT h.id, h.asset_type, h.asset_name, h.quantity
                                FROM holdings h
                                WHERE h.user_id = :user_id AND h.quantity > 0
                                ORDER BY h.asset_type, h.asset_name
                            """)
                            user_holdings = pd.read_sql(query, conn, params={"user_id": current_user_id})
                        
                        if user_holdings.empty:
                            st.warning("You don't have any assets to trade. Purchase some assets first.")
                        else:
                            # Group holdings by type for easier selection
                            holding_options = {}
                            for _, row in user_holdings.iterrows():
                                asset_type = row['asset_type']
                                if asset_type not in holding_options:
                                    holding_options[asset_type] = []
                                
                                asset_name = row['asset_name']
                                quantity = row['quantity']
                                holding_options[asset_type].append(f"{asset_name} ({quantity} shares)")
                            
                            # Select assets to offer
                            st.subheader("What You're Offering")
                            asset_types = list(holding_options.keys())
                            if asset_types:
                                offer_asset_type = st.selectbox("Asset Type to Offer", options=asset_types, key="p2p_offer_asset_type")
                                
                                if offer_asset_type and offer_asset_type in holding_options:
                                    offer_asset = st.selectbox("Asset to Offer", options=holding_options[offer_asset_type], key="p2p_offer_asset")
                                    
                                    # Extract the asset name and available quantity
                                    if offer_asset:
                                        offer_asset_name = offer_asset.split(" (")[0]
                                        available_quantity = int(offer_asset.split("(")[1].split(" ")[0])
                                        
                                        offer_quantity = st.number_input("Quantity to Offer", min_value=1, max_value=available_quantity, value=1, key="p2p_offer_quantity")
                                
                                # Select assets to request
                                st.subheader("What You're Requesting")
                                request_asset_type = st.selectbox("Asset Type to Request", options=["Player", "Team Fund"], key="p2p_request_asset_type")
                                
                                # Get available assets to request
                                if request_asset_type == "Player":
                                    with engine.connect() as conn:
                                        query = text("""
                                            SELECT name FROM players
                                            ORDER BY name
                                        """)
                                        available_assets = conn.execute(query).fetchall()
                                else:  # Team Fund
                                    with engine.connect() as conn:
                                        query = text("""
                                            SELECT name FROM team_funds
                                            ORDER BY name
                                        """)
                                        available_assets = conn.execute(query).fetchall()
                                
                                # Create a list of asset names
                                asset_names = [a[0] for a in available_assets]
                                
                                # Display assets to request
                                request_asset_name = st.selectbox("Asset to Request", options=asset_names, key="p2p_request_asset_name")
                                request_quantity = st.number_input("Quantity to Request", min_value=1, value=1, key="p2p_request_quantity")
                                
                                # Add a description/message
                                trade_description = st.text_area("Message (optional)", key="p2p_trade_description")
                                
                                # Submit button
                                if st.button("Create Trade Offer", key="create_p2p_trade"):
                                    if offer_asset_name and request_asset_name:
                                        # Create trade offer
                                        from db import create_player_trade_offer
                                        
                                        # Prepare the assets
                                        sender_assets = [{
                                            "asset_type": offer_asset_type,
                                            "asset_name": offer_asset_name,
                                            "quantity": offer_quantity
                                        }]
                                        
                                        recipient_assets = [{
                                            "asset_type": request_asset_type,
                                            "asset_name": request_asset_name,
                                            "quantity": request_quantity
                                        }]
                                        
                                        # Create the offer
                                        success, message, offer_id = create_player_trade_offer(
                                            current_user_id,
                                            None,  # We'll match with any user who accepts
                                            sender_assets,
                                            recipient_assets,
                                            trade_description
                                        )
                                        
                                        if success:
                                            st.success(f"Trade offer created successfully! Offer ID: {offer_id}")
                                            st.rerun()
                                        else:
                                            st.error(message)
                            else:
                                st.warning("You need to purchase assets before you can create a trade offer.")
                    except Exception as e:
                        st.error(f"Error creating trade offer: {str(e)}")
                
                with p2p_tab3:
                    st.write("View and manage your player-for-player trade offers")
                    
                    # Get user's active trade offers
                    try:
                        with engine.connect() as conn:
                            query = text("""
                                SELECT 
                                    to.id,
                                    to.status,
                                    to.created_at,
                                    to.description
                                FROM trading_offers to
                                WHERE to.creator_id = :user_id AND to.status = 'pending'
                                ORDER BY to.created_at DESC
                            """)
                            my_p2p_offers = pd.read_sql(query, conn, params={"user_id": current_user_id})
                        
                        if my_p2p_offers.empty:
                            st.info("You don't have any active player-for-player trade offers.")
                        else:
                            for _, offer in my_p2p_offers.iterrows():
                                offer_id = offer['id']
                                
                                # Get the offered and requested assets
                                with engine.connect() as conn:
                                    offered_query = text("""
                                        SELECT asset_name, asset_type, quantity
                                        FROM trading_offer_assets
                                        WHERE trade_id = :offer_id AND is_offered = True
                                    """)
                                    offered_assets = pd.read_sql(offered_query, conn, params={"offer_id": offer_id})
                                    
                                    requested_query = text("""
                                        SELECT asset_name, asset_type, quantity
                                        FROM trading_offer_assets
                                        WHERE trade_id = :offer_id AND is_offered = False
                                    """)
                                    requested_assets = pd.read_sql(requested_query, conn, params={"offer_id": offer_id})
                                
                                # Display offer details
                                col1, col2, col3 = st.columns([2, 2, 1])
                                
                                with col1:
                                    st.markdown(f"**Trade Offer #{offer_id}**")
                                    st.caption(f"Created: {offer['created_at']}")
                                    if offer['description']:
                                        st.caption(f"Message: {offer['description']}")
                                    
                                    st.markdown("**You're Offering:**")
                                    for _, asset in offered_assets.iterrows():
                                        st.write(f"• {asset['quantity']} shares of {asset['asset_name']} ({asset['asset_type']})")
                                
                                with col2:
                                    st.markdown("**You're Requesting:**")
                                    for _, asset in requested_assets.iterrows():
                                        st.write(f"• {asset['quantity']} shares of {asset['asset_name']} ({asset['asset_type']})")
                                
                                with col3:
                                    if st.button("Cancel Offer", key=f"cancel_p2p_offer_{offer_id}"):
                                        # Cancel the trade offer
                                        with engine.connect() as conn:
                                            cancel_query = text("""
                                                UPDATE trading_offers
                                                SET status = 'cancelled'
                                                WHERE id = :offer_id AND creator_id = :user_id
                                            """)
                                            conn.execute(cancel_query, {"offer_id": offer_id, "user_id": current_user_id})
                                            conn.commit()
                                        
                                        st.success("Trade offer cancelled.")
                                        st.rerun()
                                
                                st.markdown("---")
                    except Exception as e:
                        st.error(f"Error loading your trade offers: {str(e)}")
        
        elif page == "Add Funds":
            st.header("Add Funds to Your Wallet")
            
            st.info(f"Current Balance: ${user_wallet:.2f}")
            
            # Add funds form
            amount = st.number_input("Amount to Add ($)", min_value=10.0, value=100.0, step=10.0)
            
            col1, col2 = st.columns(2)
            
            with col1:
                payment_method = st.selectbox("Payment Method", ["Credit Card", "Debit Card", "PayPal"])
            
            with col2:
                if payment_method in ["Credit Card", "Debit Card"]:
                    card_number = st.text_input("Card Number", placeholder="**** **** **** ****")
                    expiry = st.text_input("Expiry Date", placeholder="MM/YY")
                    cvv = st.text_input("CVV", type="password", placeholder="***")
                elif payment_method == "PayPal":
                    email = st.text_input("PayPal Email", placeholder="your@email.com")
            
            if st.button("Add Funds"):
                # In a real application, this would process the payment
                # For demo purposes, we'll just add the funds directly
                success, new_balance = add_funds(current_user_id, amount)
                
                if success:
                    st.session_state.wallet_balance = new_balance
                    st.success(f"Successfully added ${amount:.2f} to your wallet. New balance: ${new_balance:.2f}")
                    
                    # Return to previous page after 3 seconds
                    st.session_state.page = "market"
                    st.rerun()
                else:
                    st.error("Failed to add funds. Please try again.")
            
            if st.button("Back to Market"):
                st.session_state.page = "market"
                st.rerun()
        
        elif page == "Friends":
            st.header("Friends & Social")
            
            # Add tabs for different friend functions
            friend_tab1, friend_tab2, friend_tab3 = st.tabs(["My Friends", "Friend Requests", "Add Friend"])
            
            with friend_tab1:
                st.subheader("My Friends")
                
                # Get friend list
                from db import get_friend_list
                friends = get_friend_list(current_user_id)
                
                if not friends:
                    st.info("You don't have any friends yet. Add some friends to get started!")
                else:
                    # Display friends in a nice format
                    for friend in friends:
                        if friend["status"] == "accepted":
                            col1, col2 = st.columns([3, 1])
                            
                            with col1:
                                st.markdown(f"**{friend['username']}**")
                                
                                # Display friend info
                                st.caption("Joined: " + friend["created_at"].strftime("%b %d, %Y") if friend["created_at"] else "Unknown")
                                
                            with col2:
                                # View Profile button
                                if st.button("View Profile", key=f"view_profile_{friend['user_id']}"):
                                    st.session_state.viewing_profile = friend["user_id"]
                                    st.session_state.viewing_username = friend["username"]
                                    st.rerun()
                            
                            st.markdown("---")
            
            with friend_tab2:
                st.subheader("Friend Requests")
                
                # Get pending friend requests
                pending_requests = [f for f in friends if f["status"] == "pending"]
                
                # Split into sent and received requests
                sent_requests = []
                received_requests = []
                
                for req in pending_requests:
                    # If the user_id is the current user, it's a received request
                    # Since the display shows the other person's info, this is a bit reversed
                    # The status of pending and "friend_id" being current user means the request was received
                    if req["user_id"] != current_user_id:
                        received_requests.append(req)
                    else:
                        sent_requests.append(req)
                
                # Display received requests
                if received_requests:
                    st.write("Requests Received:")
                    for req in received_requests:
                        col1, col2, col3 = st.columns([2, 1, 1])
                        
                        with col1:
                            st.markdown(f"**{req['username']}** wants to be friends")
                            st.caption("Sent: " + req["created_at"].strftime("%b %d, %Y") if req["created_at"] else "Unknown")
                            
                        with col2:
                            # Accept button
                            from db import respond_to_friend_request
                            if st.button("Accept", key=f"accept_{req['id']}"):
                                success, message = respond_to_friend_request(req["id"], current_user_id, "accept")
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                                    
                        with col3:
                            # Reject button
                            if st.button("Reject", key=f"reject_{req['id']}"):
                                success, message = respond_to_friend_request(req["id"], current_user_id, "reject")
                                if success:
                                    st.info(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        
                        st.markdown("---")
                else:
                    st.info("No friend requests received")
                
                # Display sent requests
                if sent_requests:
                    st.write("Requests Sent:")
                    for req in sent_requests:
                        st.markdown(f"Request to **{req['username']}** is pending")
                        st.caption("Sent: " + req["created_at"].strftime("%b %d, %Y") if req["created_at"] else "Unknown")
                        st.markdown("---")
                else:
                    st.info("No pending requests sent")
            
            with friend_tab3:
                st.subheader("Add Friend")
                
                # Form to add a new friend
                friend_username = st.text_input("Enter Username", key="friend_username")
                
                if st.button("Send Friend Request"):
                    if friend_username:
                        from db import send_friend_request
                        success, message = send_friend_request(current_user_id, friend_username)
                        if success:
                            st.success(message)
                        else:
                            st.error(message)
                    else:
                        st.warning("Please enter a username")
                
                # Or search by leaderboard, trending, etc.
                st.divider()
                st.subheader("Suggested Friends")
                
                # Get a list of top users
                with engine.connect() as conn:
                    query = text("""
                        SELECT u.id, u.username, COUNT(h.id) as asset_count
                        FROM users u
                        LEFT JOIN holdings h ON u.id = h.user_id
                        WHERE u.id != :user_id
                        GROUP BY u.id, u.username
                        ORDER BY asset_count DESC
                        LIMIT 5
                    """)
                    suggested_users = conn.execute(query, {"user_id": current_user_id}).fetchall()
                
                if suggested_users:
                    for user in suggested_users:
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.markdown(f"**{user[1]}**")
                            st.caption(f"Portfolio: {user[2]} assets")
                            
                        with col2:
                            # Add Friend button
                            if st.button("Add Friend", key=f"add_suggested_{user[0]}"):
                                from db import send_friend_request
                                success, message = send_friend_request(current_user_id, user[1])
                                if success:
                                    st.success(message)
                                else:
                                    st.error(message)
                        
                        st.markdown("---")
                else:
                    st.info("No suggested friends available")
            
        elif page == "Competitions":
            st.header("Competitions & Fantasy Teams")
            
            # Add tabs for different competition functions
            comp_tab1, comp_tab2, comp_tab3 = st.tabs(["My Competitions", "Available Competitions", "Create Competition"])
            
            with comp_tab1:
                st.subheader("My Competitions")
                
                # Get user's competitions
                from db import get_my_competitions
                my_competitions = get_my_competitions(current_user_id)
                
                if not my_competitions:
                    st.info("You're not participating in any competitions yet. Join or create one to get started!")
                else:
                    # Display each competition
                    for comp in my_competitions:
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.markdown(f"**{comp['name']}**")
                            st.caption(comp['description'])
                            
                            # Format dates nicely
                            start_date = comp['start_date'].strftime("%b %d, %Y") if comp['start_date'] else "Not set"
                            end_date = comp['end_date'].strftime("%b %d, %Y") if comp['end_date'] else "Not set"
                            
                            st.caption(f"Duration: {start_date} to {end_date}")
                            st.caption(f"Created by: {comp['creator_name']}")
                            st.caption(f"Your Score: {comp['score']:.2f} | Rank: {comp['rank']} of {comp['member_count']}")
                            
                        with col2:
                            # View Competition button
                            if st.button("View Details", key=f"view_comp_{comp['id']}"):
                                st.session_state.viewing_competition = comp['id']
                                st.session_state.competition_name = comp['name']
                                st.rerun()
                        
                        st.markdown("---")
            
            with comp_tab2:
                st.subheader("Available Competitions")
                
                # Get available competitions
                from db import get_available_competitions
                available_competitions = get_available_competitions(current_user_id)
                
                if not available_competitions:
                    st.info("No competitions available to join at the moment.")
                else:
                    # Display each competition
                    for comp in available_competitions:
                        col1, col2 = st.columns([3, 1])
                        
                        with col1:
                            st.markdown(f"**{comp['name']}**")
                            st.caption(comp['description'])
                            
                            # Format dates nicely
                            start_date = comp['start_date'].strftime("%b %d, %Y") if comp['start_date'] else "Not set"
                            end_date = comp['end_date'].strftime("%b %d, %Y") if comp['end_date'] else "Not set"
                            
                            st.caption(f"Duration: {start_date} to {end_date}")
                            st.caption(f"Created by: {comp['creator_name']}")
                            st.caption(f"Members: {comp['member_count']}")
                            
                        with col2:
                            # Join Competition button
                            from db import join_competition
                            if st.button("Join", key=f"join_comp_{comp['id']}"):
                                success, message = join_competition(current_user_id, comp['id'])
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        
                        st.markdown("---")
            
            with comp_tab3:
                st.subheader("Create New Competition")
                
                # Form to create a new competition
                comp_name = st.text_input("Competition Name", key="comp_name")
                comp_desc = st.text_area("Description", key="comp_desc")
                
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input("Start Date", value=datetime.now())
                with col2:
                    end_date = st.date_input("End Date", value=datetime.now() + timedelta(days=30))
                
                if st.button("Create Competition"):
                    if comp_name and comp_desc:
                        from db import create_competition
                        success, message, comp_id = create_competition(
                            current_user_id, 
                            comp_name, 
                            comp_desc, 
                            datetime.combine(start_date, datetime.min.time()),
                            datetime.combine(end_date, datetime.min.time())
                        )
                        if success:
                            st.success(message)
                            
                            # Prompt to create fantasy team
                            st.write("Would you like to create a fantasy team for this competition?")
                            team_name = st.text_input("Team Name", key="fantasy_team_name")
                            
                            if st.button("Create Team"):
                                if team_name:
                                    from db import create_fantasy_team
                                    team_success, team_message, team_id = create_fantasy_team(
                                        current_user_id, 
                                        team_name, 
                                        comp_id
                                    )
                                    if team_success:
                                        st.success(team_message)
                                        st.rerun()
                                    else:
                                        st.error(team_message)
                                else:
                                    st.warning("Please enter a team name")
                        else:
                            st.error(message)
                    else:
                        st.warning("Please fill in all required fields")
            
        elif page == "Sports Betting":
            st.header("Sports Betting")
            
            # Verification message
            st.success("You are verified and have access to all betting features.")
            
            # Get upcoming games
            from db import get_upcoming_games, place_bet, create_parlay_bet, get_user_bets, simulate_game_result
            upcoming_games = get_upcoming_games()
            
            # Create tabs for different betting sections
            betting_tabs = st.tabs(["Upcoming Games", "Player Props", "My Bets", "Bet Probability", "Game Results", "Betting History"])
            
            with betting_tabs[0]:
                st.subheader("Place Your Bets")
                
                if not upcoming_games:
                    st.info("No upcoming games available for betting at the moment.")
                else:
                    # Add a refresh button to update games list
                    col1, col2 = st.columns([3, 1])
                    with col1:
                        st.write("Select from current games below to place your bets")
                    with col2:
                        if st.button("🔄 Refresh Games"):
                            st.rerun()
                
                    # Display games with betting options
                    for game in upcoming_games:
                        st.markdown(f"### {game['away_team']} @ {game['home_team']}")
                        # Use game_date instead of game_time
                        game_time = game.get('game_date', datetime.now())
                        if isinstance(game_time, str):
                            try:
                                game_time = datetime.fromisoformat(game_time)
                            except ValueError:
                                game_time = datetime.now()
                                
                        # Calculate time until game
                        time_until_game = game_time - datetime.now()
                        hours_until_game = time_until_game.total_seconds() / 3600
                        
                        # Display game time and status
                        st.caption(f"Game time: {game_time.strftime('%Y-%m-%d %H:%M')}")
                        
                        # Check if game is starting soon (less than 12 hours)
                        betting_closed = hours_until_game <= 0
                        if hours_until_game <= 12 and hours_until_game > 0:
                            st.warning(f"⚠️ Betting closes in {hours_until_game:.1f} hours")
                        elif betting_closed:
                            st.error("🔒 Betting closed - Game is starting/in progress")
                            continue  # Skip to next game
                        
                        # Display odds information
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            st.markdown("**Moneyline**")
                            st.write(f"Home ({game['home_team']}): {game['home_odds']}")
                            st.write(f"Away ({game['away_team']}): {game['away_odds']}")
                            
                            # Bet selection
                            bet_type = "moneyline"
                            ml_pick = st.radio(f"Moneyline Pick (Game {game['id']})", 
                                            [f"Home: {game['home_team']}", f"Away: {game['away_team']}"],
                                            key=f"ml_{game['id']}")
                            bet_pick = "home" if "Home:" in ml_pick else "away"
                            
                            # Bet amount
                            amount = st.number_input(f"Bet Amount (Min: $5)", 
                                                min_value=5.0, 
                                                max_value=float(user_wallet),
                                                step=5.0,
                                                key=f"ml_amount_{game['id']}")
                            
                            if st.button(f"Place Moneyline Bet", key=f"ml_bet_{game['id']}"):
                                success, message, bet_id = place_bet(
                                    user_id=current_user_id,
                                    game_id=game['id'],
                                    bet_type=bet_type,
                                    bet_pick=bet_pick,
                                    amount=amount
                                )
                                
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        
                        with col2:
                            st.markdown("**Spread**")
                            st.write(f"Home ({game['home_team']}): {game['home_spread']} ({game['home_spread_odds']})")
                            st.write(f"Away ({game['away_team']}): {game['away_spread']} ({game['away_spread_odds']})")
                            
                            # Bet selection
                            bet_type = "spread"
                            spread_pick = st.radio(f"Spread Pick (Game {game['id']})", 
                                                [f"Home: {game['home_team']} {game['home_spread']}", 
                                                f"Away: {game['away_team']} {game['away_spread']}"],
                                                key=f"spread_{game['id']}")
                            bet_pick = "home" if "Home:" in spread_pick else "away"
                            
                            # Bet amount
                            amount = st.number_input(f"Bet Amount (Min: $5)", 
                                                min_value=5.0, 
                                                max_value=float(user_wallet),
                                                step=5.0,
                                                key=f"spread_amount_{game['id']}")
                            
                            if st.button(f"Place Spread Bet", key=f"spread_bet_{game['id']}"):
                                success, message, bet_id = place_bet(
                                    user_id=current_user_id,
                                    game_id=game['id'],
                                    bet_type=bet_type,
                                    bet_pick=bet_pick,
                                    amount=amount
                                )
                                
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        
                        with col3:
                            st.markdown("**Over/Under**")
                            over_under = game['over_under']
                            st.write(f"Total: {over_under}")
                            st.write(f"Over: {game['over_odds']}")
                            st.write(f"Under: {game['under_odds']}")
                            
                            # Bet selection
                            bet_type = "over_under"
                            ou_pick = st.radio(f"O/U Pick (Game {game['id']})", 
                                            [f"Over {over_under}", f"Under {over_under}"],
                                            key=f"ou_{game['id']}")
                            bet_pick = "over" if "Over" in ou_pick else "under"
                            
                            # Bet amount
                            amount = st.number_input(f"Bet Amount (Min: $5)", 
                                                min_value=5.0, 
                                                max_value=float(user_wallet),
                                                step=5.0,
                                                key=f"ou_amount_{game['id']}")
                            
                            if st.button(f"Place Over/Under Bet", key=f"ou_bet_{game['id']}"):
                                success, message, bet_id = place_bet(
                                    user_id=current_user_id,
                                    game_id=game['id'],
                                    bet_type=bet_type,
                                    bet_pick=bet_pick,
                                    amount=amount
                                )
                                
                                if success:
                                    st.success(message)
                                    st.rerun()
                                else:
                                    st.error(message)
                        
                        st.markdown("---")
            
            with betting_tabs[1]:
                st.subheader("Player Props Betting")
                st.write("Bet on individual player performance metrics across all sports")
                
                # Get current datetime
                now = datetime.now()
                
                # Get upcoming games for prop bets
                upcoming_games_for_props = []
                for game in upcoming_games:
                    # Parse game time
                    game_time = game.get('game_date', now)
                    if isinstance(game_time, str):
                        try:
                            game_time = datetime.fromisoformat(game_time)
                        except ValueError:
                            # If parsing fails, just use the string representation
                            continue
                    
                    # Check if game is within the next 12 hours
                    time_until_game = game_time - now
                    hours_until_game = time_until_game.total_seconds() / 3600
                    
                    # Only show games that are more than 0 hours away and less than 12 hours away
                    if 0 < hours_until_game <= 12:
                        game['time_until_game'] = hours_until_game
                        upcoming_games_for_props.append(game)
                
                if not upcoming_games_for_props:
                    st.info("No games available for player prop betting within the next 12 hours.")
                else:
                    # Allow user to select a game for props
                    prop_game_options = [f"{g['away_team']} @ {g['home_team']} ({g['time_until_game']:.1f} hrs)" for g in upcoming_games_for_props]
                    selected_prop_game = st.selectbox("Select Game for Player Props", prop_game_options)
                    
                    # Get the selected game index
                    game_index = prop_game_options.index(selected_prop_game)
                    game = upcoming_games_for_props[game_index]
                    
                    # Create tabs for home and away teams
                    team_tabs = st.tabs([f"{game['home_team']} Players", f"{game['away_team']} Players"])
                    
                    # Function to display player props for a team
                    def display_team_props(team_name, is_home_team):
                        sport = "NBA"  # Default for demo
                        if "Packers" in team_name or "Chiefs" in team_name or "Eagles" in team_name:
                            sport = "NFL"
                        elif "Yankees" in team_name or "Dodgers" in team_name or "Red Sox" in team_name:
                            sport = "MLB"
                            
                        # Get sample players for the team based on the sport
                        if sport == "NBA":
                            if is_home_team:
                                players = [
                                    {"name": "LeBron James", "position": "SF", "avg_points": 25.7, "avg_rebounds": 7.3, "avg_assists": 8.3},
                                    {"name": "Anthony Davis", "position": "PF", "avg_points": 24.2, "avg_rebounds": 12.1, "avg_assists": 3.1},
                                    {"name": "D'Angelo Russell", "position": "PG", "avg_points": 17.8, "avg_rebounds": 3.1, "avg_assists": 6.3}
                                ]
                            else:
                                players = [
                                    {"name": "Stephen Curry", "position": "PG", "avg_points": 28.2, "avg_rebounds": 5.2, "avg_assists": 6.3},
                                    {"name": "Klay Thompson", "position": "SG", "avg_points": 21.9, "avg_rebounds": 3.5, "avg_assists": 2.3},
                                    {"name": "Draymond Green", "position": "PF", "avg_points": 8.5, "avg_rebounds": 7.1, "avg_assists": 6.8}
                                ]
                        elif sport == "NFL":
                            if is_home_team:
                                players = [
                                    {"name": "Patrick Mahomes", "position": "QB", "avg_passing_yards": 290, "avg_passing_tds": 2.2, "avg_interceptions": 0.7},
                                    {"name": "Travis Kelce", "position": "TE", "avg_receptions": 6.5, "avg_receiving_yards": 78, "avg_tds": 0.8},
                                    {"name": "Isiah Pacheco", "position": "RB", "avg_rushing_yards": 72, "avg_rushing_tds": 0.6, "avg_receptions": 2.1}
                                ]
                            else:
                                players = [
                                    {"name": "Jalen Hurts", "position": "QB", "avg_passing_yards": 248, "avg_passing_tds": 1.8, "avg_interceptions": 0.9},
                                    {"name": "A.J. Brown", "position": "WR", "avg_receptions": 5.8, "avg_receiving_yards": 85, "avg_tds": 0.7},
                                    {"name": "DeVonta Smith", "position": "WR", "avg_receptions": 5.1, "avg_receiving_yards": 67, "avg_tds": 0.5}
                                ]
                        elif sport == "MLB":
                            if is_home_team:
                                players = [
                                    {"name": "Aaron Judge", "position": "RF", "avg_hits": 1.1, "avg_home_runs": 0.4, "avg_rbis": 1.2},
                                    {"name": "Giancarlo Stanton", "position": "DH", "avg_hits": 0.9, "avg_home_runs": 0.3, "avg_rbis": 0.8},
                                    {"name": "Gerrit Cole", "position": "P", "avg_strikeouts": 8.2, "avg_era": 3.15, "avg_innings": 6.1}
                                ]
                            else:
                                players = [
                                    {"name": "Mookie Betts", "position": "RF", "avg_hits": 1.3, "avg_home_runs": 0.3, "avg_rbis": 0.9},
                                    {"name": "Freddie Freeman", "position": "1B", "avg_hits": 1.5, "avg_home_runs": 0.2, "avg_rbis": 1.0},
                                    {"name": "Clayton Kershaw", "position": "P", "avg_strikeouts": 7.8, "avg_era": 2.85, "avg_innings": 5.7}
                                ]
                        
                        # Display player props
                        st.write(f"### {team_name} Player Props")
                        
                        # Loop through each player
                        for player in players:
                            col1, col2 = st.columns([3, 2])
                            
                            with col1:
                                st.markdown(f"**{player['name']}** ({player['position']})")
                                
                                if sport == "NBA":
                                    # NBA props
                                    points_line = round(player['avg_points'])
                                    rebounds_line = round(player['avg_rebounds'])
                                    assists_line = round(player['avg_assists'])
                                    
                                    prop_options = [
                                        f"Points: Over {points_line}.5 (-110)",
                                        f"Points: Under {points_line}.5 (-110)",
                                        f"Rebounds: Over {rebounds_line}.5 (-110)",
                                        f"Rebounds: Under {rebounds_line}.5 (-110)",
                                        f"Assists: Over {assists_line}.5 (-110)",
                                        f"Assists: Under {assists_line}.5 (-110)"
                                    ]
                                elif sport == "NFL":
                                    # NFL props
                                    if player['position'] == "QB":
                                        pass_yards_line = round(player['avg_passing_yards'] / 5) * 5  # Round to nearest 5
                                        pass_tds_line = player['avg_passing_tds']
                                        
                                        prop_options = [
                                            f"Passing Yards: Over {pass_yards_line}.5 (-110)",
                                            f"Passing Yards: Under {pass_yards_line}.5 (-110)",
                                            f"Passing TDs: Over {pass_tds_line}.5 (-110)",
                                            f"Passing TDs: Under {pass_tds_line}.5 (-110)"
                                        ]
                                    elif player['position'] in ["WR", "TE"]:
                                        rec_yards_line = round(player['avg_receiving_yards'] / 5) * 5
                                        receptions_line = round(player['avg_receptions'])
                                        
                                        prop_options = [
                                            f"Receiving Yards: Over {rec_yards_line}.5 (-110)",
                                            f"Receiving Yards: Under {rec_yards_line}.5 (-110)",
                                            f"Receptions: Over {receptions_line}.5 (-110)",
                                            f"Receptions: Under {receptions_line}.5 (-110)"
                                        ]
                                    else:  # RB
                                        rush_yards_line = round(player['avg_rushing_yards'] / 5) * 5
                                        
                                        prop_options = [
                                            f"Rushing Yards: Over {rush_yards_line}.5 (-110)",
                                            f"Rushing Yards: Under {rush_yards_line}.5 (-110)",
                                            f"Rushing TDs: Over 0.5 (+130)",
                                            f"Rushing TDs: Under 0.5 (-150)"
                                        ]
                                elif sport == "MLB":
                                    # MLB props
                                    if player['position'] == "P":
                                        strikeouts_line = round(player['avg_strikeouts'])
                                        innings_line = round(player['avg_innings'] * 2) / 2  # Round to nearest 0.5
                                        
                                        prop_options = [
                                            f"Strikeouts: Over {strikeouts_line}.5 (-110)",
                                            f"Strikeouts: Under {strikeouts_line}.5 (-110)",
                                            f"Innings Pitched: Over {innings_line} (-110)",
                                            f"Innings Pitched: Under {innings_line} (-110)"
                                        ]
                                    else:  # Batter
                                        hits_line = round(player['avg_hits'])
                                        
                                        prop_options = [
                                            f"Hits: Over {hits_line}.5 (-110)",
                                            f"Hits: Under {hits_line}.5 (-110)",
                                            f"Home Run: Yes (+350)",
                                            f"RBIs: Over 0.5 (-120)"
                                        ]
                                
                                selected_prop = st.selectbox(f"Select Prop for {player['name']}", prop_options, key=f"prop_{player['name']}")
                            
                            with col2:
                                # Bet amount
                                amount = st.number_input(f"Bet Amount (Min: $5)", 
                                                    min_value=5.0, 
                                                    max_value=float(user_wallet),
                                                    step=5.0,
                                                    key=f"prop_amount_{player['name']}")
                                
                                if st.button(f"Place Prop Bet", key=f"prop_bet_{player['name']}"):
                                    # Calculate odds based on the selected prop
                                    if "(-110)" in selected_prop:
                                        odds = 1.91  # -110 in decimal
                                    elif "(-120)" in selected_prop:
                                        odds = 1.83  # -120 in decimal
                                    elif "(-150)" in selected_prop:
                                        odds = 1.67  # -150 in decimal
                                    elif "(+130)" in selected_prop:
                                        odds = 2.30  # +130 in decimal
                                    elif "(+350)" in selected_prop:
                                        odds = 4.50  # +350 in decimal
                                    else:
                                        odds = 1.91  # Default
                                    
                                    potential_payout = amount * odds
                                    
                                    # Simulate a successful bet placement
                                    st.success(f"Prop bet placed on {selected_prop} for ${amount:.2f}. Potential payout: ${potential_payout:.2f}")
                                    
                                    # In a real implementation, we would call place_bet() here
                                    # For now, just display the success message
                            
                            st.markdown("---")
                    
                    # Display props for home and away teams
                    with team_tabs[0]:
                        display_team_props(game['home_team'], True)
                    
                    with team_tabs[1]:
                        display_team_props(game['away_team'], False)
            
            with betting_tabs[2]:
                st.subheader("My Active Bets")
                
                # Get user's active bets
                single_bets, parlays = get_user_bets(current_user_id)
                
                if not single_bets and not parlays:
                    st.info("You don't have any active bets.")
                else:
                    if single_bets:
                        st.write("Single Bets")
                        for bet in single_bets:
                            col1, col2, col3 = st.columns([3, 2, 1])
                            
                            with col1:
                                st.markdown(f"**{bet['away_team']} @ {bet['home_team']}**")
                                # Handle game_date or game_time consistently
                                game_time = bet.get('game_date', bet.get('game_time', datetime.now()))
                                if isinstance(game_time, str):
                                    try:
                                        game_time = datetime.fromisoformat(game_time)
                                    except ValueError:
                                        game_time = datetime.now()
                                st.caption(f"Game time: {game_time.strftime('%Y-%m-%d %H:%M')}")
                                
                                bet_type_display = bet['bet_type'].replace('_', '/').capitalize()
                                if bet['bet_type'] == 'moneyline':
                                    pick_display = bet['home_team'] if bet['bet_pick'] == 'home' else bet['away_team']
                                elif bet['bet_type'] == 'spread':
                                    spread = bet['home_spread'] if bet['bet_pick'] == 'home' else bet['away_spread']
                                    team = bet['home_team'] if bet['bet_pick'] == 'home' else bet['away_team']
                                    pick_display = f"{team} {spread}"
                                else:  # over_under
                                    pick_display = f"{'Over' if bet['bet_pick'] == 'over' else 'Under'} {bet['over_under']}"
                                
                                st.write(f"Bet Type: {bet_type_display}")
                                st.write(f"Pick: {pick_display}")
            
            with betting_tabs[2]:
                st.subheader("Bet Success Probability")
                st.write("View the probability of your bets paying off based on odds.")
                
                # Get user's active bets
                single_bets, parlays = get_user_bets(current_user_id)
                
                if not single_bets and not parlays:
                    st.info("You don't have any active bets to analyze.")
                else:
                    # Create data for visualization
                    bet_names = []
                    win_probs = []
                    bet_types = []
                    potential_payouts = []
                    
                    # Process single bets
                    for bet in single_bets:
                        # Calculate implied probability based on odds (1/odds)
                        odds = float(bet.get('odds', 1.0))
                        win_prob = min(100, round(100 / odds, 1))  # Cap at 100%
                        
                        # Create descriptive name
                        if bet['bet_type'] == 'moneyline':
                            bet_name = f"{bet['away_team']} @ {bet['home_team']} ({bet['bet_pick']})"
                        elif bet['bet_type'] == 'spread':
                            team = bet['home_team'] if bet['bet_pick'] == 'home' else bet['away_team']
                            bet_name = f"{team} (spread)"
                        else:  # over_under
                            bet_name = f"{bet['away_team']} @ {bet['home_team']} ({bet['bet_pick']})"
                        
                        bet_names.append(bet_name)
                        win_probs.append(win_prob)
                        bet_types.append(bet['bet_type'].capitalize())
                        potential_payouts.append(float(bet.get('potential_payout', 0)))
                    
                    # Process parlays (much lower probability of success)
                    for parlay in parlays:
                        # Parlays are harder to win, so we estimate lower probability
                        leg_count = int(parlay.get('leg_count', 2))
                        # Each leg roughly 50% chance, so total prob is 0.5^leg_count
                        win_prob = round(100 * (0.5 ** leg_count), 1)
                        
                        bet_names.append(f"Parlay ({leg_count} legs)")
                        win_probs.append(win_prob)
                        bet_types.append("Parlay")
                        potential_payouts.append(float(parlay.get('potential_payout', 0)))
                    
                    # Create a DataFrame for visualization
                    import pandas as pd
                    chart_data = pd.DataFrame({
                        'Bet': bet_names,
                        'Win Probability (%)': win_probs,
                        'Type': bet_types,
                        'Potential Payout ($)': potential_payouts
                    })
                    
                    # Display the data
                    st.write("### Your Active Bets")
                    st.dataframe(chart_data)
                    
                    # Create a horizontal bar chart
                    import plotly.express as px
                    
                    fig = px.bar(
                        chart_data, 
                        y='Bet', 
                        x='Win Probability (%)', 
                        color='Type',
                        hover_data=['Potential Payout ($)'],
                        orientation='h',
                        title='Probability of Winning Each Bet',
                        labels={'Win Probability (%)': 'Chance of Winning (%)', 'Bet': ''},
                        color_discrete_sequence=['#1E88E5', '#FFC107', '#D81B60', '#004D40']
                    )
                    
                    # Customize the layout
                    fig.update_layout(
                        xaxis_range=[0, 100],
                        xaxis_title='Probability (%)',
                        yaxis_title='',
                        legend_title='Bet Type',
                        height=max(300, len(bet_names) * 40)  # Dynamic height based on number of bets
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show explanation
                    st.info("""
                    **How to interpret this chart:**
                    - Higher probability means better chances of winning the bet
                    - Moneyline bets on favorites have higher win probability but lower payouts
                    - Parlays have lower win probability but higher potential payouts
                    - These probabilities are calculated based on the betting odds
                    """)
            
            with betting_tabs[3]:
                st.subheader("Bet Probability Chart")
                # Display betting history for this user
                single_bets, parlay_bets = get_user_bets(current_user_id)
                
                if not single_bets and not parlay_bets:
                    st.info("You don't have any active bets to analyze.")
                else:
                    # Calculate win probability for each bet
                    bet_names = []
                    win_probabilities = []
                    bet_types = []
                    payouts = []
                    
                    # Process single bets
                    for bet in single_bets:
                        if bet['status'] != 'pending':
                            continue
                            
                        game_id = bet['game_id']
                        game = next((g for g in upcoming_games if g['id'] == game_id), None)
                        
                        if not game:
                            continue
                            
                        bet_name = f"{game['away_team']} @ {game['home_team']} - {bet['bet_type'].title()} ({bet['bet_pick']})"
                        bet_names.append(bet_name)
                        
                        # Calculate win probability based on odds
                        if bet['bet_type'] == 'moneyline':
                            odds = game['home_odds'] if bet['bet_pick'] == 'home' else game['away_odds']
                            # Convert American odds to probability
                            if odds > 0:
                                probability = 100 / (odds + 100)
                            else:
                                probability = abs(odds) / (abs(odds) + 100)
                                
                            win_probabilities.append(round(probability * 100, 1))
                        elif bet['bet_type'] == 'spread':
                            # Use standard 50% with slight adjustment based on line value
                            win_probabilities.append(52.5 if bet['bet_pick'] == 'home' else 47.5)
                        elif bet['bet_type'] == 'over_under':
                            # Use standard 50% with slight adjustment based on line value
                            win_probabilities.append(48.5 if bet['bet_pick'] == 'over' else 51.5)
                        
                        bet_types.append('Single')
                        payouts.append(bet['potential_payout'])
                    
                    # Process parlay bets
                    for parlay in parlay_bets:
                        if parlay['status'] != 'pending':
                            continue
                            
                        legs = parlay['legs']
                        leg_names = []
                        
                        for leg in legs:
                            game_id = leg['game_id']
                            game = next((g for g in upcoming_games if g['id'] == game_id), None)
                            
                            if not game:
                                continue
                                
                            leg_name = f"{game['away_team']} @ {game['home_team']} - {leg['bet_type'].title()} ({leg['bet_pick']})"
                            leg_names.append(leg_name)
                        
                        bet_name = f"Parlay: {' + '.join(leg_names[:2])}" + (f" + {len(leg_names)-2} more" if len(leg_names) > 2 else "")
                        bet_names.append(bet_name)
                        
                        # For parlays, probability is the product of individual probabilities (simplified)
                        probability = 0.5 ** len(legs)  # Simplified calculation
                        win_probabilities.append(round(probability * 100, 1))
                        bet_types.append('Parlay')
                        payouts.append(parlay['potential_payout'])
                    
                    # Create data for chart
                    chart_data = pd.DataFrame({
                        'Bet': bet_names,
                        'Win Probability (%)': win_probabilities,
                        'Type': bet_types,
                        'Potential Payout ($)': payouts
                    })
                    
                    # Sort by probability
                    chart_data = chart_data.sort_values('Win Probability (%)', ascending=True)
                    
                    # Create a horizontal bar chart
                    import plotly.express as px
                    
                    fig = px.bar(
                        chart_data, 
                        y='Bet', 
                        x='Win Probability (%)', 
                        color='Type',
                        hover_data=['Potential Payout ($)'],
                        orientation='h',
                        title='Probability of Winning Each Bet',
                        labels={'Win Probability (%)': 'Chance of Winning (%)', 'Bet': ''},
                        color_discrete_sequence=['#1E88E5', '#FFC107', '#D81B60', '#004D40']
                    )
                    
                    # Customize the layout
                    fig.update_layout(
                        xaxis_range=[0, 100],
                        xaxis_title='Probability (%)',
                        yaxis_title='',
                        legend_title='Bet Type',
                        height=max(300, len(bet_names) * 40)  # Dynamic height based on number of bets
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Show explanation
                    st.info("""
                    **How to interpret this chart:**
                    - Higher probability means better chances of winning the bet
                    - Moneyline bets on favorites have higher win probability but lower payouts
                    - Parlays have lower win probability but higher potential payouts
                    - These probabilities are calculated based on the betting odds
                    """)
            
            with betting_tabs[4]:
                st.subheader("Game Results & Summaries")
                st.write("""
                View recent game results and detailed summaries across all sports.
                See how games played out with comprehensive statistics and key moments.
                """)
                
                # Add explanatory banner
                st.markdown("""
                <div style="padding:10px;background-color:#f0f9ff;border-radius:5px;margin-bottom:10px;">
                    <h4>🏆 Live Game Results</h4>
                    <p>Stay updated with the latest game results and see how they affect player values.</p>
                    <p>Select any completed game below to view detailed statistics, scoring summaries, and player performances.</p>
                </div>
                """, unsafe_allow_html=True)
                
                try:
                    with engine.connect() as conn:
                        # Get completed games
                        completed_query = text("""
                            SELECT id, home_team, away_team, game_date, home_score, away_score, 
                                   home_odds, away_odds, spread, over_under, status
                            FROM upcoming_games
                            WHERE status = 'completed'
                            ORDER BY game_date DESC LIMIT 10
                        """)
                        completed_games = pd.read_sql(completed_query, conn)
                        
                        if completed_games.empty:
                            st.info("No completed games available yet.")
                            
                            # If admin user, show button to simulate games
                            if current_user_id == 1:  # Admin user typically has ID 1
                                st.markdown("""
                                <div style="padding:10px;background-color:#ffffd0;border-radius:5px;margin-bottom:10px;margin-top:15px;">
                                    <h4>🧪 Admin Testing Tools</h4>
                                    <p>You can simulate game results to see how the system works.</p>
                                </div>
                                """, unsafe_allow_html=True)
                                
                                # Get upcoming games for simulation
                                upcoming_query = text("""
                                    SELECT id, home_team, away_team, game_date 
                                    FROM upcoming_games
                                    WHERE status = 'scheduled'
                                    ORDER BY game_date
                                    LIMIT 5
                                """)
                                upcoming_for_sim = pd.read_sql(upcoming_query, conn)
                                
                                if not upcoming_for_sim.empty:
                                    game_options = [f"{row['away_team']} @ {row['home_team']}" for _, row in upcoming_for_sim.iterrows()]
                                    selected_game = st.selectbox("Select a game to simulate:", game_options, key="sim_game_select")
                                    
                                    # Get the selected game index
                                    game_index = game_options.index(selected_game)
                                    game_id = upcoming_for_sim.iloc[game_index]['id']
                                    
                                    if st.button("🎲 Simulate Selected Game", key="sim_game_btn"):
                                        # Import the function here to avoid circular imports
                                        from game_updater import update_game_and_generate_summary
                                        success, message, summary = update_game_and_generate_summary(game_id)
                                        if success:
                                            st.success(f"Game simulated successfully! {message}")
                                            st.text_area("Game Summary", summary, height=200)
                                            # Refresh the page after simulation
                                            st.rerun()
                                        else:
                                            st.error(f"Failed to simulate game: {message}")
                                else:
                                    st.info("No upcoming games available for simulation.")
                            
                        else:
                            # Format the data for display
                            display_games = []
                            for _, game in completed_games.iterrows():
                                winner = game['home_team'] if game['home_score'] > game['away_score'] else game['away_team']
                                display_games.append({
                                    'id': game['id'],
                                    'matchup': f"{game['away_team']} @ {game['home_team']}",
                                    'score': f"{game['away_score']} - {game['home_score']}",
                                    'winner': winner,
                                    'game_date': game['game_date']
                                })
                            
                            # Display games
                            st.write("### Recent Game Results")
                            games_df = pd.DataFrame(display_games)
                            st.dataframe(games_df[['matchup', 'score', 'winner', 'game_date']])
                            
                            # Allow user to select a game to view summary
                            if not display_games:
                                st.info("No game results available yet.")
                            else:
                                game_options = [f"{g['matchup']} ({g['score']})" for g in display_games]
                                selected_game = st.selectbox("Select a game to view details:", game_options)
                                
                                # Get the selected game ID
                                game_index = game_options.index(selected_game)
                                game_id = display_games[game_index]['id']
                                
                                # Get summary if available
                                summary_query = text("""
                                    SELECT summary FROM game_summaries WHERE game_id = :game_id
                                """)
                                summary_result = conn.execute(summary_query, {"game_id": game_id}).fetchone()
                                
                                if summary_result:
                                    st.write("### Game Summary")
                                    st.text_area("Game Details", summary_result[0], height=400)
                                    
                                    # Get player performances 
                                    try:
                                        news_query = text("""
                                            SELECT pn.title, pn.content, pn.impact, pd.name, pd.team 
                                            FROM player_news pn 
                                            JOIN player_data pd ON pn.player_id = pd.id
                                            WHERE pn.published_at >= (
                                                SELECT updated_at FROM upcoming_games WHERE id = :game_id
                                            ) AND pn.published_at <= (
                                                SELECT updated_at + INTERVAL '10 minutes' FROM upcoming_games WHERE id = :game_id
                                            )
                                            LIMIT 5
                                        """)
                                        news = pd.read_sql(news_query, conn, params={"game_id": game_id})
                                        
                                        if not news.empty:
                                            st.write("### Player Performances")
                                            for _, row in news.iterrows():
                                                impact_color = "green" if row['impact'] == 'positive' else "red"
                                                st.markdown(f"**{row['name']} ({row['team']})**: {row['title']}")
                                                st.markdown(f"<span style='color:{impact_color}'>Impact: {row['impact'].title()}</span>", unsafe_allow_html=True)
                                                st.write(row['content'])
                                                st.markdown("---")
                                    except Exception as e:
                                        st.error(f"Error loading player performances: {e}")
                                else:
                                    st.info("No detailed summary available for this game.")
                except Exception as e:
                    st.error(f"Error loading game results: {e}")
            
            with betting_tabs[5]:
                st.subheader("Betting History")
                # TODO: Get and display betting history
                st.info("Betting history feature coming soon.")
            
            
            
        elif page == "Sports News":
            st.title("Sports News & Updates")
            
            # Attempt to update news from real sources
            if st.button("Refresh Sports News"):
                with st.spinner("Fetching latest sports news..."):
                    success = update_sports_news_from_real_sources()
                    if success:
                        st.success("News updated successfully!")
                    else:
                        st.warning("Could not get real-time updates. Showing available news.")
            
            # Get the latest sports news
            news_items = get_cached_sports_news(limit=20)
            
            if not news_items:
                st.info("No sports news available at the moment. Please try again later.")
            else:
                # Create tabs for different sports
                news_tabs = st.tabs(["All Sports", "NFL", "NBA", "MLB", "College"])
                
                with news_tabs[0]:
                    st.subheader("Latest Sports News")
                    
                    for i, news in enumerate(news_items):
                        with st.expander(f"{news.get('headline', 'Sports Update')} ({news.get('source', 'Sports Source')})"):
                            st.write(f"**{news.get('headline', 'Sports Update')}**")
                            st.caption(f"{news.get('date', 'Recent')} | {news.get('source', 'Sports Source')}")
                            
                            # Display image if available
                            if news.get('image_url'):
                                st.image(news.get('image_url'), caption=news.get('source'), width=350)
                            
                            st.markdown(news.get('content', 'No content available'))
                            
                            if news.get('tags'):
                                try:
                                    tags = news.get('tags').split(',') if isinstance(news.get('tags'), str) else news.get('tags')
                                    st.markdown(f"*Tags: {', '.join(tags)}*")
                                except:
                                    pass
                            
                            if news.get('url'):
                                st.markdown(f"[Read full article]({news.get('url')})")
                
                # Filter news by sport for each tab
                sports_mapping = {
                    "NFL": ["NFL", "Football"],
                    "NBA": ["NBA", "Basketball"],
                    "MLB": ["MLB", "Baseball"],
                    "College": ["NCAA", "College Football", "College Basketball"]
                }
                
                for i, sport in enumerate(["NFL", "NBA", "MLB", "College"]):
                    with news_tabs[i+1]:
                        st.subheader(f"{sport} News")
                        
                        # Filter news for this sport
                        sport_news = []
                        for n in news_items:
                            news_sport = n.get('sport', '')
                            news_tags = n.get('tags', '')
                            
                            if isinstance(news_tags, str):
                                news_tags = news_tags.split(',')
                            
                            if news_sport in sports_mapping[sport] or any(tag in sports_mapping[sport] for tag in news_tags):
                                sport_news.append(n)
                        
                        if not sport_news:
                            st.info(f"No {sport} news available at the moment.")
                        else:
                            for news in sport_news:
                                with st.expander(f"{news.get('headline', 'Sports Update')} ({news.get('source', 'Sports Source')})"):
                                    st.write(f"**{news.get('headline', 'Sports Update')}**")
                                    st.caption(f"{news.get('date', 'Recent')} | {news.get('source', 'Sports Source')}")
                                    
                                    # Display image if available
                                    if news.get('image_url'):
                                        st.image(news.get('image_url'), caption=news.get('source'), width=350)
                                    
                                    st.markdown(news.get('content', 'No content available'))
                                    
                                    if news.get('url'):
                                        st.markdown(f"[Read full article]({news.get('url')})")
        
        elif page == "Live Games":
            st.title("Live Games & Scores")
            
            # Refresh button for live games
            if st.button("Refresh Live Games"):
                st.cache_data.clear()
                st.rerun()
            
            # Get live games data
            live_games = get_cached_live_games()
            upcoming_games = get_cached_upcoming_games(limit=15)
            
            # Create tabs for live and upcoming games
            game_tabs = st.tabs(["Live Games", "Upcoming Games", "Game Stats"])
            
            with game_tabs[0]:
                st.subheader("Currently Live Games")
                
                if not live_games:
                    st.info("No games are currently in progress. Check the Upcoming Games tab for scheduled games.")
                else:
                    # Sort games by sport and then by time remaining
                    for sport in ["NFL", "NBA", "MLB", "NCAA Football", "NCAA Basketball"]:
                        sport_games = [g for g in live_games if g.get('sport') == sport]
                        
                        if sport_games:
                            st.markdown(f"### {sport}")
                            
                            # Create columns for each game
                            cols = st.columns(min(3, len(sport_games)))
                            
                            for i, game in enumerate(sport_games):
                                with cols[i % min(3, len(sport_games))]:
                                    # Create a card-like UI for each game
                                    st.markdown(f"""
                                    <div style="padding:10px; border-radius:5px; border:1px solid #ddd; margin-bottom:10px">
                                        <div style="font-weight:bold; text-align:center">{game.get('away_team')} @ {game.get('home_team')}</div>
                                        <div style="font-size:24px; text-align:center; margin:10px 0">
                                            {game.get('away_score', 0)} - {game.get('home_score', 0)}
                                        </div>
                                        <div style="text-align:center; color:#777">
                                            {game.get('period', '')} {game.get('time_remaining', '')}
                                        </div>
                                        <div style="margin-top:10px; text-align:center">
                                            {game.get('status', '')}
                                        </div>
                                    </div>
                                    """, unsafe_allow_html=True)
                                    
                                    # Add a link to the related players
                                    if st.button(f"View Players in this Game", key=f"view_game_{game.get('id')}_{i}"):
                                        # TODO: Implement player filtering by team
                                        st.session_state.page = "Market"
                                        st.rerun()
            
            with game_tabs[1]:
                st.subheader("Upcoming Games")
                
                if not upcoming_games:
                    st.info("No upcoming games scheduled at the moment.")
                else:
                    # Group by date
                    dates = sorted(set(datetime.datetime.fromisoformat(game.get('start_time')).date().strftime("%Y-%m-%d") 
                               for game in upcoming_games if game.get('start_time')))
                    
                    for date in dates:
                        st.markdown(f"### {date}")
                        date_games = [g for g in upcoming_games 
                                     if datetime.datetime.fromisoformat(g.get('start_time')).date().strftime("%Y-%m-%d") == date]
                        
                        # Group by sport
                        sports = sorted(set(game.get('sport') for game in date_games if 'sport' in game))
                        
                        for sport in sports:
                            st.markdown(f"#### {sport}")
                            sport_date_games = [g for g in date_games if g.get('sport') == sport]
                            
                            # Create a table
                            game_data = []
                            for game in sport_date_games:
                                game_time = datetime.datetime.fromisoformat(game.get('start_time')).time().strftime("%I:%M %p")
                                game_data.append({
                                    "Time": game_time,
                                    "Matchup": f"{game.get('away_team')} @ {game.get('home_team')}",
                                    "Status": game.get('status', 'UPCOMING')
                                })
                            
                            if game_data:
                                st.table(pd.DataFrame(game_data))
            
            with game_tabs[2]:
                st.subheader("Game Statistics & Performance Updates")
                
                # Combine live and upcoming games for selection
                all_games = live_games + upcoming_games
                
                if not all_games:
                    st.info("No games available for statistics.")
                else:
                    # Create a dropdown to select a game
                    game_options = [f"{g.get('away_team')} @ {g.get('home_team')} ({g.get('sport')}, {g.get('status', 'Upcoming')})" for g in all_games]
                    selected_game_idx = st.selectbox("Select a game to view stats:", range(len(game_options)), format_func=lambda x: game_options[x])
                    
                    if selected_game_idx is not None:
                        selected_game = all_games[selected_game_idx]
                        
                        st.write(f"### {selected_game.get('away_team')} @ {selected_game.get('home_team')}")
                        st.write(f"**Status:** {selected_game.get('status', 'Upcoming')}")
                        
                        # Display game details
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.markdown("#### Away Team")
                            st.write(f"**{selected_game.get('away_team')}**")
                            
                            if selected_game.get('status') == 'LIVE':
                                st.write(f"**Score:** {selected_game.get('away_score', 0)}")
                                
                                # Find players from this team in the database
                                with engine.connect() as conn:
                                    query = text("""
                                        SELECT name, position, team, current_price, fantasy_points, sport
                                        FROM players
                                        WHERE team = :team
                                        ORDER BY current_price DESC
                                        LIMIT 5
                                    """)
                                    result = conn.execute(query, {"team": selected_game.get('away_team')})
                                    players = [dict(row) for row in result]
                                    
                                    if players:
                                        st.write("**Key Players:**")
                                        for player in players:
                                            st.write(f"- {player['name']} ({player['position']}): ${player['current_price']:.2f}")
                        
                        with col2:
                            st.markdown("#### Home Team")
                            st.write(f"**{selected_game.get('home_team')}**")
                            
                            if selected_game.get('status') == 'LIVE':
                                st.write(f"**Score:** {selected_game.get('home_score', 0)}")
                                
                                # Find players from this team in the database
                                with engine.connect() as conn:
                                    query = text("""
                                        SELECT name, position, team, current_price, fantasy_points, sport
                                        FROM players
                                        WHERE team = :team
                                        ORDER BY current_price DESC
                                        LIMIT 5
                                    """)
                                    result = conn.execute(query, {"team": selected_game.get('home_team')})
                                    players = [dict(row) for row in result]
                                    
                                    if players:
                                        st.write("**Key Players:**")
                                        for player in players:
                                            st.write(f"- {player['name']} ({player['position']}): ${player['current_price']:.2f}")
                        
                        # Add a section showing the potential market impact of this game
                        st.subheader("Potential Market Impact")
                        st.write("""
                        This game could significantly impact player valuations, especially for star performers.
                        Key factors that may affect player values:
                        
                        - Individual statistical performance
                        - Team win/loss outcome
                        - Injuries or limited playing time
                        - Matchup difficulty and game script
                        """)
                        
                        # Add a link to view these players in the market
                        if st.button("View All Players in this Game in Market"):
                            st.session_state.page = "Market"
                            st.rerun()
        
        elif page == "Player Insights":
            st.title("Player Insights")
            
            # Create tabs for the different player insight sections
            insight_tabs = st.tabs(["Player News", "Historical Performance", "Performance Analysis"])
            
            with insight_tabs[0]:
                st.header("Latest Player News & Updates")
                st.write("Stay informed about key player developments that affect their market value")
                
                # Get player news from the database
                try:
                    with engine.connect() as conn:
                        news_query = text("""
                            SELECT player_name, news_type, headline, content, impact, impact_description, 
                                   published_at, source
                            FROM player_news
                            ORDER BY published_at DESC
                        """)
                        news = pd.read_sql(news_query, conn)
                        
                        if news.empty:
                            st.info("No player news available at this time.")
                        else:
                            # Filter options
                            news_filters = st.multiselect(
                                "Filter by News Type", 
                                ["All"] + list(news["news_type"].unique()),
                                default=["All"]
                            )
                            
                            # Apply filter
                            if "All" not in news_filters:
                                filtered_news = news[news["news_type"].isin(news_filters)]
                            else:
                                filtered_news = news
                            
                            # Display news items
                            for _, item in filtered_news.iterrows():
                                col1, col2 = st.columns([4, 1])
                                
                                with col1:
                                    # Display headline with impact indicator
                                    if item["impact"] == "positive":
                                        icon = "🔺"
                                        color = "green"
                                    elif item["impact"] == "negative":
                                        icon = "🔻"
                                        color = "red"
                                    else:
                                        icon = "⚪"
                                        color = "gray"
                                    
                                    st.markdown(f"### {icon} {item['headline']}")
                                    
                                    # Format date
                                    published_date = item["published_at"].strftime("%Y-%m-%d %H:%M")
                                    
                                    # Show news metadata
                                    st.caption(f"Player: **{item['player_name']}** | Type: **{item['news_type'].capitalize()}** | Source: **{item['source']}** | Published: **{published_date}**")
                                    
                                    # Display content
                                    st.write(item["content"])
                                    
                                with col2:
                                    # Impact assessment
                                    if item["impact"] == "positive":
                                        st.markdown("### Market Impact")
                                        st.markdown('<div style="padding:10px;background-color:#d4edda;color:#155724;border-radius:5px;"><strong>POSITIVE</strong></div>', unsafe_allow_html=True)
                                        st.caption(item["impact_description"])
                                    elif item["impact"] == "negative":
                                        st.markdown("### Market Impact")
                                        st.markdown('<div style="padding:10px;background-color:#f8d7da;color:#721c24;border-radius:5px;"><strong>NEGATIVE</strong></div>', unsafe_allow_html=True)
                                        st.caption(item["impact_description"])
                                    else:
                                        st.markdown("### Market Impact")
                                        st.markdown('<div style="padding:10px;background-color:#e2e3e5;color:#383d41;border-radius:5px;"><strong>NEUTRAL</strong></div>', unsafe_allow_html=True)
                                        st.caption(item["impact_description"])
                                
                                st.markdown("---")
                except Exception as e:
                    st.error(f"Error retrieving player news: {str(e)}")
                    
            with insight_tabs[1]:
                st.header("Historical Performance")
                st.write("Track how players have performed over time and how it affected their market value")
                
                # Get all player names for selection
                try:
                    with engine.connect() as conn:
                        players_query = text("SELECT DISTINCT name FROM players ORDER BY name")
                        player_names = [row[0] for row in conn.execute(players_query).fetchall()]
                        
                        if not player_names:
                            st.info("No players available in the database.")
                        else:
                            # Player selection dropdown
                            selected_player = st.selectbox("Select Player", player_names)
                            
                            # Get historical performance data for the selected player
                            history_query = text("""
                                SELECT player_name, game_date, opponent, fantasy_points,
                                       performance_stats, price_before, price_after, price_change_pct
                                FROM player_performance_history
                                WHERE player_name = :player_name
                                ORDER BY game_date DESC
                            """)
                            history = pd.read_sql(history_query, conn, params={"player_name": selected_player})
                            
                            if history.empty:
                                st.info(f"No historical performance data available for {selected_player}.")
                            else:
                                # Display performance history
                                st.subheader(f"{selected_player} Performance History")
                                
                                # Create a chart of historical fantasy points
                                try:
                                    import plotly.express as px
                                    
                                    # Create a chart of fantasy points over time
                                    fig = px.line(
                                        history.sort_values('game_date'),
                                        x='game_date',
                                        y='fantasy_points',
                                        title=f"{selected_player} Fantasy Points by Game",
                                        labels={'fantasy_points': 'Fantasy Points', 'game_date': 'Game Date'},
                                        markers=True
                                    )
                                    
                                    # Add price change as a secondary axis
                                    fig.add_scatter(
                                        x=history.sort_values('game_date')['game_date'],
                                        y=history.sort_values('game_date')['price_change_pct'],
                                        mode='lines+markers',
                                        name='Price Change %',
                                        yaxis='y2'
                                    )
                                    
                                    # Set up secondary axis and layout
                                    fig.update_layout(
                                        yaxis2=dict(
                                            title='Price Change (%)',
                                            overlaying='y',
                                            side='right'
                                        )
                                    )
                                    
                                    st.plotly_chart(fig, use_container_width=True)
                                except Exception as e:
                                    st.error(f"Error creating chart: {str(e)}")
                                
                                # Display game-by-game stats
                                st.subheader("Game-by-Game Breakdown")
                                for _, game in history.iterrows():
                                    with st.expander(f"{game['game_date'].strftime('%Y-%m-%d')} vs. {game['opponent']}"):
                                        col1, col2 = st.columns(2)
                                        
                                        with col1:
                                            st.markdown(f"**Fantasy Points:** {game['fantasy_points']:.1f}")
                                            
                                            # Display detailed performance stats
                                            if game['performance_stats']:
                                                st.markdown("**Performance Stats:**")
                                                # Parse JSON stats
                                                try:
                                                    stats = game['performance_stats']
                                                    if isinstance(stats, str):
                                                        import json
                                                        stats = json.loads(stats)
                                                        
                                                    for stat, value in stats.items():
                                                        st.write(f"- {stat.replace('_', ' ').title()}: {value}")
                                                except:
                                                    st.write("Stats data format error")
                                        
                                        with col2:
                                            # Price information with color coding
                                            price_before = game['price_before']
                                            price_after = game['price_after']
                                            price_change = game['price_change_pct']
                                            
                                            st.markdown("**Price Impact:**")
                                            st.write(f"Price Before: ${price_before:.2f}")
                                            st.write(f"Price After: ${price_after:.2f}")
                                            
                                            if price_change > 0:
                                                st.markdown(f"<span style='color:green'>↑ +{price_change:.1f}%</span>", unsafe_allow_html=True)
                                            elif price_change < 0:
                                                st.markdown(f"<span style='color:red'>↓ {price_change:.1f}%</span>", unsafe_allow_html=True)
                                            else:
                                                st.write("No change (0%)")
                except Exception as e:
                    st.error(f"Error retrieving historical performance data: {str(e)}")
            
            with insight_tabs[2]:
                st.header("Performance Analysis")
                st.write("Deep dive into what drives player performance and value")
                
                # Sample analysis - we'd use real data in a production system
                st.subheader("Fantasy Points Distribution by Position")
                
                # Generate sample data for visualization
                try:
                    # Simulate getting this from the database
                    position_data = {
                        'QB': [18.5, 24.2, 30.7, 15.3, 22.1, 19.8, 27.5, 21.3],
                        'RB': [12.3, 18.7, 15.2, 9.8, 20.5, 14.2, 11.7, 16.9],
                        'WR': [10.2, 15.7, 8.5, 22.3, 12.8, 9.9, 18.2, 13.5],
                        'TE': [8.7, 5.2, 12.3, 9.9, 7.8, 10.5, 6.2, 11.7],
                        'PG': [35.2, 28.7, 31.5, 25.8, 29.2, 33.7, 27.8, 30.5],
                        'C': [28.3, 20.5, 25.7, 30.2, 22.8, 26.5, 24.3, 27.9]
                    }
                    
                    # Create dataframe for plotting
                    import pandas as pd
                    plot_data = []
                    for position, points in position_data.items():
                        for pt in points:
                            plot_data.append({'Position': position, 'Fantasy Points': pt})
                    
                    df = pd.DataFrame(plot_data)
                    
                    # Create box plot
                    import plotly.express as px
                    fig = px.box(
                        df, 
                        x='Position', 
                        y='Fantasy Points',
                        color='Position',
                        title='Fantasy Points Distribution by Position',
                        points='all'
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Add explanation
                    st.markdown("""
                    ### Key Performance Metrics by Position
                    
                    Different positions contribute fantasy points in different ways:
                    
                    #### NFL
                    - **QB**: Passing yards (0.04 pts per yard), passing TDs (4 pts), interceptions (-1 pt)
                    - **RB**: Rushing yards (0.1 pts per yard), rushing TDs (6 pts), receiving yards/TDs
                    - **WR**: Receiving yards (0.1 pts per yard), receiving TDs (6 pts), receptions (PPR formats)
                    - **TE**: Similar to WR but typically fewer targets
                    
                    #### NBA
                    - **Guards (PG/SG)**: Points, assists, steals, 3-pointers
                    - **Forwards (SF/PF)**: Points, rebounds, blocks
                    - **Centers (C)**: Rebounds, blocks, field goal %
                    
                    #### MLB
                    - **Pitchers**: Strikeouts, innings pitched, wins, saves
                    - **Batters**: Hits, home runs, RBIs, stolen bases
                    """)
                    
                    # Performance-value correlation
                    st.subheader("Performance-Value Correlation")
                    
                    # Generate sample correlation data
                    correlation_data = {
                        'Metric': ['Fantasy Points', 'Team Success', 'Injury History', 'Age', 'Playoff Performance'],
                        'Correlation': [0.85, 0.62, -0.45, -0.28, 0.55]
                    }
                    
                    corr_df = pd.DataFrame(correlation_data)
                    
                    # Create bar chart
                    fig = px.bar(
                        corr_df,
                        x='Metric',
                        y='Correlation',
                        color='Correlation',
                        title='Factors Correlated with Player Value',
                        color_continuous_scale=[(0, "red"), (0.5, "yellow"), (1, "green")]
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
                    
                    st.markdown("""
                    ### Understanding the Value-Performance Connection
                    
                    Player values are influenced by multiple factors beyond raw fantasy points:
                    
                    - **Positive correlations** (factors that increase value):
                      - Strong fantasy performance
                      - Team success (wins, playoff appearances)
                      - Playoff/clutch performances
                      - Media attention and popularity
                      - Contract extensions with favorable terms
                      - Return from injury better than expected
                      - Strong performances against top opponents
                    
                    - **Negative correlations** (factors that decrease value):
                      - Injury history and concerns
                      - Advancing age (especially past prime years)
                      - Declining physical metrics
                      - Off-field/court issues and controversies
                      
                    ### Sport-Specific Negative Factors
                    
                    #### NFL
                      - Quarterback interceptions and fumbles
                      - Running back fumbles and decreased carries
                      - Receiver dropped passes and decreased targets
                      - Offensive line performance drops affecting skill positions
                      - Being placed on IR or PUP list
                      - Off-field legal issues or suspensions
                      - Trade to team with worse offensive scheme fit
                      
                    #### NBA
                      - High turnover rates
                      - Significant shooting percentage drops
                      - Decreased minutes in rotation
                      - Technical fouls and ejections
                      - Load management/rest days increasing
                      - Conflict with coaching staff
                      - Team acquiring competing player at same position
                      
                    #### MLB
                      - Pitchers: Increased ERA, decreased strikeouts, velocity drops
                      - Batters: Extended slumps, increased strikeout rates
                      - Defensive shifting affecting performance
                      - Decreased playing time against certain pitcher types
                      - Negative advanced metrics (exit velocity, launch angle changes)
                      - Moving down in batting order
                      - Minor league demotions
                    """)
                except Exception as e:
                    st.error(f"Error generating performance analysis: {str(e)}")

        elif page == "How It Works":
            st.title("How It Works")
            st.markdown("""
            ## Welcome to ATHL3T Trades
            
            ATHL3T Trades is a unique platform that combines fantasy sports with stock market mechanics, allowing you to invest in athletes across multiple sports leagues (NFL, NBA, MLB, etc.) and profit from their real-world performance. Here's how it all works:
            """)
            
            how_it_works_tabs = st.tabs(["Market Basics", "Performance Tracking", "Trading System", "Betting Feature"])
            
            with how_it_works_tabs[0]:
                st.header("Market Basics")
                
                st.markdown("""
                ### The Athlete Stock Market
                
                Each athlete on ATHL3T Trades has a **market cap** based on their performance potential and popularity. This market cap is divided into a fixed number of shares:
                
                - **Elite Players**: 100,000 shares
                - **Star Players**: 75,000 shares  
                - **Established Players**: 50,000 shares
                - **Rookie/Prospect Players**: 30,000 shares
                - **Depth/Bench Players**: 10,000 shares
                
                The **share price** is calculated by dividing the player's total market cap by the number of shares outstanding. For example:
                
                - Patrick Mahomes has a $5,000,000 market cap with 100,000 shares = $50.00 per share
                - Trevor Lawrence has a $3,000,000 market cap with 75,000 shares = $40.00 per share
                
                ### Team Funds
                
                In addition to individual players, you can invest in **Team Funds** that represent a group of players:
                
                - **Team Funds**: All players from a specific team (e.g., Kansas City Chiefs Fund)
                - **Position Funds**: All players of a specific position (e.g., NFL Quarterbacks Fund)
                - **Conference/Division Funds**: All players from a conference or division
                
                These funds provide diversification and reduced volatility compared to individual player shares.
                """)
                
                st.image("https://media.istockphoto.com/id/1300547130/vector/a-trading-candlestick-chart-by-a-tablet-computer-as-concept-for-invest-stock-or-forex-trade.jpg?s=612x612&w=0&k=20&c=zw9pSiZUBpE_T6RW-DaLVTr89-ROlFwGKGDhyLVfyeY=", caption="Market Dynamics Visualization")
                
            with how_it_works_tabs[1]:
                st.header("Performance Tracking")
                
                st.markdown("""
                ### How Player Values Change
                
                Player values automatically adjust based on their real-world performance using standardized fantasy sports metrics:
                
                #### Fantasy Sports Scoring System
                
                Each position in each sport has specific scoring criteria:
                
                **NFL Example:**
                - QBs: Passing yards (0.04 pts per yard), TDs (4 pts), INTs (-1 pt)
                - RBs: Rushing yards (0.1 pts per yard), TDs (6 pts), receiving
                - WRs: Receiving yards (0.1 pts per yard), TDs (6 pts), catches
                
                **NBA Example:**
                - Points scored (1 pt), rebounds (1.2 pts), assists (1.5 pts)
                - Steals (2 pts), blocks (2 pts), turnovers (-1 pt)
                - Bonus for double-doubles (1.5 pts) and triple-doubles (3 pts)
                
                **MLB Example:**
                - Batters: Hits (2 pts), runs (1.5 pts), RBIs (2 pts), HRs (4 pts), strikeouts (-2 pts)
                - Pitchers: Strikeouts (1 pt), innings pitched (2.25 pts), wins (4 pts)
                - All positions: Fielding errors (-2 pts)
                
                #### Price Adjustment Tiers
                
                Player prices change based on their percentile performance compared to others at their position:
                
                | Performance Level | Percentile | Price Change |
                |-------------------|------------|--------------|
                | Exceptional       | 95%+       | +15% 🚀      |
                | Excellent         | 90%+       | +10% ⬆️      |
                | Very Good         | 80%+       | +7% ↗️       |
                | Good              | 70%+       | +5%          |
                | Above Average     | 60%+       | +3%          |
                | Average           | 50%+       | +1%          |
                | Below Average     | 40%+       | -1%          |
                | Poor              | 30%+       | -3%          |
                | Very Poor         | 20%+       | -5% ↘️       |
                | Terrible          | 10%+       | -10% ⬇️      |
                | Disastrous        | Bottom 5%  | -15% 📉      |
                
                Performance updates typically occur after games and are reflected in the player's current share price.
                """)
                
                # Sample price history chart
                import plotly.express as px
                import pandas as pd
                import numpy as np
                
                # Create sample data for price history chart
                dates = pd.date_range(start='2023-01-01', end='2023-01-15', freq='D')
                price = [50.00]
                for i in range(1, len(dates)):
                    change = np.random.normal(0, 0.05)  # Random change with mean 0, std 0.05
                    new_price = price[-1] * (1 + change)
                    price.append(new_price)
                
                # Create and display sample chart
                df = pd.DataFrame({'Date': dates, 'Price': price})
                fig = px.line(df, x='Date', y='Price', title='Sample Player Price History')
                st.plotly_chart(fig, use_container_width=True)
                
                st.markdown("""
                ### News and Events Impact
                
                Player values are also affected by real-world events:
                
                - **Injuries**: Negative impact (usually -5% to -20%)
                - **Trades**: Positive or negative depending on new team situation
                - **Contract extensions**: Usually positive (stability)
                - **Off-field issues**: Typically negative
                - **Coaching changes**: Can be positive or negative
                
                These events are reflected in the "Player News" section and are factored into price changes along with statistical performance.
                """)
                
            with how_it_works_tabs[2]:
                st.header("Trading System")
                
                st.markdown("""
                ### Buying and Selling Shares
                
                The trading system is designed to be simple and intuitive:
                
                1. **Buy shares** when you believe a player's value will increase
                2. **Sell shares** when you believe a player's value has peaked
                3. **Hold shares** to collect long-term gains as players develop
                
                All transactions are executed at the current market price. The platform does not charge commission fees.
                
                ### Peer-to-Peer Trading
                
                In addition to market transactions, you can trade directly with other users:
                
                1. **Direct player-for-player trades**: Exchange shares with specific users
                2. **Make offers**: Propose trades to the community
                3. **Accept offers**: Respond to others' trade proposals
                
                These peer-to-peer trades allow for more complex strategies and can help you acquire shares that might otherwise be difficult to obtain.
                
                ### Portfolio Management
                
                Your portfolio shows:
                
                - Total portfolio value
                - Performance metrics (daily, weekly, monthly changes)
                - Diversification by sport, position, and team
                - Transaction history
                
                Effective portfolio management involves:
                - Diversifying across multiple sports and positions
                - Balancing high-risk/high-reward players with stable performers
                - Regularly rebalancing based on player performance trends
                """)
                
            with how_it_works_tabs[3]:
                st.header("Betting Feature")
                
                st.markdown("""
                ### Sports Betting Integration
                
                For users 21+, the platform offers sports betting features:
                
                - **Game Betting**: Traditional moneyline, spread, and over/under bets
                - **Player Props**: Bet on specific player performance metrics
                - **Parlays**: Combine multiple bets for higher potential payouts
                
                All sports betting features are subject to:
                
                - **Age verification**: Must be 21+ to access
                - **Time restrictions**: Betting closes 12 hours before game time
                - **Responsible gaming limits**: Daily and weekly betting limits
                
                ### Player Props
                
                Player prop bets are directly connected to our performance tracking system:
                
                - Bet on over/under for specific player stats (e.g., Luka Dončić over/under 25.5 points)
                - Lines are set based on season averages and matchup data
                - Results affect both bet outcomes and player share prices
                
                This integration creates a unique synergy between your investment strategy and betting activity.
                """)
                
                st.info("Note: Sports betting is for entertainment purposes only. Never bet more than you can afford to lose, and always practice responsible gaming.")
            
            st.markdown("""
            ## Getting Started
            
            Ready to begin trading? Here's how to get started:
            
            1. **Create an account** and verify your information
            2. **Add funds** to your wallet
            3. **Browse the Market** to identify promising players and funds
            4. **Build your portfolio** by purchasing shares
            5. **Monitor performance** and adjust your strategy as needed
            
            Happy trading!
            """)

        elif page == "Admin":
            st.header("Database Administration")
            
            # Add explanation and warning
            st.warning("This page provides administrative functions for database management. Use with caution.")
            
            # Add some administrative functions
            st.subheader("Database Tables")
            
            admin_tabs = st.tabs(["Users", "Holdings", "Market Data", "Transactions", "Performance Update", "Game Results", "Live Game Updates"])
            
            with admin_tabs[0]:
                st.write("User Accounts")
                st.dataframe(users)
            
            with admin_tabs[1]:
                st.write("User Holdings")
                st.dataframe(holdings)
            
            with admin_tabs[2]:
                st.write("Players")
                st.dataframe(players)
                
                st.write("Team Funds")
                st.dataframe(funds)
            
            with admin_tabs[3]:
                # Get all transactions
                try:
                    with engine.connect() as conn:
                        query = text("SELECT * FROM transactions ORDER BY date DESC")
                        all_transactions = pd.read_sql(query, conn)
                        
                    st.write("All Transactions")
                    st.dataframe(all_transactions)
                except Exception as e:
                    st.error(f"Error loading transactions: {str(e)}")
            
            with admin_tabs[4]:
                st.write("### Player Performance Updates")
                st.write("""
                This section allows you to update player prices based on their fantasy sports performance.
                The system uses standardized fantasy scoring metrics for each sport and position to determine
                how a player's market value should change.
                """)
                
                # Show an example of fantasy scoring metrics
                with st.expander("View Fantasy Scoring Metrics"):
                    st.markdown("""
                    #### NFL Scoring Metrics
                    - **Quarterbacks (QB)**: 
                      - 1 point per 25 passing yards (0.04 pts per yard)
                      - 4 points per passing TD
                      - -1 point per interception
                      - 1 point per 10 rushing yards
                      - 6 points per rushing TD
                    
                    - **Running Backs (RB)**:
                      - 1 point per 10 rushing yards
                      - 6 points per rushing TD
                      - 1 point per 10 receiving yards
                      - 6 points per receiving TD
                      
                    #### MLB Scoring Metrics
                    - **Pitchers (P)**:
                      - 2.25 points per inning pitched
                      - 1 point per strikeout
                      - 4 points for a win
                      - -2 points per earned run
                      - -2 points per fielding error
                      
                    - **Batters**:
                      - 1.5 points per run
                      - 2 points per hit
                      - 4 points per home run
                      - 2 points per RBI
                      - -2 points per strikeout
                      - -2 points per fielding error
                    
                    #### NBA/WNBA Scoring Metrics
                    - **All Positions**:
                      - 1 point per point scored
                      - 1.2 points per rebound
                      - 1.5 points per assist
                      - 2 points per steal or block
                      - 3 bonus points for triple-double
                    """)
                
                # Performance tiers explanation
                with st.expander("View Performance Adjustment Tiers"):
                    st.markdown("""
                    #### Performance Percentile Tiers
                    Player price adjustments are based on their percentile rank compared to other players at their position:
                    
                    - **Exceptional (95th percentile)**: +15% price increase
                    - **Excellent (90th percentile)**: +10% price increase
                    - **Very Good (80th percentile)**: +7% price increase
                    - **Good (70th percentile)**: +5% price increase
                    - **Above Average (60th percentile)**: +3% price increase
                    - **Average (50th percentile)**: +1% price increase
                    - **Below Average (40th percentile)**: -1% price decrease
                    - **Poor (30th percentile)**: -3% price decrease
                    - **Very Poor (20th percentile)**: -5% price decrease
                    - **Terrible (10th percentile)**: -10% price decrease
                    - **Disastrous (5th percentile)**: -15% price decrease
                    """)
                
                # Button to trigger the update
                from db import update_player_prices_from_performance
                if st.button("Update Player Prices Based on Performance"):
                    count, message = update_player_prices_from_performance()
                    
                    if count > 0:
                        st.success(message)
                    else:
                        st.warning(message)
                    
                    # Display a random selection of updated players
                    try:
                        with engine.connect() as conn:
                            # Get players with fantasy points
                            query = text("""
                                SELECT name, team, position, current_price, last_fantasy_points, weekly_change
                                FROM players 
                                WHERE last_fantasy_points > 0
                                ORDER BY last_fantasy_points DESC
                                LIMIT 10
                            """)
                            updated_players = pd.read_sql(query, conn)
                            
                            if not updated_players.empty:
                                st.write("### Top Performing Players")
                                st.dataframe(updated_players)
                    except Exception as e:
                        st.error(f"Error showing updated players: {str(e)}")
                
                with admin_tabs[5]:
                    st.write("### Game Results & Summaries")
                    st.write("""
                    This section allows you to update game results and generate detailed summaries 
                    for completed games. The system will automatically update player values, 
                    settle bets, and generate news based on the game outcomes.
                    """)
                    
                    # Get upcoming games
                    try:
                        with engine.connect() as conn:
                            # Get all games
                            games_query = text("""
                                SELECT id, home_team, away_team, game_date, status, home_score, away_score, 
                                       home_odds, away_odds, spread, over_under
                                FROM upcoming_games
                                ORDER BY game_date DESC
                            """)
                            upcoming_games = pd.read_sql(games_query, conn)
                            
                            if not upcoming_games.empty:
                                # Show scheduled games that can be updated
                                scheduled_games = upcoming_games[upcoming_games['status'] == 'scheduled']
                                if not scheduled_games.empty:
                                    st.write("### Scheduled Games")
                                    st.dataframe(scheduled_games)
                                    
                                    # Allow selecting a game to update
                                    game_ids = scheduled_games['id'].tolist()
                                    game_labels = [f"{row['home_team']} vs {row['away_team']} ({row['game_date']})" 
                                                  for _, row in scheduled_games.iterrows()]
                                    
                                    game_id_to_update = st.selectbox("Select a game to update:", 
                                                                     options=list(zip(game_ids, game_labels)),
                                                                     format_func=lambda x: x[1])
                                    
                                    if st.button("Update Game Result"):
                                        # Import game updater
                                        import game_updater
                                        success, message, summary = game_updater.update_game_and_generate_summary(game_id_to_update[0])
                                        
                                        if success:
                                            st.success(message)
                                            st.write("### Game Summary")
                                            st.text_area("Game Details", summary, height=400)
                                        else:
                                            st.error(message)
                                
                                # Show completed games
                                completed_games = upcoming_games[upcoming_games['status'] == 'completed']
                                if not completed_games.empty:
                                    st.write("### Completed Games")
                                    st.dataframe(completed_games)
                                    
                                    # Allow viewing summaries for completed games
                                    comp_game_ids = completed_games['id'].tolist()
                                    comp_game_labels = [f"{row['home_team']} {row['home_score']} - {row['away_score']} {row['away_team']}" 
                                                      for _, row in completed_games.iterrows()]
                                    
                                    game_id_to_view = st.selectbox("Select a completed game to view:", 
                                                                  options=list(zip(comp_game_ids, comp_game_labels)),
                                                                  format_func=lambda x: x[1])
                                    
                                    # Check if summary exists
                                    summary_query = text("""
                                        SELECT summary FROM game_summaries WHERE game_id = :game_id
                                    """)
                                    summary_result = conn.execute(summary_query, {"game_id": game_id_to_view[0]}).fetchone()
                                    
                                    if summary_result:
                                        st.write("### Game Summary")
                                        st.text_area("Game Details", summary_result[0], height=400)
                                    else:
                                        # Generate a new summary if none exists
                                        if st.button("Generate Summary for Completed Game"):
                                            import game_updater
                                            success, message, summary = game_updater.update_game_and_generate_summary(game_id_to_view[0])
                                            
                                            if success:
                                                st.success(message)
                                                st.write("### Game Summary")
                                                st.text_area("Game Details", summary, height=400)
                                            else:
                                                st.error(message)
                            else:
                                st.info("No games found in the database.")
                                
                            # Show player news generated from games
                            st.write("### Recent Game News")
                            try:
                                news_query = text("""
                                    SELECT pn.id, pn.title, pn.content, pn.impact, pn.published_at, pd.name, pd.team, pd.sport
                                    FROM player_news pn
                                    JOIN player_data pd ON pn.player_id = pd.id
                                    ORDER BY pn.published_at DESC
                                    LIMIT 5
                                """)
                                news = pd.read_sql(news_query, conn)
                                
                                if not news.empty:
                                    for _, row in news.iterrows():
                                        impact_color = "green" if row['impact'] == 'positive' else "red"
                                        st.markdown(f"**{row['title']}** - *{row['name']} ({row['team']}, {row['sport']})*")
                                        st.markdown(f"<span style='color:{impact_color}'>Impact: {row['impact'].title()}</span>", unsafe_allow_html=True)
                                        st.write(row['content'])
                                        st.write(f"Published: {row['published_at']}")
                                        st.markdown("---")
                                else:
                                    st.info("No recent game news available.")
                            except Exception as e:
                                st.error(f"Error loading player news: {str(e)}")
                                
                    except Exception as e:
                        st.error(f"Error loading game data: {str(e)}")
                    
    except Exception as e:
        st.error(f"Error loading application data: {str(e)}")
        st.info("Please try refreshing the page, or contact support if the problem persists.")