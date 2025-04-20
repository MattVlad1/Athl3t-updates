import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
import random

# Get database URL from environment variable
DATABASE_URL = os.environ.get("DATABASE_URL")

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

def initialize_database():
    """
    Initialize database with required tables if they don't exist
    """
    inspector = inspect(engine)
    
    # Check if tables exist and create them if they don't
    create_tables = []
    
    if 'players' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE players (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                team VARCHAR(100),
                position VARCHAR(10),
                initial_price NUMERIC(10, 2),
                current_price NUMERIC(10, 2),
                week_1_yards INTEGER,
                week_1_tds INTEGER,
                tier VARCHAR(20),
                total_worth NUMERIC(10, 2),
                shares_outstanding INTEGER,
                last_updated TIMESTAMP,
                last_fantasy_points NUMERIC DEFAULT 0,
                weekly_change NUMERIC(5, 2) DEFAULT 0
            )
        """)
    
    if 'team_funds' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE team_funds (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                players_included TEXT,
                price NUMERIC(10, 2),
                type VARCHAR(20)
            )
        """)
    
    if 'users' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE users (
                id VARCHAR(20) PRIMARY KEY,
                username VARCHAR(100),
                email VARCHAR(100),
                password VARCHAR(100),
                wallet_balance NUMERIC(10, 2),
                birthdate DATE,
                is_verified_adult BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    if 'holdings' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE holdings (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                asset_name VARCHAR(100),
                asset_type VARCHAR(20),
                quantity INTEGER,
                purchase_price NUMERIC(10, 2)
            )
        """)
    
    if 'transactions' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE transactions (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMP,
                transaction_type VARCHAR(10),
                user_id VARCHAR(20),
                asset_type VARCHAR(20),
                asset_name VARCHAR(100),
                price NUMERIC(10, 2),
                quantity INTEGER,
                purchase_price NUMERIC(10, 2),
                profit_loss NUMERIC(10, 2)
            )
        """)
        
    # New tables for friends system, competitions, and enhanced peer trading
    if 'friendships' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE friendships (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                friend_id VARCHAR(20),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, friend_id)
            )
        """)
        
    if 'competitions' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE competitions (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                description TEXT,
                start_date TIMESTAMP,
                end_date TIMESTAMP,
                created_by VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'active'
            )
        """)
        
    if 'competition_members' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE competition_members (
                id SERIAL PRIMARY KEY,
                competition_id INTEGER,
                user_id VARCHAR(20),
                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                score NUMERIC(15, 2) DEFAULT 0,
                rank INTEGER DEFAULT 0,
                UNIQUE(competition_id, user_id)
            )
        """)
        
    if 'fantasy_teams' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE fantasy_teams (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                name VARCHAR(100),
                competition_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                score NUMERIC(15, 2) DEFAULT 0
            )
        """)
        
    if 'fantasy_team_players' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE fantasy_team_players (
                id SERIAL PRIMARY KEY,
                team_id INTEGER,
                player_name VARCHAR(100),
                UNIQUE(team_id, player_name)
            )
        """)
        
    if 'trade_offers' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE trade_offers (
                id SERIAL PRIMARY KEY,
                sender_id VARCHAR(20),
                recipient_id VARCHAR(20),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
    if 'trade_offer_assets' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE trade_offer_assets (
                id SERIAL PRIMARY KEY,
                trade_offer_id INTEGER,
                user_id VARCHAR(20),
                asset_type VARCHAR(20),
                asset_name VARCHAR(100),
                quantity INTEGER DEFAULT 1
            )
        """)
        
    if 'trading_offers' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE trading_offers (
                id SERIAL PRIMARY KEY,
                creator_id VARCHAR(20),
                recipient_id VARCHAR(20),
                status VARCHAR(20) DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                description TEXT
            )
        """)
        
    if 'trading_offer_assets' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE trading_offer_assets (
                id SERIAL PRIMARY KEY,
                trade_id INTEGER,
                asset_name VARCHAR(100),
                asset_type VARCHAR(20),
                quantity INTEGER DEFAULT 1,
                is_offered BOOLEAN DEFAULT FALSE
            )
        """)
        
    # Add betting-related tables for users 21+
    if 'upcoming_games' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE upcoming_games (
                id SERIAL PRIMARY KEY,
                home_team VARCHAR(100),
                away_team VARCHAR(100),
                game_date TIMESTAMP,
                home_odds NUMERIC(10, 2),
                away_odds NUMERIC(10, 2),
                spread NUMERIC(5, 1),
                over_under NUMERIC(5, 1),
                status VARCHAR(20) DEFAULT 'scheduled',
                home_score INTEGER DEFAULT 0,
                away_score INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
    if 'user_bets' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE user_bets (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                game_id INTEGER,
                bet_type VARCHAR(20), -- 'moneyline', 'spread', 'over_under'
                bet_pick VARCHAR(20), -- 'home', 'away', 'over', 'under'
                amount NUMERIC(10, 2),
                potential_payout NUMERIC(10, 2),
                odds NUMERIC(10, 2),
                status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'won', 'lost', 'canceled'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
    if 'parlays' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE parlays (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(20),
                amount NUMERIC(10, 2),
                potential_payout NUMERIC(10, 2),
                status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'won', 'lost', 'canceled'
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
    if 'parlay_bets' not in inspector.get_table_names():
        create_tables.append("""
            CREATE TABLE parlay_bets (
                id SERIAL PRIMARY KEY,
                parlay_id INTEGER,
                game_id INTEGER,
                bet_type VARCHAR(20),
                bet_pick VARCHAR(20),
                odds NUMERIC(10, 2),
                status VARCHAR(20) DEFAULT 'pending'
            )
        """)
    
    # Create tables if any are missing
    if create_tables:
        with engine.connect() as conn:
            for table_sql in create_tables:
                conn.execute(text(table_sql))
            conn.commit()
    
    # Add default user if users table is empty
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM users")).fetchone()
        if result[0] == 0:
            conn.execute(
                text("INSERT INTO users (id, username, wallet_balance) VALUES (:id, :username, :wallet_balance)"),
                {"id": "user_001", "username": "DefaultUser", "wallet_balance": 10000.00}
            )
            conn.commit()

# Initialize database on module import
try:
    initialize_database()
except Exception as e:
    print(f"Error initializing database: {e}")

def load_data():
    """
    Load all data from database
    
    Returns:
    - players: DataFrame containing player data
    - funds: DataFrame containing team funds data
    - users: DataFrame containing user data
    - holdings: DataFrame containing user holdings data
    """
    try:
        # Make sure the database is initialized before loading data
        initialize_database()
        
        # Load data from database into pandas dataframes
        with engine.connect() as conn:
            # Check if tables have data
            result = conn.execute(text("SELECT COUNT(*) FROM players")).fetchone()
            player_count = result[0] if result else 0
            
            # If no players exist, try to import data
            if player_count == 0:
                from scraper import get_nfl_players, get_team_funds
                print("No players found in database. Importing NFL player data...")
                
                # Get players data
                players_df = get_nfl_players()
                
                # Insert players into database
                for _, player in players_df.iterrows():
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO players 
                                (name, team, position, initial_price, current_price, week_1_yards, week_1_tds, tier, total_worth, shares_outstanding)
                                VALUES (:name, :team, :position, :initial_price, :current_price, :week_1_yards, :week_1_tds, :tier, :total_worth, :shares_outstanding)
                            """),
                            {
                                "name": player["name"],
                                "team": player["team"],
                                "position": player["position"],
                                "initial_price": player["initial_price"],
                                "current_price": player["current_price"],
                                "week_1_yards": player["week_1_yards"],
                                "week_1_tds": player["week_1_tds"],
                                "tier": player["tier"] if "tier" in player else "Standard",
                                "total_worth": player["total_worth"] if "total_worth" in player else player["current_price"] * 1000,
                                "shares_outstanding": player["shares_outstanding"] if "shares_outstanding" in player else 1000
                            }
                        )
                    except Exception as e:
                        print(f"Error inserting player {player['name']}: {e}")
                
                # Get funds data
                funds_df = get_team_funds()
                
                # Insert funds into database
                for _, fund in funds_df.iterrows():
                    try:
                        conn.execute(
                            text("""
                                INSERT INTO team_funds 
                                (name, players_included, price, type)
                                VALUES (:name, :players_included, :price, :type)
                            """),
                            {
                                "name": fund["name"],
                                "players_included": fund["players_included"],
                                "price": fund["price"],
                                "type": fund["type"] if "type" in fund else "Standard"
                            }
                        )
                    except Exception as e:
                        print(f"Error inserting fund {fund['name']}: {e}")
                
                conn.commit()
        
        # Now load data from tables
        players = pd.read_sql("SELECT * FROM players", engine)
        funds = pd.read_sql("SELECT * FROM team_funds", engine)
        users = pd.read_sql("SELECT * FROM users", engine)
        holdings = pd.read_sql("SELECT * FROM holdings", engine)
        
        # Create empty dataframes with proper columns if tables are empty
        if players.empty:
            players = pd.DataFrame(columns=['id', 'name', 'team', 'position', 'initial_price', 'current_price', 'week_1_yards', 'week_1_tds', 'tier'])
        
        if funds.empty:
            funds = pd.DataFrame(columns=['id', 'name', 'players_included', 'price', 'type'])
        
        if users.empty:
            users = pd.DataFrame(columns=['id', 'username', 'wallet_balance'])
        
        if holdings.empty:
            holdings = pd.DataFrame(columns=['id', 'user_id', 'asset_type', 'asset_name', 'quantity', 'purchase_price'])
        
        # Rename columns to match original CSV format for compatibility
        players = players.rename(columns={
            'name': 'Player Name', 
            'team': 'Team', 
            'position': 'Position',
            'initial_price': 'Initial Price',
            'week_1_yards': 'Week 1 Yards',
            'week_1_tds': 'Week 1 TDs',
            'current_price': 'Current Price',
            'tier': 'Tier'
        })
        
        funds = funds.rename(columns={
            'name': 'Fund Name',
            'players_included': 'Players Included',
            'price': 'Fund Price',
            'type': 'Type'
        })
        
        users = users.rename(columns={
            'id': 'User ID',
            'username': 'Username',
            'wallet_balance': 'Wallet Balance'
        })
        
        holdings = holdings.rename(columns={
            'user_id': 'User ID',
            'asset_type': 'asset_type',
            'asset_name': 'Asset Name',
            'quantity': 'Quantity'
        })
        
        return players, funds, users, holdings
    
    except Exception as e:
        print(f"Error loading data: {e}")
        # Return empty DataFrames with proper columns to avoid errors
        players = pd.DataFrame(columns=['Player Name', 'Team', 'Position', 'Initial Price', 'Current Price', 'Week 1 Yards', 'Week 1 TDs', 'Tier'])
        funds = pd.DataFrame(columns=['Fund Name', 'Players Included', 'Fund Price', 'Type'])
        users = pd.DataFrame({'User ID': ['user_001'], 'Username': ['DefaultUser'], 'Wallet Balance': [10000.0]})
        holdings = pd.DataFrame(columns=['User ID', 'asset_type', 'Asset Name', 'Quantity'])
        return players, funds, users, holdings

def save_data(players, funds, users, holdings):
    """
    This function is maintained for compatibility with existing code
    but doesn't need to do anything as we're now using direct database operations
    """
    # Data is saved directly in the database through execute_transaction
    pass

def execute_transaction(user_id, asset_type, asset_name, transaction_type, price, users, holdings):
    """
    Execute a buy or sell transaction and update the database
    
    Parameters:
    - user_id: ID of the user
    - asset_type: Type of asset (Player or Team Fund)
    - asset_name: Name of the asset
    - transaction_type: buy or sell
    - price: Current price of the asset
    - users: Users dataframe (kept for compatibility)
    - holdings: Holdings dataframe (kept for compatibility)
    
    Returns:
    - success: Boolean indicating if transaction was successful
    - message: Message about the transaction
    - users: Updated users dataframe
    - holdings: Updated holdings dataframe
    """
    # Convert price to Python float to avoid NumPy type issues
    price = float(price)
    # Create the demo user if it doesn't exist (for demo login)
    if user_id == "demo_user_001":
        try:
            with engine.connect() as conn:
                # Check if demo user exists
                check_query = text("SELECT id FROM users WHERE id = :user_id")
                existing_user = conn.execute(check_query, {"user_id": user_id}).fetchone()
                
                if not existing_user:
                    # Create demo user with initial balance of 300.00
                    create_demo = text("""
                        INSERT INTO users (id, username, email, password, wallet_balance)
                        VALUES (:id, :username, :email, :password, :wallet_balance)
                    """)
                    conn.execute(create_demo, {
                        "id": user_id,
                        "username": "Demo User",
                        "email": "demo@example.com",
                        "password": "demo_password",
                        "wallet_balance": 300.00
                    })
                    conn.commit()
        except Exception as e:
            print(f"Error ensuring demo user exists: {str(e)}")
            # Continue anyway
    
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            try:
                # Get user's wallet balance
                user_query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                result = conn.execute(user_query, {"user_id": user_id}).fetchone()
                
                if not result:
                    return False, f"User {user_id} not found", users, holdings
                
                wallet_balance = float(result[0])
                
                if transaction_type == "buy":
                    # Check if user has enough funds
                    if wallet_balance < price:
                        return False, f"Insufficient funds. Need ${price:.2f}, but you have ${wallet_balance:.2f}", users, holdings
                    
                    # Deduct funds from wallet
                    update_wallet_query = text("""
                        UPDATE users 
                        SET wallet_balance = wallet_balance - :price 
                        WHERE id = :user_id
                    """)
                    conn.execute(update_wallet_query, {"price": price, "user_id": user_id})
                    
                    # Update holdings - check if user already has this asset
                    holding_query = text("""
                        SELECT id, quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    existing_holding = conn.execute(holding_query, {
                        "user_id": user_id, 
                        "asset_type": asset_type, 
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if existing_holding:
                        # Update existing holding
                        update_holdings_query = text("""
                            UPDATE holdings 
                            SET quantity = quantity + 1 
                            WHERE id = :holding_id
                        """)
                        conn.execute(update_holdings_query, {"holding_id": existing_holding[0]})
                    else:
                        # Create new holding
                        new_holding_query = text("""
                            INSERT INTO holdings (user_id, asset_type, asset_name, quantity) 
                            VALUES (:user_id, :asset_type, :asset_name, 1)
                        """)
                        conn.execute(new_holding_query, {
                            "user_id": user_id, 
                            "asset_type": asset_type, 
                            "asset_name": asset_name
                        })
                    
                    # Record transaction
                    transaction_query = text("""
                        INSERT INTO transactions 
                        (user_id, timestamp, transaction_type, asset_type, asset_name, price, quantity, purchase_price, profit_loss) 
                        VALUES (:user_id, :timestamp, :transaction_type, :asset_type, :asset_name, :price, 1, :purchase_price, 0)
                    """)
                    conn.execute(transaction_query, {
                        "user_id": user_id,
                        "timestamp": datetime.now(),
                        "transaction_type": "Buy",
                        "asset_type": asset_type,
                        "asset_name": asset_name,
                        "price": price,
                        "purchase_price": price,  # For buys, purchase price is the current price
                        "profit_loss": 0  # No profit/loss on initial purchase
                    })
                    
                    trans.commit()
                    
                    # Reload the data to reflect changes
                    updated_users, _, _, updated_holdings = load_data()
                    return True, f"Successfully bought 1 share of {asset_name} for ${price:.2f}", updated_users, updated_holdings
                
                elif transaction_type == "sell":
                    # Check if user has the asset
                    holding_query = text("""
                        SELECT id, quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    existing_holding = conn.execute(holding_query, {
                        "user_id": user_id, 
                        "asset_type": asset_type, 
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if not existing_holding or existing_holding[1] < 1:
                        return False, f"You don't own any shares of {asset_name} to sell", users, holdings
                    
                    # Convert quantity to Python float to avoid NumPy type issues
                    quantity = float(existing_holding[1])
                    
                    # Add funds to wallet - ensure price is a Python float
                    sale_price = float(price)
                    update_wallet_query = text("""
                        UPDATE users 
                        SET wallet_balance = wallet_balance + :price 
                        WHERE id = :user_id
                    """)
                    conn.execute(update_wallet_query, {"price": sale_price, "user_id": user_id})
                    
                    # Update holdings
                    current_quantity = existing_holding[1]
                    
                    if current_quantity == 1:
                        # Remove the holding completely
                        delete_holding_query = text("DELETE FROM holdings WHERE id = :holding_id")
                        conn.execute(delete_holding_query, {"holding_id": existing_holding[0]})
                    else:
                        # Reduce quantity
                        update_holdings_query = text("""
                            UPDATE holdings 
                            SET quantity = quantity - 1 
                            WHERE id = :holding_id
                        """)
                        conn.execute(update_holdings_query, {"holding_id": existing_holding[0]})
                    
                    # Find the purchase price for profit/loss calculation
                    # This is a simplified approach - in a real system we would use FIFO/LIFO accounting
                    purchase_query = text("""
                        SELECT AVG(price) as avg_price
                        FROM transactions
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type 
                          AND asset_name = :asset_name
                          AND transaction_type = 'Buy'
                    """)
                    purchase_result = conn.execute(purchase_query, {
                        "user_id": user_id, 
                        "asset_type": asset_type, 
                        "asset_name": asset_name
                    }).fetchone()
                    
                    avg_purchase_price = float(purchase_result[0]) if purchase_result and purchase_result[0] else float(price)
                    profit_loss = float(price) - avg_purchase_price
                    
                    # Record transaction
                    transaction_query = text("""
                        INSERT INTO transactions 
                        (user_id, timestamp, transaction_type, asset_type, asset_name, price, quantity, purchase_price, profit_loss) 
                        VALUES (:user_id, :timestamp, :transaction_type, :asset_type, :asset_name, :price, 1, :purchase_price, :profit_loss)
                    """)
                    conn.execute(transaction_query, {
                        "user_id": user_id,
                        "timestamp": datetime.now(),
                        "transaction_type": "Sell",
                        "asset_type": asset_type,
                        "asset_name": asset_name,
                        "price": price,
                        "purchase_price": avg_purchase_price,
                        "profit_loss": profit_loss
                    })
                    
                    trans.commit()
                    
                    # Reload the data to reflect changes
                    updated_users, _, _, updated_holdings = load_data()
                    return True, f"Successfully sold 1 share of {asset_name} for ${price:.2f}", updated_users, updated_holdings
                
                return False, "Invalid transaction type", users, holdings
                
            except SQLAlchemyError as e:
                trans.rollback()
                print(f"Transaction error: {e}")
                return False, f"Database error occurred: {str(e)}", users, holdings
    
    except SQLAlchemyError as e:
        print(f"Database connection error: {e}")
        return False, f"Database connection error: {str(e)}", users, holdings

def get_transaction_history(user_id):
    """
    Get transaction history for a user
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - transactions: DataFrame containing transaction history
    """
    try:
        query = text("""
            SELECT 
                timestamp, 
                transaction_type, 
                asset_type,
                asset_name, 
                price, 
                quantity, 
                purchase_price, 
                profit_loss
            FROM transactions
            WHERE user_id = :user_id
            ORDER BY timestamp DESC
        """)
        
        with engine.connect() as conn:
            transactions_df = pd.read_sql_query(
                query, 
                conn, 
                params={"user_id": user_id}
            )
            
        # Rename columns to match expected format in app.py
        transactions_df = transactions_df.rename(columns={
            'transaction_type': 'type',
            'asset_name': 'asset'
        })
        
        # Convert timestamp to string in expected format
        transactions_df['timestamp'] = transactions_df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Format profit/loss values if the column exists and has data
        if 'profit_loss' in transactions_df.columns:
            # Fill NaN or None values with 0
            transactions_df['profit_loss'] = transactions_df['profit_loss'].fillna(0)
            
            # Now format the values
            transactions_df['profit_loss'] = transactions_df.apply(
                lambda row: f"+${row['profit_loss']:.2f}" if row['profit_loss'] > 0 else 
                           (f"-${abs(row['profit_loss']):.2f}" if row['profit_loss'] < 0 else "$0.00"),
                axis=1
            )
        
        # Add calculated columns for better analysis
        transactions_df['value'] = transactions_df['price'] * transactions_df['quantity']
        
        return transactions_df
    
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        return pd.DataFrame()
        
def get_performance_summary(user_id):
    """
    Get comprehensive performance summary for a user
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - summary: Dictionary containing performance metrics
    """
    try:
        # Basic transaction metrics
        base_query = text("""
            SELECT 
                SUM(CASE WHEN transaction_type = 'Buy' THEN price * quantity ELSE 0 END) as total_invested,
                SUM(CASE WHEN transaction_type = 'Sell' THEN price * quantity ELSE 0 END) as total_sold,
                SUM(CASE WHEN transaction_type = 'Sell' THEN profit_loss ELSE 0 END) as total_profit_loss,
                COUNT(CASE WHEN transaction_type = 'Buy' THEN 1 ELSE NULL END) as buy_count,
                COUNT(CASE WHEN transaction_type = 'Sell' THEN 1 ELSE NULL END) as sell_count,
                MAX(timestamp) as last_transaction_date
            FROM transactions
            WHERE user_id = :user_id
        """)
        
        # Time-based performance metrics
        time_query = text("""
            SELECT 
                SUM(CASE WHEN transaction_type = 'Sell' AND timestamp >= current_date - interval '7 days' 
                    THEN profit_loss ELSE 0 END) as weekly_profit_loss,
                SUM(CASE WHEN transaction_type = 'Sell' AND timestamp >= current_date - interval '30 days' 
                    THEN profit_loss ELSE 0 END) as monthly_profit_loss,
                COUNT(CASE WHEN transaction_type IN ('Buy', 'Sell') AND timestamp >= current_date - interval '7 days' 
                    THEN 1 ELSE NULL END) as weekly_transaction_count,
                COUNT(CASE WHEN transaction_type IN ('Buy', 'Sell') AND timestamp >= current_date - interval '30 days' 
                    THEN 1 ELSE NULL END) as monthly_transaction_count
            FROM transactions
            WHERE user_id = :user_id
        """)
        
        # Asset type breakdown
        asset_query = text("""
            SELECT 
                asset_type, 
                SUM(CASE WHEN transaction_type = 'Sell' THEN profit_loss ELSE 0 END) as type_profit_loss,
                COUNT(CASE WHEN transaction_type IN ('Buy', 'Sell') THEN 1 ELSE NULL END) as type_transaction_count
            FROM transactions
            WHERE user_id = :user_id
            GROUP BY asset_type
        """)
        
        # Current portfolio value (using a subquery with the most recent prices)
        portfolio_query = text("""
            WITH current_prices AS (
                SELECT 
                    h.asset_name, 
                    h.asset_type, 
                    h.quantity,
                    CASE 
                        WHEN h.asset_type = 'Player' THEN p.current_price
                        WHEN h.asset_type = 'Team Fund' THEN tf.price
                        ELSE 0
                    END as current_price
                FROM holdings h
                LEFT JOIN players p ON h.asset_name = p.name AND h.asset_type = 'Player'
                LEFT JOIN team_funds tf ON h.asset_name = tf.name AND h.asset_type = 'Team Fund'
                WHERE h.user_id = :user_id
            )
            SELECT 
                SUM(quantity * current_price) as current_portfolio_value,
                COUNT(*) as distinct_assets_count,
                MAX(current_price) as highest_asset_price,
                MIN(CASE WHEN current_price > 0 THEN current_price ELSE NULL END) as lowest_asset_price
            FROM current_prices
        """)
        
        # Initialize summary dictionary with default values
        summary = {
            'total_invested': 0,
            'total_sold': 0,
            'total_profit_loss': 0,
            'buy_count': 0,
            'sell_count': 0,
            'weekly_profit_loss': 0,
            'monthly_profit_loss': 0,
            'weekly_transaction_count': 0,
            'monthly_transaction_count': 0,
            'current_portfolio_value': 0,
            'distinct_assets_count': 0,
            'highest_asset_price': 0,
            'lowest_asset_price': 0,
            'asset_type_breakdown': {},
            'last_transaction_date': None
        }
        
        with engine.connect() as conn:
            # Execute base query
            base_result = conn.execute(base_query, {"user_id": user_id}).fetchone()
            if base_result:
                summary.update({
                    'total_invested': float(base_result[0]) if base_result[0] else 0,
                    'total_sold': float(base_result[1]) if base_result[1] else 0,
                    'total_profit_loss': float(base_result[2]) if base_result[2] else 0,
                    'buy_count': int(base_result[3]) if base_result[3] else 0,
                    'sell_count': int(base_result[4]) if base_result[4] else 0,
                    'last_transaction_date': base_result[5]
                })
            
            # Execute time-based query
            time_result = conn.execute(time_query, {"user_id": user_id}).fetchone()
            if time_result:
                summary.update({
                    'weekly_profit_loss': float(time_result[0]) if time_result[0] else 0,
                    'monthly_profit_loss': float(time_result[1]) if time_result[1] else 0,
                    'weekly_transaction_count': int(time_result[2]) if time_result[2] else 0,
                    'monthly_transaction_count': int(time_result[3]) if time_result[3] else 0
                })
            
            # Execute asset type breakdown query
            asset_results = conn.execute(asset_query, {"user_id": user_id}).fetchall()
            for result in asset_results:
                asset_type = result[0]
                profit_loss = float(result[1]) if result[1] else 0
                transaction_count = int(result[2]) if result[2] else 0
                
                summary['asset_type_breakdown'][asset_type] = {
                    'profit_loss': profit_loss,
                    'transaction_count': transaction_count
                }
            
            # Execute portfolio value query
            portfolio_result = conn.execute(portfolio_query, {"user_id": user_id}).fetchone()
            if portfolio_result:
                summary.update({
                    'current_portfolio_value': float(portfolio_result[0]) if portfolio_result[0] else 0,
                    'distinct_assets_count': int(portfolio_result[1]) if portfolio_result[1] else 0,
                    'highest_asset_price': float(portfolio_result[2]) if portfolio_result[2] else 0,
                    'lowest_asset_price': float(portfolio_result[3]) if portfolio_result[3] else 0
                })
            
        return summary
    
    except SQLAlchemyError as e:
        print(f"Database error: {e}")
        # Return default values if there's an error
        return {
            'total_invested': 0,
            'total_sold': 0,
            'total_profit_loss': 0,
            'buy_count': 0,
            'sell_count': 0,
            'weekly_profit_loss': 0,
            'monthly_profit_loss': 0,
            'weekly_transaction_count': 0,
            'monthly_transaction_count': 0,
            'current_portfolio_value': 0,
            'distinct_assets_count': 0,
            'highest_asset_price': 0,
            'lowest_asset_price': 0,
            'asset_type_breakdown': {},
            'last_transaction_date': None
        }
        
# Friend system functions
def get_friend_list(user_id):
    """
    Get list of friends for a user
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - friends: List of friends with their status
    """
    try:
        with engine.connect() as conn:
            # Get all confirmed friendships where user is either the sender or receiver
            query = text("""
                -- Get friendships where user is the requester
                SELECT f.id, f.friend_id as other_user_id, u.username as friend_name, f.status, f.created_at
                FROM friendships f
                JOIN users u ON f.friend_id = u.id
                WHERE f.user_id = :user_id

                UNION

                -- Get friendships where user is the receiver
                SELECT f.id, f.user_id as other_user_id, u.username as friend_name, f.status, f.created_at
                FROM friendships f
                JOIN users u ON f.user_id = u.id
                WHERE f.friend_id = :user_id AND f.status = 'accepted'
                
                ORDER BY created_at DESC
            """)
            
            result = conn.execute(query, {"user_id": user_id}).fetchall()
            
            friends = []
            for row in result:
                friends.append({
                    "id": row[0],
                    "user_id": row[1],
                    "username": row[2],
                    "status": row[3],
                    "created_at": row[4]
                })
                
            return friends
    
    except Exception as e:
        print(f"Error getting friend list: {e}")
        return []

def send_friend_request(user_id, friend_username):
    """
    Send a friend request to another user
    
    Parameters:
    - user_id: ID of the user sending the request
    - friend_username: Username of the user to send request to
    
    Returns:
    - success: Boolean indicating if request was successful
    - message: Message about the request
    """
    try:
        with engine.connect() as conn:
            # First, get the friend's user_id
            query = text("SELECT id FROM users WHERE username = :username")
            result = conn.execute(query, {"username": friend_username}).fetchone()
            
            if not result:
                return False, f"User '{friend_username}' not found"
            
            friend_id = result[0]
            
            # Check if trying to add self
            if user_id == friend_id:
                return False, "You cannot add yourself as a friend"
            
            # Check if friendship already exists
            query = text("""
                SELECT id, status FROM friendships 
                WHERE (user_id = :user_id AND friend_id = :friend_id)
                OR (user_id = :friend_id AND friend_id = :user_id)
            """)
            result = conn.execute(query, {"user_id": user_id, "friend_id": friend_id}).fetchone()
            
            if result:
                friendship_id, status = result
                if status == 'accepted':
                    return False, f"You are already friends with {friend_username}"
                elif status == 'pending':
                    return False, f"Friend request to {friend_username} is already pending"
                elif status == 'rejected':
                    # If previously rejected, allow resending
                    conn.execute(
                        text("UPDATE friendships SET status = 'pending', created_at = CURRENT_TIMESTAMP WHERE id = :id"),
                        {"id": friendship_id}
                    )
                    conn.commit()
                    return True, f"Friend request sent to {friend_username}"
            
            # Create new friendship
            conn.execute(
                text("""
                    INSERT INTO friendships (user_id, friend_id, status, created_at)
                    VALUES (:user_id, :friend_id, 'pending', CURRENT_TIMESTAMP)
                """),
                {"user_id": user_id, "friend_id": friend_id}
            )
            conn.commit()
            
            return True, f"Friend request sent to {friend_username}"
    
    except Exception as e:
        print(f"Error sending friend request: {e}")
        return False, "Error sending friend request"

def respond_to_friend_request(request_id, user_id, action):
    """
    Respond to a friend request
    
    Parameters:
    - request_id: ID of the friendship request
    - user_id: ID of the user responding to the request
    - action: 'accept' or 'reject'
    
    Returns:
    - success: Boolean indicating if response was successful
    - message: Message about the response
    """
    try:
        with engine.connect() as conn:
            # Verify that this request is for this user
            query = text("""
                SELECT user_id, friend_id FROM friendships
                WHERE id = :request_id AND friend_id = :user_id AND status = 'pending'
            """)
            result = conn.execute(query, {"request_id": request_id, "user_id": user_id}).fetchone()
            
            if not result:
                return False, "Friend request not found or already processed"
            
            sender_id = result[0]
            
            # Get sender's username
            query = text("SELECT username FROM users WHERE id = :user_id")
            result = conn.execute(query, {"user_id": sender_id}).fetchone()
            sender_name = result[0] if result else "User"
            
            # Update friendship status
            status = 'accepted' if action == 'accept' else 'rejected'
            conn.execute(
                text("UPDATE friendships SET status = :status WHERE id = :request_id"),
                {"status": status, "request_id": request_id}
            )
            conn.commit()
            
            if action == 'accept':
                return True, f"You are now friends with {sender_name}"
            else:
                return True, f"Friend request from {sender_name} rejected"
    
    except Exception as e:
        print(f"Error responding to friend request: {e}")
        return False, "Error processing friend request"

# Competition system functions
def create_competition(user_id, name, description, start_date, end_date):
    """
    Create a new competition
    
    Parameters:
    - user_id: ID of the user creating the competition
    - name: Name of the competition
    - description: Description of the competition
    - start_date: Start date of the competition
    - end_date: End date of the competition
    
    Returns:
    - success: Boolean indicating if creation was successful
    - message: Message about the creation
    - competition_id: ID of the created competition
    """
    try:
        with engine.connect() as conn:
            # Create the competition
            query = text("""
                INSERT INTO competitions (name, description, start_date, end_date, created_by, created_at)
                VALUES (:name, :description, :start_date, :end_date, :user_id, CURRENT_TIMESTAMP)
                RETURNING id
            """)
            
            result = conn.execute(query, {
                "name": name,
                "description": description,
                "start_date": start_date,
                "end_date": end_date,
                "user_id": user_id
            }).fetchone()
            
            if not result:
                return False, "Error creating competition", None
                
            competition_id = result[0]
            
            # Add creator as a member
            query = text("""
                INSERT INTO competition_members (competition_id, user_id, joined_at)
                VALUES (:competition_id, :user_id, CURRENT_TIMESTAMP)
            """)
            
            conn.execute(query, {
                "competition_id": competition_id,
                "user_id": user_id
            })
            
            conn.commit()
            
            return True, f"Competition '{name}' created successfully", competition_id
    
    except Exception as e:
        print(f"Error creating competition: {e}")
        return False, "Error creating competition", None

def join_competition(user_id, competition_id):
    """
    Join an existing competition
    
    Parameters:
    - user_id: ID of the user joining the competition
    - competition_id: ID of the competition to join
    
    Returns:
    - success: Boolean indicating if joining was successful
    - message: Message about the joining
    """
    try:
        with engine.connect() as conn:
            # Check if competition exists and is active
            query = text("""
                SELECT name, status FROM competitions
                WHERE id = :competition_id
            """)
            result = conn.execute(query, {"competition_id": competition_id}).fetchone()
            
            if not result:
                return False, "Competition not found"
                
            competition_name, status = result
            
            if status != 'active':
                return False, f"Competition '{competition_name}' is no longer active"
            
            # Check if user is already a member
            query = text("""
                SELECT id FROM competition_members
                WHERE competition_id = :competition_id AND user_id = :user_id
            """)
            result = conn.execute(query, {"competition_id": competition_id, "user_id": user_id}).fetchone()
            
            if result:
                return False, f"You are already a member of '{competition_name}'"
            
            # Add user as a member
            query = text("""
                INSERT INTO competition_members (competition_id, user_id, joined_at)
                VALUES (:competition_id, :user_id, CURRENT_TIMESTAMP)
            """)
            
            conn.execute(query, {
                "competition_id": competition_id,
                "user_id": user_id
            })
            
            conn.commit()
            
            return True, f"You have joined the '{competition_name}' competition"
    
    except Exception as e:
        print(f"Error joining competition: {e}")
        return False, "Error joining competition"

def get_available_competitions(user_id):
    """
    Get list of available competitions for a user
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - competitions: List of available competitions
    """
    try:
        with engine.connect() as conn:
            # Get competitions that are active and user is not already a member
            query = text("""
                SELECT c.id, c.name, c.description, c.start_date, c.end_date, c.created_by, u.username as creator_name,
                       COUNT(cm.user_id) as member_count
                FROM competitions c
                LEFT JOIN users u ON c.created_by = u.id
                LEFT JOIN competition_members cm ON c.id = cm.competition_id
                WHERE c.status = 'active'
                AND c.id NOT IN (
                    SELECT competition_id FROM competition_members WHERE user_id = :user_id
                )
                GROUP BY c.id, c.name, c.description, c.start_date, c.end_date, c.created_by, u.username
                ORDER BY c.created_at DESC
            """)
            
            result = conn.execute(query, {"user_id": user_id}).fetchall()
            
            competitions = []
            for row in result:
                competitions.append({
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "start_date": row[3],
                    "end_date": row[4],
                    "created_by": row[5],
                    "creator_name": row[6],
                    "member_count": row[7]
                })
                
            return competitions
    
    except Exception as e:
        print(f"Error getting available competitions: {e}")
        return []

def get_my_competitions(user_id):
    """
    Get list of competitions a user is participating in
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - competitions: List of user's competitions
    """
    try:
        with engine.connect() as conn:
            # Get competitions user is a member of
            query = text("""
                SELECT c.id, c.name, c.description, c.start_date, c.end_date, c.created_by, 
                       u.username as creator_name, cm.score, cm.rank,
                       COUNT(cm2.user_id) as member_count
                FROM competitions c
                JOIN competition_members cm ON c.id = cm.competition_id AND cm.user_id = :user_id
                LEFT JOIN users u ON c.created_by = u.id
                LEFT JOIN competition_members cm2 ON c.id = cm2.competition_id
                GROUP BY c.id, c.name, c.description, c.start_date, c.end_date, c.created_by, 
                         u.username, cm.score, cm.rank
                ORDER BY c.start_date DESC
            """)
            
            result = conn.execute(query, {"user_id": user_id}).fetchall()
            
            competitions = []
            for row in result:
                competitions.append({
                    "id": row[0],
                    "name": row[1],
                    "description": row[2],
                    "start_date": row[3],
                    "end_date": row[4],
                    "created_by": row[5],
                    "creator_name": row[6],
                    "score": row[7],
                    "rank": row[8],
                    "member_count": row[9]
                })
                
            return competitions
    
    except Exception as e:
        print(f"Error getting user competitions: {e}")
        return []

# Fantasy team functions
def create_fantasy_team(user_id, name, competition_id=None):
    """
    Create a new fantasy team
    
    Parameters:
    - user_id: ID of the user creating the team
    - name: Name of the team
    - competition_id: ID of the competition (optional)
    
    Returns:
    - success: Boolean indicating if creation was successful
    - message: Message about the creation
    - team_id: ID of the created team
    """
    try:
        with engine.connect() as conn:
            # Create the team
            query = text("""
                INSERT INTO fantasy_teams (user_id, name, competition_id, created_at)
                VALUES (:user_id, :name, :competition_id, CURRENT_TIMESTAMP)
                RETURNING id
            """)
            
            result = conn.execute(query, {
                "user_id": user_id,
                "name": name,
                "competition_id": competition_id
            }).fetchone()
            
            if not result:
                return False, "Error creating fantasy team", None
                
            team_id = result[0]
            conn.commit()
            
            return True, f"Fantasy team '{name}' created successfully", team_id
    
    except Exception as e:
        print(f"Error creating fantasy team: {e}")
        return False, "Error creating fantasy team", None

def add_player_to_fantasy_team(team_id, player_name):
    """
    Add a player to a fantasy team
    
    Parameters:
    - team_id: ID of the fantasy team
    - player_name: Name of the player to add
    
    Returns:
    - success: Boolean indicating if adding was successful
    - message: Message about the addition
    """
    try:
        with engine.connect() as conn:
            # Check if player exists
            query = text("SELECT id FROM players WHERE name = :player_name")
            result = conn.execute(query, {"player_name": player_name}).fetchone()
            
            if not result:
                return False, f"Player '{player_name}' not found"
            
            # Check if player is already on the team
            query = text("""
                SELECT id FROM fantasy_team_players
                WHERE team_id = :team_id AND player_name = :player_name
            """)
            result = conn.execute(query, {"team_id": team_id, "player_name": player_name}).fetchone()
            
            if result:
                return False, f"'{player_name}' is already on your team"
            
            # Add player to the team
            query = text("""
                INSERT INTO fantasy_team_players (team_id, player_name)
                VALUES (:team_id, :player_name)
            """)
            
            conn.execute(query, {
                "team_id": team_id,
                "player_name": player_name
            })
            
            conn.commit()
            
            return True, f"Added '{player_name}' to your fantasy team"
    
    except Exception as e:
        print(f"Error adding player to fantasy team: {e}")
        return False, "Error adding player to team"

# Enhanced peer trading functions
def create_trade_offer(sender_id, recipient_id, sender_assets, recipient_assets, description=None):
    """
    Create a new trade offer between users
    
    Parameters:
    - sender_id: ID of the user sending the offer
    - recipient_id: ID of the user receiving the offer (can be None for open offers)
    - sender_assets: List of assets the sender is offering. Each asset is a dict with keys: asset_type, asset_name, quantity
    - recipient_assets: List of assets the sender is requesting. Each asset is a dict with keys: asset_type, asset_name, quantity
    - description: Optional message or description for the trade offer
    
    Returns:
    - success: Boolean indicating if creation was successful
    - message: Message about the creation
    - offer_id: ID of the created offer
    """
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # Create the trade offer
                query = text("""
                    INSERT INTO trade_offers (sender_id, recipient_id, status, created_at, updated_at)
                    VALUES (:sender_id, :recipient_id, 'pending', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                    RETURNING id
                """)
                
                result = conn.execute(query, {
                    "sender_id": sender_id,
                    "recipient_id": recipient_id
                }).fetchone()
                
                if not result:
                    raise Exception("Error creating trade offer")
                    
                offer_id = result[0]
                
                # Add sender assets to the offer
                for asset in sender_assets:
                    # Verify sender owns this asset
                    check_query = text("""
                        SELECT id, quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    asset_holding = conn.execute(check_query, {
                        "user_id": sender_id, 
                        "asset_type": asset["asset_type"], 
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if not asset_holding or asset_holding[1] < asset["quantity"]:
                        trans.rollback()
                        return False, f"You don't own enough shares of {asset['asset_name']} to offer", None
                    
                    # Add asset to offer
                    asset_query = text("""
                        INSERT INTO trade_offer_assets (trade_offer_id, user_id, asset_type, asset_name, quantity)
                        VALUES (:offer_id, :user_id, :asset_type, :asset_name, :quantity)
                    """)
                    
                    conn.execute(asset_query, {
                        "offer_id": offer_id,
                        "user_id": sender_id,
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"],
                        "quantity": asset["quantity"]
                    })
                
                # Add recipient assets to the offer (the assets requested)
                for asset in recipient_assets:
                    # Verify recipient owns this asset (if they don't, they'll see this when reviewing the offer)
                    check_query = text("""
                        SELECT id, quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    asset_holding = conn.execute(check_query, {
                        "user_id": recipient_id, 
                        "asset_type": asset["asset_type"], 
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    # Add asset to offer
                    asset_query = text("""
                        INSERT INTO trade_offer_assets (trade_offer_id, user_id, asset_type, asset_name, quantity)
                        VALUES (:offer_id, :user_id, :asset_type, :asset_name, :quantity)
                    """)
                    
                    conn.execute(asset_query, {
                        "offer_id": offer_id,
                        "user_id": recipient_id,
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"],
                        "quantity": asset["quantity"]
                    })
                
                trans.commit()
                
                # Get recipient username
                query = text("SELECT username FROM users WHERE id = :user_id")
                result = conn.execute(query, {"user_id": recipient_id}).fetchone()
                recipient_name = result[0] if result else "User"
                
                return True, f"Trade offer sent to {recipient_name}", offer_id
                
            except Exception as e:
                trans.rollback()
                print(f"Error in create_trade_offer transaction: {e}")
                return False, "Error creating trade offer", None
    
    except Exception as e:
        print(f"Error creating trade offer: {e}")
        return False, "Error creating trade offer", None

def respond_to_trade_offer(offer_id, user_id, action):
    """
    Respond to a trade offer
    
    Parameters:
    - offer_id: ID of the trade offer
    - user_id: ID of the user responding to the offer
    - action: 'accept' or 'reject'
    
    Returns:
    - success: Boolean indicating if response was successful
    - message: Message about the response
    """
    try:
        with engine.connect() as conn:
            # Verify that this offer is for this user
            query = text("""
                SELECT sender_id, recipient_id, status FROM trade_offers
                WHERE id = :offer_id AND recipient_id = :user_id
            """)
            result = conn.execute(query, {"offer_id": offer_id, "user_id": user_id}).fetchone()
            
            if not result:
                return False, "Trade offer not found or not for you"
                
            sender_id, recipient_id, status = result
            
            if status != 'pending':
                return False, f"This trade offer has already been {status}"
            
            # If rejecting, just update status
            if action == 'reject':
                conn.execute(
                    text("""
                        UPDATE trade_offers 
                        SET status = 'rejected', updated_at = CURRENT_TIMESTAMP 
                        WHERE id = :offer_id
                    """),
                    {"offer_id": offer_id}
                )
                conn.commit()
                
                # Get sender username
                query = text("SELECT username FROM users WHERE id = :user_id")
                result = conn.execute(query, {"user_id": sender_id}).fetchone()
                sender_name = result[0] if result else "User"
                
                return True, f"You have rejected the trade offer from {sender_name}"
            
            # If accepting, need to execute the trade - start a transaction
            trans = conn.begin()
            
            try:
                # Get the assets being traded
                query = text("""
                    SELECT user_id, asset_type, asset_name, quantity 
                    FROM trade_offer_assets
                    WHERE trade_offer_id = :offer_id
                """)
                assets = conn.execute(query, {"offer_id": offer_id}).fetchall()
                
                # Group assets by user
                sender_assets = []
                recipient_assets = []
                
                for asset in assets:
                    user = asset[0]
                    asset_data = {
                        "user_id": user,
                        "asset_type": asset[1],
                        "asset_name": asset[2],
                        "quantity": asset[3]
                    }
                    
                    if user == sender_id:
                        sender_assets.append(asset_data)
                    else:
                        recipient_assets.append(asset_data)
                
                # Verify sender still has all offered assets
                for asset in sender_assets:
                    check_query = text("""
                        SELECT quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    result = conn.execute(check_query, {
                        "user_id": asset["user_id"],
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if not result or result[0] < asset["quantity"]:
                        trans.rollback()
                        return False, f"Sender no longer has {asset['quantity']} shares of {asset['asset_name']}"
                
                # Verify recipient has all requested assets
                for asset in recipient_assets:
                    check_query = text("""
                        SELECT quantity FROM holdings 
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    result = conn.execute(check_query, {
                        "user_id": asset["user_id"],
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if not result or result[0] < asset["quantity"]:
                        trans.rollback()
                        return False, f"You no longer have {asset['quantity']} shares of {asset['asset_name']}"
                
                # Execute the trade - transfer sender assets to recipient
                for asset in sender_assets:
                    # Remove from sender
                    update_query = text("""
                        UPDATE holdings
                        SET quantity = quantity - :quantity
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    conn.execute(update_query, {
                        "quantity": asset["quantity"],
                        "user_id": asset["user_id"],
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    })
                    
                    # Add to recipient - check if they already have this asset
                    check_query = text("""
                        SELECT id FROM holdings
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    result = conn.execute(check_query, {
                        "user_id": recipient_id,
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if result:
                        # Update existing holding
                        update_query = text("""
                            UPDATE holdings 
                            SET quantity = quantity + :quantity 
                            WHERE id = :holding_id
                        """)
                        conn.execute(update_query, {
                            "quantity": asset["quantity"],
                            "holding_id": result[0]
                        })
                    else:
                        # Create new holding
                        insert_query = text("""
                            INSERT INTO holdings (user_id, asset_type, asset_name, quantity) 
                            VALUES (:user_id, :asset_type, :asset_name, :quantity)
                        """)
                        conn.execute(insert_query, {
                            "user_id": recipient_id,
                            "asset_type": asset["asset_type"],
                            "asset_name": asset["asset_name"],
                            "quantity": asset["quantity"]
                        })
                
                # Transfer recipient assets to sender
                for asset in recipient_assets:
                    # Remove from recipient
                    update_query = text("""
                        UPDATE holdings
                        SET quantity = quantity - :quantity
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    conn.execute(update_query, {
                        "quantity": asset["quantity"],
                        "user_id": asset["user_id"],
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    })
                    
                    # Add to sender - check if they already have this asset
                    check_query = text("""
                        SELECT id FROM holdings
                        WHERE user_id = :user_id AND asset_type = :asset_type AND asset_name = :asset_name
                    """)
                    result = conn.execute(check_query, {
                        "user_id": sender_id,
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if result:
                        # Update existing holding
                        update_query = text("""
                            UPDATE holdings 
                            SET quantity = quantity + :quantity 
                            WHERE id = :holding_id
                        """)
                        conn.execute(update_query, {
                            "quantity": asset["quantity"],
                            "holding_id": result[0]
                        })
                    else:
                        # Create new holding
                        insert_query = text("""
                            INSERT INTO holdings (user_id, asset_type, asset_name, quantity) 
                            VALUES (:user_id, :asset_type, :asset_name, :quantity)
                        """)
                        conn.execute(insert_query, {
                            "user_id": sender_id,
                            "asset_type": asset["asset_type"],
                            "asset_name": asset["asset_name"],
                            "quantity": asset["quantity"]
                        })
                
                # Update trade offer status
                conn.execute(
                    text("""
                        UPDATE trade_offers 
                        SET status = 'accepted', updated_at = CURRENT_TIMESTAMP 
                        WHERE id = :offer_id
                    """),
                    {"offer_id": offer_id}
                )
                
                # Remove any holdings with quantity 0
                conn.execute(text("DELETE FROM holdings WHERE quantity <= 0"))
                
                trans.commit()
                
                # Get sender username
                query = text("SELECT username FROM users WHERE id = :user_id")
                result = conn.execute(query, {"user_id": sender_id}).fetchone()
                sender_name = result[0] if result else "User"
                
                return True, f"Trade with {sender_name} completed successfully"
                
            except Exception as e:
                trans.rollback()
                print(f"Error in respond_to_trade_offer transaction: {e}")
                return False, "Error executing trade"
    
    except Exception as e:
        print(f"Error responding to trade offer: {e}")
        return False, "Error responding to trade offer"

def get_pending_trade_offers(user_id):
    """
    Get list of pending trade offers for a user
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - offers: List of pending trade offers
    """
    try:
        with engine.connect() as conn:
            # Get all pending trade offers for this user
            query = text("""
                SELECT t.id, t.sender_id, t.recipient_id, t.created_at, u.username as sender_name
                FROM trade_offers t
                JOIN users u ON t.sender_id = u.id
                WHERE (t.recipient_id = :user_id OR t.sender_id = :user_id)
                AND t.status = 'pending'
                ORDER BY t.created_at DESC
            """)
            
            result = conn.execute(query, {"user_id": user_id}).fetchall()
            
            offers = []
            for row in result:
                offer_id = row[0]
                sender_id = row[1]
                recipient_id = row[2]
                created_at = row[3]
                sender_name = row[4]
                
                # Get the assets for this offer
                assets_query = text("""
                    SELECT a.user_id, a.asset_type, a.asset_name, a.quantity, u.username
                    FROM trade_offer_assets a
                    JOIN users u ON a.user_id = u.id
                    WHERE a.trade_offer_id = :offer_id
                """)
                
                assets_result = conn.execute(assets_query, {"offer_id": offer_id}).fetchall()
                
                # Organize assets by user
                sender_assets = []
                recipient_assets = []
                
                for asset in assets_result:
                    asset_user_id = asset[0]
                    asset_data = {
                        "asset_type": asset[1],
                        "asset_name": asset[2],
                        "quantity": asset[3],
                        "username": asset[4]
                    }
                    
                    if asset_user_id == sender_id:
                        sender_assets.append(asset_data)
                    else:
                        recipient_assets.append(asset_data)
                
                # Get recipient username
                if recipient_id == user_id:
                    direction = "incoming"
                    other_username = sender_name
                else:
                    direction = "outgoing"
                    recipient_query = text("SELECT username FROM users WHERE id = :user_id")
                    recipient_result = conn.execute(recipient_query, {"user_id": recipient_id}).fetchone()
                    other_username = recipient_result[0] if recipient_result else "Unknown User"
                
                offers.append({
                    "id": offer_id,
                    "sender_id": sender_id,
                    "recipient_id": recipient_id,
                    "sender_name": sender_name,
                    "created_at": created_at,
                    "direction": direction,
                    "other_username": other_username,
                    "sender_assets": sender_assets,
                    "recipient_assets": recipient_assets
                })
                
            return offers
    
    except Exception as e:
        print(f"Error getting pending trade offers: {e}")
        return []
        
        
# Player-for-Player Trading Functions
def create_player_trade_offer(sender_id, recipient_id, sender_assets, recipient_assets, description=None):
    """
    Create a new player-for-player trade offer
    
    Parameters:
    - sender_id: ID of the user creating the offer
    - recipient_id: ID of the recipient (can be None for open offers)
    - sender_assets: List of assets the sender is offering
    - recipient_assets: List of assets the sender is requesting
    - description: Optional trade description or message
    
    Returns:
    - success: Boolean indicating if creation was successful
    - message: Message about the creation
    - offer_id: ID of the created offer
    """
    try:
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            
            try:
                # Check if sender has the assets they're offering
                for asset in sender_assets:
                    check_query = text("""
                        SELECT quantity FROM holdings
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    result = conn.execute(check_query, {
                        "user_id": sender_id,
                        "asset_type": asset["asset_type"],
                        "asset_name": asset["asset_name"]
                    }).fetchone()
                    
                    if not result or result[0] < asset["quantity"]:
                        trans.rollback()
                        return False, f"You don't have enough shares of {asset['asset_name']}", None
                
                # Create the trade offer
                create_query = text("""
                    INSERT INTO trading_offers 
                    (creator_id, recipient_id, status, description)
                    VALUES (:creator_id, :recipient_id, 'pending', :description)
                    RETURNING id
                """)
                
                result = conn.execute(create_query, {
                    "creator_id": sender_id, 
                    "recipient_id": recipient_id,
                    "description": description
                }).fetchone()
                
                if not result:
                    trans.rollback()
                    return False, "Failed to create trade offer", None
                
                trade_id = result[0]
                
                # Add offered assets
                for asset in sender_assets:
                    asset_query = text("""
                        INSERT INTO trading_offer_assets
                        (trade_id, asset_name, asset_type, quantity, is_offered)
                        VALUES (:trade_id, :asset_name, :asset_type, :quantity, TRUE)
                    """)
                    
                    conn.execute(asset_query, {
                        "trade_id": trade_id,
                        "asset_name": asset["asset_name"],
                        "asset_type": asset["asset_type"],
                        "quantity": asset["quantity"]
                    })
                
                # Add requested assets
                for asset in recipient_assets:
                    asset_query = text("""
                        INSERT INTO trading_offer_assets
                        (trade_id, asset_name, asset_type, quantity, is_offered)
                        VALUES (:trade_id, :asset_name, :asset_type, :quantity, FALSE)
                    """)
                    
                    conn.execute(asset_query, {
                        "trade_id": trade_id,
                        "asset_name": asset["asset_name"],
                        "asset_type": asset["asset_type"],
                        "quantity": asset["quantity"]
                    })
                
                # Commit the transaction
                trans.commit()
                
                return True, "Trade offer created successfully", trade_id
                
            except Exception as e:
                trans.rollback()
                print(f"Error in create player-for-player trade offer: {e}")
                return False, f"Error creating trade offer: {str(e)}", None
                
    except Exception as e:
        print(f"Error creating player-for-player trade offer: {e}")
        return False, f"Error creating trade offer: {str(e)}", None

def respond_to_player_trade_offer(offer_id, user_id, action):
    """
    Respond to a player-for-player trade offer
    
    Parameters:
    - offer_id: ID of the trade offer
    - user_id: ID of the user responding
    - action: 'accept' or 'reject'
    
    Returns:
    - success: Boolean indicating if the action was successful
    - message: Message about the action
    """
    try:
        with engine.connect() as conn:
            # First check if the offer exists and is pending
            check_query = text("""
                SELECT creator_id, recipient_id, status 
                FROM trading_offers
                WHERE id = :offer_id
            """)
            
            trade = conn.execute(check_query, {"offer_id": offer_id}).fetchone()
            
            if not trade:
                return False, "Trade offer not found"
                
            creator_id, recipient_id, status = trade
            
            # If the offer has a specific recipient, verify it's for this user
            if recipient_id is not None and recipient_id != user_id and creator_id != user_id:
                return False, "This trade offer is not for you"
                
            if status != 'pending':
                return False, f"This trade offer has already been {status}"
            
            # If rejecting, just update the status
            if action == 'reject':
                update_query = text("""
                    UPDATE trading_offers
                    SET status = 'rejected', updated_at = CURRENT_TIMESTAMP
                    WHERE id = :offer_id
                """)
                
                conn.execute(update_query, {"offer_id": offer_id})
                conn.commit()
                
                return True, "Trade offer rejected"
            
            # For accepting, need to execute the trade
            trans = conn.begin()
            
            try:
                # Get the offered and requested assets
                offered_query = text("""
                    SELECT asset_name, asset_type, quantity
                    FROM trading_offer_assets
                    WHERE trade_id = :trade_id AND is_offered = TRUE
                """)
                
                requested_query = text("""
                    SELECT asset_name, asset_type, quantity
                    FROM trading_offer_assets
                    WHERE trade_id = :trade_id AND is_offered = FALSE
                """)
                
                offered_assets = conn.execute(offered_query, {"trade_id": offer_id}).fetchall()
                requested_assets = conn.execute(requested_query, {"trade_id": offer_id}).fetchall()
                
                # Verify creator still has offered assets
                for asset in offered_assets:
                    asset_name, asset_type, quantity = asset
                    
                    check_query = text("""
                        SELECT quantity FROM holdings
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    result = conn.execute(check_query, {
                        "user_id": creator_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if not result or result[0] < quantity:
                        trans.rollback()
                        return False, f"Creator no longer has enough shares of {asset_name}"
                
                # Verify acceptor has requested assets
                for asset in requested_assets:
                    asset_name, asset_type, quantity = asset
                    
                    check_query = text("""
                        SELECT quantity FROM holdings
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    result = conn.execute(check_query, {
                        "user_id": user_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if not result or result[0] < quantity:
                        trans.rollback()
                        return False, f"You don't have enough shares of {asset_name}"
                
                # All verifications passed, execute the trade
                
                # 1. Transfer offered assets from creator to acceptor
                for asset in offered_assets:
                    asset_name, asset_type, quantity = asset
                    
                    # Remove from creator
                    update_query = text("""
                        UPDATE holdings
                        SET quantity = quantity - :quantity
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    conn.execute(update_query, {
                        "quantity": quantity,
                        "user_id": creator_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    })
                    
                    # Add to acceptor (check if they already have this asset)
                    check_query = text("""
                        SELECT id FROM holdings
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    result = conn.execute(check_query, {
                        "user_id": user_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if result:
                        # Update existing holding
                        update_query = text("""
                            UPDATE holdings
                            SET quantity = quantity + :quantity
                            WHERE id = :holding_id
                        """)
                        
                        conn.execute(update_query, {
                            "quantity": quantity,
                            "holding_id": result[0]
                        })
                    else:
                        # Create new holding
                        insert_query = text("""
                            INSERT INTO holdings
                            (user_id, type, asset_name, quantity, purchase_price)
                            VALUES (:user_id, :asset_type, :asset_name, :quantity, 0)
                        """)
                        
                        conn.execute(insert_query, {
                            "user_id": user_id,
                            "asset_type": asset_type,
                            "asset_name": asset_name,
                            "quantity": quantity
                        })
                    
                    # Record the transaction
                    transaction_query = text("""
                        INSERT INTO transactions
                        (timestamp, user_id, transaction_type, asset_type, asset_name, 
                         price, quantity, value)
                        VALUES (CURRENT_TIMESTAMP, :user_id, 'Trade In', :asset_type, 
                                :asset_name, 0, :quantity, 0)
                    """)
                    
                    conn.execute(transaction_query, {
                        "user_id": user_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name,
                        "quantity": quantity
                    })
                
                # 2. Transfer requested assets from acceptor to creator
                for asset in requested_assets:
                    asset_name, asset_type, quantity = asset
                    
                    # Remove from acceptor
                    update_query = text("""
                        UPDATE holdings
                        SET quantity = quantity - :quantity
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    conn.execute(update_query, {
                        "quantity": quantity,
                        "user_id": user_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    })
                    
                    # Add to creator (check if they already have this asset)
                    check_query = text("""
                        SELECT id FROM holdings
                        WHERE user_id = :user_id 
                          AND asset_type = :asset_type
                          AND asset_name = :asset_name
                    """)
                    
                    result = conn.execute(check_query, {
                        "user_id": creator_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name
                    }).fetchone()
                    
                    if result:
                        # Update existing holding
                        update_query = text("""
                            UPDATE holdings
                            SET quantity = quantity + :quantity
                            WHERE id = :holding_id
                        """)
                        
                        conn.execute(update_query, {
                            "quantity": quantity,
                            "holding_id": result[0]
                        })
                    else:
                        # Create new holding
                        insert_query = text("""
                            INSERT INTO holdings
                            (user_id, type, asset_name, quantity, purchase_price)
                            VALUES (:user_id, :asset_type, :asset_name, :quantity, 0)
                        """)
                        
                        conn.execute(insert_query, {
                            "user_id": creator_id,
                            "asset_type": asset_type,
                            "asset_name": asset_name,
                            "quantity": quantity
                        })
                    
                    # Record the transaction
                    transaction_query = text("""
                        INSERT INTO transactions
                        (timestamp, user_id, transaction_type, asset_type, asset_name, 
                         price, quantity, value)
                        VALUES (CURRENT_TIMESTAMP, :user_id, 'Trade In', :asset_type, 
                                :asset_name, 0, :quantity, 0)
                    """)
                    
                    conn.execute(transaction_query, {
                        "user_id": creator_id,
                        "asset_type": asset_type,
                        "asset_name": asset_name,
                        "quantity": quantity
                    })
                
                # 3. Update the trade offer status
                update_query = text("""
                    UPDATE trading_offers
                    SET status = 'completed',
                        recipient_id = :user_id,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = :offer_id
                """)
                
                conn.execute(update_query, {
                    "user_id": user_id,
                    "offer_id": offer_id
                })
                
                # Commit the transaction
                trans.commit()
                
                return True, "Trade completed successfully!"
                
            except Exception as e:
                trans.rollback()
                print(f"Error executing player-for-player trade: {e}")
                return False, f"Error completing trade: {str(e)}"
                
    except Exception as e:
        print(f"Error responding to player-for-player trade offer: {e}")
        return False, f"Error processing trade response: {str(e)}"

# Functions for betting system
def is_user_verified_adult(user_id):
    """
    Check if a user is verified as 21 or older
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - is_verified: Boolean indicating if user is verified as 21+
    """
    try:
        with engine.connect() as conn:
            query = text("SELECT is_verified_adult FROM users WHERE id = :user_id")
            result = conn.execute(query, {"user_id": user_id}).fetchone()
            
            if not result:
                return False
            
            return result[0]
    except Exception as e:
        print(f"Error checking user age verification: {e}")
        return False

def verify_user_age(user_id, birthdate):
    """
    Verify a user's age based on birthdate
    
    Parameters:
    - user_id: ID of the user
    - birthdate: Date of birth (YYYY-MM-DD)
    
    Returns:
    - success: Boolean indicating if verification was successful
    - message: Message about the verification result
    """
    try:
        from datetime import datetime, date
        
        # Parse birthdate
        birth_date = datetime.strptime(birthdate, "%Y-%m-%d").date()
        
        # Calculate age
        today = date.today()
        age = today.year - birth_date.year - ((today.month, today.day) < (birth_date.month, birth_date.day))
        
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            try:
                # Update user's birthdate
                conn.execute(
                    text("UPDATE users SET birthdate = :birthdate WHERE id = :user_id"),
                    {"user_id": user_id, "birthdate": birthdate}
                )
                
                # Set verification status based on age
                if age >= 21:
                    conn.execute(
                        text("UPDATE users SET is_verified_adult = TRUE WHERE id = :user_id"),
                        {"user_id": user_id}
                    )
                    trans.commit()
                    return True, "Age verification successful. You are 21 or older and can access betting features."
                else:
                    conn.execute(
                        text("UPDATE users SET is_verified_adult = FALSE WHERE id = :user_id"),
                        {"user_id": user_id}
                    )
                    trans.commit()
                    return False, f"You must be 21 or older to access betting features. Your current age is {age}."
            
            except Exception as e:
                trans.rollback()
                return False, f"Error verifying age: {str(e)}"
    
    except ValueError:
        return False, "Invalid date format. Please use YYYY-MM-DD format."
    except Exception as e:
        return False, f"Error: {str(e)}"

def get_upcoming_games(limit=10):
    """
    Get list of upcoming games for betting
    
    Parameters:
    - limit: Maximum number of games to return (default: 10)
    
    Returns:
    - games: List of games with betting information
    """
    try:
        with engine.connect() as conn:
            # Check if we have any upcoming games
            count_query = text("SELECT COUNT(*) FROM upcoming_games WHERE status = 'scheduled'")
            count = conn.execute(count_query).fetchone()[0]
            
            # If no upcoming games, generate some sample games
            if count == 0:
                from datetime import datetime, timedelta
                import random
                
                # NFL teams
                teams = [
                    "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
                    "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
                    "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
                    "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
                    "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
                    "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
                    "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
                    "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
                ]
                
                # Create 5 upcoming games
                for i in range(5):
                    # Select two random teams
                    home_team, away_team = random.sample(teams, 2)
                    
                    # Generate game date (in the next 7 days)
                    game_date = datetime.now() + timedelta(days=random.randint(1, 7), 
                                                          hours=random.randint(12, 19))
                    
                    # Generate odds and lines
                    if random.random() > 0.5:
                        # Home team is favorite
                        home_odds = random.uniform(1.5, 2.2)
                        away_odds = random.uniform(2.5, 3.5)
                        spread = random.choice([3.5, 4.5, 6.5, 7.5])
                    else:
                        # Away team is favorite
                        away_odds = random.uniform(1.5, 2.2)
                        home_odds = random.uniform(2.5, 3.5)
                        spread = -1 * random.choice([3.5, 4.5, 6.5, 7.5])
                    
                    # Generate over/under
                    over_under = random.choice([42.5, 44.5, 47.5, 49.5, 51.5, 54.5])
                    
                    # Insert the game
                    conn.execute(
                        text("""
                            INSERT INTO upcoming_games 
                            (home_team, away_team, game_date, home_odds, away_odds, spread, over_under, status)
                            VALUES (:home_team, :away_team, :game_date, :home_odds, :away_odds, :spread, :over_under, 'scheduled')
                        """),
                        {
                            "home_team": home_team,
                            "away_team": away_team,
                            "game_date": game_date,
                            "home_odds": round(home_odds, 2),
                            "away_odds": round(away_odds, 2),
                            "spread": spread,
                            "over_under": over_under
                        }
                    )
                
                conn.commit()
            
            # Retrieve upcoming games
            games_query = text("""
                SELECT * FROM upcoming_games 
                WHERE status = 'scheduled' AND game_date > CURRENT_TIMESTAMP
                ORDER BY game_date ASC
            """)
            games = conn.execute(games_query).fetchall()
            
            # Convert to list of dictionaries and add derived fields for UI
            result = []
            for g in games:
                game_dict = dict(g._mapping)
                
                # Add derived fields for the UI
                # Spread odds are typically -110 (1.91 in decimal)
                game_dict['home_spread'] = game_dict['spread']
                game_dict['away_spread'] = -1 * game_dict['spread']
                game_dict['home_spread_odds'] = -110  # Standard spread odds
                game_dict['away_spread_odds'] = -110
                
                # Over/Under odds are also typically -110
                game_dict['over_odds'] = -110
                game_dict['under_odds'] = -110
                
                result.append(game_dict)
                
            return result
    
    except Exception as e:
        print(f"Error getting upcoming games: {e}")
        return []

import decimal

# Helper function to convert decimal.Decimal to float
def to_float(value):
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value
    
def place_bet(user_id, game_id, bet_type, bet_pick, amount):
    """
    Place a bet on a game
    
    Parameters:
    - user_id: ID of the user placing the bet
    - game_id: ID of the game to bet on
    - bet_type: Type of bet (moneyline, spread, over_under)
    - bet_pick: The pick (home, away, over, under)
    - amount: Amount to bet
    
    Returns:
    - success: Boolean indicating if the bet was placed successfully
    - message: Message about the bet result
    - bet_id: ID of the created bet (if successful)
    """
    # Convert amount to Python float to avoid NumPy type issues
    amount = float(amount)
    try:
        # Verify user is 21+
        if not is_user_verified_adult(user_id):
            return False, "You must be 21 or older to place bets. Please verify your age first.", None
        
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            try:
                # Check if game exists and is scheduled
                game_query = text("""
                    SELECT * FROM upcoming_games 
                    WHERE id = :game_id AND status = 'scheduled' AND game_date > CURRENT_TIMESTAMP
                """)
                game = conn.execute(game_query, {"game_id": game_id}).fetchone()
                
                if not game:
                    return False, "Game not found or betting is closed for this game.", None
                
                # Get user's wallet balance
                user_query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                result = conn.execute(user_query, {"user_id": user_id}).fetchone()
                
                if not result:
                    return False, "User not found.", None
                
                wallet_balance = to_float(result[0])
                
                # Check if user has enough funds
                if wallet_balance < amount:
                    return False, f"Insufficient funds. Need ${amount:.2f}, but you have ${wallet_balance:.2f}", None
                
                # Calculate potential payout based on bet type and pick
                odds = 0
                if bet_type == 'moneyline':
                    if bet_pick == 'home':
                        odds = to_float(game.home_odds)
                    else:  # away
                        odds = to_float(game.away_odds)
                elif bet_type in ['spread', 'over_under']:
                    # Standard -110 odds for spread and over/under bets
                    odds = 1.91
                
                potential_payout = round(float(amount) * odds, 2)
                
                # Deduct amount from user's wallet
                conn.execute(
                    text("UPDATE users SET wallet_balance = wallet_balance - :amount WHERE id = :user_id"),
                    {"user_id": user_id, "amount": amount}
                )
                
                # Create the bet
                bet_query = text("""
                    INSERT INTO user_bets 
                    (user_id, game_id, bet_type, bet_pick, amount, potential_payout, odds, status)
                    VALUES (:user_id, :game_id, :bet_type, :bet_pick, :amount, :potential_payout, :odds, 'pending')
                    RETURNING id
                """)
                
                bet_id = conn.execute(
                    bet_query,
                    {
                        "user_id": user_id,
                        "game_id": game_id,
                        "bet_type": bet_type,
                        "bet_pick": bet_pick,
                        "amount": amount,
                        "potential_payout": potential_payout,
                        "odds": odds
                    }
                ).fetchone()[0]
                
                trans.commit()
                return True, f"Bet placed successfully! Potential payout: ${potential_payout:.2f}", bet_id
            
            except Exception as e:
                trans.rollback()
                return False, f"Error placing bet: {str(e)}", None
    
    except Exception as e:
        return False, f"Error: {str(e)}", None

def create_parlay_bet(user_id, bets, amount):
    """
    Create a parlay bet with multiple selections
    
    Parameters:
    - user_id: ID of the user placing the bet
    - bets: List of bets. Each bet is a dict with keys: game_id, bet_type, bet_pick
    - amount: Amount to bet on the parlay
    
    Returns:
    - success: Boolean indicating if the parlay was created successfully
    - message: Message about the parlay result
    - parlay_id: ID of the created parlay (if successful)
    """
    # Convert amount to Python float to avoid NumPy type issues
    amount = float(amount)
    try:
        # Verify user is 21+
        if not is_user_verified_adult(user_id):
            return False, "You must be 21 or older to place bets. Please verify your age first.", None
        
        # Need at least 2 bets for a parlay
        if len(bets) < 2:
            return False, "A parlay must include at least 2 selections.", None
        
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            try:
                # Get user's wallet balance
                user_query = text("SELECT wallet_balance FROM users WHERE id = :user_id")
                result = conn.execute(user_query, {"user_id": user_id}).fetchone()
                
                if not result:
                    return False, "User not found.", None
                
                wallet_balance = to_float(result[0])
                
                # Check if user has enough funds
                if wallet_balance < amount:
                    return False, f"Insufficient funds. Need ${amount:.2f}, but you have ${wallet_balance:.2f}", None
                
                # Verify all games exist and are available for betting
                total_odds = 1.0
                valid_bets = []
                
                for bet in bets:
                    game_query = text("""
                        SELECT * FROM upcoming_games 
                        WHERE id = :game_id AND status = 'scheduled' AND game_date > CURRENT_TIMESTAMP
                    """)
                    game = conn.execute(game_query, {"game_id": bet['game_id']}).fetchone()
                    
                    if not game:
                        return False, f"Game {bet['game_id']} not found or betting is closed for this game.", None
                    
                    # Calculate odds for this leg
                    odds = 0
                    if bet['bet_type'] == 'moneyline':
                        if bet['bet_pick'] == 'home':
                            odds = to_float(game.home_odds)
                        else:  # away
                            odds = to_float(game.away_odds)
                    elif bet['bet_type'] in ['spread', 'over_under']:
                        # Standard -110 odds for spread and over/under bets
                        odds = 1.91
                    
                    total_odds *= odds
                    valid_bets.append({**bet, 'odds': odds})
                
                # Calculate potential payout
                potential_payout = round(amount * total_odds, 2)
                
                # Deduct amount from user's wallet
                conn.execute(
                    text("UPDATE users SET wallet_balance = wallet_balance - :amount WHERE id = :user_id"),
                    {"user_id": user_id, "amount": amount}
                )
                
                # Create the parlay
                parlay_query = text("""
                    INSERT INTO parlays 
                    (user_id, amount, potential_payout, status)
                    VALUES (:user_id, :amount, :potential_payout, 'pending')
                    RETURNING id
                """)
                
                parlay_id = conn.execute(
                    parlay_query,
                    {
                        "user_id": user_id,
                        "amount": amount,
                        "potential_payout": potential_payout
                    }
                ).fetchone()[0]
                
                # Add individual bets to the parlay
                for bet in valid_bets:
                    conn.execute(
                        text("""
                            INSERT INTO parlay_bets 
                            (parlay_id, game_id, bet_type, bet_pick, odds)
                            VALUES (:parlay_id, :game_id, :bet_type, :bet_pick, :odds)
                        """),
                        {
                            "parlay_id": parlay_id,
                            "game_id": bet['game_id'],
                            "bet_type": bet['bet_type'],
                            "bet_pick": bet['bet_pick'],
                            "odds": bet['odds']
                        }
                    )
                
                trans.commit()
                return True, f"Parlay created successfully! Potential payout: ${potential_payout:.2f}", parlay_id
            
            except Exception as e:
                trans.rollback()
                return False, f"Error creating parlay: {str(e)}", None
    
    except Exception as e:
        return False, f"Error: {str(e)}", None

def get_player_price_history(player_name):
    """
    Get historical price data for a player
    
    Parameters:
    - player_name: Name of the player
    
    Returns:
    - history: DataFrame containing price history
    """
    try:
        with engine.connect() as conn:
            # First try to get data from player_performance_history
            query = text("""
                SELECT player_name, game_date, price_before, price_after, price_change_pct
                FROM player_performance_history
                WHERE player_name = :player_name
                ORDER BY game_date
            """)
            history = pd.read_sql(query, conn, params={"player_name": player_name})
            
            # If no history is found, create a basic history from player data
            if history.empty:
                player_query = text("""
                    SELECT name, initial_price, current_price
                    FROM players
                    WHERE name = :player_name
                """)
                player_data = pd.read_sql(player_query, conn, params={"player_name": player_name})
                
                if not player_data.empty:
                    # Create a multi-point history with simulated price changes over time
                    today = datetime.now().date()
                    # Create 365 day history (1 year)
                    start_date = today - timedelta(days=365)
                    
                    # Generate dates at 15-day intervals
                    dates = []
                    current_date = start_date
                    while current_date <= today:
                        dates.append(current_date)
                        current_date = current_date + timedelta(days=15)
                    
                    # Make sure today is included
                    if dates[-1] != today:
                        dates.append(today)
                    
                    # Calculate gradual price change between initial and current price
                    initial_price = player_data.iloc[0]['initial_price']
                    current_price = player_data.iloc[0]['current_price']
                    
                    # Create price points with some randomness for a more realistic chart
                    import random
                    random.seed(hash(player_name))  # Use player name as random seed for consistency
                    
                    price_before = []
                    price_after = []
                    price_change_pct = []
                    player_names = []
                    
                    last_price = initial_price
                    for i in range(len(dates)):
                        player_names.append(player_name)
                        
                        if i == 0:  # First point is the initial price
                            price_before.append(initial_price)
                            price_after.append(initial_price)
                            price_change_pct.append(0)
                        elif i == len(dates) - 1:  # Last point is the current price
                            price_before.append(last_price)
                            price_after.append(current_price)
                            change = ((current_price - last_price) / last_price) * 100
                            price_change_pct.append(change)
                        else:  # Intermediate points with some randomness
                            # Calculate target price for this point based on linear progression
                            progress = i / (len(dates) - 1)
                            target = initial_price + (current_price - initial_price) * progress
                            
                            # Add some randomness (-3% to +3%)
                            random_factor = 1 + (random.random() * 0.06 - 0.03)
                            new_price = target * random_factor
                            
                            price_before.append(last_price)
                            price_after.append(new_price)
                            change = ((new_price - last_price) / last_price) * 100
                            price_change_pct.append(change)
                            
                            last_price = new_price
                    
                    history = pd.DataFrame({
                        'player_name': player_names,
                        'game_date': dates,
                        'price_before': price_before,
                        'price_after': price_after,
                        'price_change_pct': price_change_pct
                    })
            
            return history
    except Exception as e:
        print(f"Error retrieving player price history: {str(e)}")
        return pd.DataFrame()  # Return empty DataFrame on error

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

def update_player_prices_from_performance():
    """
    Update player prices based on their fantasy performance
    This is a wrapper around the performance updater module
    
    Returns:
    - count: Number of players updated
    - message: Status message
    """
    try:
        # Import the performance updater module
        from performance_updater import update_player_prices_based_on_performance, add_fantasy_points_column
        
        # Make sure we have the fantasy points column
        add_fantasy_points_column()
        
        # Update player prices based on performance
        count = update_player_prices_based_on_performance()
        
        if count > 0:
            return count, f"Successfully updated prices for {count} players based on their fantasy performance."
        else:
            return 0, "No players were updated. Players may have been updated recently."
    
    except Exception as e:
        return 0, f"Error updating player prices: {str(e)}"

def get_user_bets(user_id):
    """
    Get a user's active bets
    
    Parameters:
    - user_id: ID of the user
    
    Returns:
    - single_bets: List of single bets
    - parlays: List of parlay bets
    """
    try:
        with engine.connect() as conn:
            # Get single bets
            single_bets_query = text("""
                SELECT b.*, g.home_team, g.away_team, g.game_date, g.status AS game_status
                FROM user_bets b
                JOIN upcoming_games g ON b.game_id = g.id
                WHERE b.user_id = :user_id
                ORDER BY b.created_at DESC
            """)
            single_bets = conn.execute(single_bets_query, {"user_id": user_id}).fetchall()
            
            # Get parlays
            parlays_query = text("""
                SELECT p.*, COUNT(pb.id) AS leg_count
                FROM parlays p
                JOIN parlay_bets pb ON p.id = pb.parlay_id
                WHERE p.user_id = :user_id
                GROUP BY p.id
                ORDER BY p.created_at DESC
            """)
            parlays = conn.execute(parlays_query, {"user_id": user_id}).fetchall()
            
            # Convert to dictionaries for easier usage
            return [dict(b._mapping) for b in single_bets], [dict(p._mapping) for p in parlays]
    
    except Exception as e:
        print(f"Error getting user bets: {e}")
        return [], []

def simulate_game_result(game_id):
    """
    Simulate a game result and settle all bets on the game
    
    Parameters:
    - game_id: ID of the game to simulate
    
    Returns:
    - success: Boolean indicating if simulation was successful
    - message: Message about the simulation result
    """
    try:
        import random
        
        with engine.connect() as conn:
            # Start transaction
            trans = conn.begin()
            try:
                # Get the game
                game_query = text("SELECT * FROM upcoming_games WHERE id = :game_id")
                game = conn.execute(game_query, {"game_id": game_id}).fetchone()
                
                if not game:
                    return False, "Game not found."
                
                # Detect sport type from team names
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
                
                # Update game status and scores
                conn.execute(
                    text("""
                        UPDATE upcoming_games 
                        SET status = 'completed', home_score = :home_score, away_score = :away_score, 
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = :game_id
                    """),
                    {
                        "game_id": game_id,
                        "home_score": home_score,
                        "away_score": away_score
                    }
                )
                
                # Determine game outcome
                home_won = home_score > away_score
                total_points = home_score + away_score
                home_covered = (home_score + game.spread) > away_score
                over_hit = total_points > game.over_under
                
                # Process single bets on this game
                bets_query = text("SELECT * FROM user_bets WHERE game_id = :game_id AND status = 'pending'")
                bets = conn.execute(bets_query, {"game_id": game_id}).fetchall()
                
                for bet in bets:
                    bet_won = False
                    
                    # Determine if bet won based on bet type and pick
                    if bet.bet_type == 'moneyline':
                        if bet.bet_pick == 'home':
                            bet_won = home_won
                        else:  # away
                            bet_won = not home_won
                    elif bet.bet_type == 'spread':
                        if bet.bet_pick == 'home':
                            bet_won = home_covered
                        else:  # away
                            bet_won = not home_covered
                    elif bet.bet_type == 'over_under':
                        if bet.bet_pick == 'over':
                            bet_won = over_hit
                        else:  # under
                            bet_won = not over_hit
                    
                    # Update bet status
                    status = 'won' if bet_won else 'lost'
                    conn.execute(
                        text("UPDATE user_bets SET status = :status WHERE id = :bet_id"),
                        {"bet_id": bet.id, "status": status}
                    )
                    
                    # If bet won, add payout to user's wallet
                    if bet_won:
                        conn.execute(
                            text("UPDATE users SET wallet_balance = wallet_balance + :payout WHERE id = :user_id"),
                            {"user_id": bet.user_id, "payout": bet.potential_payout}
                        )
                
                # Process parlay bets that include this game
                parlay_bets_query = text("""
                    SELECT pb.*, p.id AS parlay_id, p.user_id, p.amount, p.potential_payout
                    FROM parlay_bets pb
                    JOIN parlays p ON pb.parlay_id = p.id
                    WHERE pb.game_id = :game_id AND pb.status = 'pending' AND p.status = 'pending'
                """)
                parlay_bets = conn.execute(parlay_bets_query, {"game_id": game_id}).fetchall()
                
                # Group by parlay_id
                parlay_results = {}
                for pb in parlay_bets:
                    bet_won = False
                    
                    # Determine if this leg won
                    if pb.bet_type == 'moneyline':
                        if pb.bet_pick == 'home':
                            bet_won = home_won
                        else:  # away
                            bet_won = not home_won
                    elif pb.bet_type == 'spread':
                        if pb.bet_pick == 'home':
                            bet_won = home_covered
                        else:  # away
                            bet_won = not home_covered
                    elif pb.bet_type == 'over_under':
                        if pb.bet_pick == 'over':
                            bet_won = over_hit
                        else:  # under
                            bet_won = not over_hit
                    
                    # Update parlay bet status
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
                
                trans.commit()
                return True, f"Game simulated successfully! {game.home_team} {home_score} - {away_score} {game.away_team}"
            
            except Exception as e:
                trans.rollback()
                return False, f"Error simulating game: {str(e)}"
    
    except Exception as e:
        return False, f"Error: {str(e)}"