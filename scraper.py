import trafilatura
import pandas as pd
import json
import random

def get_nfl_players():
    """
    Get player data from web sources or use provided data for all sports
    """
    # For now we'll create a structured dataset of players
    # This would ideally be replaced with actual scraping
    
    # NFL Teams
    nfl_teams = [
        "Arizona Cardinals", "Atlanta Falcons", "Baltimore Ravens", "Buffalo Bills",
        "Carolina Panthers", "Chicago Bears", "Cincinnati Bengals", "Cleveland Browns",
        "Dallas Cowboys", "Denver Broncos", "Detroit Lions", "Green Bay Packers",
        "Houston Texans", "Indianapolis Colts", "Jacksonville Jaguars", "Kansas City Chiefs",
        "Las Vegas Raiders", "Los Angeles Chargers", "Los Angeles Rams", "Miami Dolphins",
        "Minnesota Vikings", "New England Patriots", "New Orleans Saints", "New York Giants",
        "New York Jets", "Philadelphia Eagles", "Pittsburgh Steelers", "San Francisco 49ers",
        "Seattle Seahawks", "Tampa Bay Buccaneers", "Tennessee Titans", "Washington Commanders"
    ]
    
    # MLB Teams
    mlb_teams = [
        "Arizona Diamondbacks", "Atlanta Braves", "Baltimore Orioles", "Boston Red Sox",
        "Chicago Cubs", "Chicago White Sox", "Cincinnati Reds", "Cleveland Guardians",
        "Colorado Rockies", "Detroit Tigers", "Houston Astros", "Kansas City Royals",
        "Los Angeles Angels", "Los Angeles Dodgers", "Miami Marlins", "Milwaukee Brewers",
        "Minnesota Twins", "New York Mets", "New York Yankees", "Oakland Athletics",
        "Philadelphia Phillies", "Pittsburgh Pirates", "San Diego Padres", "San Francisco Giants",
        "Seattle Mariners", "St. Louis Cardinals", "Tampa Bay Rays", "Texas Rangers",
        "Toronto Blue Jays", "Washington Nationals"
    ]
    
    # NBA Teams
    nba_teams = [
        "Atlanta Hawks", "Boston Celtics", "Brooklyn Nets", "Charlotte Hornets",
        "Chicago Bulls", "Cleveland Cavaliers", "Dallas Mavericks", "Denver Nuggets",
        "Detroit Pistons", "Golden State Warriors", "Houston Rockets", "Indiana Pacers",
        "LA Clippers", "Los Angeles Lakers", "Memphis Grizzlies", "Miami Heat",
        "Milwaukee Bucks", "Minnesota Timberwolves", "New Orleans Pelicans", "New York Knicks",
        "Oklahoma City Thunder", "Orlando Magic", "Philadelphia 76ers", "Phoenix Suns",
        "Portland Trail Blazers", "Sacramento Kings", "San Antonio Spurs", "Toronto Raptors",
        "Utah Jazz", "Washington Wizards"
    ]
    
    # WNBA Teams
    wnba_teams = [
        "Atlanta Dream", "Chicago Sky", "Connecticut Sun", "Dallas Wings",
        "Indiana Fever", "Las Vegas Aces", "Los Angeles Sparks", "Minnesota Lynx",
        "New York Liberty", "Phoenix Mercury", "Seattle Storm", "Washington Mystics"
    ]
    
    # College Teams (Top programs)
    college_teams = [
        "Alabama", "Ohio State", "Georgia", "Michigan", "Texas", "Oklahoma", 
        "LSU", "Clemson", "Florida State", "Oregon", "Penn State", "Notre Dame",
        "Iowa", "Duke", "North Carolina", "Kentucky", "Kansas", "Villanova", 
        "UCLA", "Arizona", "Gonzaga", "UConn", "South Carolina", "Tennessee",
        "Baylor", "Stanford", "Louisville", "Maryland"
    ]
    
    # NFL Positions
    nfl_positions = ["QB", "RB", "WR", "TE", "K", "DEF"]
    
    # MLB Positions
    mlb_positions = ["SP", "RP", "C", "1B", "2B", "3B", "SS", "OF", "DH"]
    
    # Basketball Positions
    bball_positions = ["PG", "SG", "SF", "PF", "C"]
    
    # Top QBs
    qbs = [
        {"name": "Patrick Mahomes", "team": "Kansas City Chiefs", "position": "QB", "price": 350.00, "tier": "Elite"},
        {"name": "Josh Allen", "team": "Buffalo Bills", "position": "QB", "price": 330.00, "tier": "Elite"},
        {"name": "Joe Burrow", "team": "Cincinnati Bengals", "position": "QB", "price": 320.00, "tier": "Elite"},
        {"name": "Lamar Jackson", "team": "Baltimore Ravens", "position": "QB", "price": 315.00, "tier": "Elite"},
        {"name": "Justin Herbert", "team": "Los Angeles Chargers", "position": "QB", "price": 305.00, "tier": "Elite"},
        {"name": "Jalen Hurts", "team": "Philadelphia Eagles", "position": "QB", "price": 300.00, "tier": "Elite"},
        {"name": "Trevor Lawrence", "team": "Jacksonville Jaguars", "position": "QB", "price": 280.00, "tier": "Star"},
        {"name": "Dak Prescott", "team": "Dallas Cowboys", "position": "QB", "price": 275.00, "tier": "Star"},
        {"name": "Tua Tagovailoa", "team": "Miami Dolphins", "position": "QB", "price": 265.00, "tier": "Star"},
        {"name": "Aaron Rodgers", "team": "New York Jets", "position": "QB", "price": 255.00, "tier": "Star"},
        {"name": "Matthew Stafford", "team": "Los Angeles Rams", "position": "QB", "price": 240.00, "tier": "Starter"},
        {"name": "Kirk Cousins", "team": "Atlanta Falcons", "position": "QB", "price": 230.00, "tier": "Starter"},
        {"name": "C.J. Stroud", "team": "Houston Texans", "position": "QB", "price": 275.00, "tier": "Star"},
        {"name": "Caleb Williams", "team": "Chicago Bears", "position": "QB", "price": 250.00, "tier": "Rookie"},
        {"name": "Jayden Daniels", "team": "Washington Commanders", "position": "QB", "price": 225.00, "tier": "Rookie"},
        {"name": "Drake Maye", "team": "New England Patriots", "position": "QB", "price": 215.00, "tier": "Rookie"},
    ]
    
    # Top RBs
    rbs = [
        {"name": "Christian McCaffrey", "team": "San Francisco 49ers", "position": "RB", "price": 325.00, "tier": "Elite"},
        {"name": "Jonathan Taylor", "team": "Indianapolis Colts", "position": "RB", "price": 290.00, "tier": "Elite"},
        {"name": "Saquon Barkley", "team": "Philadelphia Eagles", "position": "RB", "price": 285.00, "tier": "Elite"},
        {"name": "Derrick Henry", "team": "Baltimore Ravens", "position": "RB", "price": 275.00, "tier": "Elite"},
        {"name": "Nick Chubb", "team": "Cleveland Browns", "position": "RB", "price": 260.00, "tier": "Star"},
        {"name": "Josh Jacobs", "team": "Green Bay Packers", "position": "RB", "price": 245.00, "tier": "Star"},
        {"name": "Alvin Kamara", "team": "New Orleans Saints", "position": "RB", "price": 240.00, "tier": "Star"},
        {"name": "Austin Ekeler", "team": "Washington Commanders", "position": "RB", "price": 230.00, "tier": "Star"},
        {"name": "Tony Pollard", "team": "Tennessee Titans", "position": "RB", "price": 225.00, "tier": "Starter"},
        {"name": "Travis Etienne", "team": "Jacksonville Jaguars", "position": "RB", "price": 220.00, "tier": "Starter"},
        {"name": "Jahmyr Gibbs", "team": "Detroit Lions", "position": "RB", "price": 235.00, "tier": "Star"},
        {"name": "Bijan Robinson", "team": "Atlanta Falcons", "position": "RB", "price": 230.00, "tier": "Star"},
    ]
    
    # Top WRs
    wrs = [
        {"name": "Justin Jefferson", "team": "Minnesota Vikings", "position": "WR", "price": 320.00, "tier": "Elite"},
        {"name": "Ja'Marr Chase", "team": "Cincinnati Bengals", "position": "WR", "price": 310.00, "tier": "Elite"},
        {"name": "Tyreek Hill", "team": "Miami Dolphins", "position": "WR", "price": 305.00, "tier": "Elite"},
        {"name": "CeeDee Lamb", "team": "Dallas Cowboys", "position": "WR", "price": 295.00, "tier": "Elite"},
        {"name": "A.J. Brown", "team": "Philadelphia Eagles", "position": "WR", "price": 290.00, "tier": "Elite"},
        {"name": "Amon-Ra St. Brown", "team": "Detroit Lions", "position": "WR", "price": 285.00, "tier": "Elite"},
        {"name": "Davante Adams", "team": "Las Vegas Raiders", "position": "WR", "price": 270.00, "tier": "Star"},
        {"name": "Stefon Diggs", "team": "Houston Texans", "position": "WR", "price": 265.00, "tier": "Star"},
        {"name": "Deebo Samuel", "team": "San Francisco 49ers", "position": "WR", "price": 255.00, "tier": "Star"},
        {"name": "DK Metcalf", "team": "Seattle Seahawks", "position": "WR", "price": 250.00, "tier": "Star"},
        {"name": "Marvin Harrison Jr.", "team": "Arizona Cardinals", "position": "WR", "price": 245.00, "tier": "Rookie"},
        {"name": "Garrett Wilson", "team": "New York Jets", "position": "WR", "price": 240.00, "tier": "Star"},
    ]
    
    # Top TEs
    tes = [
        {"name": "Travis Kelce", "team": "Kansas City Chiefs", "position": "TE", "price": 285.00, "tier": "Elite"},
        {"name": "Mark Andrews", "team": "Baltimore Ravens", "position": "TE", "price": 250.00, "tier": "Elite"},
        {"name": "T.J. Hockenson", "team": "Minnesota Vikings", "position": "TE", "price": 230.00, "tier": "Star"},
        {"name": "George Kittle", "team": "San Francisco 49ers", "position": "TE", "price": 225.00, "tier": "Star"},
        {"name": "Kyle Pitts", "team": "Atlanta Falcons", "position": "TE", "price": 215.00, "tier": "Star"},
        {"name": "Dallas Goedert", "team": "Philadelphia Eagles", "position": "TE", "price": 200.00, "tier": "Starter"},
        {"name": "Evan Engram", "team": "Jacksonville Jaguars", "position": "TE", "price": 195.00, "tier": "Starter"},
        {"name": "Pat Freiermuth", "team": "Pittsburgh Steelers", "position": "TE", "price": 185.00, "tier": "Starter"},
    ]
    
    # Kickers
    ks = [
        {"name": "Justin Tucker", "team": "Baltimore Ravens", "position": "K", "price": 200.00, "tier": "Elite"},
        {"name": "Harrison Butker", "team": "Kansas City Chiefs", "position": "K", "price": 180.00, "tier": "Elite"},
        {"name": "Jake Elliott", "team": "Philadelphia Eagles", "position": "K", "price": 165.00, "tier": "Star"},
        {"name": "Evan McPherson", "team": "Cincinnati Bengals", "position": "K", "price": 160.00, "tier": "Star"},
        {"name": "Brandon Aubrey", "team": "Dallas Cowboys", "position": "K", "price": 155.00, "tier": "Star"},
        {"name": "Cameron Dicker", "team": "Los Angeles Chargers", "position": "K", "price": 150.00, "tier": "Starter"},
    ]
    
    # Team Defenses
    defenses = [
        {"name": f"{team} Defense", "team": team, "position": "DEF", "price": 150.00 + random.randint(0, 50), "tier": "Team"}
        for team in nfl_teams
    ]
    
    # College Football Stars
    cf_players = [
        {"name": "Quinn Ewers", "team": "Texas", "position": "QB", "price": 200.00, "tier": "College", "sport": "Football"},
        {"name": "Carson Beck", "team": "Georgia", "position": "QB", "price": 195.00, "tier": "College", "sport": "Football"},
        {"name": "Jalen Milroe", "team": "Alabama", "position": "QB", "price": 190.00, "tier": "College", "sport": "Football"},
        {"name": "Cade Klubnik", "team": "Clemson", "position": "QB", "price": 185.00, "tier": "College", "sport": "Football"},
        {"name": "Drew Allar", "team": "Penn State", "position": "QB", "price": 180.00, "tier": "College", "sport": "Football"},
        {"name": "Jaxson Dart", "team": "Ole Miss", "position": "QB", "price": 175.00, "tier": "College", "sport": "Football"},
        {"name": "Ollie Gordon II", "team": "Oklahoma State", "position": "RB", "price": 190.00, "tier": "College", "sport": "Football"},
        {"name": "TreVeyon Henderson", "team": "Ohio State", "position": "RB", "price": 185.00, "tier": "College", "sport": "Football"},
        {"name": "Donovan Edwards", "team": "Michigan", "position": "RB", "price": 180.00, "tier": "College", "sport": "Football"},
        {"name": "Will Shipley", "team": "Clemson", "position": "RB", "price": 175.00, "tier": "College", "sport": "Football"},
        {"name": "Tetairoa McMillan", "team": "Arizona", "position": "WR", "price": 185.00, "tier": "College", "sport": "Football"},
        {"name": "Luther Burden III", "team": "Missouri", "position": "WR", "price": 180.00, "tier": "College", "sport": "Football"},
        {"name": "Emeka Egbuka", "team": "Ohio State", "position": "WR", "price": 175.00, "tier": "College", "sport": "Football"},
        {"name": "Brock Bowers", "team": "Georgia", "position": "TE", "price": 180.00, "tier": "College", "sport": "Football"},
    ]
    
    # MLB Stars
    mlb_players = [
        {"name": "Shohei Ohtani", "team": "Los Angeles Dodgers", "position": "DH", "price": 340.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Aaron Judge", "team": "New York Yankees", "position": "OF", "price": 330.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Juan Soto", "team": "New York Yankees", "position": "OF", "price": 320.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Mookie Betts", "team": "Los Angeles Dodgers", "position": "OF", "price": 315.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Freddie Freeman", "team": "Los Angeles Dodgers", "position": "1B", "price": 305.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Corbin Burnes", "team": "Baltimore Orioles", "position": "SP", "price": 300.00, "tier": "Elite", "sport": "MLB"},
        {"name": "Bobby Witt Jr.", "team": "Kansas City Royals", "position": "SS", "price": 290.00, "tier": "Star", "sport": "MLB"},
        {"name": "Gunnar Henderson", "team": "Baltimore Orioles", "position": "SS", "price": 285.00, "tier": "Star", "sport": "MLB"},
        {"name": "Vladimir Guerrero Jr.", "team": "Toronto Blue Jays", "position": "1B", "price": 280.00, "tier": "Star", "sport": "MLB"},
        {"name": "Spencer Strider", "team": "Atlanta Braves", "position": "SP", "price": 275.00, "tier": "Star", "sport": "MLB"},
        {"name": "Zack Wheeler", "team": "Philadelphia Phillies", "position": "SP", "price": 270.00, "tier": "Star", "sport": "MLB"},
        {"name": "Adley Rutschman", "team": "Baltimore Orioles", "position": "C", "price": 265.00, "tier": "Star", "sport": "MLB"},
        {"name": "Fernando Tatis Jr.", "team": "San Diego Padres", "position": "OF", "price": 260.00, "tier": "Star", "sport": "MLB"},
        {"name": "Gerrit Cole", "team": "New York Yankees", "position": "SP", "price": 255.00, "tier": "Star", "sport": "MLB"},
        {"name": "Ronald Acuña Jr.", "team": "Atlanta Braves", "position": "OF", "price": 250.00, "tier": "Star", "sport": "MLB"},
        {"name": "Jackson Holliday", "team": "Baltimore Orioles", "position": "2B", "price": 245.00, "tier": "Rookie", "sport": "MLB"},
        {"name": "Paul Skenes", "team": "Pittsburgh Pirates", "position": "SP", "price": 235.00, "tier": "Rookie", "sport": "MLB"},
    ]
    
    # NBA Stars  
    nba_players = [
        {"name": "Nikola Jokic", "team": "Denver Nuggets", "position": "C", "price": 335.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Luka Doncic", "team": "Dallas Mavericks", "position": "PG", "price": 330.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Giannis Antetokounmpo", "team": "Milwaukee Bucks", "position": "PF", "price": 325.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Jayson Tatum", "team": "Boston Celtics", "position": "SF", "price": 315.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Joel Embiid", "team": "Philadelphia 76ers", "position": "C", "price": 310.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Shai Gilgeous-Alexander", "team": "Oklahoma City Thunder", "position": "SG", "price": 305.00, "tier": "Elite", "sport": "NBA"},
        {"name": "Anthony Edwards", "team": "Minnesota Timberwolves", "position": "SG", "price": 300.00, "tier": "Elite", "sport": "NBA"},
        {"name": "LeBron James", "team": "Los Angeles Lakers", "position": "SF", "price": 290.00, "tier": "Star", "sport": "NBA"},
        {"name": "Stephen Curry", "team": "Golden State Warriors", "position": "PG", "price": 285.00, "tier": "Star", "sport": "NBA"},
        {"name": "Kevin Durant", "team": "Phoenix Suns", "position": "SF", "price": 280.00, "tier": "Star", "sport": "NBA"},
        {"name": "Jaylen Brown", "team": "Boston Celtics", "position": "SG", "price": 275.00, "tier": "Star", "sport": "NBA"},
        {"name": "Devin Booker", "team": "Phoenix Suns", "position": "SG", "price": 270.00, "tier": "Star", "sport": "NBA"},
        {"name": "Victor Wembanyama", "team": "San Antonio Spurs", "position": "C", "price": 280.00, "tier": "Star", "sport": "NBA"},
        {"name": "Chet Holmgren", "team": "Oklahoma City Thunder", "position": "PF", "price": 250.00, "tier": "Star", "sport": "NBA"},
        {"name": "Jalen Brunson", "team": "New York Knicks", "position": "PG", "price": 255.00, "tier": "Star", "sport": "NBA"},
        {"name": "Zaccharie Risacher", "team": "Atlanta Hawks", "position": "SF", "price": 240.00, "tier": "Rookie", "sport": "NBA"},
        {"name": "Alexandre Sarr", "team": "Washington Wizards", "position": "C", "price": 235.00, "tier": "Rookie", "sport": "NBA"},
    ]
    
    # WNBA Stars
    wnba_players = [
        {"name": "A'ja Wilson", "team": "Las Vegas Aces", "position": "PF", "price": 300.00, "tier": "Elite", "sport": "WNBA"},
        {"name": "Breanna Stewart", "team": "New York Liberty", "position": "PF", "price": 290.00, "tier": "Elite", "sport": "WNBA"},
        {"name": "Caitlin Clark", "team": "Indiana Fever", "position": "PG", "price": 285.00, "tier": "Rookie", "sport": "WNBA"},
        {"name": "Sabrina Ionescu", "team": "New York Liberty", "position": "PG", "price": 275.00, "tier": "Elite", "sport": "WNBA"},
        {"name": "Napheesa Collier", "team": "Minnesota Lynx", "position": "PF", "price": 270.00, "tier": "Elite", "sport": "WNBA"},
        {"name": "Alyssa Thomas", "team": "Connecticut Sun", "position": "PF", "price": 265.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Jackie Young", "team": "Las Vegas Aces", "position": "SG", "price": 255.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Kelsey Plum", "team": "Las Vegas Aces", "position": "PG", "price": 250.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Angel Reese", "team": "Chicago Sky", "position": "PF", "price": 245.00, "tier": "Rookie", "sport": "WNBA"},
        {"name": "Aliyah Boston", "team": "Indiana Fever", "position": "C", "price": 240.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Arike Ogunbowale", "team": "Dallas Wings", "position": "SG", "price": 235.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Jewell Loyd", "team": "Seattle Storm", "position": "SG", "price": 230.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Rhyne Howard", "team": "Atlanta Dream", "position": "SG", "price": 225.00, "tier": "Star", "sport": "WNBA"},
        {"name": "Cameron Brink", "team": "Los Angeles Sparks", "position": "PF", "price": 220.00, "tier": "Rookie", "sport": "WNBA"},
    ]
    
    # College Basketball Stars (Men's)
    men_cbb_players = [
        {"name": "Kyle Filipowski", "team": "Duke", "position": "C", "price": 215.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Hunter Dickinson", "team": "Kansas", "position": "C", "price": 210.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Zach Edey", "team": "Purdue", "position": "C", "price": 205.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "RJ Davis", "team": "North Carolina", "position": "PG", "price": 200.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Wade Taylor IV", "team": "Texas A&M", "position": "PG", "price": 195.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Mark Sears", "team": "Alabama", "position": "PG", "price": 190.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Baylor Scheierman", "team": "Creighton", "position": "SF", "price": 185.00, "tier": "College", "sport": "Men's Basketball"},
        {"name": "Donovan Clingan", "team": "UConn", "position": "C", "price": 180.00, "tier": "College", "sport": "Men's Basketball"},
    ]
    
    # College Basketball Stars (Women's)
    women_cbb_players = [
        {"name": "Paige Bueckers", "team": "UConn", "position": "PG", "price": 220.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "Kiki Rice", "team": "UCLA", "position": "PG", "price": 210.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "JuJu Watkins", "team": "USC", "position": "SG", "price": 205.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "Flau'jae Johnson", "team": "LSU", "position": "SG", "price": 200.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "Hannah Hidalgo", "team": "Notre Dame", "position": "PG", "price": 195.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "Georgia Amoore", "team": "Kentucky", "position": "PG", "price": 190.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "Kamilla Cardoso", "team": "South Carolina", "position": "C", "price": 185.00, "tier": "College", "sport": "Women's Basketball"},
        {"name": "MiLaysia Fulwiley", "team": "South Carolina", "position": "SG", "price": 180.00, "tier": "College", "sport": "Women's Basketball"},
    ]
    
    # College Baseball Stars
    college_baseball_players = [
        {"name": "Charlie Condon", "team": "Georgia", "position": "1B", "price": 200.00, "tier": "College", "sport": "Baseball"},
        {"name": "Jac Caglianone", "team": "Florida", "position": "1B/SP", "price": 195.00, "tier": "College", "sport": "Baseball"},
        {"name": "Travis Bazzana", "team": "Oregon State", "position": "2B", "price": 190.00, "tier": "College", "sport": "Baseball"},
        {"name": "Braden Montgomery", "team": "Texas A&M", "position": "OF", "price": 185.00, "tier": "College", "sport": "Baseball"},
        {"name": "Chase Burns", "team": "Wake Forest", "position": "SP", "price": 180.00, "tier": "College", "sport": "Baseball"},
        {"name": "Luke Holman", "team": "LSU", "position": "SP", "price": 175.00, "tier": "College", "sport": "Baseball"},
    ]
    
    # College Softball Stars
    college_softball_players = [
        {"name": "Tiare Jennings", "team": "Oklahoma", "position": "2B", "price": 200.00, "tier": "College", "sport": "Softball"},
        {"name": "Jayda Coleman", "team": "Oklahoma", "position": "OF", "price": 195.00, "tier": "College", "sport": "Softball"},
        {"name": "NiJaree Canady", "team": "Stanford", "position": "SP", "price": 190.00, "tier": "College", "sport": "Softball"},
        {"name": "Kennedy Powell", "team": "Oklahoma State", "position": "SS", "price": 185.00, "tier": "College", "sport": "Softball"},
        {"name": "Kendra Falby", "team": "Florida", "position": "OF", "price": 180.00, "tier": "College", "sport": "Softball"},
        {"name": "Ruby Meylan", "team": "Washington", "position": "SP", "price": 175.00, "tier": "College", "sport": "Softball"},
    ]
    
    # Add sport to NFL players
    for player in qbs + rbs + wrs + tes + ks:
        player["sport"] = "NFL"
        
    for def_player in defenses:
        def_player["sport"] = "NFL"
    
    # Combine all players
    all_players = qbs + rbs + wrs + tes + ks + defenses + cf_players + mlb_players + nba_players + \
                  wnba_players + men_cbb_players + women_cbb_players + college_baseball_players + college_softball_players
    
    # Add market cap style pricing and random stats
    for player in all_players:
        player["week_1_yards"] = random.randint(0, 200) if player["position"] != "K" and player["position"] != "DEF" else 0
        player["week_1_tds"] = random.randint(0, 3) if player["position"] != "K" else random.randint(0, 7)
        
        # Calculate total worth based on tier
        if "tier" in player:
            if player["tier"] == "Elite":
                total_worth = player["price"] * 10000  # Keep original market cap
                shares = 100000  # Increase shares 10x to reduce per-share price
            elif player["tier"] == "Star":
                total_worth = player["price"] * 7500  
                shares = 75000  # Increase shares 10x
            elif player["tier"] == "Rookie":
                total_worth = player["price"] * 5000  
                shares = 50000  # Increase shares 10x
            elif player["tier"] == "College":
                total_worth = player["price"] * 3000  
                shares = 30000  # Increase shares 10x
            else:
                total_worth = player["price"] * 1000  
                shares = 10000  # Increase shares 10x
        else:
            total_worth = player["price"] * 1000
            shares = 10000  # Increase shares 10x
            
        # Set prices to be share price (total worth / shares)
        player["total_worth"] = total_worth
        player["shares_outstanding"] = shares
        player["initial_price"] = round(total_worth / shares, 2)  # Initial price is per share
        player["current_price"] = round(total_worth / shares, 2)  # Current price is per share
    
    return pd.DataFrame(all_players)

def get_team_funds():
    """
    Generate team and position-based funds
    """
    # Team-based funds
    team_funds = [
        {"name": "Kansas City Chiefs Fund", "players_included": "Mahomes,Kelce,Chiefs Defense,Butker", "price": 500.00, "type": "Team"},
        {"name": "Buffalo Bills Fund", "players_included": "Josh Allen,Bills Defense", "price": 450.00, "type": "Team"},
        {"name": "Philadelphia Eagles Fund", "players_included": "Jalen Hurts,A.J. Brown,Saquon Barkley,Eagles Defense", "price": 475.00, "type": "Team"},
        {"name": "San Francisco 49ers Fund", "players_included": "Christian McCaffrey,Deebo Samuel,49ers Defense", "price": 460.00, "type": "Team"},
        {"name": "Dallas Cowboys Fund", "players_included": "Dak Prescott,CeeDee Lamb,Cowboys Defense", "price": 450.00, "type": "Team"},
        {"name": "Baltimore Ravens Fund", "players_included": "Lamar Jackson,Mark Andrews,Derrick Henry,Ravens Defense", "price": 480.00, "type": "Team"},
        {"name": "Detroit Lions Fund", "players_included": "Amon-Ra St. Brown,Jahmyr Gibbs,Lions Defense", "price": 430.00, "type": "Team"},
        {"name": "Cincinnati Bengals Fund", "players_included": "Joe Burrow,Ja'Marr Chase,Bengals Defense", "price": 460.00, "type": "Team"},
    ]
    
    # Position-based funds
    position_funds = [
        {"name": "Elite QB Fund", "players_included": "Mahomes,Allen,Burrow,Jackson,Herbert,Hurts", "price": 600.00, "type": "Position"},
        {"name": "Top RB Fund", "players_included": "McCaffrey,Taylor,Barkley,Henry,Chubb", "price": 550.00, "type": "Position"},
        {"name": "Top WR Fund", "players_included": "Jefferson,Chase,Hill,Lamb,Brown", "price": 575.00, "type": "Position"},
        {"name": "TE Stars Fund", "players_included": "Kelce,Andrews,Hockenson,Kittle", "price": 450.00, "type": "Position"},
        {"name": "Rookie QB Fund", "players_included": "Caleb Williams,Jayden Daniels,Drake Maye", "price": 400.00, "type": "Position"},
        {"name": "College Stars Fund", "players_included": "Quinn Ewers,Carson Beck,Jalen Milroe,Ollie Gordon II,Brock Bowers", "price": 425.00, "type": "Position"},
        {"name": "Defensive Powers Fund", "players_included": "49ers Defense,Cowboys Defense,Ravens Defense,Eagles Defense", "price": 375.00, "type": "Position"},
    ]
    
    all_funds = team_funds + position_funds
    return pd.DataFrame(all_funds)

def update_player_data_in_database(engine):
    """
    Update the database with the new player data
    """
    # Get players dataframe
    players_df = get_nfl_players()
    
    # Insert players into database
    for _, player in players_df.iterrows():
        # First check if player already exists
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM players WHERE name = :name"),
                {"name": player["name"]}
            ).fetchone()
            
            if result:
                # Update existing player
                conn.execute(
                    text("""
                        UPDATE players 
                        SET team = :team, position = :position, 
                            initial_price = :initial_price, current_price = :current_price,
                            week_1_yards = :week_1_yards, week_1_tds = :week_1_tds,
                            tier = :tier, total_worth = :total_worth, shares_outstanding = :shares_outstanding,
                            sport = :sport
                        WHERE name = :name
                    """),
                    {
                        "team": player["team"],
                        "position": player["position"],
                        "initial_price": player["initial_price"],
                        "current_price": player["current_price"],
                        "week_1_yards": player["week_1_yards"],
                        "week_1_tds": player["week_1_tds"],
                        "tier": player["tier"],
                        "total_worth": player["total_worth"],
                        "shares_outstanding": player["shares_outstanding"],
                        "sport": player.get("sport", "NFL"),  # Default to NFL for existing data
                        "name": player["name"]
                    }
                )
            else:
                # Insert new player
                conn.execute(
                    text("""
                        INSERT INTO players 
                        (name, team, position, initial_price, current_price, week_1_yards, week_1_tds, tier, total_worth, shares_outstanding, sport)
                        VALUES (:name, :team, :position, :initial_price, :current_price, :week_1_yards, :week_1_tds, :tier, :total_worth, :shares_outstanding, :sport)
                    """),
                    {
                        "name": player["name"],
                        "team": player["team"],
                        "position": player["position"],
                        "initial_price": player["initial_price"],
                        "current_price": player["current_price"],
                        "week_1_yards": player["week_1_yards"],
                        "week_1_tds": player["week_1_tds"],
                        "tier": player["tier"],
                        "total_worth": player["total_worth"],
                        "shares_outstanding": player["shares_outstanding"],
                        "sport": player.get("sport", "NFL")  # Default to NFL for existing data
                    }
                )
            conn.commit()
    
    # Get funds dataframe
    funds_df = get_team_funds()
    
    # Insert funds into database
    for _, fund in funds_df.iterrows():
        # First check if fund already exists
        with engine.connect() as conn:
            result = conn.execute(
                text("SELECT id FROM team_funds WHERE name = :name"),
                {"name": fund["name"]}
            ).fetchone()
            
            if result:
                # Update existing fund
                conn.execute(
                    text("""
                        UPDATE team_funds 
                        SET players_included = :players_included, 
                            price = :price, type = :type
                        WHERE name = :name
                    """),
                    {
                        "players_included": fund["players_included"],
                        "price": fund["price"],
                        "type": fund["type"],
                        "name": fund["name"]
                    }
                )
            else:
                # Insert new fund
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
                        "type": fund["type"]
                    }
                )
            conn.commit()

# When run directly, print sample data
if __name__ == "__main__":
    players = get_nfl_players()
    funds = get_team_funds()
    print(f"Generated {len(players)} players and {len(funds)} funds")
    print(players.head())