import mysql.connector
import pandas as pd
from mysql.connector import Error
import os

# --- IMPORTANT: UPDATE YOUR DATABASE CREDENTIALS HERE ---
db_config = {
    "host": "localhost",
    "user": "your_username",
    "password": "your_password",
    "database": "your_database" # Make sure this database already exists
}

# --- SQL STATEMENTS (Updated with Foreign Keys in Creation) ---

create_tables_sql = {
    "teams": """
        CREATE TABLE IF NOT EXISTS teams (
            team_id INT PRIMARY KEY,
            team_name VARCHAR(255) NOT NULL
        )
    """,
    "agents": """
        CREATE TABLE IF NOT EXISTS agents (
            agent_id INT PRIMARY KEY,
            agent_name VARCHAR(255) NOT NULL,
            role VARCHAR(255)
        )
    """,
    "players": """
        CREATE TABLE IF NOT EXISTS players (
            player_id INT PRIMARY KEY,
            player_name VARCHAR(255) NOT NULL,
            team_id INT,
            CONSTRAINT fk_players_teams FOREIGN KEY (team_id) REFERENCES teams(team_id)
        )
    """,
    "games": """
        CREATE TABLE IF NOT EXISTS games (
            game_id INT PRIMARY KEY,
            team1_id INT,
            team2_id INT,
            map VARCHAR(255),
            winner_id INT,
            w_score INT,
            l_score INT,
            CONSTRAINT fk_games_team1 FOREIGN KEY (team1_id) REFERENCES teams(team_id),
            CONSTRAINT fk_games_team2 FOREIGN KEY (team2_id) REFERENCES teams(team_id),
            CONSTRAINT fk_games_winner FOREIGN KEY (winner_id) REFERENCES teams(team_id)
        )
    """,
    "stats": """
        CREATE TABLE IF NOT EXISTS stats (
            player_id INT,
            game_id INT,
            agent_id INT,
            rating FLOAT,
            acs INT,
            kills INT,
            deaths INT,
            assists INT,
            kast_percent INT,
            adr INT,
            hs_percent INT,
            first_kills INT,
            first_deaths INT,
            PRIMARY KEY (game_id, player_id),
            CONSTRAINT fk_stats_players FOREIGN KEY (player_id) REFERENCES players(player_id),
            CONSTRAINT fk_stats_games FOREIGN KEY (game_id) REFERENCES games(game_id),
            CONSTRAINT fk_stats_agents FOREIGN KEY (agent_id) REFERENCES agents(agent_id)
        )
    """
}

# --- MAIN FUNCTION ---

def setup_valorant_db_strict():
    """
    Connects to MySQL, creates tables WITH constraints, and loads data
    in a strict, required order.
    """
    try:
        with mysql.connector.connect(**db_config) as connection:
            cursor = connection.cursor()
            print("✅ Connection to MySQL successful.")

            print("\n--- Creating Tables with Foreign Key Constraints ---")
            # IMPORTANT: We must create tables in order of dependency
            table_creation_order = ['teams', 'agents', 'players', 'games', 'stats']
            for table_name in table_creation_order:
                print(f"Creating table '{table_name}'...")
                cursor.execute(create_tables_sql[table_name])
            print("✅ All tables created successfully.")

            print("\n--- Loading Data from CSVs (in strict order) ---")
            
            # CRITICAL: The loading order MUST match the table dependencies
            files_to_load = [
                {'name': 'teams', 'file': 'teams.csv', 'query': 'INSERT INTO teams (team_id, team_name) VALUES (%s, %s)'},
                {'name': 'agents', 'file': 'agents.csv', 'query': 'INSERT INTO agents (agent_id, agent_name, role) VALUES (%s, %s, %s)'},
                {'name': 'players', 'file': 'players.csv', 'query': 'INSERT INTO players (player_id, player_name, team_id) VALUES (%s, %s, %s)'},
                {'name': 'games', 'file': 'games.csv', 'query': 'INSERT INTO games (game_id, team1_id, team2_id, map, winner_id, w_score, l_score) VALUES (%s, %s, %s, %s, %s, %s, %s)'},
                {'name': 'stats', 'file': 'stats.csv', 'query': 'INSERT INTO stats (player_id, game_id, agent_id, rating, acs, kills, deaths, assists, kast_percent, adr, hs_percent, first_kills, first_deaths) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'}
            ]

            for item in files_to_load:
                print(f"Loading data for '{item['name']}'...")
                df = pd.read_csv(item['file'])
                
                if item['name'] == 'stats':
                    df.rename(columns={'player_id_y': 'player_id', 'kill': 'kills', 'death': 'deaths', 'assist': 'assists', 'kast%': 'kast_percent', 'hs%': 'hs_percent', 'fk': 'first_kills', 'fd': 'first_deaths'}, inplace=True)
                    df['kast_percent'] = df['kast_percent'].str.replace('%', '').astype(int)
                    df['hs_percent'] = df['hs_percent'].str.replace('%', '').astype(int)
                    df = df[['player_id', 'game_id', 'agent_id', 'rating', 'acs', 'kills', 'deaths', 'assists', 'kast_percent', 'adr', 'hs_percent', 'first_kills', 'first_deaths']]

                data_tuples = [tuple(row) for row in df.itertuples(index=False)]
                cursor.executemany(item['query'], data_tuples)
                connection.commit()
                print(f"✅ Loaded {cursor.rowcount} records into '{item['name']}'.")

            print("✅ All data loaded successfully.")
            cursor.close()
            print("\nDatabase setup is complete!")

    except FileNotFoundError as e:
        print(f"❌ Error: The file '{e.filename}' was not found. Ensure it's in the same folder as the script.")
    except Error as e:
        if e.errno == 1452: # Foreign key constraint fails
            print(f"❌ Foreign Key Error: A row in your CSV file refers to an ID that does not exist in a parent table. Error: {e}")
        elif e.errno == 2003: print("❌ Error: Could not connect to the MySQL server. Is it running?")
        elif e.errno == 1045: print("❌ Error: Access denied. Please check your username and password.")
        elif e.errno == 1049: print(f"❌ Error: Database '{db_config['database']}' does not exist.")
        else: print(f"❌ A database error occurred: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")

if __name__ == "__main__":
    setup_valorant_db_strict()