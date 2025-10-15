import streamlit as st
import pandas as pd
import mysql.connector

st.title("Stat Track")

#Connect to database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="stattrack"
)
cursor = conn.cursor(dictionary=True)

# Tabs
tabs = st.tabs(["Teams", "Players", "Agents", "Games", "Player Stats"])

# TEAMS TAB
with tabs[0]:
    st.header("Manage Teams")
    
    # CREATE TEAM
    with st.form("create_team"):
        st.subheader("Create Team")
        team_name = st.text_input("Team Name")

        st.write("Add Players (at least 5 required)")
        player_names = []
        st.caption("Blank names are not allowed")
        for i in range(5):
            player_name = st.text_input(f"Player {i+1}")
            if(player_name.strip()):
                player_names.append(player_name.strip())
                

        extra_player_count = st.number_input("Add extra players?", min_value=0, max_value=2, step=1)
        for i in range(extra_player_count):
            player_name = st.text_input(f"Extra Player {i+6} Name")
            player_names.append(player_name)

        submitted = st.form_submit_button("Add Team")

        if submitted:
            if not team_name:
                st.error("Please enter a team name.")
            elif len(player_names) < 5:
                st.error("A team must have at least 5 players.")
            else:
                try:
                    cursor.execute("SELECT MAX(team_id) FROM teams")
                    result = cursor.fetchone()["MAX(team_id)"]
                    max_team_id = result if result is not None else 100
                    new_team_id = max_team_id + 1

                    cursor.execute("SELECT MAX(player_id) FROM players")
                    result = cursor.fetchone()["MAX(player_id)"]
                    max_player_id = result if result is not None else 1000

                    conn.start_transaction()
                    cursor.execute(
                        "INSERT INTO teams (team_id, team_name) VALUES (%s, %s)",
                        (new_team_id, team_name)
                    )

                    for i, player_name in enumerate(player_names):
                        cursor.execute(
                            "INSERT INTO players (player_id, player_name, team_id) VALUES (%s, %s, %s)",
                            (max_player_id + i + 1, player_name, new_team_id)
                        )

                    conn.commit()
                    st.success(f"Team '{team_name}' and {len(player_names)} players added successfully!")

                except mysql.connector.Error as e:
                    conn.rollback()
                    st.error(f"Transaction failed: {e.msg} (Error code: {e.errno})")

    # READ TEAMS
    st.subheader("Teams")
    cursor.execute("SELECT * FROM teams")
    teams_df = pd.DataFrame(cursor.fetchall())
    st.dataframe(teams_df.reset_index(drop = True), use_container_width=True)

    # UPDATE / DELETE
    with st.form("update_team"):
        team_ids = teams_df['team_id'].tolist()
        selected_team = st.selectbox("Select a team to Edit/Delete", team_ids)
        new_name = st.text_input("New Team Name")
        if st.form_submit_button("Update Team"):
            cursor.execute("UPDATE teams SET team_name=%s WHERE team_id=%s", (new_name, selected_team))
            conn.commit()
            st.success("Team updated successfully!")
        if st.form_submit_button("Delete Team"):
            cursor.execute("DELETE FROM teams WHERE team_id=%s", (selected_team,))
            conn.commit()
            st.success("Team deleted successfully!")

# ------------------- PLAYERS -------------------
with tabs[1]:
    st.header("Manage Players")
    
    # CREATE PLAYER
    cursor.execute("SELECT * FROM teams")
    teams_df = pd.DataFrame(cursor.fetchall())
    team_options = teams_df.set_index('team_id')['team_name'].to_dict()
    
    with st.form("create_player"):
        player_name = st.text_input("Player Name")
        team_id = st.selectbox("Select Team", options=list(team_options.keys()), format_func=lambda x: team_options[x])
        submitted = st.form_submit_button("Add Player")
        if submitted and player_name:
            cursor.execute("SELECT MAX(player_id) FROM players;")
            result = cursor.fetchone()["MAX(player_id)"]
            max_player_id = result if result is not None else 1000
            cursor.execute("INSERT INTO players (player_id, player_name, team_id) VALUES (%s, %s, %s)", (max_player_id+1, player_name, team_id))
            conn.commit()
            st.success(f"Player '{player_name}' added successfully!")
    
    # READ PLAYERS
    st.subheader("Players")
    cursor.execute("""
        SELECT p.player_id, p.player_name, t.team_name
        FROM players p
        LEFT JOIN teams t ON p.team_id = t.team_id
    """)
    players_df = pd.DataFrame(cursor.fetchall())
    st.dataframe(players_df.reset_index(drop = True), use_container_width=True)

# ------------------- AGENTS -------------------
with tabs[2]:
    st.header("Manage Agents")
    
    # CREATE AGENT
    with st.form("create_agent"):
        agent_name = st.text_input("Agent Name")
        role = st.selectbox("Role", options = ['Duelist', 'Initiator', 'Controller', 'Sentinel'])
        submitted = st.form_submit_button("Add Agent")
        if submitted and agent_name:
            cursor.execute("SELECT MAX(agent_id) FROM agents;")
            result = cursor.fetchone()["MAX(agent_id)"]
            max_agent_id = result if result is not None else 0
            cursor.execute("INSERT INTO agents (agent_id, agent_name, role) VALUES (%s, %s, %s)", (max_agent_id+1, agent_name, role))
            conn.commit()
            st.success(f"Agent '{agent_name}' added successfully!")

    # READ AGENTS
    st.subheader("All Agents")
    cursor.execute("SELECT * FROM agents")
    agents_df = pd.DataFrame(cursor.fetchall())
    st.dataframe(agents_df.reset_index(drop = True), use_container_width=True)

#  GAMES
with tabs[3]:
    st.header("Manage Games")
    
    # CREATE GAME
    with st.form("create_game"):
        cursor.execute("SELECT * FROM teams")
        teams_df = pd.DataFrame(cursor.fetchall())
        team_options = teams_df.set_index('team_id')['team_name'].to_dict()
        team1_id = st.selectbox("Team 1", options=list(team_options.keys()), format_func=lambda x: team_options[x])
        team2_id = st.selectbox("Team 2", options=[tid for tid in team_options.keys() if tid != team1_id], format_func=lambda x: team_options[x])
        game_map = st.text_input("Map")
        submitted = st.form_submit_button("Add Game")
        if submitted:
            cursor.execute("INSERT INTO games (team1_id, team2_id, map) VALUES (%s, %s, %s)", (team1_id, team2_id, game_map))
            conn.commit()
            st.success("Game added successfully!")

    # UPDATE GAMES (only games with missing winner or scores)
    st.subheader("Update Games with missing scores/winner")
    cursor.execute("SELECT * FROM games WHERE winner_id IS NULL OR w_score IS NULL OR l_score IS NULL")
    incomplete_games_df = pd.DataFrame(cursor.fetchall())
    st.dataframe(incomplete_games_df.reset_index(drop = True), use_container_width=True)
    
    if not incomplete_games_df.empty:
        game_to_update = st.selectbox("Select Game ID to Update", options=incomplete_games_df['game_id'].tolist())
        winner_id = st.selectbox("Winner", options=[team1_id, team2_id], format_func=lambda x: team_options[x])
        w_score = st.number_input("Winning Team Score", min_value=0, step=1)
        l_score = st.number_input("Losing Team Score", min_value=0, step=1)
        if st.button("Update Game Scores/Winner"):
            cursor.execute("UPDATE games SET winner_id=%s, w_score=%s, l_score=%s WHERE game_id=%s",
                           (winner_id, w_score, l_score, game_to_update))
            conn.commit()
            st.success("Game updated successfully!")
