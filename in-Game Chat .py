from cassandra.cluster import Cluster
from datetime import datetime

# Step 1: Connect to Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('test')  # Replace 'test' if you used another keyspace

# Step 2: Insert a chat message related to the game's team-based battle
insert_query = """
INSERT INTO in_game_chat (game_id, channel, message_time, sender_id, sender_name, message)
VALUES (%s, %s, %s, %s, %s, %s)
"""

# Example: player sending a tactical message in the team chat during a game
session.execute(insert_query, (
    205,                      # game_id for "Dragon Fortress Match"
    'team_alpha',            # in-game chat channel for team Alpha
    datetime.now(),          # current timestamp
    9012,                    # player ID
    'ShadowHunter',          # player username
    'Regroup at the north tower — enemies approaching!'  # the message
))

# Step 3: Retrieve messages for this game and channel
select_query = """
SELECT * FROM in_game_chat WHERE game_id = 205 AND channel = 'team_alpha'
"""

rows = session.execute(select_query)

# Step 4: Display chat log
print("\n Team Alpha Chat Log (Dragon Fortress Match):")
for row in rows:
    print(f"{row.message_time} | {row.sender_name}: {row.message}")
