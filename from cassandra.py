from cassandra.cluster import Cluster
from cassandra.query import tuple_factory
from datetime import datetime

# 1. Connect to Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)  # Adjust host/port if needed
session = cluster.connect('battle_legends')  # Use your keyspace
session.row_factory = tuple_factory

print("Connected to Cassandra and keyspace 'battle_legends'.")

# 2. Insert sample data
insert_query = """
INSERT INTO leaderboard_scores (
    game_mode, season_id, snapshot_id, player_id, score, submission_time, details)
VALUES (%s, %s, %s, %s, %s, toTimestamp(now()), %s)
"""

print("\nInserting a sample score...")
session.execute(insert_query, (
    'BattleRoyale', 'Season2025', 'Snapshot_01', 1234, 1750, '{"match_id":303,"kills":5}'
))

# 3. Read/Select top 10 scores
select_query = """
SELECT player_id, score, submission_time
FROM leaderboard_scores
WHERE game_mode=%s AND season_id=%s AND snapshot_id=%s
LIMIT 10
"""

print("\nTop 10 scores for BattleRoyale / Season2025 / Snapshot_01:")
rows = session.execute(select_query, ('BattleRoyale', 'Season2025', 'Snapshot_01'))
for row in rows:
    print(f"Player ID: {row[0]}, Score: {row[1]}, Time: {row[2]}")

# 4. Update details for a specific score submission
# You need to provide the exact primary key including submission_time
# We'll fetch the submission_time from the select above, for demo purposes

update_details = '{"match_id":303,"kills":6,"bonus":"yes"}'
if rows:
    row_to_update = list(rows)[0]  # Take first result as example
    update_query = """
    UPDATE leaderboard_scores SET details=%s
    WHERE game_mode=%s AND season_id=%s AND snapshot_id=%s
    AND score=%s AND player_id=%s AND submission_time=%s
    """
    print(f"\nUpdating details for Player ID {row_to_update[0]}, Score {row_to_update[1]}...")
    session.execute(update_query, (
        update_details,
        'BattleRoyale', 'Season2025', 'Snapshot_01',
        row_to_update[1], row_to_update[0], row_to_update[2]
    ))
else:
    print("No rows found to update.")

# 5. Delete a score submission (demonstration)
# Again, you need the exact primary key
if rows:
    row_to_delete = list(rows)[0]
    delete_query = """
    DELETE FROM leaderboard_scores
    WHERE game_mode=%s AND season_id=%s AND snapshot_id=%s
    AND score=%s AND player_id=%s AND submission_time=%s
    """
    print(f"\nDeleting Player ID {row_to_delete[0]}, Score {row_to_delete[1]}...")
    session.execute(delete_query, (
        'BattleRoyale', 'Season2025', 'Snapshot_01',
        row_to_delete[1], row_to_delete[0], row_to_delete[2]
    ))
else:
    print("No rows found to delete.")

# 6. List all snapshots for the season
print("\nAll leaderboard snapshots for BattleRoyale / Season2025:")
snapshot_query = """
SELECT snapshot_id, snapshot_name, created_at, config
FROM leaderboard_snapshots
WHERE game_mode=%s AND season_id=%s
"""
snapshots = session.execute(snapshot_query, ('BattleRoyale', 'Season2025'))
for snap in snapshots:
    print(f"Snapshot ID: {snap[0]}, Name: {snap[1]}, Time: {snap[2]}, Config: {snap[3]}")

# 7. Clean up
cluster.shutdown()
print("\nConnection to Cassandra closed.")
