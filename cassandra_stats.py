from cassandra.cluster import Cluster
import uuid
from datetime import datetime

# Connect to Cassandra cluster and keyspace
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('battle_legends')

# Prepared statement for inserting player stats
insert_query = """
INSERT INTO player_statistics (
    player_id, season_id, match_id, total_kills, total_deaths, total_assists,
    matches_played, wins, losses, accuracy, headshots, longest_kill_streak,
    total_time_played, last_active, rank, average_score_per_match, damage_dealt,
    damage_taken, achievements
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""
prepared_insert = session.prepare(insert_query)

# Insert example data
session.execute(prepared_insert, (
    1234,
    'Season2025',
    uuid.uuid1(),
    15,
    3,
    7,
    50,
    30,
    20,
    0.78,
    10,
    5,
    3600000,
    datetime.utcnow(),
    'Gold',
    1500.5,
    12000,
    9000,
    set(['Sharpshooter', 'MVP'])
))
print("Insert successful!")

# Prepared statement for querying player stats
select_query = """
SELECT * FROM player_statistics WHERE player_id=? AND season_id=? LIMIT 5
"""
prepared_select = session.prepare(select_query)
rows = session.execute(prepared_select, (1234, 'Season2025'))

# Consume iterator into list to reuse results
rows_list = list(rows)

print("\nRecent Matches:")
for row in rows_list:
    print(f"Match {row.match_id}: Kills={row.total_kills}, Deaths={row.total_deaths}, Assists={row.total_assists}")

# Prepared statement for updating achievements (adding new achievement)
update_query = """
UPDATE player_statistics SET achievements = achievements + ?
WHERE player_id=? AND season_id=? AND match_id=?
"""
prepared_update = session.prepare(update_query)

# Use first row from list for update/delete if exists
row_to_update = rows_list[0] if rows_list else None

if row_to_update:
    session.execute(prepared_update, (set(['Unstoppable']), 1234, 'Season2025', row_to_update.match_id))
    print("\nUpdate successful!")

    # Prepared statement for deleting a match record
    delete_query = """
    DELETE FROM player_statistics WHERE player_id=? AND season_id=? AND match_id=?
    """
    prepared_delete = session.prepare(delete_query)

    session.execute(prepared_delete, (1234, 'Season2025', row_to_update.match_id))
    print("Delete successful!")
else:
    print("No rows found to update or delete.")

# Close connection
cluster.shutdown()
