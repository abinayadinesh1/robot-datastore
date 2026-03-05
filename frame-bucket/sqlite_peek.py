import sqlite3
import sys

robot_id = sys.argv[1] if len(sys.argv) > 1 else "reachy-003"
print(f"=== {robot_id} ===\n")
conn = sqlite3.connect(f'data/{robot_id}.db')
conn.row_factory = sqlite3.Row
rows = conn.execute('''
    SELECT id, type, s3_key, substr(description, 1, 80) as desc_preview
    FROM segments WHERE description IS NOT NULL
    ORDER BY id DESC LIMIT 10
''').fetchall()
if rows:
    for r in rows:
        rid, rtype, rkey, rdesc = r["id"], r["type"], r["s3_key"][:50], r["desc_preview"]
        print(f"{rid:4d} | {rtype:6s} | {rkey} | {rdesc}")
else:
    print('No annotated segments found yet.')

# Coverage stats
stats = conn.execute('''
    SELECT type, COUNT(*) as total,
           SUM(CASE WHEN description IS NOT NULL THEN 1 ELSE 0 END) as annotated
    FROM segments GROUP BY type
''').fetchall()
print('\n--- Coverage ---')
for s in stats:
    print(f'{s[0]:6s}: {s[2]}/{s[1]} annotated')
conn.close()