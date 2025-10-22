import asyncio
import asyncpg
import time

POSTGRES_URL = "postgres server url, either vercel or any"

TOKEN_TABLES = [
    'source_1', 'source_2', 'source_3', 'source_4',
    'source_5', 'source_6', 'source_7', 'source_8'
]

async def prune_table(conn, table):
    # Count total rows
    row_count = await conn.fetchval(f'SELECT COUNT(*) FROM {table}')
    if row_count is None or row_count <= 100:
        print(f"{table}: Only {row_count} records, no pruning needed.")
        return

    # Delete all rows NOT in the newest 100 by timestamp using CTE and 'vid' primary key
    deleted = await conn.execute(f'''
        WITH latest_100 AS (
            SELECT vid FROM {table} ORDER BY timestamp DESC LIMIT 100
        )
        DELETE FROM {table} WHERE vid NOT IN (SELECT vid FROM latest_100);
    ''')
    print(f"{table}: Pruned old records, kept latest 100. {deleted}")

    # Reclaim storage space with VACUUM (non-locking)
    await conn.execute(f'VACUUM {table}')
    print(f"{table}: Vacuum completed.")

async def main():
    conn = await asyncpg.connect(POSTGRES_URL)
    for table in TOKEN_TABLES:
        try:
            await prune_table(conn, table)
        except Exception as e:
            print(f"Error pruning {table}: {e}")
    await conn.close()

if __name__ == "__main__":
    while True:
        asyncio.run(main())
        print("Done pruning and vacuuming all tables, sleeping 30 minutes...")
        time.sleep(1800)
