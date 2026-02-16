import sqlite3
import json

def check_real_db_search():
    conn = sqlite3.connect('data/priceweb.db')
    cursor = conn.cursor()
    
    query = "зазеркалье"
    queries = [query.lower(), query.title(), query.upper()]
    print(f"Testing permutations on REAL DB: {queries}")
    
    found = False
    for q in queries:
        # Search in suppliers_json in items_latest
        sql = "SELECT count(*) FROM items_latest WHERE suppliers_json LIKE ?"
        match = cursor.execute(sql, (f"%{q}%",)).fetchone()[0]
        if match > 0:
            print(f"Match found with '{q}': {match} items")
            found = True
            
    if not found:
        print("No matches found with permutations.")
        # Try to find what IS there
        # We know from previous run it might be there?
        # Let's search for just '%' to verify DB has data
        count = cursor.execute("SELECT count(*) FROM items_latest").fetchone()[0]
        print(f"Total items in DB: {count}")

if __name__ == "__main__":
    check_real_db_search()


