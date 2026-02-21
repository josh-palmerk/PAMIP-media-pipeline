from storage.database import Database

db = Database()

with db.get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("INSERT INTO jobs (filename, status) VALUES (?, ?)", 
                   ("test.mp4", "pending"))
    conn.commit()

    cursor.execute("SELECT * FROM jobs;")
    rows = cursor.fetchall()
    for row in rows:
        print(dict(row))