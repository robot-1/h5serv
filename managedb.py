import sqlite3

class Manage(sqlite3.Connection):
    def __init__(self,dbname):
        super(Manage,self).__init__(dbname)
        self.__cur = self.cursor()
        self.__cur.execute('PRAGMA foreign_keys = ON')

    def __exec_query(self, query):
        self.__cur.execute(query)

    def create_table(self, query):
        self.__exec_query(query)
        self.commit()

    def get_all_addr(self):
        self.__exec_query("SELECT * FROM locations")
        return self.__cur.fetchall()

    def check_loc(self, lat, lon):
        try:
            query = """SELECT description FROM locations WHERE lat=? AND lon=?"""
            self.__cur.execute(query,(lat,lon,))
            description = self.__cur.fetchone()
            self.close()
            return description
        except:
            self.close()
            return None

    def save_location(self, lat, lon, location):
        query = '''INSERT INTO locations(lat, lon, description) VALUES(?,?,?)'''
        self.__cur.execute(query, (lat, lon, location))
        self.commit()
        self.close()
    
    def add_user(self, uname, organization, key, token):
        try:
            query = '''INSERT INTO owners(username, organization, pk, token, privacy) VALUES(?,?,?,?,?)'''
            self.__cur.execute(query, (uname, organization, key, token, 0))
            self.commit()
            self.close()
            return 1
        except:
            return 0

    def get_user(self, uname):
        query = '''SELECT pk FROM owners WHERE username=?'''
        self.__cur.execute(query,(uname,))
        return self.__cur.fetchone()

    def insert_data(userid, date, sensor):
        query = '''INSERT INTO measurements(measurement_id, date, sensor_type) VALUES(?,?,?)'''
        self.__cur.execute(query, (userid, date, sensor)) 
        self.__cur.commit()
        self.close()

    def validate_token(self, token):
        query = '''SELECT owner_id FROM owners WHERE token=?'''
        self.__cur.execute(query, (token)) 
        return self.__cur.fetchone()

if __name__ == "__main__":
    main_groups = ['station', 'adhoc']
    sub_groups = ['public', 'private']
    location_dbname = 'addresses.db'
    measurement_dbname = 'measurements.db'
    db = Manage(measurement_dbname)
    #measurements.db
    query = """CREATE TABLE IF NOT EXISTS owners(
            owner_id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            organization TEXT NOT NULL,
            school TEXT,
            pk TEXT NOT NULL UNIQUE,
            token TEXT NOT NULL UNIQUE,
            privacy INTEGER NOT NULL)"""
    db.create_table(query)
    query = """CREATE TABLE IF NOT EXISTS measurements(
            measurement_id INTEGER NOT NULL,
            date INTEGER NOT NULL,
            sensor_type TEXT,
            FOREIGN KEY(measurement_id) REFERENCES owners(owner_id))"""
    db.create_table(query)
    query = """CREATE TABLE IF NOT EXISTS groups(
            group_id INTEGER NOT NULL,
            group_path TEXT NOT NULL,
            FOREIGN KEY(group_id) REFERENCES measurements(measurement_id))"""

    db.create_table(query)
    query = """CREATE TABLE IF NOT EXISTS locations(
            location_id INTEGER NOT NULL,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            description TEXT NOT NULL,
            FOREIGN KEY(location_id) REFERENCES measurements(measurement_id))"""
    db.create_table(query)
    db.close()

