import pymysql
from pymysql import Error

class DB:
    def __init__(self):
        try:
            self.conn = pymysql.connect(
                host="bzd0ukpckhpgzbo1f6w9-mysql.services.clever-cloud.com",
                user="uzjqi9fmpbwhdkky",
                password="2NSrCLcU3SeS1Yh6KTXg",
                database="bzd0ukpckhpgzbo1f6w9",
                port=3306
            )
            self.cursor = self.conn.cursor()
            print("✅ Connection Established")
        except Error as e:
            print("❌ Connection Error:", e)

    def fetch_city_names(self):
        try:
            query = """
                SELECT DISTINCT source AS city FROM flights
                UNION
                SELECT DISTINCT destination AS city FROM flights;
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            city_list = [row[0] for row in result]
            return city_list
        except Exception as e:
            print("❌ Query error:", e)
            return []
        
    def fetch_all_flights(self,source,destination):

        self.cursor.execute("""
        SELECT Airline,Route,Dep_Time,Duration,Price FROM flights
        WHERE Source = '{}' AND Destination = '{}'
        """.format(source,destination))

        data = self.cursor.fetchall()

        return data

        
    def fetch_airline_frequency(self):

        airline = []
        frequency = []

        self.cursor.execute("""
        SELECT Airline,COUNT(*) FROM flights
        GROUP BY Airline
        """)

        data = self.cursor.fetchall()

        for item in data:
            airline.append(item[0])
            frequency.append(item[1])

        return airline,frequency
    

    def busy_airport(self):

        city = []
        frequency = []

        self.cursor.execute("""
        SELECT Source,COUNT(*) FROM (SELECT Source FROM flights
							UNION ALL
							SELECT Destination FROM flights) t
        GROUP BY t.Source
        ORDER BY COUNT(*) DESC
        """)

        data = self.cursor.fetchall()

        for item in data:
            city.append(item[0])
            frequency.append(item[1])

        return city, frequency

    def daily_frequency(self):

        date = []
        frequency = []

        self.cursor.execute("""
        SELECT Date_of_Journey,COUNT(*) FROM flights
        GROUP BY Date_of_Journey
        """)

        data = self.cursor.fetchall()

        for item in data:
            date.append(item[0])
            frequency.append(item[1])

        return date, frequency
        
