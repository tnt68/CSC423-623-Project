import sqlite3
import pandas as pd

db_conn = sqlite3.connect('test3.db')
cursor = db_conn.cursor()

query="""
   CREATE TABLE Employee (
   StaffNumber INTEGER PRIMARY KEY,
   FirstName TEXT NOT NULL,
   LastName TEXT NOT NULL,
   Address TEXT NOT NULL,
   Salary REAL NOT NULL CHECK (Salary > 0),
   TelephoneNumber TEXT NOT NULL
);
"""
cursor.execute(query)

query = """
   CREATE TABLE Client (
   ClientNumber INTEGER PRIMARY KEY,
   FirstName TEXT NOT NULL,
   LastName TEXT NOT NULL,
   Address TEXT NOT NULL,
   TelephoneNumber TEXT NOT NULL
);
"""
cursor.execute(query)

query = """
   CREATE TABLE Equipment (
   EquipmentId INTEGER PRIMARY KEY,
   Description TEXT NOT NULL,
   Usage TEXT NOT NULL,
   Cost REAL NOT NULL CHECK (Cost > 0)
);
"""
cursor.execute(query)

query = """
   CREATE TABLE Request (
   RequestId INTEGER PRIMARY KEY,
   ClientNumber INTEGER NOT NULL,
   StartDate DATE CHECK (StartDate > '2023-12-06') NOT NULL,
   DayOfWeek TEXT NOT NULL,
   StartTime TEXT NOT NULL,
   EndTime TEXT NOT NULL,
   Comments TEXT,
   FOREIGN KEY (ClientNumber) REFERENCES Client(ClientNumber),
   CHECK (strftime('%H:%M', StartTime) < strftime('%H:%M', EndTime))
);
"""
cursor.execute(query)

# 2 Intermediate/junction tables to create the many-to-many relationships

query = """
   CREATE TABLE RequestEmployee (
   RequestId INTEGER NOT NULL,
   StaffNumber INTEGER NOT NULL,
   PRIMARY KEY (RequestId, StaffNumber),
   FOREIGN KEY (RequestId) REFERENCES Request(RequestId),
   FOREIGN KEY (StaffNumber) REFERENCES Employee(StaffNumber)
);
"""
cursor.execute(query)

query = """
   CREATE TABLE RequestEquipment (
   RequestId INTEGER NOT NULL,
   EquipmentId INTEGER NOT NULL,
   PRIMARY KEY (RequestId, EquipmentId),
   FOREIGN KEY (RequestId) REFERENCES Request(RequestId),
   FOREIGN KEY (EquipmentId) REFERENCES Equipment(EquipmentId)
);
"""
cursor.execute(query)

query =  """INSERT INTO Employee (StaffNumber, FirstName, LastName, Address, Salary, TelephoneNumber) VALUES


(565648, 'John', 'Snow', '1000 NE 154th st North Miami', 45000, '305-658-9542'),
(565649, 'Jane', 'done', '1030 NE 155th st West Miami', 55000, '305-658-9543'),
(565640, 'Tyler', 'Ross', '1200 NW 154th st South Miami', 60000, '305-658-8542'),
(428643, 'Sara', 'Stryker', '1500 N 153rd st Miami', 60000, '305-758-8931'),
(546231, 'Daneil', 'Brown', '1625 SW 170th st Everglades', 80000, '654-925-1000')
"""

cursor.execute(query)

query = """
   SELECT *
   FROM Employee
   """
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

query =  """INSERT INTO Client (ClientNumber, FirstName, LastName, Address, TelephoneNumber) VALUES


(232879, 'Bob', 'Winter', '1756 SE 102nd st Miami Beach', '305-578-2742'),
(256898, 'Nathan', 'Summers', '1459 W 175th st Tampa', '395-645-9543'),
(236547, 'Dylan', 'Jones', '1357 N 150th st Homestead', '305-358-2539'),
(289754, 'Sara', 'Falls', '1800 SW 168th st Miami', '305-205-3231'),
(283523, 'Everette', 'Silver', '3125 SW 50th st Jacksonville', '623-945-1452')
"""

cursor.execute(query)

query = """
   SELECT *
   FROM Client
   """
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

query =  """INSERT INTO Equipment (EquipmentId, Description, Usage, Cost) VALUES


(1564, 'Vacuum Cleaner', 'Cleaning dirt off the floor', 15.56),
(1245, 'Pressure Cleaner', 'Cleaning hard to remove dirt', 54.32),
(4568, 'Glass Cleaner', 'Keeping the glass spotless', 22.32),
(1745, 'Disinfectant', 'Kills Germs in order to keep the area sanitary', 45.00),
(7890, 'Floor Buffer', 'Polishing and maintaining smooth floors', 75.99)


"""
cursor.execute(query)

query = """
   SELECT *
   FROM Equipment
   """
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

query =  """INSERT INTO Request (RequestId, ClientNumber, StartDate, DayOfWeek, StartTime, EndTime, Comments) VALUES


(9865142, 232879, '2025-10-15', 'Monday', '11:45 AM', '1:45 PM', 'Cleaning session'),
(9876231, 256898, '2025-08-12', 'Monday', '12:30 PM', '2:30 PM', 'Maintenance work'),
(9854214, 236547, '2025-01-04', 'Friday', '9:00 AM', '11:00 AM', 'Regular cleaning'),
(9842132, 289754, '2026-03-27', 'Wednesday', '8:00 PM', '10:00 PM', 'Special event setup'),
(9832154, 283523, '2027-08-01', 'Thursday', '7:00 AM', '11:00 AM', 'Office cleaning')
"""
cursor.execute(query)


query = """
   SELECT *
   FROM Request
   """
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# 2 intermediate tables to create the many-to-many relationships between equipment-request and staff-request

query = """
   INSERT INTO RequestEmployee (RequestId, StaffNumber) VALUES
   (9865142, 565648),
   (9876231, 565649),
   (9854214, 565640),
   (9842132, 546231),
   (9832154, 546231),
   (9842132, 428643),
   (9876231, 428643);
"""
cursor.execute(query)

query = """
   SELECT *
   FROM RequestEmployee
   """
cursor.execute(query)


column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

query = """
   INSERT INTO RequestEquipment (RequestId, EquipmentId) VALUES
   (9865142, 7890),
   (9876231, 1564),
   (9854214, 1245),
   (9842132, 4568),
   (9832154, 1745),
   (9842132, 1745),
   (9842132, 7890),
   (9854214, 4568);
"""
cursor.execute(query)

query = """
   SELECT *
   FROM RequestEquipment
   """
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# Identify clients who have made requests for equipment with a cost greater than $50:
query = """
   SELECT DISTINCT C.FirstName, C.LastName, C.TelephoneNumber
   FROM Client C
   JOIN Request R ON C.ClientNumber = R.ClientNumber
   JOIN RequestEquipment RE ON R.RequestId = RE.RequestId
   JOIN Equipment E ON RE.EquipmentId = E.EquipmentId
   WHERE E.Cost > 50;
"""
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# Find the equipment that has been requested the most and order the usage in descending order:
query = """
   SELECT E.Description, COUNT(RE.EquipmentId) AS RequestCount
   FROM Equipment E
   JOIN RequestEquipment RE ON E.EquipmentId = RE.EquipmentId
   GROUP BY E.EquipmentId
   ORDER BY RequestCount DESC;
"""
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# Retrieve the staff names of clients whose phone number starts with '305':
query = """
   SELECT DISTINCT E.FirstName, E.LastName
   FROM Employee E
   JOIN RequestEmployee RE ON E.StaffNumber = RE.StaffNumber
   JOIN Request R ON RE.RequestId = R.RequestId
   JOIN Client C ON R.ClientNumber = C.ClientNumber
   WHERE C.TelephoneNumber LIKE '305%';
"""
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# Find all staff who have been assigned to a request that involves cleaning equipment:
query = """
   SELECT DISTINCT E.FirstName, E.LastName, E.Salary
   FROM Employee E
   JOIN RequestEmployee RE ON E.StaffNumber = RE.StaffNumber
   JOIN Request R ON RE.RequestId = R.RequestId
   JOIN RequestEquipment REQ ON R.RequestId = REQ.RequestId
   JOIN Equipment EQ ON REQ.EquipmentId = EQ.EquipmentId
   WHERE EQ.Usage LIKE '%cleaning%';
"""
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

# Average Cost of Equipment Used by Each Employee:
query = """
   SELECT E.FirstName, E.LastName, AVG(Equip.Cost) AS AvgEquipmentCost
   FROM Employee E
   JOIN RequestEmployee ReqEmp ON E.StaffNumber = ReqEmp.StaffNumber
   JOIN Request R ON ReqEmp.RequestId = R.RequestId
   JOIN RequestEquipment ReqEquip ON R.RequestId = ReqEquip.RequestId
   JOIN Equipment Equip ON ReqEquip.EquipmentId = Equip.EquipmentId
   GROUP BY E.StaffNumber
   ORDER BY AvgEquipmentCost DESC;
"""
cursor.execute(query)

column_names = [row[0] for row in cursor.description]

table_data = cursor.fetchall()
df = pd.DataFrame(table_data, columns=column_names)

print(df)
print(df.columns)

