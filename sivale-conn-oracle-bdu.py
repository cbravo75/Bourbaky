import getpass
import oracledb

pw = getpass.getpass("Enter password: ")

connection = oracledb.connect(
    user="cbravo",
    password="1S7JEST6PnC8+)Z",
    dsn="172.16.161.41/DBFEDEV")

print("Successfully connected to Oracle Database")

cursor = connection.cursor()

# Create a table

cursor.execute("""
    create table bdusivale.CBVTMP (
        id number generated always as identity,
        description varchar2(4000),
        creation_ts timestamp with time zone default current_timestamp,
        done number(1,0),
        primary key (id))""")

# Insert some data

rows = [ ("Task 1", 0 ),
         ("Task 2", 0 ),
         ("Task 3", 1 ),
         ("Task 4", 0 ),
         ("Task 5", 1 ) ]

cursor.executemany("insert into todoitem (description, done) values(:1, :2)", rows)
print(cursor.rowcount, "Rows Inserted")

connection.commit()

qryClientes = """
        select a.compania, count(*)
        from bdusivale.joel_fiserv_datosdemog_veh a
        group by a.compania
        order by 2
        """)

# Now query the rows back
for row in cursor.execute(qryClientes):
        print("No de Cliente: ",row[0])
