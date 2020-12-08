"""
Create database to house assignment datasets

"""

###  Create the Initial Database

import psycopg2
import csv

try:
    dbConnection  = psycopg2.connect(user = "dap",
                                    password = "dap",
                                    host = "192.168.56.30",
                                    port = "5432",
                                    database = "postgres")
    dbConnection.set_isolation_level(0) #AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute('CREATE DATABASE SchoolData;')
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print("Error while connecting to postgresql: ", dbError)
else:
    print("connection successful")
finally:
    if(dbConnection):
        dbConnection.close()


#Create table for Primary Schools data

## want to achieve 1NF
## step 1: Atomise the Principal Name Field

"""
Need to do some data prep since we have single quoes in the data and Postgresql requires double single quotes here
We also manage missing values in various ways
For 1NF we also atomise some fields 

"""

totalPrimary["Principal NameA"] = totalPrimary["Principal Name"].str.replace(
    "Acting Principal,|UAS.|Ms|Ms.|MRS.|Mrs|Mr|MR.|DR.|Dr.|Dr|Sr.|SR.|REV.|MR.|Mrs.|MS.|FR.|MISS|", "")
totalPrimary["Principal NameA1"] = totalPrimary["Principal NameA"].astype(str).str.replace("^ ", "")

totalPrimary["Principal NameB"] = totalPrimary["Principal Name"].astype(str).str.replace("'", "''")
totalPrimary[["Principal First Name", "Principal Last Name"]] = totalPrimary["Principal NameB"].str.split(" ", 1, expand = True,)
totalPrimary['Male'].fillna(0, inplace = True)
totalPrimary['Female'].fillna(0, inplace = True)

totalPrimary['Latitude'] = totalPrimary['Latitude'].replace('.', 999)
totalPrimary['Longitude'] = totalPrimary['Longitude'].replace('.', 999)

totalPrimary['Official NameB'] = totalPrimary['Official Name'].astype(str).str.replace(r"'", "''")
totalPrimary['Address (Line 1)B'] = totalPrimary['Address (Line 1)'].str.replace(r"'", r"''", regex = True)
totalPrimary['Address (Line 2)B'] = totalPrimary['Address (Line 2)'].str.replace(r"'", r"''", regex = True)
totalPrimary['Address (Line 3)B'] = totalPrimary['Address (Line 3)'].str.replace(r"'", r"''", regex = True)

"""

1. Primary School Data

"""



"""
Create table for Primary schools

"""

createString = """CREATE TABLE primaryschools(
Roll_Number varchar(20) PRIMARY KEY,
SchoolName varchar(150),
Address1 varchar(150),
Address2 varchar(150),
Address3  varchar(150),
Address4 varchar(150),
County varchar(25),
Local_Auth varchar(50),
Phone_No bigint,
PrincipalFirstName varchar(150),
PrincipalLastName varchar(150),
Email varchar(45),
Eircode varchar(12),
Gaeltacht_Indicator Char(2),
DEIS char(2),
Island char(2),	
Irish_Classification varchar(45),
Ethos varchar(20),	
FemaleN numeric(7,2),	
MaleN numeric(7,2),
TotalN numeric(7,2),
Latitude numeric(12,8),
Longitude numeric(12,8) 
);"""

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createString)
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print("Error while connecting to postgresql: ", dbError)
else:
    print("connection successful")
finally:
    if (dbConnection):
        dbConnection.close()

    """
    Populate table from dataframe
    We need to use the dataframe since there are non standard characters in the dataset- UTF8 isnt handling these well
    when exported to csv so we'll keep them internal for now to prevent them being corrupted pre any fur=ture cross table joins
    
    """

    # Import pandas dataframe into postgresql



try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()

    for index, row in totalPrimary.iterrows():

        query = r"""INSERT INTO primaryschools VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
        row['Roll Number'], row['Official NameB'],
        row['Address (Line 1)B'], row['Address (Line 2)B'], row['Address (Line 3)B'],
        row['Address (Line 4)'],
        row['County Description'], row['Local Authority Description'], row['Phone No.'],
        row['Principal First Name'], row['Principal Last Name'],
        row['Email'], row['Eircode'], row['Gaeltacht Indicator (Y/N)'],
        row['DEIS (Y/N)'], row['Island (Y/N)'], row['Irish Classification Description'],
        row['Ethos Description'], row['Female'], row['Male'], row['Total'],
        row['Latitude'], row['Longitude'])

        dbCursor.execute(query)

        query2 = """UPDATE primaryschools
                    SET address3 = NULL 
                    WHERE address3 = 'nan';
                    UPDATE primaryschools
                    SET address4 = NULL 
                    WHERE address4 = 'nan';"""
        dbCursor.execute(query2)

    dbCursor.close()

except (Exception, psycopg2.Error) as dbError:
    print("Error:", dbError)
finally:
    if (dbConnection):
        dbConnection.close()




"""

2. Secondary School Data

"""

"""
Create table for Secondary schools

"""


createString = """CREATE TABLE secondaryschools(
Roll_Number varchar(20) PRIMARY KEY,
SchoolName varchar(150),
Address1 varchar(150),
Address2 varchar(150),
Address3  varchar(150),
Address4 varchar(150),
County varchar(50),
Local_Auth varchar(50),
Phone_No varchar(25),
PrincipalFirstName varchar(150),
PrincipalLastName varchar(150),
Email varchar(45),
Eircode varchar(12),
Gaeltacht_Indicator Char(2),
DEIS char(2),
Island char(2),	
Feepaying char(2),	
Attendancetype varchar(75),
gender char(8),
Irish_Classification varchar(75),
schoolType varchar(75),
Ethos varchar(20),	
FemaleN numeric(7,2),	
MaleN numeric(7,2),
TotalN numeric(7,2),
Latitude numeric(12,8),
Longitude numeric(12,8) 
);"""

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createString)
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print("Error while connecting to postgresql: ", dbError)
else:
    print("connection successful")
finally:
    if (dbConnection):
        dbConnection.close()

    """
    Populate table from dataframe
    We need to use the dataframe since there are non standard characters in the dataset- UTF8 isnt handling these well
    when exported to csv so we'll keep them internal for now to prevent them being corrupted pre any future cross table joins

    """

    ## want to achieve 1NF
    ## step 1: Atomise the Principal Name Field

    """
    Need to do some data prep since we have single quoes in the data and Postgresql requires double single quotes here
    We also manage missing values in various ways
    For 1NF we also atomise some fields 

    """

    totalSecondary["Principal NameA"] = totalSecondary["Principal Name"].str.replace("Acting Principal,|UAS.|Ms|Ms.|MRS.|Mrs|Mr|MR.|DR.|Dr.|SR.|REV.|MR.|Mrs.|MS.|FR.|MISS|", "")
    totalSecondary["Principal NameA1"] = totalSecondary["Principal NameA"].astype(str).str.replace("^ ", "")

    totalSecondary["Principal NameB"] = totalSecondary["Principal NameA1"].astype(str).str.replace("'", "''")
    totalSecondary[["Principal First Name", "Principal Last Name"]] = totalSecondary["Principal NameB"].str.split(" ", 1, expand=True, )
    totalSecondary['MALE'].fillna(0, inplace=True)
    totalSecondary['FEMALE'].fillna(0, inplace=True)

    totalSecondary['Latitude'] = totalSecondary['Latitude'].replace('.', 999)
    totalSecondary['Longitude'] = totalSecondary['Longitude'].replace('.', 999)

    totalSecondary['Official NameB'] = totalSecondary['Official School Name'].astype(str).str.replace(r"'", "''")
    totalSecondary['Address (Line 1)B'] = totalSecondary['Address 1'].str.replace(r"'", r"''", regex=True)
    totalSecondary['Address (Line 2)B'] = totalSecondary['Address 2'].str.replace(r"'", r"''", regex=True)
    totalSecondary['Address (Line 3)B'] = totalSecondary['Address 3'].str.replace(r"'", r"''", regex=True)

    # Import pandas dataframe into postgresql

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()


    for index, row in totalSecondary.iterrows():
        query = r"""INSERT INTO secondaryschools VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');""" % (
            row['Roll Number'], row['Official NameB'],
            row['Address (Line 1)B'], row['Address (Line 2)B'], row['Address (Line 3)B'],
            row['Address 4'],
            row['County'], row['Local Authority'], row['Phone'],
            row['Principal First Name'], row['Principal Last Name'],
            row['Email'], row['Eircode'], row['Gaeltacht Area Location (Y/N)'],
            row['DEIS (Y/N)'], row['Island Location (Y/N)'],  row['Fee Paying School (Y/N)'],
            row['Pupil Attendance Type'],  row['School Gender - Post Primary'],
            row['Irish Classification - Post Primary'], row['Post Primary School Type'],
            row['Ethos/Religion'], row['FEMALE'], row['MALE'], row['TOTAL (2019-20)'],
            row['Latitude'], row['Longitude'])

        dbCursor.execute(query)


        query2 = """UPDATE secondaryschools
                    SET address3 = NULL 
                    WHERE address3 = 'nan';
                    UPDATE secondaryschools
                    SET address4 = NULL 
                    WHERE address4 = 'nan';"""
        dbCursor.execute(query2)


    dbCursor.close()

except (Exception, psycopg2.Error) as dbError:
    print("Error:", dbError)
finally:
    if (dbConnection):
        dbConnection.close()


"""

2. Secondary School Ranking Data

"""

"""
Create table for schools rankings

"""



createString = """CREATE TABLE ranking(
ranking varchar(5) ,
SchoolName varchar(150),
Address1 varchar(150),
Address2 varchar(150),
Latitude numeric(12,8),
Longitude numeric(12,8) 
);"""

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(createString)
    dbCursor.close()
except (Exception, psycopg2.Error) as dbError:
    print("Error while connecting to postgresql: ", dbError)
else:
    print("connection successful")
finally:
    if (dbConnection):
        dbConnection.close()

    """
    Populate table from dataframe
    We need to use the dataframe since there are non standard characters in the dataset- UTF8 isnt handling these well
    when exported to csv so we'll keep them internal for now to prevent them being corrupted pre any future cross table joins

    Again we need to do some (minimal) data prep since we have single quotes in the data and Postgresql requires double single quotes here
    We also manage missing values in various ways
    For 1NF we also atomise some fields 

    """

##little bit of tidying
    BestSch['NameB'] = BestSch['Name'].astype(str).str.replace(r"'", "''")
    BestSch['Address1B'] = BestSch['Address1'].str.replace(r"'", r"''", regex=True)
    BestSch['Address1c'] = BestSch['Address1B'].str.replace(r'C[A-Z].*', "", regex=True)

    BestSch['Address2b'] = BestSch['Address2'].str.replace(r'\\n.*', "", regex=True)
    BestSch['Address2b'] = BestSch['Address2'].str.replace(r'\\n.*', "", regex=True)
    BestSch['Address2c'] = BestSch['Address2b'].str.replace(r'C[A-Z].*', "", regex=True)
    BestSch['Address2d'] = BestSch['Address2c'].str.replace(r"'", r"''", regex=True)


"""
Results her dont match particularly well to the secondary school dataset - this is mainly due to small character  
differences etc.  To get around this we'll geocode and try to match on that
"""

BestSch['FullAddress'] = BestSch['NameB'] + ',' + BestSch['Address1c'] + ',' + BestSch['Address2d']

rankingsCoord, GeoDataRankings = GoGetter(BestSch['FullAddress'], APIkey = key)

pc = pd.DataFrame(rankingsCoord, columns = ['School', 'FullAddress', 'Latitude', 'Longitude'])
#pc.drop(pc.tail(1).index,inplace=True)

BestSch = pd.merge(BestSch, pc, left_on = BestSch['FullAddress'] , right_on = 'School')

BestSch['Latitude'] = BestSch['Latitude'].replace('.', 999)
BestSch['Longitude'] = BestSch['Longitude'].replace('.', 999)

# Import pandas dataframe into postgresql

try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()


    for index, row in BestSch.iterrows():
        query = r"""INSERT INTO ranking VALUES ('{}','{}','{}','{}','{}','{}');""" .format(
            row['Ranking'], row['NameB'], row['Address1c'], row['Address2d'], row['Latitude'], row['Longitude'])

        dbCursor.execute(query)

    dbCursor.close()

except (Exception, psycopg2.Error) as dbError:
    print("Error:", dbError)
finally:
    if (dbConnection):
        dbConnection.close()

"""
Create a table using CSO population stats data for each county
"""

    createString = """CREATE TABLE population(
    county varchar(25) ,
    pop2011 numeric(12, 4),
    pop2016 numeric(12, 4) 
    );"""

    try:
        dbConnection = psycopg2.connect(user="dap",
                                        password="dap",
                                        host="192.168.56.30",
                                        port="5432",
                                        database="schooldata")
        dbConnection.set_isolation_level(0)  # AUTOCOMMIT
        dbCursor = dbConnection.cursor()
        dbCursor.execute(createString)
        dbCursor.close()
    except (Exception, psycopg2.Error) as dbError:
        print("Error while connecting to postgresql: ", dbError)
    else:
        print("connection successful")
    finally:
        if (dbConnection):
            dbConnection.close()

        """
        Populate table from dataframe
        """

    try:
        dbConnection = psycopg2.connect(user="dap",
                                        password="dap",
                                        host="192.168.56.30",
                                        port="5432",
                                        database="schooldata")
        dbConnection.set_isolation_level(0)  # AUTOCOMMIT
        dbCursor = dbConnection.cursor()


        for index, row in popdf.iterrows():
            query = r"""INSERT INTO population VALUES ('{}','{}','{}');""" .format(row['county'], row['pop11'], row['pop16'])

            dbCursor.execute(query)

        dbCursor.close()

    except (Exception, psycopg2.Error) as dbError:
        print("Error:", dbError)
    finally:
        if (dbConnection):
            dbConnection.close()



"""
Now we will work some more on getting normal form.  we aim to satisfy 3 conditions for each table:
-must have a primary key
-must not be atomisable
-entries in a column must be of the same data type 

Then we seek 2NF by removing transient dependencies on PKs

script below will achieve 2NF and partial 3NF by creating new tables from the original datasets

"""

NFscript = """--Add primary key to population table
ALTER TABLE population 
ADD PRIMARY KEY (county);

--list of all schools

create table schoollisting
as
select roll_number, schoolname, address1, address2, address3, address4, county, local_auth, principalfirstname, principallastname, eircode, latitude, longitude,
phone_no, email, schooltype as schooltype
from secondaryschools
UNION
select roll_number, schoolname, address1, address2, address3, address4, county, local_auth, principalfirstname, principallastname, eircode, latitude, longitude, 
cast(phone_no as varchar(25)), email, 'primary' as schooltype
from primaryschools;

Alter table schoollisting
ADD PRIMARY KEY (roll_number);
Alter table schoollisting
ADD CONSTRAINT schoollisting_fk 
FOREIGN KEY (county) 
REFERENCES population (county);

--table of student numbers
UPDATE secondaryschools
                    SET femalen = NULL 
                    WHERE femalen = 0;
					
UPDATE primaryschools
                    SET femalen = NULL 
                    WHERE femalen = 0;
UPDATE secondaryschools
                    SET malen = NULL 
                    WHERE malen = 0;
					
UPDATE primaryschools
                    SET malen = NULL 
                    WHERE malen = 0;					


create table students 
as
select roll_number, femalen as studentn, 'F' as gender
from secondaryschools
UNION
select Roll_Number, malen as studentn, 'M' as gender
from secondaryschools
UNION
select roll_number, femalen as studentn, 'F' as gender
from primaryschools
UNION
select Roll_Number, malen as studentn, 'M' as gender
from primaryschools;

ALTER TABLE students 
ADD PRIMARY KEY (roll_number, gender);
ALTER TABLE students 
ADD CONSTRAINT students_fk 
FOREIGN KEY (roll_number) 
REFERENCES schoollisting (roll_number);

--school demographic details

create table schooldemographic
as
select roll_number, gaeltacht_Indicator, deis, island, irish_classification, ethos 
from secondaryschools
UNION
select roll_number, gaeltacht_Indicator, deis, island, irish_classification, ethos 
from primaryschools;

ALTER TABLE schooldemographic 
ADD PRIMARY KEY (roll_number);
ALTER TABLE schooldemographic 
ADD CONSTRAINT schooldemographic_fk 
FOREIGN KEY (roll_number) 
REFERENCES schoollisting (roll_number);


--Update ranking table to include roll number where possible 
ALTER TABLE ranking
ADD COLUMN roll_number  varchar(20);
ALTER TABLE ranking
ADD id serial;
UPDATE ranking
SET roll_number = A.roll_number
FROM secondaryschools as A
where A.latitude = ranking.latitude
and A.longitude = ranking.longitude;

delete from ranking as A
using ranking as B
where A.id < b.id
and A.roll_number = B.roll_number;

ALTER TABLE ranking 
ADD PRIMARY KEY (ranking);
ALTER TABLE ranking
ADD CONSTRAINT ranking_fk 
FOREIGN KEY (roll_number) 
REFERENCES schoollisting (roll_number)

;

"""


try:
    dbConnection = psycopg2.connect(user="dap",
                                    password="dap",
                                    host="192.168.56.30",
                                    port="5432",
                                    database="schooldata")
    dbConnection.set_isolation_level(0)  # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute(NFscript)
    dbCursor.close()

except (Exception, psycopg2.Error) as dbError:
    print("Error:", dbError)
finally:
    if (dbConnection):
        dbConnection.close()



