# -*- coding: utf-8 -*-
"""

File:           SpinlabSC.py

Author:         Steve Fromm

Last Modified:  2017-07-31

Description:    SpinlabSC is a multi-platform library that exposes an interface

                to the Spinlab Slow Control MySQL Database (DB).  This library

                provides the following functionality:            

                    * A complete object-oriented model of the slow control DB
"""

# Required modules
import mysql.connector
from mysql.connector import errorcode

# Custom exceptions
class DatabaseException(Exception):
    pass

class ModelException(Exception):
    pass

# Useful 'Macros'
def EQ(val):
    return {'comp':'=','val':str(val)}

def NEQ(val):
    return {'comp':'!=','val':str(val)}

def LT(val):
    return {'comp':'<','val':str(val)}

def LE(val):
    return {'comp':'<=','val':str(val)}

def GT(val):
    return {'comp':'>','val':str(val)}

def GE(val):
    return {'comp':'>=','val':str(val)}

def InRange(low,high):
    return {'comp':'BETWEEN','val':' '.join([str(low),'AND',str(high)])}

def Enquote(string):
    return '\'' + string + '\''

def Sep(*args):
    return '.'.join(args)

def EnDate(date):
    return 'TIMESTAMP(' + Enquote(str(date)) + ')'

class Database(object):
    """Encapsulates a MySQL database connection"""  

    def __init__(self,host,dbname,user,pw,devmode=False):

        """Initializes the database connection
        Parameters:
            host - string, URL of the DB server
            dbname - string, name of the schema to connect to
            user - string, user name for login
            pw - string, password associated with provided user name"""

        # Make sure that the input arguments are filled
        if "" in (host,dbname,user,pw):
            raise DatabaseException("You must provide all initialization parameters")

        # Build database configuration
        config = { 'user' : user,
                   'password' : pw,
                   'host' : host,
                   'database' : dbname }

        
        self.devmode = devmode

        if devmode:
            config['raise_on_warnings'] = True
            self.recTable = 'playground'
        else:
            self.recTable = 'Records'

            

        # Attempt the connection
        try:
            self.conn = mysql.connector.connect(**config)


        except mysql.connector.Error as e:
            # Authentication failed
            if e.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Invalid user name or password")

            # Schema does not exist
            if e.errno == errorcode.ER_BAD_DB_ERROR:
                print("Schema",dbname,"does not exist")

    def Close(self):
        """Close the database connection"""
        self.conn.close()

    def Insert(self,table,**kwargs):
        """Execute an INSERT MySQL command.
        Pass in a dictionary of key/value pairs to insert."""
        # Make sure all values are formatted as strings
        pairs = {k:str(v) for k,v in kwargs.items()}

        # Form the query string
        query = " ".join(("INSERT INTO",table,"(",",".join(pairs.keys()),")","VALUES (",",".join(pairs.values()),")"))

        if self.devmode:
            print(query)

        # Execute this command
        cursor = self.conn.cursor()
        cursor.execute(query)
        lastID = cursor.getlastrowid()
        cursor.close()

        self.conn.commit()

        return lastID

    def Select(self,table,columns=[],order=None,**kwargs):
        """Execute a SELECT MySQL command
        Parameters:
            table - string, name of the table to query
            columns - string list, names of columns to select, optional
            kwargs - key/value pairs of conditions, i.e. for an ID == 1:
                        ID=EQ(1)"""
        # Determine if any columns are requested
        cols = ",".join(columns) if columns else '*'

        # Build the base query
        query = " ".join(["SELECT",cols,"FROM",table])

        # Add on any conditions
        if kwargs:
            options = [" ".join([k,v['comp'],str(v['val'])]) for k,v in kwargs.items()]
            query += " WHERE " + " AND ".join(options)

        # Optional ordering
        if order:
            query += ' ORDER BY ' + order

        if self.devmode:
            print(query)

        # Execute this command
        cursor = self.conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()

        return rows

    def CreateNewOwner(self,name,desc):
        """Add a new owner to the database
        Parameters:
            name - string, 12 char max, name to use in full nomenclature
            desc - string, 255 char max, brief description of the owner"""
        # Make sure this is not a duplicate entry
        if self.GetOwner(name):
            print('This owner already exists')
            raise DatabaseException('This owner already exists in the databse')

        # Insert into the database
        ID = self.Insert('Owners',ownerName=Enquote(name),ownerDesc=Enquote(desc))

        # Pull the new entry to fill in Owner object
        DTG = self.Select('Owners',['ownerDTG'],ownerID=EQ(ID))[0][0]

        return Owner(name,desc,ID,DTG)

    

    def GetOwner(self,name="",ID=None):
        """Selects an owner from the database
        Parameters:
            name - string, 12 char max, name used in full nomenclature"""
        # Pull the full row for this owner from the DB
        if ID:
            rows = self.Select('Owners',['ownerName','ownerDesc','ownerID','ownerDTG'],ownerID=EQ(ID))
        else:
            rows = self.Select('Owners',['ownerName','ownerDesc','ownerID','ownerDTG'],ownerName=EQ(Enquote(name)))

        # check if there was a result found
        if not rows:
            return None

        row = rows[0]
        return Owner(row[0],row[1],row[2],row[3])

    def CreateNewProject(self,name,desc):
        """Add a new project to the database
        Parameters:
            name - string, 12 char max, name to be used in full nomenclature
            desc - string, 255 char max, brief description of the project
            owner - Owner, the owner of the project"""
        # Get owner and project name
        oName,pName = name.split('.')

        # Make sure this is not a duplicate entry (raises exception if owner doesnt exist)
        if self.GetProject(name):
            print('Project',name,'already exists')
            raise DatabaseException('Project already exists')

        # Get owner info
        owner = self.GetOwner(oName)

        # Insert into the database
        ID = self.Insert('Projects',projName=Enquote(pName),projDesc=Enquote(desc),ownerID=owner.ID)

        # Pull new entry
        DTG = self.Select('Projects',['projDTG'],projID=EQ(ID))[0][0]      
        return Project(pName,desc,owner,ID,DTG)

    def GetProject(self,name='',ID=None):
        """Select a project from the database
        Parameters:
            name - string, full nomenclature"""
            
        if ID:
            rows = self.Select('Projects',['projName','projDesc','projID','projDTG','ownerID'],projID=EQ(ID))
        else:
            # Get owner and project name
            oName,pName = name.split('.')
    
            # Check if this owner exists
            owner = self.GetOwner(oName)
            if not owner:
                print('Owner',oName,'is not in the database')
                raise DatabaseException('Invalid owner name')
    
            # Pull entry from the database
            rows = self.Select('Projects',['projName','projDesc','projID','projDTG','ownerID'],projName=EQ(Enquote(pName)),ownerID=EQ(owner.ID))

        # check if there was a result found
        if not rows:
            return None

        row = rows[0]
        return Project(row[0],row[1],self.GetOwner(ID=row[4]),row[2],row[3])

    def CreateNewSystem(self,name,desc):
        """Create a new system in the database
        Parameters:
            name - string, full nomenclature
            desc - string, 255 char max, brief description of the system"""
        # Get the names
        oName,pName,sName = name.split('.')
        # This checks if system exists and if parents dont
        if self.GetSystem(name):
            print('System',name,'already exists')
            raise DatabaseException('System already exists')

        # Get project info
        project = self.GetProject(Sep(oName,pName))

        # Insert into database
        ID = self.Insert('Systems',sysName=Enquote(sName),sysDesc=Enquote(desc),projID=project.ID)

        # Pull timestamp
        DTG = self.Select('Systems',['sysDTG'],sysID=EQ(ID))[0][0]

        return System(sName,desc,project,ID,DTG)

    
    def GetSystem(self,name='',ID=None):
        """Select a system from the database
        Parameters:
            name - string, full nomenclature"""
        
        if ID:
            rows = self.Select('Systems',['sysName','sysDesc','sysID','sysDTG','projID'],sysID=EQ(ID))
        else:
            # Get the names
            oName,pName,sName = name.split('.')
            top = Sep(oName,pName)
    
            # Check if the project (and owner) exist
            project = self.GetProject(top)
            if not project:
                print('Project',top,'is not in the database')
                raise DatabaseException('Invalid project name')
    
            # Pull from the database
            rows = self.Select('Systems',['sysName','sysDesc','sysID','sysDTG','projID'],sysName=EQ(Enquote(sName)),projID=EQ(project.ID))

        # Check if a result was found
        if not rows:
            return None

        row = rows[0]
        return System(row[0],row[1],self.GetProject(ID=row[4]),row[2],row[3])

    def CreateNewManufacturer(self,name,desc,URL):
        """Add a new manufacturer to the database
        Parameters:
            name - string, 255 char max, name (unique) of the manufacturer
            desc - string, 255 char max, brief description of the manufacturer
            URL - string, 255 char max, URL of the manufacturer's website"""
        # Check if this manufacturer exists already
        if self.GetManufacturer(name):
            print('Manufacturer',name,'already exists')
            raise DatabaseException('Manufacturer already exists')

        # Add to the database
        ID = self.Insert('Manufacturers',mfgName=Enquote(name),mfgDesc=Enquote(desc),mfgURL=Enquote(URL))

        # Get the timestamp
        DTG = self.Select('Manufacturers',['mfgDTG'],mfgID=EQ(ID))[0][0]

        return Manufacturer(name,desc,URL,ID,DTG)

    def GetManufacturer(self,name="",ID=None):
        """Select the manufacturer from the database
        Parameters:
            name - string, 255 char max, unique name of the manufacturer
            ID - int, internal database ID"""
        # Pull row from database
        if ID:
            rows = self.Select('Manufacturers',['mfgName','mfgDesc','mfgURL','mfgID','mfgDTG'],
                           mfgID=EQ(ID))
        else:
            rows = self.Select('Manufacturers',['mfgName','mfgDesc','mfgURL','mfgID','mfgDTG'],
                           mfgName=EQ(Enquote(name)))

        # Check for a result
        if not rows:
            return None

        row = rows[0]
        return Manufacturer(row[0],row[1],row[2],row[3],row[4])

    def CreateNewDevice(self,name,desc,URL,mfg):
        """Create a new device in the database
        Parameters:
            name - string, full nomenclature
            desc - string, 255 char max, brief description of the device
            URL - string, URL of the documentation for this device
            mfg - Manufacturer, the manufacturer of this device"""
        # Get the names
        oName,pName,sName,dName = name.split('.')
        
        # Make sure a valid mfg were passed in
        if not mfg:
            print('Manufacturer is required to create a new device')
            raise DatabaseException('Manufacturer required')

        # Check if this device exists
        if self.GetDevice(name):
            print('Device',name,'already exists')
            raise DatabaseException('Device already exists')

        # Get system info
        system = self.GetSystem(Sep(oName,pName,sName))

        # Add to database
        ID = self.Insert('Devices',devName=Enquote(dName),devDesc=Enquote(desc),
                         devURL=Enquote(URL),mfgID=mfg.ID,sysID=system.ID)

        # Get timestamp
        DTG = self.Select('Devices',['devDTG'],devID=EQ(ID))[0][0]

        return Device(dName,desc,system,URL,mfg,ID,DTG)

    def GetDevice(self,name='',ID=None):
        """Select a device from the database
        Parameters:
            name - string, full nomenclature"""
            
        if ID:
            rows = self.Select('Devices',['devName','devDesc','sysID','devURL','mfgID','devID','devDTG'],
                           devID=EQ(ID))
        else:
            # Get names
            oName,pName,sName,dName = name.split('.')
            top = Sep(oName,pName,sName)
    
            # Check if parents exist
            system = self.GetSystem(top)
            if not system:
                print('System',top,'does not exist')
                raise DatabaseException('System does not exist')
    
            # Pull from database
            rows = self.Select('Devices',['devName','devDesc','sysID','devURL','mfgID','devID','devDTG'],
                               devName=EQ(Enquote(dName)),sysID=EQ(system.ID))

        # check if there was a result
        if not rows:
            return None

        row = list(rows[0])
        row[2] = self.GetSystem(ID=row[2])
        row[4] = self.GetManufacturer(ID=row[4])
        return Device(*row)

    def CreateNewUnits(self,short,long,desc):
        """Add new unit of measure to the database
        Parameters:
            short - string, 25 char max, short form of units, i.e. m/s
            long - string, 255 char max, long form of units, i.e. meters per second
            desc - string, 255 char max, description of units, i.e. velocity"""
        # Verify these units do not exist yet - long name is unique index
        if self.GetUnits(long):
            print('Units',long,'already exist in the database')
            raise DatabaseException('Units already exist')

        # Add to the database
        ID = self.Insert('Units',unitShort=Enquote(short),unitLong=Enquote(long),
                         unitDesc=Enquote(desc))

        return Units(short,long,desc,ID)

    def GetUnits(self,long='',ID=None):
        """Select unit of measure from the database
        Parameters:
            long - string, 255 char max, long form of units, i.e. meters per second"""
        # Select from database
        if ID:
            rows = self.Select('Units',['unitShort','unitLong','unitDesc','unitID'],
                               unitID=EQ(ID))
        else:
            rows = self.Select('Units',['unitShort','unitLong','unitDesc','unitID'],
                               unitLong=EQ(Enquote(long)))

        # Check if there was a result
        if not rows:
            return None

        row = rows[0]
        return Units(*row)

    def CreateNewSensor(self,name,desc,units):
        """Add new sensor to the database
        Parameters:
            name - string, full nomenclature
            desc - string, 255 char max, brief description of the sensor
            units - Units, units of measure that the sensor reads in"""
        # Get names
        oName,pName,sName,dName,SName = name.split('.')
        top = Sep(oName,pName,sName,dName)

        # Make sure valid units were passed in
        if not units:
            print('Units are required to create a new sensor')
            raise DatabaseException('Units required')

        # Check if this sensor exists already
        if self.GetSensor(name):
            print('Sensor',name,'already exists')
            raise DatabaseException('Sensor already exists')

        # Get info on the device
        device = self.GetDevice(top)

        # Add to the database
        ID = self.Insert('Sensors',senName=Enquote(SName),senDesc=Enquote(desc),
                         devID=device.ID,unitID=units.ID)

        # Pull timestamp
        DTG = self.Select('Sensors',['senDTG'],senID=EQ(ID))[0][0]

        return Sensor(SName,desc,device,units,ID,DTG)

    def GetSensor(self,name='',ID=None):
        """Select a sensor from the database
        Parameters:
            name - string, full nomenclature"""
        if ID:
            rows = self.Select('Sensors',['senName','senDesc','devID','unitID','senID','senDTG'],
                           senID=EQ(ID))
        else:
            # Get names
            oName,pName,sName,dName,SName = name.split('.')
            top = Sep(oName,pName,sName,dName)
    
            # Check if parent exists
            device = self.GetDevice(top)
            if not device:
                print('Device',top,'does not exist')
                raise DatabaseException('Device not found')
    
            # Pull from database
            rows = self.Select('Sensors',['senName','senDesc','devID','unitID','senID','senDTG'],
                               senName=EQ(Enquote(SName)),devID=EQ(device.ID))

        # Check for result
        if not rows:
            return None

        row = list(rows[0])
        row[2] = self.GetDevice(ID=row[2])
        row[3] = self.GetUnits(ID=row[3])
        return Sensor(*row)

    def RecordMeasurement(self,sensor,data,error):
        """Record a measurement to the database
        Parameters:
            sensor - Sensor, sensor taking the measurement
            data - double, value of the measurement
            error - double, uncertainty in the measurement"""
        # Check that a sensor exists
        if not sensor:
            print('A sensor is reuqired to take a measurement')
            raise DatabaseException('Invalid sensor')

        ID = self.Insert(self.recTable,recData=data,recError=error,senID=sensor.ID)

        # Pull timestamp
        DTG = self.Select(self.recTable,['recDTG'],recID=EQ(ID))[0][0]

        return Record(data,error,sensor,ID,DTG)

    def GetRecords(self,sensor,startTime,endTime):
        """Obtain records from a date range for a sensor
        Parameters:
            sensor - Sensor, sensor to obtain records from
            startTime - DateTime, starting time
            endTime - DateTime, ending time"""
        # Check for a sensor
        if not sensor:
            print('Sensor required to obtain record form')
            raise DatabaseException('Sensor Required')

        rows = self.Select(self.recTable,['recData','recError','recDTG','recID'],
                           'recDTG ASC',senID=EQ(sensor.ID),
                           recDTG=InRange(EnDate(startTime),EnDate(endTime)))
        
        # Check if there was a result
        if not rows:
            return None
        
        records = []
        for row in rows:
            record = Record(row[0],row[1],sensor,row[3],row[2])
            records.append(record)

        return RecordSet(records)
    
    def GetMostRecentRecord(self,sensor):
        """Obtain the most recent record for a sensor
        Parameters:
            sensor - Sensor, sensor to obtain record from"""
        # Check for a sensor
        if not sensor:
            print('Sensor required to obtain record form')
            raise DatabaseException('Sensor Required')
            
        rows = self.Select(self.recTable,['recData','recError','recDTG','recID'],
                          'recDTG DESC LIMIT 1',senID=EQ(sensor.ID))
        
        # Check if there was a result
        if not rows:
            return None
        
        row = rows[0]
        return Record(row[0],row[1],sensor,row[3],row[2])
        
    
    def GetOwners(self):
        """Obtain a list of all owners"""
        rows = self.Select('Owners',['ownerName','ownerDesc','ownerID','ownerDTG'])
        
        if not rows:
            return None
        
        owners = []
        
        for row in rows:
            owners.append(Owner(*row))
            
        return owners
    
    def GetProjects(self,ownID=None):
        """Obtain a list of all projects"""
        if ownID:
            rows = self.Select('Projects',['projName','projDesc','ownerID','projID','projDTG'],ownerID=EQ(ownID))
        else:
            rows = self.Select('Projects',['projName','projDesc','ownerID','projID','projDTG'])
            
        if not rows:
            return None
        
        projects = []
        
        for row in rows:
            r = list(row)
            r[2] = self.GetOwner(ID=row[2])
            projects.append(Project(*r))
            
        return projects
    
    def GetSystems(self,pID=None):
        """Obtain a list of all systems
        Parameters:
            pID - int, optional, selects only systems belonging to this project"""
        if pID:
            rows = self.Select('Systems',['sysName','sysDesc','projID','sysID','sysDTG'],projID=EQ(pID))
        else:
            rows = self.Select('Systems',['sysName','sysDesc','projID','sysID','sysDTG'])
            
        if not rows:
            return None
        
        systems = []
        for row in rows:
            r = list(row)
            r[2] = self.GetProject(ID=row[2])
            systems.append(System(*r))
            
        return systems
    
    def GetManufacturers(self):
        """Select all manufacturers from the database"""
        rows = self.Select('Manufacturers',['mfgName','mfgDesc','mfgURL','mfgID','mfgDTG'])
        
        if not rows:
            return None
        
        mfgs = []
        for row in rows:
            mfgs.append(Manufacturer(*row))
            
        return mfgs
    
    def GetDevices(self,sID=None):
        """Obtain a list of all devices
        Parameters:
            sID - int, optional, selects only devices belonging to this system"""
        if sID:
            rows = self.Select('Devices',['devName','devDesc','sysID','devURL','mfgID','devID','devDTG'],
                           sysID=EQ(sID))
        else:
            rows = self.Select('Devices',['devName','devDesc','sysID','devURL','mfgID','devID','devDTG'])
            
        if not rows:
            return None
        
        devices = []
        for row in rows:
            r = list(row)
            r[2] = self.GetSystem(ID=row[2])
            r[4] = self.GetManufacturer(ID=row[4])
            devices.append(Device(*r))
            
        return devices
    
    def GetAllUnits(self):
        """Obtain a list of all units of measure in the database"""
        rows = self.Select('Units',['unitShort','unitLong','unitDesc','unitID'])
        
        if not rows:
            return None
        
        units = []
        for row in rows:
            units.append(Units(*row))
            
        return units
    
    def GetSensors(self,dID=None):
        """Obtain a list of all sensors
        Parameters:
            dID - int, optional, selects only sensors  belonging to this device"""
        if dID:
            rows = self.Select('Sensors',['senName','senDesc','devID','unitID','senID','senDTG'],
                           devID=EQ(dID))
        else:
            rows = self.Select('Sensors',['senName','senDesc','devID','unitID','senID','senDTG'])
            
        if not rows:
            return None
        
        sensors = []
        for row in rows:
            r = list(row)
            r[2] = self.GetDevice(ID=row[2])
            r[3] = self.GetUnits(ID=row[3])
            sensors.append(Sensor(*r))
            
        return sensors
            
# Database model classes

# Group - base class that all subsequent groups are based on
# Handles common functionality, such as nomenclature, descriptions and DTGs
class Group(object):
    """Base model for all groups; contains shared functionality"""  
    def __init__(self,name,desc,parent,ID=None,DTG=None):
        """Creates a Group base object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of group
            ID - int, internal DB identifier
            DTG - datetime, time the group was created"""

        # Verify text fields are not empty and not too long
        if "" in (name,desc):
            raise ModelException('All groups must have a valid name and desc')
        if len(name) > 12:
            raise ModelException('Name of group exceeds 12 char max')
        if len(desc) > 255:
            raise ModelException('Description of group exceeds 255 char max')

        self.name = name
        self.desc = desc
        self.ID = ID
        self.DTG = DTG
        self.parent = parent

    def Nomenclature(self):
        """Forms the nomenclature for this group"""
        nom = self.name
        if self.parent:
            nom = self.parent.Nomenclature() + '.' + nom

        return nom

class Owner(Group):
    """Model for the data in the Owners table"""
    def __init__(self,name,desc,ID=None,DTG=None):
        """Create an owner object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of owner
            ID - int, internal DB identifier
            DTG- datetime, time the owner was created"""
        # Owners do not have a parent group
        super().__init__(name,desc,None,ID,DTG)

class Project(Group):
    """Model for the data in the Projects table"""
    def __init__(self,name,desc,owner,ID=None,DTG=None):
        """Create a owner object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of project
            owner - Owner, the owner the system belongs to
            ID - int, internal DB identifier
            DTG- datetime, time the project was created"""
        # Verify that the parent group is a valid owner
        if not owner:
            raise ModelException('Projects require a valid owner')

        if not isinstance(owner,Owner):
            raise ModelException('Projects must belong to an owner')

        super().__init__(name,desc,owner,ID,DTG)

class System(Group):
    """Model for the data in the Systems table"""
    def __init__(self,name,desc,project,ID=None,DTG=None):
        """Create a system object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of system
            project - Project, the project the system belongs to
            ID - int, internal DB identifier
            DTG- datetime, time the system was created"""
        # Verify that the parent group is a valid project
        if not project:
            raise ModelException('Systems require a valid project')

        if not isinstance(project,Project):

            raise ModelException('Systems must belong to a project')

        super().__init__(name,desc,project,ID,DTG)

class Manufacturer(Group):
    """Model for the data in the Manufacturers table"""
    def __init__(self,name,desc,URL,ID=None,DTG=None):
        """Create a manufacturer objec
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of manufacturer
            URL - string, 255 char max, URL of the manufacturer
            ID - int, internal DB identifier
            DTG- datetime, time the manufacturer was created"""

        # Verify that the manufacturer URL exists
        if not URL:
            raise ModelException('Manufacturer require a valid URL')
        if len(URL) > 255:
            raise ModelException('URL exceeds 255 char max')

        super().__init__(name,desc,None,ID,DTG)
        self.URL = URL

            

class Device(Group):
    """Model for the data in the Devices table"""
    def __init__(self,name,desc,system,URL,mfg,ID=None,DTG=None):
        """Create a device object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of device
            system - System, the system the device belongs to
            URL - string, 255 char max, URL of device documentation
            ID - int, internal DB identifier
            DTG- datetime, time the device was created"""
        # Verify that the parent group is a valid system
        if not system:
            raise ModelException('Devices require a valid system')

        if not isinstance(system,System):
            raise ModelException('Devices must belong to a system')
        # Verify that the documentation URL exists
        if not URL:
            raise ModelException('Devices require a valid docs URL')
        if len(URL) > 255:
            raise ModelException('URL exceeds 255 char max')

        # Verify that there is a valid Manufacturer
        if not mfg:
            raise ModelException('Devices require a vailid manufacturer')

        super().__init__(name,desc,system,ID,DTG)
        self.mfg = mfg
        self.URL = URL

class Units(object):
    """Model for the data in the units table"""
    def __init__(self,short,long,desc,ID=None):
        """Create a units object
        Parameters:
            short - string, 25 char max, abbreviated form of units
            long - string, 255 char max, full form of units
            desc - string, 255 char max, decription of units
            ID - int, internal DB identifier"""
        # Verify all text arguments are filled
        if "" in (short,long,desc):
            raise ModelException('Description and short/long forms of units must be provided')

        # Check lengths of text inputs
        if len(short) > 25:
            raise ModelException('Short name of units exceeds 25 char max')
        if len(long) > 255:
            raise ModelException('Long name of units exceeds 255 char max')
        if len(desc) > 255:
            raise ModelException('Description of units exceeds 255 char max')

        self.short = short
        self.long = long
        self.desc = desc
        self.ID = ID

class Sensor(Group):
    """Model for the data in the Sensors table"""
    def __init__(self,name,desc,device,units,ID=None,DTG=None):
        """Create a Sensor object
        Parameters:
            name - string, 12 char max, nomenclature used name
            desc - string, 255 char max, decription of sensor
            device - Device, the device the sensor is part of
            units - Units, the units the sensor measures in
            ID - int, internal DB identifier
            DTG- datetime, time the sensor was created"""
        # Verify that the parent group is a valid device
        if not device:
            raise ModelException('Sensors require a valid device')
        if not isinstance(device,Device):
            raise ModelException('sensors must belong to a device')

        # Verify that there is a valid Manufacturer
        if not units:
            raise ModelException('Sensors require valid units')

        super().__init__(name,desc,device,ID,DTG)
        self.units = units

class Record(object):
    """Model for a data point in the records table"""
    def __init__(self,data,error,sensor,ID=None,DTG=None):
        """Create a Record object
        Parameters:
            data - double, value of the record
            error - double, uncertainty of the record
            sensor - Sensor, sensor that took the measurement
            ID - int, internal DB identifier
            DTG - datetime, time the record was entered into the DB"""

        # Argument validation
        if not sensor:
            raise ModelException('Records must belong to a valid sensor')

        self.data = data
        self.error = error
        self.sensor = sensor
        self.ID = ID
        self.DTG = DTG

class RecordSet(object):
    """Holds multiple records"""

    def __init__(self,records):
        """Build  set of records
        Parameters:
            records - Record array, data points"""
        self.times = [r.DTG for r in records]
        self.data = [r.data for r in records]
        self.error = [r.error for r in records]
        self.sensor = records[0].sensor
        self.N = len(self.times)

    def GetUnitsLabel(self):
        """Obtain the units label for the data points"""
        units = self.sensor.units
        return units.desc + ' (' + units.short + ')'
    
    def GetPlotLabel(self):
        """Returns a formatted plot title"""
        units = self.sensor.units
        title = units.desc + ' Measured by ' + self.sensor.Nomenclature() + '\nfrom ' + str(min(self.times)) + ' until ' + str(max(self.times)) + '\n'
        return title
    
    def WriteCSV(self,fileName,delim=',',header=False):
        """Write a CSV file of the data"""
        fp = open(fileName,'w')
        
        if header:
            fp.write(delim.join(['DTG',self.GetUnitsLabel(),'Error'])+'\n')
            
        for t,y,dy in zip(self.times,self.data,self.error):
            fp.write(delim.join([str(t),str(y),str(dy)])+'\n')
            
        fp.close()
        
    def Mean(self):
        """The average value of this measurement set"""
        return sum(self.data)/self.N
    
    def Variance(self):
        """The variance of the measurements"""
        xbar = self.Mean()
        return sum([(x-xbar)**2 for x in self.data])/self.N
    
    def StandardDeviation(self):
        """The standard deviation of the measurements"""
        return (self.Variance()*self.N/(self.N-1))**(0.5)
