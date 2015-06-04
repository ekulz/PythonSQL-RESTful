#!/usr/bin/env python2

# Luke Hickin            [223033]
# Samuel O'Connell       [7850740]
# Louie Murphy           [1912785]
# Alister Butterfield    [4281953]

import cx_Oracle
import socket
import thread
import signal
import string
import urlparse


SERVER_STRING = "INFO 214 Assignment 3 Web Server"

# Oracle database connection details:
ORACLE_SERVICE="info214"
ORACLE_USERNAME="hiclu995"
ORACLE_PASSWORD="223033"


global info214_db

# The following variable sets the listening port for the socket.
# You might have to change this sometimes, as the server port can hang around.
# Also, if you happen to be sharing a Student Desktop server with another student, only one will be able to open any given port.
# Normally HTTP would use port 80, but it's a privileged port under Windows in the labenv, so you will have to use something larger.
SERVICE_PORT = 8015
SERVICE_NAME = 'localhost'
URI='http://'+ SERVICE_NAME + ':' + str(SERVICE_PORT)

#ORACLE FUNCTIONS
#catches a database error (e.g. if connection details are incorrect)
def db_connect():
        global info214_db
        print "Logging in to Oracle..."
        try:
                info214_db = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_SERVICE)
                info214_db.autocommit = 0
                print "Logged in to Oracle OK."
        except cx_Oracle.DatabaseError as e:
                report_error(e)
                
        

def db_disconnect():
        info214_db.close()


def report_error(exception):
        print "\nERROR: ", str(exception), "\n"

# Helper function for retrieving a single value from the database.
def singleton_query(sql):
        global info214_db
        cursor = info214_db.cursor()
        cursor.execute(sql)
        try:
                result = cursor.fetchone()[0]
        except Exception, err:
                #report_error(err)
                result = ""
                return result
        finally:
                cursor.close()
                return result

# Another helper for returning a single-row, multi-column result:
def singleton_row_query(sql):
	global info214_db
	cursor = info214_db.cursor()
	cursor.execute(sql)
	try:
		result = cursor.fetchone()
	except Exception, err:
		report_error(err)
		result = ""
	finally:
		cursor.close()
		return result

#function which will return a dict for a multi row sql statement	
def rows_to_dict_list(sql):
        global info214_db
	cursor = info214_db.cursor()
	cursor.execute(sql)
	try:
                columns = [i[0] for i in cursor.description]
                return [dict(zip(columns, row)) for row in cursor]
        except Exception as e:
                report_error(e)
                return

# Helper function for putting suitable EOL marker on a string:
def make_line(string):
        return string + "\r\n"
        

# Callback function for handling a client request.
#this function is the 'brains' of the program. comments will explain what is happening along the way
def http_handler(client_socket, address):
                                   
        request = ""
        while True:
                # Apparently chunks of 4096 or 8192 bytes are usually efficient for TCP buffers.
                new_data = client_socket.recv(8192)                
                # If a socket read returns the empty string, we're definitely at EOF.
                if len(new_data) == 0:                        
                        break
                # However, HTTP clients don't necessarily trigger EOF - they just rely on sending a blank line to denote end-of-header.
                request = request + new_data
                #break here to fix PUT from restclient...
                break
                if request[-4:] == "\r\n\r\n" or request[-4:] == "l>\r\n":
                        print "breaking"
                        break

        print
        print "Request = " + request
        # A useful starting point would be to split the request into lines. The first line will always contain the request method specifier, e.g. "GET".
        request_lines = request.split("\r\n")
        # The following loop splits the remaining lines and places them in a dict (associative array):
        request_headers = {}
        for header in request_lines[1:]:
                try:
                        (var, val) = header.split(': ')
                        request_headers[var] = val
                except (ValueError):
                        pass
        print
        if not request_headers:
                print "There were no headers sent"
        else:
                print "Headers = ", request_headers
        print


        #***Request Handling***
        
        #handle empty request for telnet testing
        if request == "\r\n\r\n":
                print "No request received"
                handleHeaders(400, 'text/html')
                closeConnection()
                return
        
        #if legit request
        else:
                #split method, body, path from request, assign, then print
                request_method = request.split(" ")
                request_body = request.split("\r\n\r\n")
                reqMethod = request_method[0]
                reqPath = request_method[1]
                print "Request method = ", reqMethod

                #clean up the path (for telnet etc)
                path2 = handlePath(reqPath)
                print "Requested path (cleaned up) = ", path2

                #create sql for retrieving the content type (if in db)
                blob = selectDataFromPath(path2)
                if isinstance(blob, basestring):
                        #if not in db, get from headers
                        try:
                                reqType = request_headers['Content-Type'].split("; ")[0]
                        #if not in db or headers, set to text/html as default
                        except KeyError as e:
                                report_error(e)
                                reqType = 'text/html'
                        print "Content type retrieved is = ", reqType
                #content type is in db
                else:
                        reqTypeString = "select media_type from web_resource where resource_path = \'" + path2 + "\'"
                        reqType = singleton_query(reqTypeString)
                        print "Content type retrieved is = ", reqType
                                
        
                #get scenario
                if reqMethod == "GET":
                        print "GET requested..."
                        try:
                                getCalled(path2, reqType)
                                print "...GET responded to"
                        except NameError as e:
                                print "\r\nInternal Server Error \r\nError report = "
                                report_error(e)
                                handleHeaders(500, '')
                                auditLog(path2, reqMethod, 500)
                        
                #put scenario
                elif reqMethod == "PUT":
                        print "PUT requested..."
                        try:
                                putCalled(path2, reqType, request_body[1], request_headers)
                                print "...PUT responded to"
                        except NameError as e:
                                print "\r\nInternal Server Error \r\nError report = "
                                report_error(e)
                                handleHeaders(500, '')
                                auditLog(path2, reqMethod, 500)

                #everything else                
                else:
                        print "Got something other than GET or PUT"
                        handleHeaders(500, '')
                        auditLog(path2, reqMethod, 500)
               
                #handle closing the connection
                closeConnection()
                return


#this function will implement what occurs when the request is GET
#pass in the requested path and content type
def getCalled(path, contentType):
        
        #'/' path giving HTML table of resources
        if path == '/':
                http_response = resourceTable()
                handleHeaders(200, 'text/html')
                client_socket.send(http_response)
                return

        #use the requested path to generate sql statement, and query
        else:
                LOB = selectDataFromPath(path)
                
                #if the queried resource was not found, LOB will be a string rather than an object
                if isinstance(LOB, basestring):
                        handleHeaders(404, 'text/html')
                        client_socket.send(singleton_query("select Resource_Data from Web_Resource where Resource_Path = '/err/404'").read())
                        auditLog('/err/404', 'GET', 404)
                        
                #otherwise the resource was found, send it        
                else:
                        handleHeaders(200, contentType)
                        client_socket.send(LOB.read())
                        auditLog(path, 'GET', 200)
                return

#this function will imeplement what occurs when the request is PUT
#pass in requested path, the content type, and the body to be replaced
def putCalled(path, contentType, body, request_headers):        
        #'/' path giving HTML table of resources
        if path == '/':
                http_response = resourceTable()
                handleHeaders(200, 'text/html')
                client_socket.send(http_response)
                return

        #use the requested path to generate sql statement, and query
        else:
                LOB = selectDataFromPath(path)
                
                #if the queried resource was not found, LOB will be a string rather than an object
                if isinstance(LOB, basestring):
                        #transaction will be called but use insert rather than update
                        print "transaction begin..."
                        transaction(path, body, contentType, 0)
                        print "...transaction complete"
                        handleHeaders(201, contentType)
                        LOB = selectDataFromPath(path)
                        client_socket.send(LOB.read())
                        auditLog(path, 'PUT', 201)
                        
                #otherwise the resource was found, update it        
                else:
                        print "transaction begin..."
                        contentTypeNew = request_headers['Content-Type'].split("; ")[0]
                        print "Content type for PUT update is: ", contentTypeNew
                        transaction(path, body, contentTypeNew, 1)
                        print "...transaction complete"
                        handleHeaders(200, contentTypeNew)
                        LOB = selectDataFromPath(path)
                        client_socket.send(LOB.read())
                        auditLog(path, 'PUT', 200)
                
                return

#this method will dynamically create the resource table
def resourceTable():
        html_table = """
<html>
<head>
<title>Resources</title>
</head>
<body>
<h1>Resources</h1>
<table class="tftable" border="2">
<tr><th>Resource</th><th>Media Type</th></tr>
"""

        #gather data
        serverResources = rows_to_dict_list("select * from Web_Resource")
        serverResourceCount = singleton_query("select count(*) from web_resource")
        print "Server Resource Count is: ", serverResourceCount
        resourceList = []
        resourceTypeList = []

        #select data, put in list
        for x in range(0, int(serverResourceCount)):                
                resourceList.append(serverResources[x]['RESOURCE_PATH'])
                resourceTypeList.append(serverResources[x]['MEDIA_TYPE'])

        #append list data to html
        for item in resourceList:
                index = resourceList.index(item)
                html_table = html_table + """<tr><td><a href=""" + URI + str(item) + """>""" + str(item) + """</a></td>"""
                html_table = html_table + """<td>""" + str(resourceTypeList[index]) + """</td></tr>
"""

        #finish the html
        html_table = html_table + """</table>
</body>
</html>"""        
        return html_table

#transaction method will take the path, the data to put put in, the content type, and a flag (1 = resource found, 0 = not found)
def transaction(Path, ResourceData, ContentType, flag):
        # When updating the database, it's best to use a transaction:
        info214_db.begin()
        # Also, you have to configure the bind variables on the cursor:
        lob_cursor = info214_db.cursor()

        #create new variable resource_data, set as blob then set the values of the blob
        Resource_Data = lob_cursor.var(cx_Oracle.BLOB)
        Resource_Data.setvalue(0, ResourceData)

        #when using PUT to create
        if flag == 0:
                lob_cursor.execute("""INSERT INTO Web_Resource(Resource_Path, Resource_Data, Media_Type)
                                        VALUES (:myPath,:myBlob,:myType)""",
                                   {'myPath' : Path, 'myBlob' : Resource_Data, 'myType' : ContentType})
        else:
        #when using PUT to update
                lob_cursor.execute("""UPDATE Web_Resource
                                        SET Resource_Data = :myBlob, Media_Type = :myType
                                        WHERE Resource_Path = :myPath""",
                                   {'myPath' : Path, 'myBlob' : Resource_Data, 'myType' : ContentType})
        info214_db.commit()
        return

def selectDataFromPath(path):
        sql = "select Resource_Data from Web_Resource where Resource_Path = \'" + path + "\'"
        #will handle exception and show ERROR:  'NoneType' object has no attribute '__getitem__' if path not in DB
        LOB = singleton_query(sql)
        return LOB

#this will handle requests from telnet, and fix up the request path
def handlePath(path):        
        if '\r\n\r\n' in path:
                n = 2
                path2 = path.split('\r\n\r\n')
        
        else:
                path2 = path.split(' ')        

        return path2[0]

#handles headers, takes a code and a resource
def handleHeaders(code, resource):
        if code == 200:
                client_socket.send('HTTP/1.1 200 OK\r\n')

        elif code == 201:
                client_socket.send('HTTP/1.1 201 Created\r\n')

        elif code == 400:
                client_socket.send('HTTP/1.1 400 Bad Request\r\n')
                
        elif code == 404:
                client_socket.send('HTTP/1.1 404 Not Found\r\n')
                                   
        elif code == 500:
                client_socket.send('HTTP/1.1 500 Internal Server Error\r\n')

        else:
                print 'response code for this code/resource not handled yet'


        client_socket.send("Content-Type: " + resource + "\r\n\r\n")

        if code == 500:
                http_response="""<html>
    <head>
        <title>HTTP 500 Error: Internal Server Error</title>
    </head>
    <body>
        <h1>500 Internal Server Error!</h1>
        <p>Sorry about that :(</p>
    </body>
</html>

"""
                client_socket.send(http_response)
                
        
        return


#this method will handle adding any request into the audit log
#it is called whenever a request was successful, and whenever a request was unsuccessful (e.g. will log /err/404 access)
def auditLog(path, method, status):
        #check whether the path is in the db, otherwise don't log request
        pathLOB = selectDataFromPath(path)
        if isinstance(pathLOB, basestring):
                print "path not found, not adding to audit log"
                return
        else:
                print "path found, adding to audit log"
                
                #since the path was legit, we will get our values to insert
                myPath = singleton_query("select * from web_resource where resource_path = \'" + path + "\'")
                myAddress = client_socket.getpeername()[0]              
                myTime = singleton_query('select Current_Timestamp as Now from Dual')
                myMethod = getMethodFromDb(method)
                myStatus = getStatusFromDb(status)
                #if getMethodFromDb returned an empty string, the method is not found in DB
                if myMethod == '':
                        print "method not found in DB"
                        handleHeaders(500, 'text/html')
                        return
                #otherwise the requested method was found in DB
                else:
                        #if getStatusFromDb returned an empty string, the status number has not been added to db yet
                        if myStatus == '':
                                print "status not found in DB"
                                handleHeaders(500, 'text/html')
                                return
                        #otherwise all values are ok
                        else:
                                info214_db.begin()
                                cursor = info214_db.cursor()
                                #insert an audit log instance
                                cursor.execute("""INSERT INTO Audit_Log(Resource_Path, Remote_Address, Access_Timestamp, HTTP_Method, HTTP_Status_Number)
                                                        VALUES (:myPath,:myAddress,:myTime,:myMethod,:myStatusNumber)""",
                                                        {'myPath' : myPath, 'myAddress' : myAddress, 'myTime' : myTime, 'myMethod' : myMethod, 'myStatusNumber' : myStatus})

                                print 'successfully added request for: ' + myPath + ' into the audit log'
                                info214_db.commit()
        return

#used to check whether method is in db
def getMethodFromDb(method):
        dbMethod = singleton_query("select * from http_method where http_method = \'" + method.upper() + "\'")
        if not dbMethod:
                dbMethod = ''
                return dbMethod                
        else:
                return dbMethod

#used to check whether status code is in db
def getStatusFromDb(status):
        dbStatus = singleton_query("select http_status_number from http_status where http_status_number = \'" + str(status) + "\'")
        if not dbStatus:
                dbStatus = ''
                return dbStatus                
        else:
                return dbStatus

#needs to be updated when new status numbers are added
def insertStatusNumbers():
        #check whether db contains them (if first time populating it shouldn't)
        status1 = singleton_query("select http_status_number from http_status where http_status_number = '201'")
        status2 = singleton_query("select http_status_number from http_status where http_status_number = '500'")
        # When updating the database, it's best to use a transaction:
        info214_db.begin()
        # Also, you have to configure the bind variables on the cursor:
        lob_cursor = info214_db.cursor()
        #these are not the status codes we're looking for:
        if status1 != 201:
                lob_cursor.execute("""INSERT INTO http_status(http_status_number, http_status_string, http_status_description)
                                                        VALUES (:statusNumber,:statusString,:statusDescription)""",
                                                        {'statusNumber' : 201, 'statusString' : 'Created', 'statusDescription' : 'The operation was successful and content created'})
        if status2 != 500:
                lob_cursor.execute("""INSERT INTO http_status(http_status_number, http_status_string, http_status_description)
                                                        VALUES (:statusNumber,:statusString,:statusDescription)""",
                                                        {'statusNumber' : 500, 'statusString' : 'Internal Server Error', 'statusDescription' : 'There was a server side error'})
        info214_db.commit()
        print "status numbers have been updated in db"
        return


#handles closing connection, seperated to method for easier visibility
def closeConnection():
        try:
                client_socket.shutdown(socket.SHUT_RDWR)
                print "Disconnected."
                client_socket.close()
                print address, " closed connection"; print; print
        except Exception, err:
                print "Error while disconnecting: ", str(err)
        return
        

# This function should clean up any persistent resources (e.g. socket and database handles) and exit.
def shutdown():
        print "shutdown() called..."
        try:
                print "Closing server socket..."
                server_socket.close()
                print "Closing database connection..."
                info214_db.close()
        except Exception, err:
                print "Error during shutdown: ", str(err)
        finally:
                print "Going bye..."
                exit()


# If we receive a signal (e.g. Ctrl-C/Ctrl-Break), terminate gracefully:
def signal_handler(signal, frame):
        print "\n\nSignal received; exiting..."
        shutdown

#test database functionality
def test_db():
        print "**********IGNORE TESTS************"
        # 0. print the db time
        print '\r\nThe current time (according to the server) is', singleton_query('select Current_Timestamp as Now from Dual')
        # 1. Retrieve a "LOB locator" from the database using a query:
        LOB = singleton_query("select Resource_Data from Web_Resource where Resource_Path = '/index.html'")
        # 2. Read from the LOB.
        print "\r\nThe resource data from web resource, where resource path is /index.html:"
        print LOB.read()
        # 3. Retrieve all resource paths
        #print singleton_query("select 
        print "\r\n********TESTS DONE**********"
        return

# That's it for the preliminaries - now we'll actually run some code:

db_connect()
#test_db()
insertStatusNumbers()


# Bind the signal handler:

signal.signal(signal.SIGINT, signal_handler)


# Now, set up the server socket.
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind(('localhost', SERVICE_PORT))
server_socket.listen(5)

print "Service starting up on port " + str(SERVICE_PORT) + "..."



# Start the server listening:
while 1:
        print "Entering server loop..."
        (client_socket, address) = server_socket.accept()
        print "Got connection from ", address
        print "Remote address is: ", client_socket.getpeername()
#       Also possibly useful: client_socket.family client_socket.type client_socket.proto, client_socket.getpeername()
        thread.start_new_thread(http_handler, (client_socket, address))

