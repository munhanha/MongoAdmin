import os
import sys
import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId

version = "1.00"


def parse_config(file=None):
	f = None
	path = "".join(os.path.dirname(os.path.abspath(__file__))) +'/'
	try:
		if file:
			f = open(path + filename, 'r')
		else:
			f = open(path + 'saas.ini', 'r')
	except:
		print "saas.ini file does not exist!"
		print "Create it (on the same path as this script) and add url=<connection string> and run again."
		exit_process()
	
	file_url = f.readline()
	file_db = f.readline()
	file_collection = f.readline()
	
	if not file_url or not file_db or not file_collection:
		print "The saas.ini file lacks information."
		exit_process()
	
	file_url = file_url.replace('url=','').strip()
	file_db = file_db.replace('default_db=','').strip()
	file_collection = file_collection.replace('default_collection=','').strip()
	
	return file_url,file_db,file_collection
	
def create_connection():
	url, db, collection = parse_config()
	
	conn = MongoClient(url)

	return conn, db, collection

	
def close_connection(conn):
	try:
		conn.close()
	except:
		pass

def get_all_collection_names(conn,bd):
    return conn[bd].collection_names()
    
def get_all_databases(conn):
    return conn.database_names()

def print_collection_info(conn,db,collection):
	print_msg("The collection:%s in the Database:%s has %d documents" %( collection, db, conn[db][collection].count() )) 
	return True
		
def getAllEntriesCollection(conn,db,collection):
	return conn[db][collection].find()
		
def delete_collection(conn,db,collection):
	conn[db][collection].drop()
	print_msg("Collection:%s from database:%s deleted with success" % ( collection, db ))
	return True	
	
def update_bd_bulk(conn, bd, collection, filename):
	values = load_from_local_bd(filename)

	updated = 0
	
	for v in values:
		if v.get('_id',None):
			conn[bd][collection].update({'_id':v['_id']},v,True)
		else:
			conn[bd][collection].insert_one(v)
		updated += 1
			
	print_msg("Created/updated %d values" % ( updated ))		
	return True


def getCollectionKeys(conn,bd,collection):
	example = conn[bd][collection].find_one()
	if not example:
		return None
	return [ key for key in example if str(key) != '_id' ] 
	

	
def create_value(conn,bd,collection,filename):
	keys = getCollectionKeys(conn,bd,collection)
	
	new_obj = {}

	if keys:
		for key in keys:
			value = raw_input("\n%s: " % key)
			new_obj[key] = value
	else:
		print "\nThere are no objects in the collection, please add the object by \"hand\""
		print "Example: {'name':'yoda','age':'900'}\n"
		line = raw_input("\nnew_object:")
		new_obj = eval(line)

	id = conn[bd][collection].insert_one(new_obj).inserted_id
	new_obj['_id'] = id

	f = open(filename,'a')
	f.write(str(new_obj))
	f.close()

	return id

	
def create_local_bd(conn,bd,connection,filename):
	
	#TODO replace with function that makes backups
	if os.path.exists(filename):
		while(True):
			option = raw_input("\nLocal bd already exists, do you want to replace it? (y/n): ")
			if option in ['y','n']:
				if option == 'n':
					print "\n Very well"
					return
				break
			
	
	entries = getAllEntriesCollection(conn,bd,connection)
	n = 0
	
	f = open(filename, 'w')
	
	for e in entries:
		f.write(str(e) + '\n')
		n += 1
	
	f.close()
	
	if n:
		print_msg("Created a new local BD with %d values" % ( n ))
	else:
		print_msg("No local BD was created, the remote BD had no values")
	
	return True

def load_from_local_bd(filename):
	f = open(filename, 'r')

	lines =  f.read()
	lines = lines.split('\n')
	
	output = []
	
	for l in lines:
		l.strip()
		if l != "":
			output.append(eval(l))
	
	return output

def change_current_collection(conn,bd,collection):
    
    new_collection = []
    try:
        new_collection = raw_input("\nDesired Collection: ")
    except SyntaxError:
        new_collection = ""
    
    if new_collection != "":
        collection_list = get_all_collection_names(conn,bd)
        if new_collection not in collection_list:
            while(True):
                try:
                    option = raw_input("\nThe chosen collection does not exist, create? [y/n]: ")
		        
                    if option == 'y':
                        return new_collection
                        return
                    elif option == 'n':
                        break
                    else:
                        print "\n--Invalid value please use y or n--\n"
		        
                except SyntaxError:
                    new_collection = ""
        else:
            return new_collection
    else:
        print_msg("Invalid value for a collection")
        change_current_collection(conn,bd,collection)
    

def change_current_db(conn,db):
    
    new_db = []
    try:
        new_db = raw_input("\nDesired DB: ")
    except SyntaxError:
        new_db = ""
    
    if new_db != "":
        db_list = get_all_databases(conn)
        if new_db not in db_list:
            while(True):
                try:
                    option = raw_input("\nThe chosen DB does not exist, create? [y/n]: ")
		        
                    if option == 'y':
                        return new_db
                        return
                    elif option == 'n':
                        break
                    else:
                        print "\n--Invalid value please use y or n--\n"
		        
                except SyntaxError:
                    new_db = ""
        else:
            return new_db
    else:
        print_msg("Invalid value for a DB")
        change_current_collection(conn,bd)
    

    

def exit_process(conn=None):
	print "\nThou art done for today, goodbye!\n"
	if conn:
		close_connection(conn)
	sys.exit(0)

def print_msg(msg):
	print "\n"
	print "--------------------------------------------------------------------------------"
	print "\t\t%s" % msg
	print "--------------------------------------------------------------------------------"
	print "\n"
	
def validate_option(option):
	if option in ['d','l','e','i','c','p','a','cdb','cc','h']:
		return True
	else:
		return False

def print_pre_activity(db,collection):
	print "******************************************************"    
	print "Current DB: %s " % db
	print "Current Collection: %s " % collection
	print "******************************************************"
	print "Please select an activity:"
	print "\t(i) Insert a new value to the database"
	print "\t(d) Delete current collection"
	print "\t(c) Create local DB entire collection"
	print "\t(l) Load a new file(collection) to the database"
	print "\t(p) Print infomation about the current collection"
	print "******************************************************"
	print "\t(a) Add key to local DB"
	print "\t(cdb) Change working database"
	print "\t(cc) Change working collection"
	print "\t(h) Print a brief tutorial about this script"
	print "\t(e) Exit program"
	option = ''
	try:
		option = raw_input("\nYOU DECIDE: ")
	except SyntaxError:
		option = ""
		
	if validate_option(option):
		return str(option)
	else:
		print_msg("--Invalid option, please select again--\n")
		print_pre_activity(db,collection)
		
		
def process_option(option,conn,db,collection,filename):
	if option == 'e':
		exit_process(conn)
	elif option == 'd':
		delete_collection(conn,db,collection)
	elif option == 'l':
		update_bd_bulk(conn,db,collection,filename)
	elif option == 'i':
		create_value(conn,db,collection,filename)
	elif option == 'c':
		create_local_bd(conn,db,collection,filename)
	elif option == 'p':
		print_collection_info(conn,db,collection)
	#-----------------------------------------------------------------#	
	elif option == 'a':
	    add_key_local_bd(conn,db,collection,filename)
    	    
	elif option == 'cc':
	    new_collection = change_current_collection(conn,db,collection)
	    if new_collection:
	        collection = new_collection
	        
	elif option == 'cdb':
	    new_db = change_current_db(conn,db)
	    if new_db:
	        db = new_db

	elif option == 'h':
	    print_help()
	    
	return db,collection,filename
		
def greeting():
	print "\n------------------------------------------------------"
	print "---------------------Hello Sir------------------------"
	print "-----Welcome to the MongoDB administration script-----"
	print "-------------------Version: %s----------------------" % version
	print "******************************************************"
	print "******************************************************"
	t = datetime.datetime.now()
	currentHour = t.hour
	if currentHour > 0 and currentHour < 6:
		print "\tHello, how are you in this fine dawn?"
	elif currentHour > 5 and currentHour < 13:
		print "\tSplending morning is it not?"
	elif currentHour > 12 and currentHour < 19:
		print "\tAh, yet another sunny afternoon."
	elif currentHour > 18 and currentHour < 23:
		print "\tOh gosh what a night to be alive!!!"
	print "******************************************************"


if __name__ == "__main__":
    conn, db, collection = create_connection()
    filename = "%s_%s.txt" % (db,collection)
	
    greeting()
    while(True):
        option = print_pre_activity(db,collection)
        db,collection,filename = process_option(option,conn,db,collection,filename)
	