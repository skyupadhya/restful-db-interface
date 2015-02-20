import json
import bottle
import pymongo
from bottle import route, run, request, response
from pymongo import Connection
import logging
import _mysql
from datetime import datetime
from time import gmtime, strftime

# *******************************
# LOGGING
# *******************************
date_fmt = '%d/%b/%Y %H:%M:%S'
log_fmt = '%(name)s - - [%(asctime)s %(levelname)s] [%(filename)s:%(lineno)d]: %(message)s'
logger = logging.getLogger('rest-srvc')
handler = logging.StreamHandler()
formatter = logging.Formatter(log_fmt, date_fmt)
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



# *******************************
# MONGO DB
# *******************************
# MongoDB connection
# db = products, collection = shirts
#client = MongoClient('mongodb://localhost:27017/')
#db = client.products
#collection = db.shirts


connection = Connection('localhost', 27017)
dbmongo = connection.product
collection = dbmongo.shirt

# *******************************
# MYSQL DB
# *******************************
# db = 273db, table=shoe

dbsql = _mysql.connect(host="localhost",
                       user="sandy",
                       passwd="locked",
                       db="shoeJson")




# *******************************
# UTILITY FUNCTIONS
# *******************************

# Function to check login credentials
def check_login(username,password):
  if username == "niveditha.h87@gmail.com" and password == "nived":
    return '''true'''


# Function to build dictionary response
def build_json_resp(code=200, status="OK", message='Success!'):
  resp = {}
  resp['status'] = status
  resp['code'] = code
  resp['message'] = message
  return resp

def buil_json_insert_resp(code=200,status="OK", message='Success!',createdDate='null'):
  resp = {}
  resp['status'] = status
  resp['code'] = code
  resp['message'] = message
  resp['createdDate'] = createdDate
  return resp

# Function to validate the parameters in json request
# Return True if request is valid
disallowed_params = ['_id']
required_params = ['shirtId','shoeId']

def validate_json_req(req):
  # check for disallowed params. These should not be there in req
  for param in disallowed_params:
    if param in req:
      return False
  # Check if the required params are present. These should be in req.
  for param in required_params:
    if param not in req:
      return False
  # All checks done. Return true.
  return True


# Function to parse json request.
# Returns valid json req object or None
def parse_request(req):
  if not req:
    return None

  # logging the request
  logger.info('Input request = {0}'.format(req))

  # try to parse the request body to json
  try:
    json_req = json.loads(req)
    ret = validate_json_req(json_req)
    if ret == False:
      return None
  except:
    return None

  # logging the JSON request
  logger.info('Input JSON request = {0}'.format(json_req))

  # return json req
  return json_req





#********************************
# BOTTLE APPLICATION
# *******************************

# Create bottle app
app = bottle.Bottle()


# Main app route mapping logic

@app.route('/login', method='POST')
def do_login():
  username = request.forms.get('username')
  password = request.forms.get('password')
  if check_login(username, password):
    return "<p>Your login information was correct.</p>"
  else:
    return "<p>Login failed.</p>"


@app.route('/shirts', method='POST')
def add_shirt():
  # get the request body
  req = request.body.read()

  createdDatem=strftime("%Y-%m-%d %H:%M:%S", gmtime());
  # Parse the request
  #json_req = parse_request(req)
  json_req = json.loads(req)

  if not json_req:
    #response.code = 400
    return build_json_resp(400,'ERR','Invalid JSON request received!')

  # Update the mongodb
  try:
    # Check if the shirt is already in mongodb
    shirtId = json_req['shirtId']
    shirt = collection.find_one({'shirtId': shirtId})
    if shirt:
      # shirt already present in mongodb
      msg = 'shirt with id = {0} already exists!'.format(shirtId)
      return build_json_resp(400,"ERR",msg)

    # shirt not present in mongodb. Add shirt
      current_time = str(datetime.now())
      json_req["createdDate"] = current_time
      logger.info('get the body of post ={0}'.format(json_req))
      collection.save(json_req)
  except:
    return build_json_resp(500,"ERR", 'Updating mongodb failed!')

  # Successful operation
  return buil_json_insert_resp(200,"OK","SUCCESS",strftime("%Y-%m-%d %H:%M:%S", gmtime()))
  # return build_json_resp()


@app.route('/shirt/:shirtId', method='GET')
def get_shirt(shirtId):
  # logging the request
  logger.info('shirt id = {0}'.format(shirtId))

  # Find the shirt in mongodb
  shirt = collection.find_one({'shirtId':shirtId})
  if not shirt:
    # shirt with given id not found
    msg = 'shirt with id {0} not found!'.format(shirtId)
    return build_json_resp(400,"ERR",msg)

  # shirt found. Returned value is a dictionary
  # Delete the _id field. This is internal to mongo
  del shirt['_id']

  # Return the shirt
  return shirt


@app.route('/shirts', method='PUT')
def update_shirt():
  # get the request body
  req = request.body.read()

  # Parse the request
  #json_req = parse_request(req)
  json_req = json.loads(req)
  if not json_req:
    return build_json_resp(400,'ERR','Invalid JSON request received!')

  # Update the shirt in mongodb
  shirtId = json_req['shirtId']
  try:
    # Check if the shirt exists. If not return.
    shirt = collection.find_one({'shirtId':shirtId})
    if not shirt:
      # shirt with given id not found
      msg = 'shirt with id {0} not found!'.format(shirtId)
      return build_json_resp(400,"ERR",msg)

    logger.info('Mongo db shirt info = {0}'.format(shirt))

    # merge the shirt content from mongo with update request
    # both shirt and update_req are dictionaries
    updated_shirt = dict(shirt, **json_req)
    logger.info('Updated shirt info = {0}'.format(updated_shirt))
    collection.save(updated_shirt)
  except:
    return build_json_resp(500, 'ERR', 'Updating mongodb failed!')

  # Successful operation
  return build_json_resp()


@app.route('/shirts', method='DELETE')
def delete_shirt():
  # get the request body
  req = request.body.read()

  # Parse the request
  #json_req = parse_request(req)
  json_req = json.loads(req)
  if not json_req:
    return build_json_resp(400,'ERR','Invalid JSON request received!')
  #TODO : delete shirt from mongodb
  shirtId = json_req['shirtId']
  try:
    #check if the shirt exists.If not return.
    shirt = collection.find_one({'shirtId':shirtId})
    if not shirt:
      #shirt with given id not found
      msg = 'shirt with id {0} not found!'.format(shirtId)
      return build_json_resp(400,'ERR',msg)

    logger.info("Mongo db shirt info = {0}".format(shirt))

    collection.remove({'shirtId':shirtId})

  except:
    return build_json_resp(500,'ERR','Deleting document from MongoDB failed!')
  # Successful operation
  return build_json_resp()


@app.route('/shoe/<shoeId>',method='GET')
def show(shoeId):
  # logging the request
  logger.info('shoe id = {0}'.format(shoeId))

  # Find the show in MySQL db
  query = "select * from shoe where shoeId = '{0}'".format(shoeId)
  dbsql.query(query)
  r = dbsql.store_result()
  row = r.fetch_row()
  if not row:
    # shoe with given id not found
    msg = 'shoe with id {0} not found!'.format(shoeId)
    return build_json_resp(400,"ERR",msg)

    #shoe obtained
  if row:
    #return str(row)
     return json.dumps(row)


@app.route('/shoes', method='POST')
def addingShoeMysql():
 # get the request body
  req = request.body.read()
  logger.info('Input request in addingShoeMysql = {0}'.format(req))
  # Parse the request
  json_req = json.loads(req)
  shoeId = json_req['shoeId']
  shoeName = json_req['shoeName']
  shoeQuantity = json_req['shoeQuantity']
  createdBy = json_req['createdBy']
  createdDate = strftime("%Y-%m-%d %H:%M:%S", gmtime());
  logger.info('shoe name in addingShoeMysql = {0},{1}'.format(shoeId,shoeName))
  if not json_req:
    #response.code = 400
    return build_json_resp(400,'ERR','Invalid JSON request received!')
  logger.info('json request in addingShoeMysql = {0}'.format(json_req))

  # Insert into Mysqldb
  try:
    logger.info('hello')
    query = "insert into shoe(shoeId,shoeName,shoeQuantity,createdBy,createdDate) values ('{0}','{1}','{2}','{3}','{4}')".format(shoeId,shoeName,shoeQuantity,createdBy,createdDate)
    dbsql.query(query)
    logger.info('after running query')

  except:
    return build_json_resp(500,"ERR", 'Insertion in mysql failed!')

  # Successful operation
  return buil_json_insert_resp(200,"OK","SUCCESS",createdDate)
#return build_json_resp()



@app.route('/shoes', method='DELETE')
def delete_shoe():
        req = request.body.read()
        # get the request body
        json_req = json.loads(req)
        if not json_req:
                return build_json_resp(400,'ERR','Invalid JSON request received!')


        #TODO : delete shirt from mysql
        shoeId = json_req['shoeId']

        try:
                #check if the shoe exists.If not return.
                query = "select * from shoe where shoeId = '{0}'".format(shoeId)
                dbsql.query(query)
                r = dbsql.store_result()
                row = r.fetch_row()
                if row:
                        logger.info("My SQL shoe info = {0}".format(shoeId))
                        dbsql.query("delete from shoe where shoeId = '{0}'".format(shoeId))

        except:
                return build_json_resp(500,'ERR','Deleting document from Mysql failed!')
        # Successful operation
        return build_json_resp()

@app.route('/shoes', method='PUT')
def update_shoe():
        req = request.body.read()
        # get the request body
        json_req = json.loads(req)
        if not json_req:
                return build_json_resp(400,'ERR','Invalid JSON request received!')
        # Update the shoe in mysql
        shoeId = json_req['shoeId']
        shoeName = json_req['shoeName']
        shoeQuantity = json_req['shoeQuantity']
        createdBy = json_req['createdBy']
        try:
                query = "select * from shoe where shoeId = '{0}'".format(shoeId)
                dbsql.query(query)
                r = dbsql.store_result()
                row = r.fetch_row()
                if row:
                         dbsql.query("update shoe set shoeName = '{0}',shoeQuantity ='{1}',createdBy = '{2}' where shoeId = '{3}'".format(shoeName,shoeQuantity,createdBy,shoeId))
                else:
                        # shoe id was not found
                        msg = 'shoe with id {0} not found!'.format(shoeId)
                        return build_json_resp(400,"ERR",msg)
        except:
                return build_json_resp(500, 'ERR', 'Updating mysql failed!')

        # Successful operation
        return build_json_resp()


run(app, host='localhost', port=8081)
