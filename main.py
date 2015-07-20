# -*- coding: utf-8 -*-

from flask import Flask
from flask_restful import reqparse, abort, Api, Resource
#from lib.Database import Database

import sqlite3 as lite
import sys
import pandas as pd

app = Flask(__name__)
api = Api(app)

def abort_if_todo_doesnt_exist(todo_id):
    if todo_id not in TODOS:
        abort(404, message="Todo {} doesn't exist".format(todo_id))

parser = reqparse.RequestParser()
parser.add_argument('word', type=str)
parser.add_argument('type', type=int)



class Database:
    # conexion a la bd normal:
    _conn = None
    # conexion que devuelve diccionarios:
    # __dictConn = None

    # ids internos de la base de datos para los distintos tipos de crawler
    # infojobs, infoempleo etc etc
    __crawlersTypeIds = dict()
    

    def __init__(self):
        try:
            self._conn = lite.connect("db/IaD2015_perfilado.sqlite")
            #print 'estoyen init'

            #self.__dictConn = lite.connect(db_path)
            #self.__dictConn.row_factory = lite.Row

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
            
            
    #def __del__(self):
    #    self._conn.close()

# Todo
# shows a single todo item and lets you delete a todo item
class Word(Resource, Database):

    def __init__(self):
        Database.__init__(self)
           

    def get(self, word):      
        list_dict = []
   
        query = 'SELECT * FROM KEYWORD WHERE word == "%s"' % str(word)
        df = pd.read_sql_query(query, self._conn)
        
        if df.shape[0] == 0:
            print 'Id not found in KEYWORD database'
            return -2
        list_dict = [df.irow(0).to_dict()]
        return list_dict
 #       abort_if_todo_doesnt_exist(url_id)
 #       return TODOS[url_id]

    def delete(self, word):
        cur = self._conn.cursor()
        try:
            query = ('DELETE FROM KEYWORD WHERE word="%s"' % (word))
            cur.execute(query)
            cur = self._conn.commit()
            return 'Removed'
        except:
            return -1

    def put(self, word):
        args = parser.parse_args()
        cur = self._conn.cursor()
        try:
            query = ('UPDATE KEYWORD SET word="%s", type=%s WHERE word="%s"' % (args['word'],args['type'], word))
            cur.execute(query)
            cur = self._conn.commit()
            return 'Updated'
        except:
            return -1


# TodoList
# shows a list of all todos, and lets you POST to add new tasks
class WordList(Resource, Database):
    def __init__(self):
        Database.__init__(self)
           
    def get(self):
        list_dict = []

        try:
            df = pd.read_sql_query('SELECT * FROM KEYWORD', self._conn)
            n_rows = df.shape[0]
            list_dict = [df.irow(ii).to_dict() for ii in range(n_rows)]

            return list_dict

        except Exception as e:
            print 'Error in Url.get, %s ' % str(e)
            return -1
            

    def post(self):
        args = parser.parse_args()
        cur = self._conn.cursor()
        try:
            query = ('INSERT INTO KEYWORD  (word,type) VALUES  ("%s",%s)' % (args['word'],args['type']))
            cur.execute(query)
            cur = self._conn.commit()
            return 'Added'
        except:
            return -1
            
##
## Actually setup the Api resource routing here
##
api.add_resource(WordList, '/words')
api.add_resource(Word, '/words/<word>')


if __name__ == '__main__':
    app.run(debug=True)
