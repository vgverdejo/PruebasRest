# -*- coding: utf-8 -*-


import sqlite3 as lite
import sys
import pandas as pd


###############################################################################
###############################################################################
###############################################################################
###############################################################################

#Clase padre que guarda la conexion a la bd:
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
            self._conn = lite.connect("../db/IaD2015_perfilado.sqlite")

            #self.__dictConn = lite.connect(db_path)
            #self.__dictConn.row_factory = lite.Row

        except lite.Error, e:
            print "Error %s:" % e.args[0]
            sys.exit(1)
            
 
            
    def __del__(self):
        self._conn.close()
        
        
#Clase para leer las urls de la base de datos:        
class Url (Database):

    """

    """
    # ids internos de la base de datos para los tipos de keywords:
    __kwsIds = dict()


    # Se insertan urls del tipo especificado.
    # El formato es un diccionario de diccionarios: [id,[code,name,type,url]]
    def insertUrl(self, tipo, dicturl):
        try:
            if tipo == 'RUCT':
                cur = self._conn.cursor()
                # cur.execute()
                for k, v in dicturl.iteritems():
                    query = 'INSERT INTO URL_RUCT (id, branch_code, branch_name, level, url) VALUES ("%s", "%s", "%s", "%s", "%s")' % (
                        k, v['code_rama'], v['name_rama'], v['type'], v['url'])

                    cur.execute(query)
                    self._conn.commit()

            if tipo == 'FP':
                cur = self._conn.cursor()
                # cur.execute()
                for k, v in dicturl.iteritems():
                    query = 'INSERT INTO URL_FP (id, branch_code, branch_name, url) VALUES ("%s", "%s", "%s", "%s")' % (
                        k, v['code_rama'], v['name_rama'], v['url'])
                    cur.execute(query)
                    self._conn.commit()

        except Exception as e:
            print 'Error in Url.insert, %s ' % str(e)
            return -1

    # Se insertan las palabras claves para cada sector.
    # Es un diccionario de diccionarios del formato[sector_(pass | stop )
    # [palabra1, palabra2...]]
    def insertKws(self,  kws):

        try:
            types = self.__getKwsId()
            cur = self._conn.cursor()
            for k, v in kws.iteritems():
                for d in v:
                    query = 'INSERT INTO KEYWORD (type, word ) VALUES ("%s", "%s" )' % (
                        types[k], d)
                    cur.execute(query)
                    self._conn.commit()
        except Exception as e:
            print 'Error in Url.insertKws, %s ' % str(e)
            return -1

            
    def get (self, url_rowid):
        list_dict = []
        try:
            query = 'SELECT * FROM URL WHERE id == "%s"' % str(url_rowid)
            df = pd.read_sql_query(query, self._conn)
            
            if df.shape[0] == 0:
                print 'Id not found in URL database'
                return -2
            list_dict = [df.irow(0).to_dict()]
            idtype = list_dict[0]['idtype']
            # Saul change this code, this is your fucking problem, bich!!!!!!
            for  value, idu in self._getCrawlersTypeId().iteritems():
                    if idu == idtype:
                        list_dict[0]['idtype'] = value
            return list_dict

        except Exception as e:
            print 'Error in Url.get, %s ' % str(e)
            return -1        
    
   


