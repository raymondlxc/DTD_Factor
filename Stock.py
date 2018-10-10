#coding=utf-8
import pandas as pd
import pymysql
db = pymysql.connect("localhost","root","","bbt")
cursor = db.cursor()
cursor.execute("select version()")
class Stock:


