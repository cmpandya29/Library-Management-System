import mysql.connector
import pandas as pd
from pandas import Series, DataFrame
from sqlalchemy import create_engine
import datetime as dt
from mysql.connector import errorcode
import traceback
from prettytable import PrettyTable
from texttable import Texttable
from dateutil.relativedelta import *
import time
from datetime import date

def db_connection():
    try:
        global mydb
        mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
            database="universityLibrary",
            db='mydb'
        )
   
        global cursor 
        cursor = mydb.cursor()
        global engine
        engine = create_engine('mysql+mysqlconnector://root:password@:3306/universityLibrary')
        return "DB Connected!"
    except Exception as e:
        return e
    
def create_db():
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
    )
    cursor = mydb.cursor()
    query = "create database universityLibrary"
    try:
            print("Creating database university library: ")
            cursor.execute(query)            
            
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("Done!")
        
def delete_db():
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
            database="universityLibrary"
    )
    cursor = mydb.cursor()
    query = "drop database universityLibrary"
    try:
            print("Deleting database university library: ")
            cursor.execute(query)            
            
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("does not exists.")
        else:
            print(err.msg)
    else:
        print("Done!")
    
def create_table():
    
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
            database="universityLibrary",
            db='mydb'
        )
   
        
    cursor = mydb.cursor()
    tables = {}
    tables['BOOK'] = (
        "CREATE TABLE `BOOK` ("
        "  `ISBN` varchar(10) NOT NULL,"
        "  `Title` varchar(100) NOT NULL,"
        "  `AUTHOR` varchar(100) NOT NULL,"
        "  `SUBJECT_AREA` varchar(50) NOT NULL,"
        "  `LANGUAGE` varchar(10) NOT NULL,"
        "  `BINDING_TYPE` varchar(10) NOT NULL,"
        "  `EDITION` varchar(10) NOT NULL,"
        "  Primary key(`ISBN`)"
        ")")

    tables['BOOKS_AVAILABLE_IN_LIB'] = (
        "CREATE TABLE `BOOKS_AVAILABLE_IN_LIB` ("
        "  `ISBN` varchar(10) NOT NULL,"
        "  `CAN_BE_RENTED_FLAG` Boolean NOT NULL,"
        "  `TOTAL_NO_COPIES` int NOT NULL,"
        "  `NO_OF_COPIES_LENTED` int NOT NULL,"
        "  `BOOK_DESCRIPTION` varchar(200),"
        "  `BOOK_TYPE` varchar(40),"
        "  Primary key(`ISBN`)"
        ")")

    tables['BOOKS_REQUIRED'] = (
        "CREATE TABLE `BOOKS_REQUIRED` ("
        "  `ISBN` varchar(10) NOT NULL,"
        "  `NO_OF_REQUIRED_COPIES` int NOT NULL,"
        "  Primary key(`ISBN`)"
        ")")

    tables['LIBRARY_MEMBER'] = (
        "CREATE TABLE `LIBRARY_MEMBER` ("
        "  `SSN` varchar(10) NOT NULL,"
        "  `NAME` varchar(30) NOT NULL,"
        "  `CAMPUS_ADDRESS` varchar(40) NOT NULL,"
        "  `HOME_ADDRESS` varchar(40) NOT NULL,"
        "  `PHONE_NUMBER` varchar(20) NOT NULL,"
        "  `CARD_NUMBER` varchar(10) NOT NULL,"
        "  `CARD_EXPIRY_DATE` date NOT NULL,"
        "  `IS_PROFESSOR_FLAG` Boolean,"
        "  `Member_Type` VARCHAR(30),"
        "  primary key(`SSN`)"
        ")")

    tables['LIBRARY_STAFF'] = (
        "CREATE TABLE `LIBRARY_STAFF` ("
        "  `SSN` varchar(10) NOT NULL,"
        "  `Name` varchar(40) NOT NULL,"
        "  `Staff_Type` varchar(40) NOT NULL,"
        "  Primary Key(`SSN`)"
        ")")
    
    tables['ISSUE_BOOK'] = (
        "CREATE TABLE `ISSUE_BOOK` ("
        "  `ISSUE_ID` int NOT NULL,"
        "  `MEMBER_SSN` varchar(10) NOT NULL,"
        "  `STAFF_SSN` varchar(10) NOT NULL,"
        "  `ISBN` varchar(10) NOT NULL,"
        "  `ISSUE_DATE` date NOT NULL,"
        "  `NOTICE_DATE` date NOT NULL,"
        "  `GRACE_PERIOD_DAYS` int NOT NULL,"
        "  `BOOK_DUE_DATE` date,"
        "  Primary key(`ISSUE_ID`),"
        "  FOREIGN KEY (`MEMBER_SSN`) REFERENCES LIBRARY_MEMBER(`SSN`),"
        "  FOREIGN KEY (`STAFF_SSN`) REFERENCES LIBRARY_STAFF(`SSN`),"
        "  FOREIGN KEY (`ISBN`) REFERENCES BOOK(`ISBN`)"
        ")")

    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
           
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("Done!")

            
def del_table():
    mydb = mysql.connector.connect(
            host="127.0.0.1",
            user="root",
            password="password",
            database="universityLibrary",
            db='mydb'
        )
   
        
    cursor = mydb.cursor()
    tables = {}
    
    tables['ISSUE_BOOK'] = ("drop table `ISSUE_BOOK`")
    tables['LIBRARY_STAFF'] = ("drop table `LIBRARY_STAFF`")
    tables['LIBRARY_MEMBER'] = ("drop table `LIBRARY_MEMBER`")
    tables['BOOKS_REQUIRED'] = ("drop table `BOOKS_REQUIRED`")
    tables['BOOKS_AVAILABLE_IN_LIB'] = ("drop table `BOOKS_AVAILABLE_IN_LIB`")
    tables['BOOK'] = ("drop table `BOOK`")
    
    for table_name in tables:
        table_description = tables[table_name]
        try:
            print("Droping table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_CANT_FIND_SYSTEM_REC:
                print("Can't delete the table")
            else:
                print(err.msg)
        else:
            print("Done!")
              
def import_library_staff():
    
    log = open("log.txt", "w")
    try:
        df_library_staff = pd.read_excel('Library_Staff.xlsx',encoding = "cp1252")
        df_library_staff.to_sql(name='library_staff', con=engine, if_exists = 'append', index=False)
        
        print("Data imported to Library Staff Table")
    except Exception as e:
        #traceback.print_exc(file=log)
        print("Error Occurred.")

def import_books_available_in_lib():
    try:
        df_books_available_in_lib = pd.read_excel('Books_Available_In_Library.xlsx',encoding = "cp1252")
        df_books_available_in_lib.to_sql(name='books_available_in_lib', con=engine, if_exists = 'append', index=False)
        
        print("Data imported to Books Available In Library Table")
    except Exception as e:
        print("Error Occurred.")
        
def import_books():
    try:
        df_books = pd.read_excel('Books.xlsx',encoding = "cp1252")
        df_books.to_sql(name='book', con=engine, if_exists = 'append', index=False)
        print("Data imported to Match Books Table")
    except Exception as e:
        print("Error Occurred.")
    
def import_books_required():
    try:
        df_books_required = pd.read_excel('BOOKS_REQUIRED.xlsx',encoding = "cp1252")
        df_books_required.to_sql(name='books_required', con=engine, if_exists = 'append', index=False)
        print("Data imported to Books Required Table")
    
    except Exception as e:
        print("Error Occurred.")
    
def import_library_member():
    try:
        df_library_member = pd.read_excel('Library_Member.xlsx',encoding = "cp1252")
        df_library_member['CARD_EXPIRY_DATE'] = pd.to_datetime(df_library_member['CARD_EXPIRY_DATE'])
        df_library_member.to_sql(name='library_member', con=engine, if_exists = 'append', index=False)
        print("Data imported to Library Member Table")
    
    except Exception as e:
        print("Error Occurred.")
        
def import_issue_book():
    try:
        df_issue_book = pd.read_excel('ISSUE_BOOK.xlsx',encoding = "cp1252")
        df_issue_book['ISSUE_DATE'] = pd.to_datetime(df_issue_book['ISSUE_DATE'])
        df_issue_book['NOTICE_DATE'] = pd.to_datetime(df_issue_book['NOTICE_DATE'])
        df_issue_book['BOOK_DUE_DATE'] = pd.to_datetime(df_issue_book['BOOK_DUE_DATE'])
        df_issue_book.to_sql(name='issue_book', con=engine, if_exists = 'append', index=False)
        print("Data imported to Issue Book Table")
    
    except Exception as e:
        print("Error Occurred.")

def display_library_staff():
    table = PrettyTable(['Index','SSN','Name','Staff Type'])
    sql_select_Query = "select * from library_staff"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1
    for row in records:
        table.add_row([index,row[0],row[1],row[2]])
        index+=1

    print(table)
    
def display_book():
    sql_select_Query = "select * from book"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1
    
    table = Texttable()
    table.set_max_width(max_width=150)

    table.add_row(['Index','ISBN','Title','Author','Subject Area','Language','Binding Type','Edition'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4],row[5],row[6]])
        index+=1

    print(table.draw())

def display_books_available_in_lib():
    sql_select_Query = "select * from books_available_in_lib"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1

    table = Texttable()
    table.set_max_width(max_width=120)

    table.add_row(['Index','ISBN','Can be rented flag','Total No Copies','No of copies lented','Book Description','Book Type'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4],row[5]])
        index+=1

    print(table.draw())
    
def display_books_required():
    table = PrettyTable(['Index','ISBN','No of required copies'])
    sql_select_Query = "select * from books_required"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1
    for row in records:
        table.add_row([index,row[0],row[1]])
        index+=1

    print(table)
    
def display_library_member():
    sql_select_Query = "select * from library_member"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1

    table = Texttable()
    table.set_max_width(max_width=110)

    table.add_row(['Index','SSN','Name','Campus Address','Home Address','Phone Number','Card Number','Card Expiry Date','Is Professor Flag','Member Type'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8]])
        index+=1

    print(table.draw())
    
def display_issue_book():
    sql_select_Query = "select * from issue_book"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1

    table = Texttable()
    table.set_max_width(max_width=110)

    table.add_row(['Index','Issue Id','Member SSN','Staff SSN','ISBN','Issue Date','Notice Date','Grace Period Days','Book Due Date'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]])
        index+=1

    print(table.draw())
    
def inset_library_member():
    try:
        print("\n\nEnter Library member information: ")
        ssn = input("Enter SSN: ")
        name = input("Enter name: ")
        camp_add = input("Enter campus address: ")
        home_add = input("Enter home address: ")
        phone = input("Enter phone number: ")
        card = input("Enter card number: ")
        card_date = input("Enter card expiry date(mm/dd/yy): ")
        member_flag = input("Is member professor(1/0): ")
        member_type = input("Enter member type: ")

        mycursor = mydb.cursor()
        card_date = pd.to_datetime(card_date)
        sql = "INSERT INTO library_member VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (ssn, name, camp_add, home_add, phone, card, card_date, member_flag, member_type)
        mycursor.execute(sql, val)

        mydb.commit()

        print("1 record inserted, ID:", mycursor.lastrowid)
        
    except mysql.connector.Error as error :
        print("Failed to insert into MySQL table {}".format(error))
        
        
def insert_book():
    try:
        print("\n\nEnter New Book information: ")
        isbn = input("Enter ISBN: ")
        title = input("Enter Title: ")
        author = input("Enter Author: ")
        sub_area = input("Enter subject area: ")
        lang = input("Enter language: ")
        binding_type = input("Enter binding type: ")
        edition = input("Enter book edition: ")

        mycursor = mydb.cursor()
        
        sql = "INSERT INTO book VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (isbn, title, author, sub_area, lang, binding_type, edition)
        mycursor.execute(sql, val)

        mydb.commit()

        print("1 record inserted, ID:", mycursor.lastrowid)
        
    except mysql.connector.Error as error :
        print("Failed to insert into MySQL table {}".format(error))
        
def borrow_book():
    print("\nInformation to borrow a new book!")
    flag = True
    while(flag):
        ssn = input("Enter member ssn: ")
        sql_select_Query = "select * from library_member where SSN='{}'".format(ssn)
        cursor = mydb.cursor()
        cursor.execute(sql_select_Query)
        memberRecords = cursor.fetchall()
        if(len(memberRecords)==0):
            print("No member found! Enter valid ssn.")
        else:

            flag = False

    flag = True
    while(flag):
        isbn = input("Enter book ISBN: ")
        sql_select_Query = "select * from book where ISBN='{}'".format(isbn)
        cursor = mydb.cursor()
        cursor.execute(sql_select_Query)
        bookRecords = cursor.fetchall()
        if(len(bookRecords)==0):
            print("No book found! Enter valid ISBN.")
        else:

            flag = False

    sql_select_Query = "select max(issue_id) from issue_book"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    issue_id = records[0][0]+1
    issue_date = dt.datetime.now()

    issue_date = pd.to_datetime(issue_date)

    is_professor_flag = memberRecords[0][7]

    if(is_professor_flag==1):
        due_date = issue_date + relativedelta(months=+3)
        grace_period = 14
        notice_date = due_date + relativedelta(days=-14)
    else:
        due_date = issue_date + relativedelta(days=+21)
        grace_period = 7
        notice_date = due_date + relativedelta(days=-7)

    mycursor = mydb.cursor()
    #card_date = pd.to_datetime(card_date)
    sql = "INSERT INTO issue_book VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (issue_id, ssn, '1451648537', isbn, issue_date, notice_date, grace_period, due_date)
    mycursor.execute(sql, val)

    mydb.commit()

    print("1 record inserted, ID:", mycursor.lastrowid)

    
def return_book():
    print("\nReturn a book!")
    flag = True
    while(flag):
        ssn = input("Enter member ssn: ")
        sql_select_Query = "select * from issue_book where MEMBER_SSN='{}'".format(ssn)
        cursor = mydb.cursor()
        cursor.execute(sql_select_Query)
        memberRecords = cursor.fetchall()

        if(len(memberRecords)==0):
            print("You dont have any book issued!")
        else:
            flag = False

    flag = True
    while(flag):
        isbn = input("Enter book ISBN: ")
        sql_select_Query = "select * from issue_book where ISBN='{}'".format(isbn)
        cursor = mydb.cursor()
        cursor.execute(sql_select_Query)
        bookRecords = cursor.fetchall()

        if(len(bookRecords)==0):
            print("No book found! Enter valid ISBN.")
        else:
            flag = False


    file=open("receipt.txt","w+")

    file.write("Return Receipt\n\n")
    file.write("Member SSN: {}\n".format(memberRecords[0][1]))
    file.write("Book ISBN: {}\n".format(bookRecords[0][3]))
    file.write("Issue date: {}\n".format(bookRecords[0][4]))
    file.write("Return date: {}".format(pd.to_datetime(dt.datetime.now())))
    file.close()
    print("Data is saved to receipt.txt file.")
    mycursor = mydb.cursor()
    sql = "DELETE FROM issue_book WHERE MEMBER_SSN='{}' and ISBN='{}'".format(ssn,isbn)

    mycursor.execute(sql)
    mydb.commit()

    print(mycursor.rowcount, "record(s) deleted")

def renew_membership():
    todays_date = date.today()
    now_date = dt.datetime.now()

    sql_select_Query = "select * from library_member where CARD_EXPIRY_DATE='{}'".format(todays_date)
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    memberRecords = cursor.fetchall()

    new_date = now_date + relativedelta(years=+4)

    mycursor = mydb.cursor()
    sql = "UPDATE library_member SET CARD_EXPIRY_DATE = '{}' WHERE CARD_EXPIRY_DATE = '{}'".format(new_date,todays_date)
    mycursor.execute(sql)
    mydb.commit()


    table = PrettyTable(['Index','Name','SSN','Card Number','Old Card Expiry Date','New Card Expiry date'])
    index = 1
    for row in memberRecords:
        table.add_row([index,row[1],row[0],row[5],row[6],new_date])
        index+=1
    print(table)
    
def weekly_report():
    print("Generate report!")
    start = input("Enter start date(yyyy-mm-dd): ")
    end = input("Enter end date(yyyy-mm-dd): ")

    sql_select_Query = "select b.isbn, ib.issue_date, count(*) as number_of_copies, datediff(ib.book_due_date, ib.issue_date) as no_of_days, b.author, b.title, lm.ssn, lm.name from issue_book as ib, library_member as lm, book as b where ib.issue_date between '{}' and '{}' and b.isbn=ib.isbn and lm.ssn=ib.member_ssn group by b.isbn".format(start,end)
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1

    table = Texttable()
    table.set_max_width(max_width=110)

    table.add_row(['Index','Book Title','Book Author','Book ISBN','Member Name','Member SSN','Issue Date','No of Copies','Loan Days'])
    for row in records:
        table.add_row([index,row[5],row[4],row[0],row[7],row[6],row[1],row[2],row[3]])
        index+=1

    print("Report generated to report.txt file!")
    file=open("report.txt","w+",encoding='utf-8')

    file.write("Weekly Report!\n\n")

    file.write(table.draw())

    file.close()
    
def execute_triggers():
    mycursor = mydb.cursor()
    sql = "INSERT INTO tempTrigger VALUES (2)"
    mycursor.execute(sql)
    mydb.commit()

    sql_select_Query = "select * from tempdata"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1
    
    table = Texttable()
    table.set_max_width(max_width=110)

    table.add_row(['Index','SSN','Name','Phone Number','Card Number','Card expiry date'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4]])
        index+=1

    print("Triggered data for membership renewal saved in trigger5_1.txt file!")
    file=open("trigger5_1.txt","w+")

    file.write("Membership renewal data!\n\n")

    file.write(table.draw())

    file.close()
    
    
    sql_select_Query = "select * from tempdata2"
    cursor = mydb.cursor()
    cursor.execute(sql_select_Query)
    records = cursor.fetchall()
    index = 1
    
    table = Texttable()
    table.set_max_width(max_width=100)

    table.add_row(['Index','ISBN','Issue date','Book Due Date','Over due days','Author','Title','SSN','Name'])
    for row in records:
        table.add_row([index,row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]])
        index+=1

    print("Triggered data for outstanding overdue book in trigger5_2.txt file!")
    file=open("trigger5_2.txt","w+",encoding='utf-8')

    file.write("Outstanding overdue data!\n\n")

    file.write(table.draw())

    file.close()
