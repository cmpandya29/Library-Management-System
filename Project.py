import Queries as qi


if __name__ == '__main__':
    qi.db_connection()
    
    print("1. Create database")
    print("2. Create tables")
    print("3. Populate the table from CSV")
    print("4. Print table data")
    print("5. Add new transaction")
    print("6. Return a book!")
    print("7. Renew membership!")
    print("8. Generate report!")
    print("9. Execute triggers!")
    print("10. Drop Table")
    print("11. Drop database")
    val = int(input("Enter your option: "))
    if val==3:
        qi.import_books()
        qi.import_books_available_in_lib()
        qi.import_books_required()
        qi.import_library_member()
        qi.import_library_staff()
        qi.import_issue_book()
        
    elif val==2:
        qi.create_table()
        
    elif val==1:
        qi.create_db()
        qi.db_connection()
        
    elif val==4:
        
        print("\n1. Book")
        print("2. Books Available in Library")
        print("3. Books Required")
        print("4. Library Member")
        print("5. Library Staff")
        print("6. Issue Book")
        select = int(input("Select table to display: "))
        if select==1:
            qi.display_book()
        elif select==2:
            qi.display_books_available_in_lib()
        elif select==3:
            qi.display_books_required()
        elif select==4:
            qi.display_library_member()
        elif select==5:
            qi.display_library_staff()
        else:
            qi.display_issue_book()
        
    elif val==5:
        qi.inset_library_member()
        qi.insert_book()
        qi.borrow_book()
    elif val==6:
        qi.return_book()
    elif val==7:
        qi.renew_membership()
    elif val==8:
        qi.weekly_report()
    elif val==9:
        qi.execute_triggers()
    elif val==11:
        qi.delete_db()
    else:
        qi.del_table()