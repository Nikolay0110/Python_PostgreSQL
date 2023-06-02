import psycopg2
from psycopg2 import Error


# Функция, создающая структуру БД
def create_table_clients(con):
    create_table_name = '''CREATE TABLE IF NOT EXISTS clients
    (id SERIAL PRIMARY KEY,
    name VARCHAR(40) NOT NULL,
    last_name VARCHAR(40) NOT NULL,
    email VARCHAR(40) NOT NULL
    );'''

    create_table_contact = '''CREATE TABLE IF NOT EXISTS contacts
        (id SERIAL PRIMARY KEY,
        phone_number VARCHAR(40),
        id_clients INTEGER NOT NULL REFERENCES clients(id)
        );'''
    cur = con.cursor()
    cur.execute(create_table_name)
    cur.execute(create_table_contact)

    print("Table created successfully")
    con.commit()
    cur.close()


def add_table_clients(con, name, last_name, id_clients, email, phone_number=None):
    insert_table_name = """ INSERT INTO clients (name, last_name, email)
                                           VALUES (%s,%s,%s);"""
    insert_table_contact = '''INSERT INTO contacts (id_clients, phone_number) 
                                                    VALUES (%s,%s);'''
    record_to_insert_1 = (name, last_name, email)
    record_to_insert_2 = (id_clients, phone_number)
    cur = con.cursor()
    cur.execute(insert_table_name, record_to_insert_1)
    cur.execute(insert_table_contact, record_to_insert_2)
    print("Записи успешно добавлены")
    print("Таблица после обновления записи")
    cur.execute("""SELECT * FROM clients""")
    cur.execute("""SELECT * FROM contacts""")
    record = cur.fetchall()
    print(record)
    con.commit()
    cur.close()


# Функция, позволяющая добавить телефон для существующего клиента
def add_clients_info(con, id_clients, phone_number):
    cur = con.cursor()
    a = """SELECT phone_number FROM contacts WHERE id_clients = %s"""
    cur.execute(a, (id_clients,))
    data = cur.fetchone()
    if data[0] == None:
        add_client_info = """UPDATE contacts SET phone_number = %s WHERE id_clients = %s"""
        cur = con.cursor()
        cur.execute(add_client_info, (phone_number, id_clients))
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно обновлена")
    else:
        add_client_info = '''INSERT INTO contacts (id_clients, phone_number) 
                                                    VALUES (%s,%s);'''
        record_to_info = (id_clients, phone_number)
        cur = con.cursor()
        cur.execute(add_client_info, record_to_info)
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно добавлена")
        print("Таблица после обновления записи")
    sql_select_query = """SELECT * FROM contacts WHERE id_clients = %s"""
    cur.execute(sql_select_query, (id_clients,))
    record = cur.fetchone()
    print(record)


# Функция, позволяющая изменить данные о клиенте
def change_clients_info(con, id_clients, name=None, last_name=None, email=None, phone_number=None,
                        new_phone_number=None):
    if name != None:
        change_client_name = """UPDATE clients SET name = %s WHERE id = %s"""
        cur = con.cursor()
        cur.execute(change_client_name, (name, id_clients))
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно обновлена")
    elif last_name != None:
        change_client_last_name = """UPDATE clients SET last_name = %s where id = %s"""
        cur = con.cursor()
        cur.execute(change_client_last_name, (last_name, id_clients))
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно обновлена")
    elif email != None:
        change_client_email = """UPDATE contacts SET email = %s WHERE id_clients = %s"""
        cur = con.cursor()
        cur.execute(change_client_email, (email, id_clients))
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно обновлена")

    elif new_phone_number != None:
        cur = con.cursor()
        a = """SELECT id FROM contacts WHERE id_clients = %s"""
        cur.execute(a, (id_clients,))
        data = cur.fetchone()
        id = data[0]
        change_client_phone_number = """UPDATE contacts SET phone_number = %s WHERE id = %s"""
        cur.execute(change_client_phone_number, (new_phone_number, id))
        con.commit()
        count = cur.rowcount
        print(count, "Запись успешно обновлена")
        cur.close()


def delete_clients_number(con, id_clients, phone_number):
    cur = con.cursor()
    a = """SELECT id, id_clients FROM contacts WHERE phone_number = %s"""
    cur.execute(a, (str(phone_number),))
    data = cur.fetchone()
    if data[1] == id_clients:
        id = data[0]
        delete_clients_number = """DELETE FROM contacts WHERE id = %s"""
        cur.execute(delete_clients_number, (id,))
        con.commit()
        count = cur.rowcount
        print(count, f"Телефон {phone_number} успешно удален")
    else:
        print(f"У клиента нет такого телефона {phone_number}")
    cur.close()


# Функция, позволяющая удалить существующего клиента
def delete_client(con, id_clients):
    cur = con.cursor()
    delete_client_1 = """DELETE FROM contacts WHERE id_clients = %s"""
    cur.execute(delete_client_1, (id_clients,))
    con.commit()
    id = id_clients
    delete_client_2 = """DELETE FROM clients WHERE id = %s"""
    cur.execute(delete_client_2, (id,))
    con.commit()
    print(f"Клиент с id_client {id_clients} успешно удален из БД clients")
    cur.close()


# Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
def find_client(con, name=None, last_name=None, email=None, phone_number=None):
    cur = con.cursor()

    find_info_client = """SELECT name, last_name, email, phone_number
                          FROM clients
                          LEFT JOIN contacts ON clients.id = contacts.id_clients
                          WHERE name = %s or last_name = %s or email = %s or phone_number = %s"""
    cur.execute(find_info_client, (name, last_name, email, phone_number))
    info = cur.fetchall()
    print(*info)
    cur.close()


try:
    con = psycopg2.connect(
        dbname='clients',
        user='postgres',
        password='',
        host='localhost',
        port='5432'
    )

    # create_table_clients(con)
    # add_table_clients(con, 'Ann', 'Vol', 1, 'assdf@mail.ru', 8753228586)
    # add_table_clients(con, 'Bob', 'Dovan', 2, 'djsaas45@mail.ru')
    # add_clients_info(con, 2, 599594030858)
    # change_clients_info(con, 2, name='Mary', phone_number=55555, new_phone_number=599594030858)
    # delete_clients_number(con, 2, 55555)
    # delete_client(con, 1)
    # find_client(con, name='Mary')



except (Exception, Error):
    print('Error!!', error)
finally:
    if con:
        con.close()
        print('соединение с PostgerSQL закрыто')
