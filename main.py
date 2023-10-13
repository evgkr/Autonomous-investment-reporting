from datetime import timedelta, datetime
from tokenAPI import db_name, db_user, db_password, db_host, db_port, token, account_id
from tinkoff.invest import Client
from functions import create_connection, execute_read_query

# Подключение к БД
connection = create_connection(
    db_name, db_user, db_password, db_host, db_port
)

print('-' * 170)

# Извлечение даты и времени последней операции сразу по обеим таблицам
datetimelast = f"""
    SELECT	
	    date,
	    time 
    FROM
	    (SELECT 
		    date, 
		    time 
	    FROM 
		    operations
		    
	    UNION ALL
	    
	    SELECT 
		    date, 
		    time 
	    FROM 
		    account_procedures
	    ) as dtlast
    ORDER BY
	    date desc,
	    time desc 
    LIMIT 1
    """

datetimelast = execute_read_query(connection, datetimelast)

datetimelast = [datetime.combine(date, time) for date, time in datetimelast]
for datetimelast in datetimelast:
    datetimelast = datetimelast

# Если таблицы пустые, дата устанавливается хардом
if datetimelast == []:
    datetimelast = datetime(1999, 1, 1, 1, 1, 1)
    print("Таблицы пока пусты")
else:
    print("Последняя записанная операция в таблицах operations и account_procedures датируется", datetimelast.date(), "в", datetimelast.time())


datelast = datetimelast.date()
timelast = datetimelast.time()

# Извлечение из таблиц всех строк, где дата и время сходятся с последними датой и временем
operationlast = f"""
    SELECT 
        operation_id,
        parent_operation_id,
	    date, 
	    time,
	    strategy_id,
	    instrument_id,
	    currency,
	    operation_type,
	    price,
	    quantity,
	    payment
    FROM 
	    operations
    WHERE
        date = '{datelast}' and time = '{timelast}'
    
    UNION ALL
    
    SELECT 
        operation_id,
        '-',
	    date, 
	    time,
	    strategy_id,
	    '-',
	    currency,
	    operation_type,
	    0,
	    0,
	    payment
    FROM 
	    account_procedures
    WHERE
        date = '{datelast}' and time = '{timelast}'
    """

operationlast = execute_read_query(connection, operationlast)

connection.commit()

oplastlist = []

# Добавление в список всех извлёченных отформатированных операций и их вывод в таком же виде, как они представлены в таблицах
for operationlast in operationlast:

    date = operationlast[2].strftime('%Y-%m-%d')

    time = operationlast[3].strftime('%H:%M:%S')

    price = float(operationlast[8])

    payment = float(operationlast[10])

    operationlast = [(operationlast[0], operationlast[1], date, time, operationlast[4], operationlast[5], operationlast[6], operationlast[7], price, operationlast[9], payment)]

    if operationlast[0][5] != '-':
        print(operationlast)
    else:
        print([(operationlast[0][0], date, time, operationlast[0][4], operationlast[0][6], operationlast[0][7], payment)])

    oplastlist += operationlast


print('-' * 170)


opl = []

# Поочерёдное получение операций для каждого счёта, начиная с последней даты, присутствующей в таблицах,
# форматирование и добавление в список новых операций (с последней даты включительно)
for account_id in account_id:
    with Client(token) as client:
        r = client.operations.get_operations(
            account_id=account_id,
            from_=datetimelast,
            state=1
        )

    for Operation in r.operations:

        operation_id = Operation.id

        if Operation.parent_operation_id != '':
            parent_operation_id = Operation.parent_operation_id
        else:
            parent_operation_id = '-'

        date = Operation.date

        strategy_id = account_id

        if Operation.instrument_uid != '':
            instrument_id = Operation.instrument_uid
        else:
            instrument_id = '-'

        if Operation.currency == 'rub':
            currency = '1'
        elif Operation.currency == 'usd':
            currency = '2'
        else:
            currency = '000'              ###unknown for now currency

        operation_type = str(Operation.operation_type.numerator)


        price = Operation.price.units + (Operation.price.nano * 10 ** (-9))


        quantity = Operation.quantity


        payment = Operation.payment.units + (Operation.payment.nano * 10 ** (-9))


        operationlist = [(operation_id, parent_operation_id, date, strategy_id, instrument_id, currency, operation_type, price, quantity, payment)]


        opl += operationlist


# Сортировка полученного списка по дате+времени
opl = sorted(opl, key=lambda x: x[2])

# Если полученный список больше списка с последними операциями, значит присутствуют новые операции для вывода и добавления в БД
if len(opl) > len(oplastlist):
    print('В таблицы operations и account_procedures внесены следующие данные:')
else:
    print('Таблицы operations и account_procedures актуальны, новых данных нет!')


# Форматирование, проверка операций на их присутствие в списке с последними операциями,
# вывод операций в нужном формате на экран и их добавление в соответствующие таблицы
for operationlist in opl:

    date = (operationlist[2] + timedelta(hours=3)).strftime('%Y-%m-%d')

    time = (operationlist[2] + timedelta(hours=3)).strftime('%H:%M:%S')

    operationlistsorted = [(operationlist[0], operationlist[1], date, time, operationlist[3], operationlist[4], operationlist[5], operationlist[6], operationlist[7], operationlist[8], operationlist[9])]

    operationlist_records = ", ".join(["%s"] * len(operationlistsorted))

    if operationlistsorted[0][5] != '-' and operationlistsorted[0] not in oplastlist:

        print(operationlistsorted)

        insert_query = (
            f"INSERT INTO "
            f"  operations (operation_id, parent_operation_id, date, time, strategy_id, instrument_id, currency, operation_type, price, quantity, payment) "
            f"VALUES "
            f"  {operationlist_records}"
        )

        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, operationlistsorted)


    if operationlistsorted[0][5] == '-' and operationlistsorted[0] not in oplastlist:

        operationlistsorted = [(operationlist[0], date, time, operationlist[3], operationlist[5], operationlist[6], operationlist[9])]

        operationlist_records = ", ".join(["%s"] * len(operationlistsorted))

        print(operationlistsorted)

        insert_query = (
            f"INSERT INTO "
            f"  account_procedures (operation_id, date, time, strategy_id, currency, operation_type, payment) "
            f"VALUES "
            f"  {operationlist_records}"
        )

        connection.autocommit = True
        cursor = connection.cursor()
        cursor.execute(insert_query, operationlistsorted)