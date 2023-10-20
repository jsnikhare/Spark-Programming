# Use case for Homework for Data Engineer. 
# Implement solution to read data from .json file format and insert the data into a RDBMS of your choice. 
# 2nd part -Answer some questions using SQL which apply below business logics, data quality rules for data cleansing and transform data into final output
# 1. Sum value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" per year
# 2. Year with max value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" from year 2008 and later (inclusive)
# 3. Max value of each measurement per state
# 4. Average value of "Number of person-days with PM2.5 over the National Ambient Air Quality Standard (monitor and modeled data)" per year and state in ascending order
# 5. State with the max accumulated value of "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" overall years
# 6. Average value of "Number of person-days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" in the state of Florida
# 7. County with min "Number of days with maximum 8-hour average ozone concentration over the National Ambient Air Quality Standard" per state per year
# Read data from AQM_Data1.json file

@route('/shoes', method='POST')
def createorder():
    cursor = db.cursor()
    data = request.json
    p_id = request.json['product_id']
    p_desc = request.json['product_desc']
    color = request.json['color']
    price = request.json['price']
    p_name = request.json['product_name']
    q = request.json['quantity']
    createDate = datetime.now().isoformat()
    print (createDate)
    response.content_type = 'application/json'
    print(data)
    if not data:
        abort(400, 'No data received')

    sql = "insert into productshoes (product_id, product_desc, color, price, product_name, quantity, createDate) values ('%s', '%s','%s','%d','%s','%d', '%s')" %(p_id, p_desc, color, price, p_name, q, createDate)
    print (sql)
    try:
    # Execute dml and commit changes
        cursor.execute(sql,data)
        db.commit()
        cursor.close()        
    except:
    # Rollback changes
        db.rollback()
    return dumps(("OK"),default=json_util.default)
