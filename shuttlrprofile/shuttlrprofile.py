import pandas as pd

def get_age(df, conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
#     print str_users
    
    age_query = """select USER_ID, DOB
    from USER_MANAGEMENT_SYSTEMS.USER_ATTRIBUTES
    where user_id in ({0})
    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    age_df = pd.read_sql(age_query.format(str_users), conn)
    age_df['age'] = age_df.apply(lambda x: (dt.datetime.now().year- pd.to_datetime(x.DOB/1000, unit='s').year), axis =1)
    age_df = age_df[['USER_ID', 'age']]
    
    new_df = df.merge(age_df, on='USER_ID', how='left')
    new_df['age'] = new_df['age'].fillna(0)
    return new_df


def get_gender(df,conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    gender_query = """select gender_a.user_id as USER_ID, gender_b.gender from
    (
    select user_id , max(created) as created 
    from customer_relationship_management.gender_data 
    where user_id in ({0})
    group by 1
    ) gender_a
    left join
    (
    select user_id , created, gender
    from customer_relationship_management.gender_data
    where user_id in ({0})
    ) gender_b
    on gender_a.user_id = gender_b.user_id and gender_a.created = gender_b.created
    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    gender_df = pd.read_sql(gender_query.format(str_users), conn)
    
    
    new_df = df.merge(gender_df, on='USER_ID', how='left')
    
    new_df['gender'] = new_df['gender'].fillna('NA')
    new_df['gender'] = new_df.gender.str.lower()
    return new_df
                         
                      
def get_salary(df,conn):
    
    def new_salary(x):
        if x.new_salary:
            return x.new_salary
        elif x.old_salary:
            return x.old_salary
        else:
            return 0
    
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    salary_query = """
    select old_naukri.user_id as USER_ID, old_salary, new_salary from
    (
    select user_id, ctc as old_salary, last_modified_cv as old_salary_date
    from customer_relationship_management.archive_naukri_data
    where user_id in ({0})
    ) old_naukri
    left join
    (
    select user_id, ctc as new_salary, last_modified_cv as new_salary_date
    from customer_relationship_management.naukri_data_new
    where user_id in ({0})
    ) new_naukri
    on old_naukri.user_id = new_naukri.user_id
    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    salary_df = pd.read_sql(salary_query.format(str_users), conn)
   
    
    salary_df['salary'] = salary_df.apply(lambda x: new_salary(x), axis =1)
   
    
    salary_df = salary_df[['USER_ID', 'salary']]
    salary_df['USER_ID'] = salary_df.USER_ID.astype(int)
   
    
    new_df = df.merge(salary_df, on='USER_ID', how='left')
   
    new_df['salary'] = new_df['salary'].fillna(0)

    return new_df
                         
                      




def get_preferred_route_morning(df, conn, from_date, to_date):
    
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID, 
    route_id as route_id_morn,
    count(*) as rides_morn
    from USER_MANAGEMENT_SYSTEMS.BOOKINGS
    where is_delete = False
    and hour(from_unixtime(boarding_time/1000)) < 13
    and status in ('CONFIRMED','RESCHEDULED')
    and booking_type not in ('PRE_BOOK_ONLINE_BOOKING','PRE_BOOK_ONLINE_BOOKING_V2','B2B_JOB_BOOKING')
    and date(from_unixtime(boarding_time/1000)) between '{0}' and '{1}'
    and user_id in ({2})
    group by 1, 2

    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(from_date, to_date, str_users), conn)
    
    df2 = df2.sort_values(['USER_ID','rides_morn'], ascending=False)
    df2 = df2.drop_duplicates('USER_ID')
#     df2.columns = 
   
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    new_df['route_id_morn'] = new_df['route_id_morn'].fillna('NA')
    new_df['rides_morn'] = new_df['rides_morn'].fillna('NA')

    return new_df
                         
                      





def get_preferred_route_evening(df, conn, from_date, to_date):
    
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID, 
    route_id as route_id_eve,
    count(*) as rides_eve
    from USER_MANAGEMENT_SYSTEMS.BOOKINGS
    where is_delete = False
    and hour(from_unixtime(boarding_time/1000)) >= 13
    and status in ('CONFIRMED','RESCHEDULED')
    and booking_type not in ('PRE_BOOK_ONLINE_BOOKING','PRE_BOOK_ONLINE_BOOKING_V2','B2B_JOB_BOOKING')
    and date(from_unixtime(boarding_time/1000)) between '{0}' and '{1}'
    and user_id in ({2})
    group by 1, 2

    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(from_date, to_date, str_users), conn)
    
    df2 = df2.sort_values(['USER_ID','rides_eve'], ascending=False)
    df2 = df2.drop_duplicates('USER_ID')
#     df2.columns = 
   
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    new_df['route_id_eve'] = new_df['route_id_eve'].fillna('NA')
    new_df['rides_eve'] = new_df['rides_eve'].fillna('NA')

    return new_df
                         
                      





def get_refer_attempts(df, refer_option_file, refer_code_file): #incomplete

    refer_screen = pd.read_csv(refer_option_file)
    # refer_screen = pd.read_csv('refer_option_selected_' + str(from_date) + '_to_' + str(to_date) +  '.csv')
    refer_screen = refer_screen.set_index('Date')
    refer_screen = refer_screen.T
    refer_screen = refer_screen.unstack().reset_index()
    refer_screen.columns = ['date', 'phone_number', 'count_refer_option_selected']
    refer_screen = refer_screen[(refer_screen.count_refer_option_selected > 0) & (refer_screen.phone_number != 'undefined')]
    refer_screen['date'] = pd.to_datetime(refer_screen['date'], format="%Y/%m/%d")
    refer_screen.phone_number = refer_screen.phone_number.astype('int64')
    refer_screen['month'] = pd.DatetimeIndex(refer_screen['date']).month
    refer_screen_month = refer_screen.groupby(['phone_number', 'month'])['count_refer_option_selected'].sum().reset_index()
    
    refer_screen_month = refer_screen_month[['phone_number','count_refer_option_selected']]
    refer_screen_month = refer_screen_month.fillna(0)

    
    refer_code = pd.read_csv(refer_code_file)
    # refer_code = pd.read_csv('refer_code_copied_' + str(from_date) + '_to_' + str(to_date) +  '.csv')
    refer_code = refer_code.drop('Unnamed: 1', axis=1)
    refer_code = refer_code.set_index('Date')
    refer_code = refer_code.T
    refer_code = refer_code.unstack().reset_index()
    refer_code.columns = ['date', 'phone_number', 'count_refer_code_copied']
    refer_code = refer_code[(refer_code.count_refer_code_copied> 0) & (refer_code.phone_number != 'undefined')]
    refer_code['date'] = pd.to_datetime(refer_code['date'], format="%Y/%m/%d")
    refer_code.phone_number = refer_code.phone_number.astype('int64')
    refer_code['month'] = pd.DatetimeIndex(refer_code['date']).month
    refer_code_month = refer_code.groupby(['phone_number', 'month'])['count_refer_code_copied'].sum().reset_index()
    
    refer_code_month = refer_code_month[['phone_number', 'count_refer_code_copied']]
    refer_code_month = refer_code_month.fillna(0)
        
        
    new_df = df.merge(refer_screen_month, on='phone_number', how='left')
    
    new_df2 = new_df.merge(refer_code_month, on='phone_number', how='left')
    
    new_df2['count_refer_code_copied'] = new_df2['count_refer_code_copied'].fillna(0)
    new_df2['count_refer_option_selected'] = new_df2['count_refer_option_selected'].fillna(0)
    
    new_df2['refer_attempts'] = (new_df2['count_refer_code_copied']).astype(int) +  (new_df2['count_refer_option_selected']).astype(int)
    new_df2 = new_df2.drop(['count_refer_option_selected'], axis=1)
    new_df2 = new_df2.drop(['count_refer_code_copied'], axis=1)
    
    
    return new_df2


def get_csat(df,conn, from_date, to_date):
    
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
#     q = """select * from USER_MANAGEMENT_SYSTEMS.USER_TRIP_RATINGS limit 10"""
    query = """
    select USER_ID, 
    AVG(rating) as csat,
    count(rating) as rated_rides
    from USER_MANAGEMENT_SYSTEMS.USER_TRIP_RATINGS
    where is_delete = 0
    and date(from_unixtime(created_time/1000)) between '{0}' and '{1}'
    and user_id in ({2})
    group by 1

    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
#     test =  pd.read_sql(q, conn)
#     print test
    df2 = pd.read_sql(query.format(from_date, to_date, str_users), conn)
    
    
    new_df = df.merge(df2, on='USER_ID', how='left')

    return new_df
                         
                      





def get_signup_date(df, conn):
    
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID, date(from_unixtime(created_time/1000)) as signup_date
    from USER_MANAGEMENT_SYSTEMS.USERS
    where is_delete = False
    and user_id in ({0})

    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(str_users), conn)
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    return new_df 




def get_first_ride_date(df,conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID,  
    min(date(from_unixtime(created_time/1000))) as first_ride_date
    from USER_MANAGEMENT_SYSTEMS.BOOKINGS
    where is_delete = False
    and status in ('CONFIRMED','RESCHEDULED')
    and booking_type not in ('PRE_BOOK_ONLINE_BOOKING','PRE_BOOK_ONLINE_BOOKING_V2','B2B_JOB_BOOKING')
    and user_id in ({0})
    group by 1


    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(str_users), conn)
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    return new_df 
    


def get_first_paid_date(df, conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID,  
    min(date(from_unixtime(created_time/1000))) as first_paid_date
    from USER_MANAGEMENT_SYSTEMS.BOOKINGS
    where is_delete = False
    and discounted_fare > 0 
    and coupon_code not like 'SUBS%'
    and status in ('CONFIRMED','RESCHEDULED')
    and booking_type not in ('PRE_BOOK_ONLINE_BOOKING','PRE_BOOK_ONLINE_BOOKING_V2','B2B_JOB_BOOKING')
    and user_id in ({0})
    group by 1


    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(str_users), conn)
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    return new_df 
    

def get_first_sub_date(df,conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
    
    query = """
    select USER_ID,  
    min(date(from_unixtime(created_time/1000))) as first_sub_date
    from USER_MANAGEMENT_SYSTEMS.BOOKINGS
    where is_delete = False
    and discounted_fare = 0 
    and coupon_code like 'SUBS%'
    and status in ('CONFIRMED','RESCHEDULED')
    and booking_type not in ('PRE_BOOK_ONLINE_BOOKING','PRE_BOOK_ONLINE_BOOKING_V2','B2B_JOB_BOOKING')
    and user_id in ({0})
    group by 1


    """
    
    # conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    df2 = pd.read_sql(query.format(str_users), conn)
    
    new_df = df.merge(df2, on='USER_ID', how='left')
    return new_df 
    


def salary_buckets(df):
    if int(df.salary)>0 and int(df.salary) <=500000:
        return '0to5lac'
    elif int(df.salary)>500000 and int(df.salary) <=1000000:
        return '5to10lac'
    elif int(df.salary)>1000000 and int(df.salary) <=1500000:
        return '10to15lac'
    elif int(df.salary)>1500000 and int(df.salary) <=2000000:
        return '15to20lac'
    elif int(df.salary)>2000000 and int(df.salary) <=2500000:
        return '20to25lac'
    elif int(df.salary)>2500000:
        return '25lac+'
    else:
        return 'Other'
    

def get_referred_users_count(df, conn):
    list_users = list(df.USER_ID)
    str_users = ','.join(map(str, list_users))
#     print str_users
    
    age_query = """select * from
    (select USER_ID as referred_user_id, referred_by from USER_MANAGEMENT_SYSTEMS.USERS
    where referred_by is not null ) a
    right join
    (select user_id as referring_user_id, refer_id, old_refer_id from USER_MANAGEMENT_SYSTEMS.USERS
    where user_id in ({0})) b
    on a.referred_by = b. refer_id or a.referred_by = b.old_refer_id
    """
    
    conn = MySQLdb.connect(host="localhost",port=3307,passwd="abcd1234",user="sa1209IU")
    age_df = pd.read_sql(age_query.format(str_users), conn)
    
    new_df = age_df.groupby('referring_user_id')['referred_user_id'].count().reset_index()
    new_df.columns = ['USER_ID', 'signups_generated']
#     print new_df.head()
    new_df = df.merge(new_df, on='USER_ID',  how='left')
    new_df['signups_generated'] = new_df['signups_generated'].fillna(0)
    
    return new_df




def age_bucket(x):
    if x.age>=20 and x.age <25:
        return '20to25'
    elif x.age>=25 and x.age <30:
        return '25to30'
    elif x.age>=30 and x.age <35:
        return '30to35'
    elif x.age>=35 and x.age <40:
        return '35to40'
    elif x.age>=40 and x.age <45:
        return '40to45'
    elif x.age>=45 and x.age <50:
        return '45to50'
    elif x.age>=50:
        return '50+'
    elif x.age=='NA':
        return 'Other'
    else:
        return 'Other'
