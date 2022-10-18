# Данные взяты с сайта tvmaze  

host - https://api.tvmaze.com/  
login - airflow  
password - airflow  

# Также используется второй контейнер с postgres  

conn_id - postgres2  
host - postgres2  
schema - postgres  
login - root  
password - root  
port - 5432  


# Два дага, где первый - берет информацию за каждый день, а второй - каждый месяц группирует данные и заносит в таблицу  

проверила, все классненько работает