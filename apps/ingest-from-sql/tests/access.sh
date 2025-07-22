if [[ "${SQL_SERVER}" == "mysql" ]];
then
    mysql --protocol=TCP -h localhost -u root -proot
else
    PGPASSWORD=root psql -U root -h127.0.0.1 -d aperturedb
fi
