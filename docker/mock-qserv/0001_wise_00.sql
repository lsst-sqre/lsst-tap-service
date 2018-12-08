CREATE DATABASE wise_00;
CREATE USER 'qsmaster'@'%' IDENTIFIED WITH mysql_native_password;
GRANT SELECT ON wise_00.* TO 'qsmaster'@'%';
FLUSH PRIVILEGES;
