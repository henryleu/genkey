CREATE DATABASE realpaasdb CHARACTER SET utf8 COLLATE utf8_general_ci;
GRANT ALL PRIVILEGES ON realpaasdb.* TO 'realpaas'@'localhost' IDENTIFIED BY 'realpaas';