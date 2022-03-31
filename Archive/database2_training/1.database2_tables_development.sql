CREATE TABLE employee(
    id BIGSERIAL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender VARCHAR(20),
    email VARCHAR(150),
    date_of_birth DATE

);


CREATE TABLE employee(
    id BIGSERIAL NOT NULL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    gender VARCHAR(6) NOT NULL,
    email VARCHAR(150),
    date_of_birth DATE NOT NULL
);

--A>B то A, иначе В. 
--A<B то А, иначе В.