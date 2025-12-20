package org.dbsp.sqlCompiler.compiler.sql.quidem;

import org.dbsp.sqlCompiler.compiler.DBSPCompiler;
import org.dbsp.sqlCompiler.compiler.sql.tools.SqlIoTest;

/* https://github.com/apache/calcite/blob/main/core/src/test/resources/sql/scalar.iq */
public class FoodmartBaseTests extends SqlIoTest {
    @Override
    public void prepareInputs(DBSPCompiler compiler) {
        // https://github.com/apache/calcite/blob/main/innodb/src/test/resources/scott.sql
        compiler.submitStatementsForCompilation("""
                DROP TABLE IF EXISTS DEPT;
                CREATE TABLE DEPT(
                    DEPTNO TINYINT NOT NULL,
                    DNAME VARCHAR(50) NOT NULL,
                    LOC VARCHAR(20)
                );
                CREATE TABLE EMP(
                    EMPNO INT NOT NULL,
                    ENAME VARCHAR(100) NOT NULL,
                    JOB VARCHAR(15) NOT NULL,
                    AGE SMALLINT,
                    MGR BIGINT,
                    HIREDATE DATE,
                    SAL DECIMAL(8,2) NOT NULL,
                    COMM DECIMAL(6,2),
                    DEPTNO TINYINT,
                    EMAIL VARCHAR(100),
                    CREATE_DATETIME TIMESTAMP,
                    CREATE_TIME TIME,
                    UPSERT_TIME TIMESTAMP NOT NULL
                );

                INSERT INTO DEPT VALUES(10,'ACCOUNTING','NEW YORK');
                INSERT INTO DEPT VALUES(20,'RESEARCH','DALLAS');
                INSERT INTO DEPT VALUES(30,'SALES','CHICAGO');
                INSERT INTO DEPT VALUES(40,'OPERATIONS','BOSTON');

                INSERT INTO EMP VALUES(7369,'SMITH','CLERK',30,7902,'1980-12-17',800,NULL,20,'smith@calcite','2020-01-01 18:35:40','18:35:40','2020-01-01 18:35:40');
                INSERT INTO EMP VALUES(7499,'ALLEN','SALESMAN',24,7698,'1981-02-20',1600,300,30,'allen@calcite','2018-04-09 09:00:00','09:00:00','2018-04-09 09:00:00');
                INSERT INTO EMP VALUES(7521,'WARD','SALESMAN',41,7698,'1981-02-22',1250,500,30,'ward@calcite','2019-11-16 10:26:40','10:26:40','2019-11-16 10:26:40');
                INSERT INTO EMP VALUES(7566,'JONES','MANAGER',28,7839,'1981-02-04',2975,NULL,20,'jones@calcite','2015-03-09 22:16:30','22:16:30','2015-03-09 22:16:30');
                INSERT INTO EMP VALUES(7654,'MARTIN','SALESMAN',27,7698,'1981-09-28',1250,1400,30,'martin@calcite','2018-09-02 12:12:56','12:12:56','2018-09-02 12:12:56');
                INSERT INTO EMP VALUES(7698,'BLAKE','MANAGER',38,7839,'1981-01-05',2850,NULL,30,'blake@calcite','2018-06-01 14:45:00','14:45:00','2018-06-01 14:45:00');
                INSERT INTO EMP VALUES(7782,'CLARK','MANAGER',32,7839,'1981-06-09',2450,NULL,10,NULL,'2019-09-30 02:14:56','02:14:56','2019-09-30 02:14:56');
                INSERT INTO EMP VALUES(7788,'SCOTT','ANALYST',45,7566,'1987-04-19',3000,NULL,20,'scott@calcite','2019-07-28 12:12:12','12:12:12','2019-07-28 12:12:12');
                INSERT INTO EMP VALUES(7839,'KING','PRESIDENT',22,NULL,'1981-11-17',5000,NULL,10,'king@calcite','2019-06-08 15:15:15',NULL,'2019-06-08 15:15:15');
                INSERT INTO EMP VALUES(7844,'TURNER','SALESMAN',54,7698,'1981-09-08',1500,0,30,'turner@calcite','2017-08-17 22:01:37','22:01:37','2017-08-17 22:01:37');
                INSERT INTO EMP VALUES(7876,'ADAMS','CLERK',35,7788,'1987-05-23',1100,NULL,20,'adams@calcite',NULL,'23:11:06','2017-08-18 23:11:06');
                INSERT INTO EMP VALUES(7900,'JAMES','CLERK',40,7698,'1981-12-03',950,NULL,30,'james@calcite','2020-01-02 12:19:00','12:19:00','2020-01-02 12:19:00');
                INSERT INTO EMP VALUES(7902,'FORD','ANALYST',28,7566,'1981-12-03',3000,NULL,20,'ford@calcite','2019-05-29 00:00:00',NULL,'2019-05-29 00:00:00');
                INSERT INTO EMP VALUES(7934,'MILLER','CLERK',32,7782,'1982-01-23',1300,NULL,10,NULL,'2016-09-02 23:15:01','23:15:01','2016-09-02 23:15:01');
                
                CREATE TABLE DAYS(DAY INTEGER, WEEK_DAY VARCHAR);
                INSERT INTO DAYS VALUES(1,'Sunday');
                INSERT INTO DAYS VALUES(2,'Monday');
                INSERT INTO DAYS VALUES(5,'Thursday');
                INSERT INTO DAYS VALUES(4,'Wednesday');
                INSERT INTO DAYS VALUES(3,'Tuesday');
                INSERT INTO DAYS VALUES(6,'Friday');
                INSERT INTO DAYS VALUES(7,'Saturday');
                
                CREATE TABLE STORE(
                    STORE_ID INT,
                    STORE_TYPE VARCHAR,
                    REGION_ID INT,
                    STORE_NAME VARCHAR,
                    STORE_NUMBER INT,
                    store_street_address VARCHAR,
                    store_city VARCHAR,
                    store_state CHAR(2),
                    store_postal_code INT,
                    store_country VARCHAR,
                    store_manager VARCHAR,
                    store_phone VARCHAR,
                    store_fax VARCHAR,
                    first_opened_date TIMESTAMP,
                    last_remodel_date TIMESTAMP,
                    store_sqft INT,
                    grocery_sqft INT,
                    frozen_sqft INT,
                    meat_sqft INT,
                    coffee_bar BOOLEAN,
                    video_store BOOLEAN,
                    salad_bar BOOLEAN,
                    prepared_food BOOLEAN,
                    florist BOOLEAN
                );
                INSERT INTO STORE VALUES(0,'HeadQuarters',0,'HQ',0,'1 Alameda Way','Alameda','CA',55555,'USA',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,false,false,false,false,false);
                INSERT INTO STORE VALUES(1,'Supermarket',28,'Store 1',1,'2853 Bailey Rd','Acapulco','Guerrero',55555,'Mexico','Jones','262-555-5124','262-555-5121','1982-01-09 00:00:00.0','1990-12-05 00:00:00.0',23593,17475,3671,2447,false,false,false,false,false);
                INSERT INTO STORE VALUES(2,'Small Grocery',78,'Store 2',2,'5203 Catanzaro Way','Bellingham','WA',55555,'USA','Smith','605-555-8203','605-555-8201','1970-04-02 00:00:00.0','1973-06-04 00:00:00.0',28206,22271,3561,2374,true,false,false,false,false);
                INSERT INTO STORE VALUES(3,'Supermarket',76,'Store 3',3,'1501 Ramsey Circle','Bremerton','WA',55555,'USA','Davis','509-555-1596','509-555-1591','1959-06-14 00:00:00.0','1967-11-19 00:00:00.0',39696,24390,9184,6122,false,false,true,true,false);
                INSERT INTO STORE VALUES(4,'Gourmet Supermarket',27,'Store 4',4,'433 St George Dr','Camacho','Zacatecas',55555,'Mexico','Johnson','304-555-1474','304-555-1471','1994-09-27 00:00:00.0','1995-12-01 00:00:00.0',23759,16844,4149,2766,true,false,true,true,true);
                INSERT INTO STORE VALUES(5,'Small Grocery',4,'Store 5',5,'1250 Coggins Drive','Guadalajara','Jalisco',55555,'Mexico','Green','801-555-4324','801-555-4321','1978-09-18 00:00:00.0','1991-06-29 00:00:00.0',24597,15012,5751,3834,true,false,false,false,false);
                INSERT INTO STORE VALUES(6,'Gourmet Supermarket',47,'Store 6',6,'5495 Mitchell Canyon Road','Beverly Hills','CA',55555,'USA','Maris','958-555-5002','958-555-5001','1981-01-03 00:00:00.0','1991-03-13 00:00:00.0',23688,15337,5011,3340,true,true,true,true,true);
                INSERT INTO STORE VALUES(7,'Supermarket',3,'Store 7',7,'1077 Wharf Drive','Los Angeles','CA',55555,'USA','White','477-555-7967','477-555-7961','1971-05-21 00:00:00.0','1981-10-20 00:00:00.0',23598,14210,5633,3755,false,false,false,false,true);
                INSERT INTO STORE VALUES(8,'Deluxe Supermarket',26,'Store 8',8,'3173 Buena Vista Ave','Merida','Yucatan',55555,'Mexico','Williams','797-555-3417','797-555-3411','1958-09-23 00:00:00.0','1967-11-18 00:00:00.0',30797,20141,6393,4262,true,true,true,true,true);
                INSERT INTO STORE VALUES(9,'Mid-Size Grocery',2,'Store 9',9,'1872 El Pintado Road','Mexico City','DF',55555,'Mexico','Stuber','439-555-3524','439-555-3521','1955-03-18 00:00:00.0','1959-06-07 00:00:00.0',36509,22450,8435,5624,false,false,false,false,false);
                INSERT INTO STORE VALUES(10,'Supermarket',24,'Store 10',10,'7894 Rotherham Dr','Orizaba','Veracruz',55555,'Mexico','Merz','212-555-4774','212-555-4771','1979-04-13 00:00:00.0','1982-01-30 00:00:00.0',34791,26354,5062,3375,false,false,true,true,false);
                INSERT INTO STORE VALUES(11,'Supermarket',22,'Store 11',11,'5371 Holland Circle','Portland','OR',55555,'USA','Erickson','685-555-8995','685-555-8991','1976-09-17 00:00:00.0','1982-05-15 00:00:00.0',20319,16232,2452,1635,false,false,false,false,false);
                INSERT INTO STORE VALUES(12,'Deluxe Supermarket',25,'Store 12',12,'1120 Westchester Pl','Hidalgo','Zacatecas',55555,'Mexico','Kalman','151-555-1702','151-555-1701','1968-03-25 00:00:00.0','1993-12-18 00:00:00.0',30584,21938,5188,3458,true,true,true,true,true);
                INSERT INTO STORE VALUES(13,'Deluxe Supermarket',23,'Store 13',13,'5179 Valley Ave','Salem','OR',55555,'USA','Inmon','977-555-2724','977-555-2721','1957-04-13 00:00:00.0','1997-11-10 00:00:00.0',27694,18670,5415,3610,true,true,true,true,true);
                INSERT INTO STORE VALUES(14,'Small Grocery',1,'Store 14',14,'4365 Indigo Ct','San Francisco','CA',55555,'USA','Strehlo','135-555-4888','135-555-4881','1957-11-24 00:00:00.0','1958-01-07 00:00:00.0',22478,15321,4294,2863,true,false,false,false,false);
                INSERT INTO STORE VALUES(15,'Supermarket',18,'Store 15',15,'5006 Highland Drive','Seattle','WA',55555,'USA','Ollom','893-555-1024','893-555-1021','1969-07-24 00:00:00.0','1973-10-19 00:00:00.0',21215,13305,4746,3164,true,false,false,false,false);
                INSERT INTO STORE VALUES(16,'Supermarket',87,'Store 16',16,'5922 La Salle Ct','Spokane','WA',55555,'USA','Mantle','643-555-3645','643-555-3641','1974-08-23 00:00:00.0','1977-07-13 00:00:00.0',30268,22063,4923,3282,false,false,false,false,false);
                INSERT INTO STORE VALUES(17,'Deluxe Supermarket',84,'Store 17',17,'490 Risdon Road','Tacoma','WA',55555,'USA','Mays','855-555-5581','855-555-5581','1970-05-30 00:00:00.0','1976-06-23 00:00:00.0',33858,22123,7041,4694,true,false,true,true,true);
                INSERT INTO STORE VALUES(18,'Mid-Size Grocery',25,'Store 18',18,'6764 Glen Road','Hidalgo','Zacatecas',55555,'Mexico','Brown','528-555-8317','528-555-8311','1969-06-28 00:00:00.0','1975-08-30 00:00:00.0',38382,30351,4819,3213,false,false,false,false,false);
                INSERT INTO STORE VALUES(19,'Deluxe Supermarket',5,'Store 19',19,'6644 Sudance Drive','Vancouver','BC',55555,'Canada','Ruth','862-555-7395','862-555-7391','1977-03-27 00:00:00.0','1990-10-25 00:00:00.0',23112,16418,4016,2678,true,true,true,true,true);
                INSERT INTO STORE VALUES(20,'Mid-Size Grocery',6,'Store 20',20,'3706 Marvelle Ln','Victoria','BC',55555,'Canada','Cobb','897-555-1931','897-555-1931','1980-02-06 00:00:00.0','1987-04-09 00:00:00.0',34452,27463,4193,2795,true,false,false,false,true);
                INSERT INTO STORE VALUES(21,'Deluxe Supermarket',106,'Store 21',21,'4093 Steven Circle','San Andres','DF',55555,'Mexico','Jones','493-555-4781','493-555-4781','1986-02-07 00:00:00.0','1990-04-16 00:00:00.0',NULL,NULL,NULL,NULL,true,false,true,true,true);
                INSERT INTO STORE VALUES(22,'Small Grocery',88,'Store 22',22,'9606 Julpum Loop','Walla Walla','WA',55555,'USA','Byrg','881-555-5117','881-555-5111','1951-01-24 00:00:00.0','1969-10-17 00:00:00.0',NULL,NULL,NULL,NULL,false,false,false,false,false);
                INSERT INTO STORE VALUES(23,'Mid-Size Grocery',89,'Store 23',23,'3920 Noah Court','Yakima','WA',55555,'USA','Johnson','170-555-8424','170-555-8421','1977-07-16 00:00:00.0','1987-07-24 00:00:00.0',NULL,NULL,NULL,NULL,false,false,false,false,false);
                INSERT INTO STORE VALUES(24,'Supermarket',7,'Store 24',24,'2342 Waltham St.','San Diego','CA',55555,'USA','Byrd','111-555-0303','111-555-0304','1979-05-22 00:00:00.0','1986-04-20 00:00:00.0',NULL,NULL,NULL,NULL,true,false,true,false,true);
                """);
    }
}
