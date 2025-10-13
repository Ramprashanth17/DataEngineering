--
-- ER/Studio Data Architect SQL Code Generation
-- Project :      Dimensional_Model.DM1
--
-- Date Created : Sunday, December 08, 2024 01:51:57
-- Target DBMS : Snowflake
--

-- 
-- TABLE: DIM_ADDRESS 
--

CREATE TABLE DIM_ADDRESS(
    Address_SK     INTEGER      IDENTITY(1,1),
    Street_Name    CHAR(500)    NOT NULL,
    Latitude       FLOAT        NOT NULL,
    Longitude      FLOAT        NOT NULL,
    City           CHAR(50)     NOT NULL,
    Job_ID         CHAR(100)    NOT NULL,
    Created_Dt     DATE         NOT NULL,
    CONSTRAINT PK1 PRIMARY KEY (Address_SK)
)
;



-- 
-- TABLE: DIM_CONTRIBUTION 
--

CREATE TABLE DIM_CONTRIBUTION(
    Contribution_SK           INTEGER      IDENTITY(1,1),
    CNT_ID                    INT          NOT NULL,
    CODE                      INT          NOT NULL,
    DESCRIPTION               CHAR(550)    NOT NULL,
    NORMALIZED_DESCRIPTION    CHAR(550)    NOT NULL,
    START_DATE                DATETIME     NOT NULL,
    END_DATE                  DATETIME     NOT NULL,
    ACTIVE_STATUS             INT          NOT NULL,
    VERSION                   INT          NOT NULL,
    JobID                     CHAR(550)    NOT NULL,
    Created_Dt                DATE         NOT NULL,
    CONSTRAINT PK2 PRIMARY KEY (Contribution_SK)
)
;



-- 
-- TABLE: DIM_DATE 
--

CREATE TABLE DIM_DATE(
    Date_SK       INTEGER      IDENTITY(1,1),
    Date          DATE         NOT NULL,
    Month         CHAR(100)    NOT NULL,
    Year          INTEGER      NOT NULL,
    Day           CHAR(50)     NOT NULL,
    Quarter       CHAR(50)     NOT NULL,
    Is_Weekend    INTEGER      NOT NULL,
    Season        CHAR(50)     NOT NULL,
    Job_ID        CHAR(100)    NOT NULL,
    Created_Dt    DATE         NOT NULL,
    CONSTRAINT PK6 PRIMARY KEY (Date_SK)
)
;



-- 
-- TABLE: DIM_EXT_FACTORS 
--

CREATE TABLE DIM_EXT_FACTORS(
    EXT_SK                INTEGER      IDENTITY(1,1),
    Weather               CHAR(100)    NOT NULL,
    Lighting_Condition    CHAR(100)    NOT NULL,
    Job_ID                CHAR(100)    NOT NULL,
    Created_Dt            DATE         NOT NULL,
    CONSTRAINT PK5 PRIMARY KEY (EXT_SK)
)
;



-- 
-- TABLE: DIM_TIME 
--

CREATE TABLE DIM_TIME(
    Time_SK            INTEGER      NOT NULL,
    Time               CHAR(50)     NOT NULL,
    Hour               INTEGER      NOT NULL,
    Minutes            INTEGER      NOT NULL,
    Seconds            INTEGER      NOT NULL,
    Time_Band_Label    CHAR(50)     NOT NULL,
    Job_ID             CHAR(100)    NOT NULL,
    Created_Dt         DATE         NOT NULL,
    CONSTRAINT PK7_1 PRIMARY KEY (Time_SK)
)
;



-- 
-- TABLE: FACT_COLLISION 
--

CREATE TABLE FACT_COLLISION(
    Fact_SK                   INTEGER      IDENTITY(1,1),
    EXT_SK                    INTEGER      NOT NULL,
    Address_SK                INTEGER      NOT NULL,
    Collision_ID              CHAR(100)    NOT NULL,
    Is_Injury_Not_Fatal       INTEGER      NOT NULL,
    Is_Pedestrian             INTEGER      NOT NULL,
    Motorist_Damage           INTEGER      NOT NULL,
    Pedestrian_Fatal_Count    INTEGER      NOT NULL,
    RoadUser_Fatal_Count      INTEGER      NOT NULL,
    Total_Fatal_Count         INTEGER      NOT NULL,
    Contribution_SK           INTEGER      NOT NULL,
    Job_ID                    CHAR(100)    NOT NULL,
    Created_Dt                DATE         NOT NULL,
    Date_SK                   INTEGER      NOT NULL,
    Time_SK                   INTEGER      NOT NULL,
    CONSTRAINT PK3 PRIMARY KEY (Fact_SK, EXT_SK, Address_SK)
)
;



-- 
-- TABLE: FACT_COLLISION 
--

ALTER TABLE FACT_COLLISION ADD CONSTRAINT RefDIM_EXT_FACTORS3 
    FOREIGN KEY (EXT_SK)
    REFERENCES DIM_EXT_FACTORS(EXT_SK)
;

ALTER TABLE FACT_COLLISION ADD CONSTRAINT RefDIM_ADDRESS4 
    FOREIGN KEY (Address_SK)
    REFERENCES DIM_ADDRESS(Address_SK)
;

ALTER TABLE FACT_COLLISION ADD CONSTRAINT RefDIM_CONTRIBUTION5 
    FOREIGN KEY (Contribution_SK)
    REFERENCES DIM_CONTRIBUTION(Contribution_SK)
;

ALTER TABLE FACT_COLLISION ADD CONSTRAINT RefDIM_DATE8 
    FOREIGN KEY (Date_SK)
    REFERENCES DIM_DATE(Date_SK)
;

ALTER TABLE FACT_COLLISION ADD CONSTRAINT RefDIM_TIME9 
    FOREIGN KEY (Time_SK)
    REFERENCES DIM_TIME(Time_SK)
;


