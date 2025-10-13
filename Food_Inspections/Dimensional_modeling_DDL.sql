--
-- ER/Studio Data Architect SQL Code Generation
-- Project :      Food_Inspection.DM1
--
-- Date Created : Sunday, November 03, 2024 00:16:24
-- Target DBMS : Snowflake
--

-- 
-- TABLE: Date_Dimension 
--

CREATE TABLE Date_Dimension(
    Date_SK            INTEGER        IDENTITY(1,1),
    Date               DATE           NOT NULL,
    Month              CHAR(20)       NOT NULL,
    Year               INTEGER        NOT NULL,
    Quarter            INTEGER        NOT NULL,
    DI_Created_date    DATE           NOT NULL,
    DI_WorkflowID      STRING(100)    NOT NULL,
    CONSTRAINT PK6 PRIMARY KEY (Date_SK)
)
;



-- 
-- TABLE: Facility_Dimension 
--

CREATE TABLE Facility_Dimension(
    Facility_SK        INTEGER              IDENTITY(1,1),
    Name               VARCHAR(16777216)    NOT NULL,
    Facility_Type      VARCHAR(16777216)    NOT NULL,
    DI_Created_date    DATE                 NOT NULL,
    DI_WorkflowID      STRING(100)          NOT NULL,
    CONSTRAINT PK2 PRIMARY KEY (Facility_SK)
)
;



-- 
-- TABLE: Food_Inspection_Fact 
--

CREATE TABLE Food_Inspection_Fact(
    Fact_SK                  INTEGER        IDENTITY(1,1),
    Location_SK              INTEGER        NOT NULL,
    Facility_SK              INTEGER        NOT NULL,
    Inspection_SK            INTEGER        NOT NULL,
    Violation_SK             INTEGER        NOT NULL,
    Risk_Category            VARCHAR(10)    NOT NULL,
    Total_Violation_Score    INTEGER        NOT NULL,
    Inspection_Result        VARCHAR(17)    NOT NULL,
    DI_Created_date          DATE           NOT NULL,
    DI_WorkflowID            STRING(100)    NOT NULL,
    CONSTRAINT PK5 PRIMARY KEY (Fact_SK, Location_SK, Facility_SK, Inspection_SK, Violation_SK)
)
;



-- 
-- TABLE: Inspection_Dimension 
--

CREATE TABLE Inspection_Dimension(
    Inspection_SK      INTEGER              IDENTITY(1,1),
    Inspection_ID      VARCHAR(16777216)    NOT NULL,
    Inspection_Type    VARCHAR(41)          NOT NULL,
    Date_SK            INTEGER              NOT NULL,
    DI_Created_date    DATE                 NOT NULL,
    DI_WorkflowID      STRING(100)          NOT NULL,
    CONSTRAINT PK3 PRIMARY KEY (Inspection_SK)
)
;



-- 
-- TABLE: Location_Dimension 
--

CREATE TABLE Location_Dimension(
    Location_SK        INTEGER         IDENTITY(1,1),
    Address            VARCHAR(100)    NOT NULL,
    City               VARCHAR(30)     NOT NULL,
    State              VARCHAR(10)     NOT NULL,
    Zip_Code           INTEGER         NOT NULL,
    Latitude           FLOAT           NOT NULL,
    Longitude          FLOAT           NOT NULL,
    DI_Created_date    DATE            NOT NULL,
    DI_WorkflowID      STRING(100)     NOT NULL,
    CONSTRAINT PK1 PRIMARY KEY (Location_SK)
)
;



-- 
-- TABLE: Violation_Dimension 
--

CREATE TABLE Violation_Dimension(
    Violation_SK             INTEGER         IDENTITY(1,1),
    Violation_Category_ID    INTEGER         NOT NULL,
    Violation_Category       VARCHAR(134)    NOT NULL,
    Violation_Score          INTEGER         NOT NULL,
    DI_Created_date          DATE            NOT NULL,
    DI_WorkflowID            STRING(100)     NOT NULL,
    CONSTRAINT PK4 PRIMARY KEY (Violation_SK)
)
;



-- 
-- TABLE: Food_Inspection_Fact 
--

ALTER TABLE Food_Inspection_Fact ADD CONSTRAINT RefLocation_Dimension11 
    FOREIGN KEY (Location_SK)
    REFERENCES Location_Dimension(Location_SK)
;

ALTER TABLE Food_Inspection_Fact ADD CONSTRAINT RefFacility_Dimension21 
    FOREIGN KEY (Facility_SK)
    REFERENCES Facility_Dimension(Facility_SK)
;

ALTER TABLE Food_Inspection_Fact ADD CONSTRAINT RefInspection_Dimension31 
    FOREIGN KEY (Inspection_SK)
    REFERENCES Inspection_Dimension(Inspection_SK)
;

ALTER TABLE Food_Inspection_Fact ADD CONSTRAINT RefViolation_Dimension41 
    FOREIGN KEY (Violation_SK)
    REFERENCES Violation_Dimension(Violation_SK)
;


-- 
-- TABLE: Inspection_Dimension 
--

ALTER TABLE Inspection_Dimension ADD CONSTRAINT RefDate_Dimension51 
    FOREIGN KEY (Date_SK)
    REFERENCES Date_Dimension(Date_SK)
;


