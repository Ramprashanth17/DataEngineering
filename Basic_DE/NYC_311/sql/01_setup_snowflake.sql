-- Snowflake Database Setup for NYC 311 Project
-- Run this in Snowflake to create database structure

-- Create database
CREATE DATABASE IF NOT EXISTS DE_LEARNING;

-- Create schemas for data layers
CREATE SCHEMA IF NOT EXISTS DE_LEARNING.RAW;
CREATE SCHEMA IF NOT EXISTS DE_LEARNING.STAGING;
CREATE SCHEMA IF NOT EXISTS DE_LEARNING.MARTS;

-- Create raw table for NYC 311 data
CREATE TABLE IF NOT EXISTS DE_LEARNING.RAW.NYC_311 (
    unique_key VARCHAR(50),
    created_date TIMESTAMP,
    agency VARCHAR(50),
    complaint_type VARCHAR(100),
    descriptor VARCHAR(200),
    location_type VARCHAR(100),
    incident_zip VARCHAR(10),
    borough VARCHAR(50),
    latitude FLOAT,
    longitude FLOAT,
    status VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    COMMENT 'Raw NYC 311 service requests from NYC Open Data API'
);

-- Verify setup
SHOW SCHEMAS IN DATABASE DE_LEARNING;
DESC TABLE DE_LEARNING.RAW.NYC_311;
