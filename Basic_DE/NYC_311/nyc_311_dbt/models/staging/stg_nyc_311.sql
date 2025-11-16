{{
    config(
        materialized='view',
        tags=['staging']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('raw', 'nyc_311') }}
),

renamed AS (
    SELECT
        -- Primary key
        UNIQUE_KEY AS request_id,
        
        -- Timestamps
        CREATED_DATE AS created_at,
        LOADED_AT AS loaded_at,
        
        -- Dimensions
        AGENCY AS agency_code,
        COMPLAINT_TYPE AS complaint_type,
        BOROUGH AS borough_name,
        STATUS AS request_status,
        
        -- Metadata
        CURRENT_TIMESTAMP() AS dbt_updated_at
        
    FROM source
),

cleaned AS (
    SELECT
        request_id,
        created_at,
        loaded_at,
        
        -- Clean and standardize text fields
        UPPER(TRIM(agency_code)) AS agency_code,
        UPPER(TRIM(complaint_type)) AS complaint_type,
        UPPER(TRIM(borough_name)) AS borough_name,
        UPPER(TRIM(request_status)) AS request_status,
        
        dbt_updated_at
        
    FROM renamed
    WHERE created_at IS NOT NULL  -- Remove invalid dates
)

SELECT * FROM cleaned
