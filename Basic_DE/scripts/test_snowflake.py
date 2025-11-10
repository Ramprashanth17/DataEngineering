"""
Test Snowflake Connection
"""
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

print("🔍 Testing Snowflake connection...")
print(f"Account: {os.getenv('SNOWFLAKE_ACCOUNT')}")
print(f"User: {os.getenv('SNOWFLAKE_USER')}")

try:
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        role=os.getenv('SNOWFLAKE_ROLE')
    )
    
    cursor = conn.cursor()
    cursor.execute("SELECT CURRENT_VERSION()")
    version = cursor.fetchone()
    
    print("✅ Connection successful!")
    print(f"Snowflake version: {version[0]}")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print("❌ Connection failed!")
    print(f"Error: {str(e)}")
    print("\n💡 Check your .env file credentials")