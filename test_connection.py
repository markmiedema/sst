# D:\sst\test_connection.py
import sys
from config import get_connection

print("Attempting to connect to database using centralized configuration...")

try:
    # Try to connect using centralized config
    connection = get_connection()

    # Create a cursor to execute a test query
    cursor = connection.cursor()

    print("✅ Connection successful!")

    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("   Current server time:", result[0])

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("   Connection closed.")

except Exception as e:
    print("\n❌ Connection Failed.")
    print("   Error:", e, file=sys.stderr)
    sys.exit(1) # Exit with an error code