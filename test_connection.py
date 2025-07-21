# D:\sst\test_connection.py
import psycopg2
import sys

# Use the full, direct connection string for the IPv4 pooler
# The asterisk (*) in the password has been URL-encoded to %2A
DB_URL = "postgresql://postgres.yixxvtnynvthxnpdgsro:Smeads0225%2A@aws-0-us-east-2.pooler.supabase.com:5432/postgres?sslmode=require"

print("Attempting to connect to Supabase...")

try:
    # Try to connect
    connection = psycopg2.connect(DB_URL)

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