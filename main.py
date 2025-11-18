from db_operations import (
    insert_customers_from_csv,
    insert_products_from_csv,
    list_orders_with_customers,
    total_sales_per_customer, 
    insert_orders_from_csv,
    insert_order_items_from_csv,)
from db_connection import create_connection, close_connection
from db_operations import insert_sample_data, list_orders_with_customers, total_sales_per_customer
from datetime import datetime
from visuals import plot_total_sales_per_customer, plot_sales_over_time
from db_operations import total_sales_over_time
from db_operations import total_sales_over_time_weekly
from visuals import plot_sales_over_time_weekly



def main():
    # Main execution: connect to DB, insert data, run queries, print results, close connection
    conn = create_connection()  # Connect to the PostgreSQL database
    cur = conn.cursor()         # Create a cursor for executing SQL commands

    insert_sample_data(cur)     # Insert the sample data into tables
    conn.commit()               # Commit the transaction to save changes
     # Load customer and product data from CSV
    insert_customers_from_csv(cur, 'data/customers.csv')
    insert_products_from_csv(cur, 'data/products.csv')
     # Load orders and order items from CSV
    insert_orders_from_csv(cur, 'data/orders.csv')
    insert_order_items_from_csv(cur, 'data/order_items.csv')
    conn.commit()

     # Query and print orders with customer info
    print("Orders with customer names:")
    orders = list_orders_with_customers(cur)  # Retrieve list of orders with customer info
    for order in orders:
        # Print each order record by
        order_id, customer_name, order_date = order
        # Format datetime object to readable string
        order_date_str = order_date.strftime('%Y-%m-%d %H:%M:%S') if order_date else "N/A"
        print(f"Order ID: {order_id}, Customer: {customer_name}, Date: {order_date_str}")

     # Query and print total sales per customer
    print("\nTotal sales per customer:")
    sales = total_sales_per_customer(cur)  # Retrieve total spending per customer
    for sale in sales:
            # Print each total sales record
        customer_name, total_spent = sale
        print(f"Customer: {customer_name}, Total Spent: ${total_spent:.2f}")

         # Generate and display the visualization
    plot_total_sales_per_customer(sales)

    # Get sales over time data
    sales_time = total_sales_over_time(cur)

    # Print sales over time (optional)
    print("\nSales over time:")
    for sale_date, total in sales_time:
        print(f"Date: {sale_date}, Total Sales: ${total:.2f}")

    # Plot sales over time
    plot_sales_over_time(sales_time)

    # Fetch weekly aggregated sales
    weekly_sales = total_sales_over_time_weekly(cur)

    # Optional: print weekly sales data
    print("\nWeekly sales:")
    for week_start, total in weekly_sales:
        print(f"Week starting {week_start.date()}: ${total:.2f}")

    # Plot weekly sales chart
    plot_sales_over_time_weekly(weekly_sales)

    cur.close()                 # Close the cursor
    close_connection(conn)      # Close the database connection

if __name__ == "__main__":
    main()                      # Run the main function if script is executed directly
