from db import BabyNameDatabase

def main():
    db = BabyNameDatabase()
    db.create_tables()
    
    while True:
        print("\n=== Baby Name Database ===")
        print("1. Load SSA data")
        print("2. Search for a name")
        print("3. Add a new record")
        print("4. Update a record")
        print("5. Delete a record")
        print("6. Get name statistics")
        print("7. Exit")
        
        choice = input("\nEnter your choice (1-7): ").strip()
        
        if choice == '1':
            print("Loading SSA data from babynames folder...")
            db.load_data_from_csv()
        
        elif choice == '2':
            name = input("Enter name to search: ").strip()
            results = db.search_name(name)
            if results:
                print(f"\nResults for '{name}':")
                for result_name, year, gender, count in results:
                    print(f"  {year} - {gender}: {count}")
            else:
                print("Name not found.")
        
        elif choice == '3':
            name = input("Enter name: ").strip()
            try:
                year = int(input("Enter year: ").strip())
                gender = input("Enter gender (M/F): ").strip().upper()
                count = int(input("Enter count: ").strip())
                
                if db.add_name(name, year, gender, count):
                    print("Record added!")
                else:
                    print("Record already exists.")
            except ValueError:
                print("Invalid input. Year and count must be numbers.")
        
        elif choice == '4':
            name = input("Enter name: ").strip()
            try:
                year = int(input("Enter year: ").strip())
                gender = input("Enter gender (M/F): ").strip().upper()
                new_count = int(input("Enter new count: ").strip())
                
                if db.update_count(name, year, gender, new_count):
                    print("Record updated!")
                else:
                    print("Record not found.")
            except ValueError:
                print("Invalid input. Year and count must be numbers.")
        
        elif choice == '5':
            name = input("Enter name: ").strip()
            try:
                year = int(input("Enter year: ").strip())
                gender = input("Enter gender (M/F): ").strip().upper()
                
                if db.delete_record(name, year, gender):
                    print("Record deleted!")
                else:
                    print("Record not found.")
            except ValueError:
                print("Invalid input. Year must be a number.")
        
        elif choice == '6':
            name = input("Enter name: ").strip()
            stats = db.get_name_stats(name)
            if stats:
                print(f"\nStatistics for '{name}':")
                print(f"  First year recorded: {stats['first_year']}")
                print(f"  Most popular year: {stats['most_popular_year']}")
                print(f"  Top 10 years by popularity: {stats['top_10_years']}")
            else:
                print("Name not found.")
        
        elif choice == '7':
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Try again.")

if __name__ == "__main__":
    main()
