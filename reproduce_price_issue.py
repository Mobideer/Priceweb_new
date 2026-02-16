import json

def check_price_change(prev_price, new_price, name="Test Item"):
    print(f"Checking {name}: {prev_price} -> {new_price}")
    
    try:
        old_min = float(prev_price if prev_price is not None else 0)
        new_min = float(new_price if new_price is not None else 0)
        
        if old_min > 0 and new_min > 0:
            diff_pct = (new_min - old_min) / old_min * 100.0
            print(f"  Diff %: {diff_pct:.2f}%")
            if abs(diff_pct) >= 30.0:
                print("  ALARM TRIGGERED!")
            else:
                print("  No alarm.")
        else:
            print("  Skipped (one value is 0 or None)")
    except Exception as e:
        print(f"  Error: {e}")

# Scenarios
print("--- Scenario 1: Normal large change ---")
check_price_change(10000, 15000)

print("\n--- Scenario 2: Small change ---")
check_price_change(10000, 10500)

print("\n--- Scenario 3: 0 to Value (New supplier?) ---")
check_price_change(0, 10000)

print("\n--- Scenario 4: None to Value ---")
check_price_change(None, 10000)

print("\n--- Scenario 5: Value to 0 (Lost supplier?) ---")
check_price_change(10000, 0)

print("\n--- Scenario 6: String '0' ---")
check_price_change("0", 10000)

print("\n--- Scenario 7: String floats ---")
check_price_change("10000.00", "5000.0")
