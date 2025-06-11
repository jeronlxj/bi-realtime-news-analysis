from simulator import NewsSimulator
import json

def test_simulator():
    print("Testing NewsSimulator...")
    
    try:
        # Initialize simulator
        simulator = NewsSimulator()
        
        # Generate test logs (small batch)
        print("Generating test logs...")
        logs = simulator.simulate_exposure_logs(num_logs=5)
        
        print("\nSample logs:")
        for log in logs:
            print(json.dumps(log, indent=2))
            
        # Test file writing
        test_output = 'test_exposure_logs.json'
        simulator.save_logs_to_file(filename=test_output, batch_size=5)
        print(f"\nSaved logs to {test_output}")
        
        return True
        
    except Exception as e:
        print(f"Error during testing: {e}")
        return False

if __name__ == "__main__":
    success = test_simulator()
    print("\nTest result:", "SUCCESS" if success else "FAILED")
