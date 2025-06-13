"""
Time Manager utility for handling virtual time in the application
Provides functionality to simulate time starting from a specific date
"""
import threading
import time
from datetime import datetime, timedelta

class TimeManager:
    """
    Manages virtual time in the application
    Simulates time starting from a specific date and progressing at a configurable rate
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(TimeManager, cls).__new__(cls)
                # Initialize with June 24, 2019, 2 AM as starting point
                cls._instance.start_time = datetime(2019, 6, 24, 2, 0, 0)
                cls._instance.real_start_time = datetime.now()
                # By default, 1 real minute = 1 virtual minute
                cls._instance.time_ratio = 1.0
            return cls._instance
    
    def get_current_time(self) -> datetime:
        """
        Get the current virtual time based on the elapsed real time
        Returns a datetime object representing the current virtual time
        """
        real_elapsed = datetime.now() - self.real_start_time
        virtual_elapsed = timedelta(seconds=real_elapsed.total_seconds() * self.time_ratio)
        return self.start_time + virtual_elapsed
    
    def set_time_ratio(self, ratio: float):
        """
        Set the ratio of virtual time to real time
        E.g., ratio=2.0 means virtual time passes twice as fast as real time
        """
        if ratio <= 0:
            raise ValueError("Time ratio must be positive")
        
        # Calculate current virtual time before changing ratio
        current_virtual_time = self.get_current_time()
        
        # Update ratio
        self.time_ratio = ratio
        
        # Reset start times with the current virtual time as new reference
        self.start_time = current_virtual_time
        self.real_start_time = datetime.now()
    
    def format_time(self, format_str="%Y-%m-%d %H:%M:%S") -> str:
        """Format the current virtual time as a string"""
        return self.get_current_time().strftime(format_str)
    
    def get_time_window(self, hours=1) -> tuple:
        """
        Get a time window based on current virtual time
        Returns (start_time, end_time) for the last 'hours' from current virtual time
        """
        current = self.get_current_time()
        start_time = current - timedelta(hours=hours)
        return start_time, current

def get_time_manager() -> TimeManager:
    """Get the singleton instance of TimeManager"""
    return TimeManager()