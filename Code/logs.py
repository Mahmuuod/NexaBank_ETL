import logging
import os
from datetime import datetime

logging.basicConfig(
    filename='pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def log_start_end(func):
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        logging.info(f"Started {func.__name__} at {start_time}")
        try:
            result = func(*args, **kwargs)
            end_time = datetime.now()
            logging.info(f"Finished {func.__name__} at {end_time}")
            return result
        except Exception as e:
            logging.error(f"Error in {func.__name__}: {e}")
            raise
    return wrapper

@log_start_end
def test_function(file_path):
    """A test function that reads a file and returns its line count"""
    logging.info(f"Processing file: {file_path}")
    with open(file_path, 'r') as f:
        lines = f.readlines()
    logging.info(f"File contains {len(lines)} lines")
    return len(lines)

# if __name__ == "__main__":
#     try:
#         test_file = "test_logging.txt"
#         with open(test_file, 'w') as f:
#             f.write("Line 1\nLine 2\nLine 3\n")
        
#         line_count = test_function(test_file)
#         print(f"Test successful. Line count: {line_count}")
        
#         @log_start_end
#         def failing_function():
#             return 1/0
            
#         failing_function()
        
#     except Exception as e:
#         print(f"Test completed with expected exception: {e}")
#     finally:
#         if os.path.exists(test_file):
#             os.remove(test_file)
        
#         print("\nLog file contents:")
#         with open('pipeline.log', 'r') as log_file:
#             print(log_file.read())