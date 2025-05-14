from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from queue import Queue
import os

def dir_loop(dir:str):
    result=[]
    for folder_name, subfolders, filenames in os.walk(dir):
        for filename in filenames:
            print('FILE INSIDE ' + folder_name + ': '+ filename)
            result.append(filename)
    return result

class MyHandler(FileSystemEventHandler):
    """check if the modified or created is dir or file"""
    def __init__(self, listen_queue: Queue):
        super().__init__()
        self.captured_path = listen_queue
        self.processed_files = set()  # A set to track processed files

    def on_modified(self, event):
        if not event.is_directory:
            if event.src_path not in self.processed_files:  # Check if file already processed
                self.processed_files.add(event.src_path)
                self.captured_path.put(event.src_path)
        elif event.is_directory:
            pass
        else:
            print("path doesn't exist")

    def on_created(self, event):
        if not event.is_directory:
            if event.src_path not in self.processed_files:  # Check if file already processed
                self.processed_files.add(event.src_path)
                self.captured_path.put(event.src_path)
        elif event.is_directory:
            pass  # we care here
        else:
            print("path doesn't exist")
def watchDir(dir:str):
    listen_queue=Queue()
    event_handler = MyHandler(listen_queue)
    observer = Observer()
    observer.schedule(event_handler, dir, recursive=True)
    observer.start()
    return observer,event_handler
