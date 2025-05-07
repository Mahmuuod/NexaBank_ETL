import time
from ETL import *
from Code.Watch_Dog import *
landing=os.getcwd()+'/Final Project/incoming_data'


def main():
    observer,event_handler=watchDir(landing)
    while True:
        if  event_handler.captured_path :
            print(event_handler.captured_path.get())
        time.sleep(1)




if __name__=="__main__":
    main()



