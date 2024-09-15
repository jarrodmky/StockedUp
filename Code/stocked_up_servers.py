from Code.Pipeline import PrefectServer, PipelineServer

import time

def guarded_server_run() :
    try :
        PrefectServer.start()
        time.sleep(5)
        PipelineServer.start()
        time.sleep(3)
        if PrefectServer.is_running() and PipelineServer.is_running() :
            print("Servers started!")   
            while True :
                time.sleep(1)
        else :
            print("Servers failed to start!")
            PipelineServer.stop()
            PrefectServer.stop()
    except Exception as e :
        if e is not KeyboardInterrupt :
            print(f"Exception when running servers: {e}")
        else :
            print("Servers stopped!")
        PipelineServer.stop()
        PrefectServer.stop()
