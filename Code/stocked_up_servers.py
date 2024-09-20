from Code.Pipeline import PipelineServer

import time

def guarded_server_run(serve_tests : bool) -> None :
    pipeline_server = PipelineServer()
    try :
        pipeline_server.start(serve_tests)
        if pipeline_server.is_running() :
            print("Servers started!")   
            while True :
                time.sleep(1)
        else :
            print("Servers failed to start!")
            pipeline_server.stop()
    except Exception as e :
        if e is not KeyboardInterrupt :
            print(f"Exception when running servers: {e}")
        else :
            print("Servers stopped!")
        if pipeline_server.is_running() :
            pipeline_server.stop()
