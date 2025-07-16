from asyncio import run
from asyncio.log import logger
from tradepulse.fetchdata import Server
async def main(address:str, config):
    server = Server(address,config)    
    await server.start()
    try :
        while True:
            await server.recv()
    except Exception as e:
        logger.info("Server stopped")        
    await server.recv()      
    



if __name__ == '__main__':
    from argparse import ArgumentParser
    
    run(main("fetchdata",{}))