from typing import Literal




TimeFrame = Literal['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']

def parse_timeStr(timeframe: str) :
    unit = timeframe[-1]
    match unit:
        case 'y' :
            scale = 60 * 60 * 24 * 365
        case 'M' :
            scale = 60 * 60 * 24 * 30
        case 'w' :
            scale = 60 * 60 * 24 * 7
        case 'd' :
            scale = 60 * 60 * 24
        case 'h' :
            scale = 60 * 60
        case 'm' :
            scale = 60
        case 's' :
            scale = 1
        case _:
             raise ValueError(f'Invalid timeframe unit: {unit}')
    return scale * int(timeframe[:-1])
    