import logging
import sys

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    # Avoid duplicate logs if the logger is called multiple times
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # Professional format: Timestamp | Level | Filename | Message
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        # Output to terminal
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        
    return logger