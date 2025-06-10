import logging
 
class LoggerActor:
    """Logger actor for handling logs in the FedFCA framework."""
    
    def __init__(self, actor_id):
        """Initialize the logger actor.
        
        Args:
            actor_id (str): The ID of the actor.
        """
        self.actor_id = actor_id
        self.logger = logging.getLogger(f"FedFCA-{actor_id}")
        self.setup_logger()
        
    def setup_logger(self):
        """Set up the logger with appropriate handlers and formatters."""
        self.logger.setLevel(logging.INFO)
        
        # Create console handler
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(ch)
        
        # Create file handler
        fh = logging.FileHandler(f"fedfca_{self.actor_id}.log")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
    
    def info(self, message):
        """Log an info message.
        
        Args:
            message (str): The message to log.
        """
        self.logger.info(message)
    
    def error(self, message):
        """Log an error message.
        
        Args:
            message (str): The message to log.
        """
        self.logger.error(message)
    
    def debug(self, message):
        """Log a debug message.
        
        Args:
            message (str): The message to log.
        """
        self.logger.debug(message)

