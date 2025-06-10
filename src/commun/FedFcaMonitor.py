import psutil
import time
import logging

logger = logging.getLogger(__name__)

class FedFcaMonitor:
    def __init__(self):
        self.process = psutil.Process()
        self.cpu_start = None
        self.cpu_end = None
        self.mem_start = None
        self.mem_end = None
        self.net_io_start = None
        self.net_io_end = None
        self.proc_net_start = None
        self.proc_net_end = None
        self.start_time = None
        self.end_time = None
        self.metrics_collected = False
        self.use_proc_net = False

        # Timer tracking for specific operations
        self.timers = {}

    def start_monitoring(self):
        """Start monitoring system and process-level metrics."""
        self.start_time = time.time()
        self.cpu_start = self.process.cpu_times()
        self.mem_start = self.process.memory_info()

        # Attempt to use per-process network counters (Linux only)
        try:
            self.proc_net_start = self.process.net_io_counters()
            self.use_proc_net = True
        except (AttributeError, psutil.AccessDenied):
            self.net_io_start = psutil.net_io_counters(pernic=False)
            self.use_proc_net = False
            logger.warning("Per-process network monitoring not available. Using system-wide counters.")

        self.metrics_collected = True

    def stop_monitoring(self):
        """Stop monitoring and collect final metrics."""
        if not self.metrics_collected:
            logger.warning("Monitoring was not started.")
            return

        self.end_time = time.time()
        try:
            self.cpu_end = self.process.cpu_times()
            self.mem_end = self.process.memory_info()

            if self.use_proc_net:
                self.proc_net_end = self.process.net_io_counters()
            else:
                self.net_io_end = psutil.net_io_counters(pernic=False)
        except Exception as e:
            logger.error(f"Error collecting end metrics: {e}")
            self.metrics_collected = False

    def get_metrics(self):
        """
        Returns a dictionary containing:
        - Computation metrics (CPU, memory)
        - Communication metrics (network I/O)
        - Duration of the monitored period
        """
        if not self.metrics_collected:
            return {}

        cpu_user = self.cpu_end.user - self.cpu_start.user
        cpu_system = self.cpu_end.system - self.cpu_start.system
        cpu_total = cpu_user + cpu_system
        duration = self.end_time - self.start_time

        mem_rss = self.mem_end.rss - self.mem_start.rss
        mem_vms = self.mem_end.vms - self.mem_start.vms

        if self.use_proc_net:
            bytes_sent = self.proc_net_end.bytes_sent - self.proc_net_start.bytes_sent
            bytes_received = self.proc_net_end.bytes_recv - self.proc_net_start.bytes_recv
        else:
            bytes_sent = self.net_io_end.bytes_sent - self.net_io_start.bytes_sent
            bytes_received = self.net_io_end.bytes_recv - self.net_io_start.bytes_recv

        return {
            "computation": {
                "cpu": {
                    "user_seconds": round(cpu_user, 6),
                    "system_seconds": round(cpu_system, 6),
                    "total_seconds": round(cpu_total, 6),
                    "cpu_percent": round((cpu_total / duration) * 100, 2) if duration > 0 else 0,
                },
                "memory": {
                    "rss_bytes": mem_rss,
                    "vms_bytes": mem_vms,
                },
            },
            "communication": {
                "network": {
                    "bytes_sent": bytes_sent,
                    "bytes_received": bytes_received,
                    "sent_rate_bps": round(bytes_sent / duration, 2) if duration > 0 else 0,
                    "received_rate_bps": round(bytes_received / duration, 2) if duration > 0 else 0,
                }
            },
            "duration_seconds": round(duration, 6),
        }
        
    def start_timer(self, timer_name):
        """
        Start a timer for a specific operation
        
        Args:
            timer_name (str): Unique identifier for the timer
        """
        self.timers[timer_name] = time.time()
        
    def stop_timer(self, timer_name):
        """
        Stop a timer and return the elapsed time
        
        Args:
            timer_name (str): Identifier of the timer to stop
            
        Returns:
            float: Elapsed time in seconds, or 0 if timer was not started
        """
        if timer_name not in self.timers:
            logger.warning(f"Timer '{timer_name}' was not started")
            return 0
            
        elapsed = time.time() - self.timers[timer_name]
        del self.timers[timer_name]  # Clean up the timer
        return elapsed