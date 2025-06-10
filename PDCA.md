# FedFCA Framework Performance Monitoring PDCA Cycle

## PLAN: Comprehensive Monitoring Strategy for FedFCA Framework

### 1. Core Monitoring Infrastructure

Create a dedicated monitoring module:
- `src/commun/PerformanceMonitor.py`

This module will:
- Track computation metrics (CPU, RAM, execution time)
- Record communication metrics (message size, transmission time)
- Log metrics for each phase of the FedFCA process
- Generate visualizations and reports

### 2. Implementation Plan

#### Phase 1: Set up Basic Monitoring Infrastructure

1. **Create the PerformanceMonitor class**:
   - Implement methods to track CPU, RAM usage
   - Add timing functions for operations
   - Track message sizes for network communications
   - Log all metrics to structured format (JSON/CSV)

2. **Add dependency requirements**:
   - Add `psutil` for CPU/RAM monitoring (already in requirements)
   - Add `prometheus_client` for metrics exposure

#### Phase 2: Instrument Core Components

3. **Server Instrumentation (agmactor.py)**:
   - Track time spent on coordination
   - Measure computational load during aggregation
   - Monitor message sizes for client communications
   - Log key exchange overhead

4. **Provider Instrumentation (almactor.py)**:
   - Track local computation time
   - Measure lattice generation overhead
   - Monitor encryption/decryption times
   - Log message sizes during communication

5. **Communication Layer Instrumentation**:
   - Add wrappers around Kafka producers/consumers
   - Track message sizes and transmission times
   - Monitor serialization/deserialization overhead

#### Phase 3: Advanced Analysis and Reporting

6. **Implement visualization module**:
   - Create comparative charts (FedFCA vs. Centralized)
   - Break down time by computation vs. communication
   - Generate architecture overhead analysis

7. **Add experiment configuration**:
   - Allow selective enabling of monitoring
   - Support different levels of detail
   - Configure output formats

## DO: Implementation Details

### PerformanceMonitor Class

```python
# src/commun/PerformanceMonitor.py
import time
import psutil
import json
import os
import threading
from collections import defaultdict
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

class PerformanceMonitor:
    def __init__(self, component_name, log_dir="./logs"):
        self.component_name = component_name
        self.log_dir = log_dir
        self.metrics = defaultdict(list)
        self.timers = {}
        self.message_sizes = defaultdict(list)
        self.process = psutil.Process(os.getpid())
        
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Start background monitoring thread
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._background_monitor)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
    
    def start_timer(self, operation_name):
        """Start timing an operation"""
        self.timers[operation_name] = time.time()
    
    def end_timer(self, operation_name, phase=None):
        """End timing and record the duration"""
        if operation_name in self.timers:
            duration = time.time() - self.timers[operation_name]
            metric_name = f"{operation_name}_time"
            if phase:
                metric_name = f"{phase}_{metric_name}"
            self.metrics[metric_name].append(duration)
            return duration
        return None
    
    def record_message_size(self, message_type, size_bytes, phase=None):
        """Record message size for communication overhead analysis"""
        metric_name = f"{message_type}_size"
        if phase:
            metric_name = f"{phase}_{metric_name}"
        self.message_sizes[metric_name].append(size_bytes)
    
    def _background_monitor(self):
        """Background thread to monitor system resources"""
        while self.monitoring:
            # CPU usage as percentage
            self.metrics["cpu_percent"].append(self.process.cpu_percent())
            
            # Memory usage in MB
            memory_info = self.process.memory_info()
            self.metrics["memory_rss_mb"].append(memory_info.rss / (1024 * 1024))
            
            time.sleep(1)  # Sample every second
    
    def get_metrics_summary(self):
        """Generate summary statistics for all metrics"""
        summary = {}
        
        # Process timing metrics
        for metric, values in self.metrics.items():
            if len(values) > 0:
                summary[metric] = {
                    "mean": np.mean(values),
                    "median": np.median(values),
                    "min": np.min(values),
                    "max": np.max(values),
                    "std": np.std(values)
                }
        
        # Process message size metrics
        for metric, values in self.message_sizes.items():
            if len(values) > 0:
                summary[metric] = {
                    "mean": np.mean(values),
                    "median": np.median(values),
                    "total": np.sum(values),
                    "count": len(values),
                    "min": np.min(values),
                    "max": np.max(values)
                }
        
        return summary
    
    def save_metrics(self, experiment_id=None):
        """Save metrics to disk"""
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        exp_id = experiment_id if experiment_id else timestamp
        
        # Save raw metrics
        filename = f"{self.log_dir}/{self.component_name}_{exp_id}_metrics.json"
        with open(filename, 'w') as f:
            json.dump({
                "metrics": {k: list(v) for k, v in self.metrics.items()},
                "message_sizes": {k: list(v) for k, v in self.message_sizes.items()},
            }, f, indent=2)
        
        # Save summary
        summary_filename = f"{self.log_dir}/{self.component_name}_{exp_id}_summary.json"
        with open(summary_filename, 'w') as f:
            json.dump(self.get_metrics_summary(), f, indent=2)
        
        return filename, summary_filename
    
    def generate_report(self, experiment_id=None, comparison_data=None):
        """Generate visualization report"""
        # Implementation for generating visualizations and reports
        pass
        
    def __del__(self):
        """Clean up resources"""
        self.monitoring = False
        if hasattr(self, 'monitor_thread'):
            self.monitor_thread.join(timeout=1)
```

### Integration Plan for Existing Components

#### Server (AGM) Integration
Add monitoring to key phases in `agmactor.py`:
- Client coordination
- Aggregation
- Key management
- Lattice processing

#### Provider (ALM) Integration
Add monitoring to key phases in `almactor.py`:
- Local computation
- Encryption/decryption
- Data transmission

### Communication Layer Monitoring

```python
# src/commun/MonitoredKafkaClient.py
```

This will measure:
- Message transmission time
- Message size
- Serialization/deserialization overhead

## CHECK: Measurement and Analysis

Create an analysis module:

```python
# src/commun/PerformanceAnalyzer.py
```

This will:
- Compare FedFCA vs centralized FCA
- Break down computation vs. communication overhead
- Analyze the impact of the microservice architecture

Key metrics to analyze:
1. **Computation overhead**:
   - CPU time for local computation
   - Memory usage during computation
   - Time spent on encryption/decryption

2. **Communication overhead**:
   - Total bytes transmitted
   - Message transmission time
   - Number of messages exchanged

3. **Architectural overhead**:
   - Time spent on microservice coordination
   - Kafka-related delays
   - Key management overhead

## ACT: Implementation Timeline and Improvements

1. **Week 1**: Implement core `PerformanceMonitor` class
2. **Week 2**: Integrate monitoring into server (AGM) and provider (ALM)
3. **Week 3**: Add communication monitoring and message size tracking
4. **Week 4**: Implement visualization and reporting
5. **Week 5**: Run experiments and generate analysis for reviewer comments

### Expected Outcomes

This comprehensive monitoring plan will:
1. Accurately measure computation and communication overhead
2. Compare FedFCA with centralized approaches fairly
3. Quantify the overhead introduced by the architecture
4. Address the reviewer's specific questions with concrete data

### Continual Improvement

After initial implementation, we'll:
1. Analyze results to identify bottlenecks
2. Optimize the identified performance issues
3. Repeat measurements to validate improvements
4. Document findings for publication and further research
