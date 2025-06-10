
# Assuming these custom modules are in the path
from commun.logactor import LoggerActor # For structured logging if preferred over standard logging
from commun.FedFcaMonitor import FedFcaMonitor # For performance monitoring
from commun.ConceptStabilityAnalyzer import ConceptStabilityAnalyzer # Likely not used directly by ALM

import uuid
import time
class FederationManager:
    """Manage federation operations for the FedFCA framework."""
    
    def __init__(self, agm_actor):
        """Initialize the federation manager.
        
        Args:
            agm_actor (AGMActor): Reference to the AGM actor.
        """
        self.agm = agm_actor
        self.federation_id = None
        self.providers = {}
        self.federation_status = "inactive"
        self.federation_config = {}
        self.round_in_progress = False
        self.current_round = 0
        self.encryption_key_id = None
        self.monitor=FedFcaMonitor()
    
    def create_federation(self, config):
        """Create a new federation.
        
        Args:
            config (dict): Federation configuration.
            
        Returns:
            str: Federation ID.
        """
        self.federation_id = f"fed_{uuid.uuid4().hex[:8]}"
        self.federation_config = config
        self.providers = {p: {"status": "invited"} for p in config.get("providers", [])}
        self.federation_status = "created"
        self.current_round = 0
        
        # Generate encryption key for the federation
        self.encryption_key_id = self.agm.crypto.generate_key(f"fed_{self.federation_id}")
        
        self.agm.logactor.info(f"Created federation {self.federation_id} with {len(self.providers)} providers")
        
        # Broadcast federation creation to all providers
        self._broadcast_federation_info()
        
        return self.federation_id
    
    def _broadcast_federation_info(self):
        """Broadcast federation information to all providers."""
        for provider_id in self.providers:
            federation_info = {
                "action": "federation_invite",
                "federation_id": self.federation_id,
                "provider_id": provider_id,
                "config": {
                    "threshold": self.federation_config.get("threshold", 0.5),
                    "context_sensitivity": self.federation_config.get("context_sensitivity", 0.5),
                    "encryption_key": self.agm.crypto.export_key(self.encryption_key_id)
                }
            }
            
            self.agm.send_message_to_provider(provider_id, federation_info)
    
    def update_provider_status(self, provider_id, status):
        """Update the status of a provider in the federation.
        
        Args:
            provider_id (str): Provider ID.
            status (str): New status.
        """
        if provider_id in self.providers:
            self.providers[provider_id]["status"] = status
            self.agm.logactor.info(f"Provider {provider_id} status updated to {status}")
            
            # Check if all providers are ready to start a round
            if status == "ready" and all(p["status"] == "ready" for p in self.providers.values()):
                self.federation_status = "ready"
    
    def start_round(self):
        """Start a new federation round.
        
        Returns:
            bool: True if the round was started, False otherwise.
        """
        if self.federation_status != "ready" or self.round_in_progress:
            self.agm.logactor.warning("Cannot start round: federation not ready or round in progress")
            return False
        
        self.current_round += 1
        self.round_in_progress = True
        self.agm.logactor.info(f"Starting federation round {self.current_round}")
        """Start a new federation round."""
        self.monitor.start_monitoring()  # Start system monitoring
        # Broadcast round start message to all providers
        for provider_id in self.providers:
            round_info = {
                "action": "round_start",
                "federation_id": self.federation_id,
                "round": self.current_round,
                "request_time": time.time()
            }
            
            self.agm.send_message_to_provider(provider_id, round_info)
        
        return True
    
    def process_lattice(self, provider_id, lattice_data):
        """Process a lattice received from a provider.
        
        Args:
            provider_id (str): Provider ID.
            lattice_data (dict): Encrypted lattice data.
            
        Returns:
            bool: True if the lattice was processed, False otherwise.
        """
        if not self.round_in_progress:
            self.agm.logactor.warning(f"Received lattice from {provider_id} but no round is in progress")
            return False
        
        if provider_id not in self.providers:
            self.agm.logactor.warning(f"Received lattice from unknown provider {provider_id}")
            return False
        
        # Store the lattice
        self.agm.provider_lattices[provider_id] = lattice_data
        self.providers[provider_id]["status"] = "submitted"
        self.agm.logactor.info(f"Received lattice from {provider_id}")
        
        # Check if all providers have submitted their lattices
        if all(p["status"] == "submitted" for p in self.providers.values()):
            self.agm.logactor.info("All providers have submitted their lattices")
            self._finalize_round()
        
        return True
    
    def _finalize_round(self):
        """Finalize the current federation round."""
        self.agm.logactor.info(f"Finalizing round {self.current_round}")
        
        # Perform aggregation
        self.agm.aggregate_lattices()
        
        # Reset provider statuses for next round
        for provider_id in self.providers:
            self.providers[provider_id]["status"] = "ready"
        
        # Mark round as complete
        self.round_in_progress = False
        
        # Broadcast results to all providers
        self._broadcast_results()
        # Existing finalization code
        self.monitor.stop_monitoring()  # Stop system monitoring
        
        # Get and log system metrics
        metrics = self.monitor.get_metrics()
    def _broadcast_results(self):
        """Broadcast aggregated results to all providers."""
        for provider_id in self.providers:
            result_info = {
                "action": "round_result",
                "federation_id": self.federation_id,
                "round": self.current_round,
                "global_stability": self.agm.global_stability,
                "lattice_size": len(self.agm.global_lattice) if self.agm.global_lattice else 0,
                "quality_metrics": self.agm.quality.analyze_stability(self.agm.global_lattice)
            }
            
            self.agm.send_message_to_provider(provider_id, result_info)

