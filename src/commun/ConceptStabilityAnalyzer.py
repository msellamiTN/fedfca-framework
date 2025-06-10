class ConceptStabilityAnalyzer:
    """Analyze the stability of formal concepts in the FedFCA framework."""
    
    def __init__(self):
        """Initialize the concept stability analyzer."""
        self.stability_thresholds = {
            'low': 0.3,
            'medium': 0.6,
            'high': 0.9
        }
    
    def analyze_stability(self, lattice):
        """Analyze the stability of a concept lattice.
        
        Args:
            lattice (list): List of formal concepts with stability values.
            
        Returns:
            dict: Stability analysis results.
        """
        if not lattice:
            return {'avg_stability': 0, 'stability_dist': {}, 'quality_score': 0}
        
        stability_values = [stab for _, stab in lattice]
        avg_stability = sum(stability_values) / len(stability_values)
        
        # Calculate distribution of stability values
        stability_dist = {
            'low': len([s for s in stability_values if s < self.stability_thresholds['medium']]),
            'medium': len([s for s in stability_values if self.stability_thresholds['medium'] <= s < self.stability_thresholds['high']]),
            'high': len([s for s in stability_values if s >= self.stability_thresholds['high']])
        }
        
        # Calculate quality score (weighted average)
        quality_score = (stability_dist['low'] * 0.1 + 
                         stability_dist['medium'] * 0.5 + 
                         stability_dist['high'] * 1.0) / len(stability_values)
        
        return {
            'avg_stability': avg_stability,
            'stability_dist': stability_dist,
            'quality_score': quality_score
        }
    
    def compare_lattices(self, lattice1, lattice2):
        """Compare two concept lattices for similarity.
        
        Args:
            lattice1 (list): First concept lattice.
            lattice2 (list): Second concept lattice.
            
        Returns:
            float: Similarity score between 0 and 1.
        """
        if not lattice1 or not lattice2:
            return 0.0
        
        # Extract concept structures (without stability values)
        concepts1 = [self._normalize_concept(c[0]) for c in lattice1]
        concepts2 = [self._normalize_concept(c[0]) for c in lattice2]
        
        # Calculate Jaccard similarity
        intersection = len(set(concepts1) & set(concepts2))
        union = len(set(concepts1) | set(concepts2))
        
        return intersection / union if union > 0 else 0.0
    
    def _normalize_concept(self, concept):
        """Normalize a concept for comparison purposes.
        
        Args:
            concept (tuple): A concept as (extent, intent).
            
        Returns:
            tuple: Normalized concept as (frozenset(extent), frozenset(intent)).
        """
        extent, intent = concept
        return (frozenset(extent), frozenset(intent))
