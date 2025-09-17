#!/usr/bin/env python3
"""Unit tests for region canonicalization that should be failing initially."""

import pytest
import json
from pathlib import Path
from scripts.build_region_map import guess_region, load_canonical_regions


class TestRegionCanonicalization:
    """Test region canonicalization with cases that are currently failing."""
    
    @pytest.fixture
    def canonicals(self):
        """Load canonical regions."""
        return load_canonical_regions()
    
    @pytest.fixture
    def region_map(self):
        """Load current region mapping."""
        region_map_path = Path("data/lookups/region_map.json")
        if region_map_path.exists():
            with open(region_map_path) as f:
                return json.load(f)
        return {}
    
    def test_na_abbreviation_mapping(self, region_map, canonicals):
        """Test that 'NA' maps to 'North America' - CURRENTLY FAILING."""
        # NA is in the generation constants but not in region_map.json
        assert "NA" in region_map, "NA should be mapped in region_map.json"
        assert region_map["NA"] == "North America", "NA should map to North America"
        
        # The JSON mapping is what matters for data cleaning
    
    def test_case_insensitive_na_mapping(self, region_map, canonicals):
        """Test that various cases of NA map correctly - CURRENTLY FAILING."""
        na_variants = ["NA", "na", "Na", "nA"]
        
        for variant in na_variants:
            # Should be in the mapping
            assert variant in region_map, f"'{variant}' should be mapped in region_map.json"
            assert region_map[variant] == "North America", f"'{variant}' should map to North America"
            
            # The JSON mapping is what matters for data cleaning
    
    def test_all_generated_regions_are_mappable(self, region_map, canonicals):
        """Test that all regions from REGIONS constant can be mapped - CURRENTLY FAILING."""
        from data_pipeline.generation.constants import REGIONS
        
        unmappable = []
        for region in REGIONS:
            # Either directly in map or guessable
            if region not in region_map:
                guess = guess_region(region, canonicals)
                if guess is None:
                    unmappable.append(region)
        
        assert len(unmappable) == 0, f"These regions cannot be mapped: {unmappable}"
    
    def test_typo_regions_are_mappable(self, region_map, canonicals):
        """Test that common typo'd regions can be mapped - MIGHT BE FAILING."""
        # Test some realistic typos that might be generated
        from data_pipeline.generation.typo_utils import generate_typo
        
        base_regions = ["North America", "Europe", "Asia", "United States"]
        unmappable_typos = []
        
        for base_region in base_regions:
            # Generate several typos
            for _ in range(10):
                typo_region = generate_typo(base_region, typo_probability=1.0)
                if typo_region != base_region:  # Only test actual typos
                    if typo_region not in region_map:
                        guess = guess_region(typo_region, canonicals)
                        if guess is None:
                            unmappable_typos.append(typo_region)
        
        # We expect some typos to be unmappable, but not too many
        failure_rate = len(unmappable_typos) / (len(base_regions) * 10) if unmappable_typos else 0
        assert failure_rate < 0.1, f"Too many typos unmappable ({failure_rate:.1%}): {unmappable_typos[:5]}..."
    
    def test_region_synonyms_coverage(self, region_map):
        """Test that essential synonyms are covered in mapping."""
        # These are the important synonyms that should be mapped
        essential_synonyms = {
            "us": "United States",
            "u.s": "United States", 
            "u.s.": "United States",
            "usa": "United States",
            "united states": "United States",
            "uk": "United Kingdom",
            "u.k": "United Kingdom",
            "u.k.": "United Kingdom",
            "emea": "Europe",
            "apac": "Asia",
            "na": "North America",
            "n america": "North America",
            "latam": "South America",
            "s america": "South America",
        }
        
        missing_synonyms = []
        wrong_mappings = []
        
        for synonym, expected_canonical in essential_synonyms.items():
            if synonym not in region_map:
                missing_synonyms.append(synonym)
            elif region_map[synonym] != expected_canonical:
                wrong_mappings.append(f"'{synonym}' maps to '{region_map[synonym]}', expected '{expected_canonical}'")
        
        assert len(missing_synonyms) == 0, f"These synonyms are missing from region_map.json: {missing_synonyms}"
        assert len(wrong_mappings) == 0, f"These synonyms have wrong mappings: {wrong_mappings}"
    
    def test_n_slash_a_handling(self, region_map, canonicals):
        """Test that N/A (not applicable) is handled properly - CURRENTLY FAILING."""
        # N/A should probably map to UNKNOWN, not be unmappable
        na_values = ["N/A", "n/a", "N/a", "not applicable"]
        
        for na_val in na_values:
            # Should either be mapped or guessable to something reasonable
            if na_val not in region_map:
                guess = guess_region(na_val, canonicals)
                # N/A probably shouldn't guess to a real region
                assert guess is None or guess == "UNKNOWN", f"'{na_val}' should not guess to a real region: {guess}"
    
    def test_empty_and_null_regions(self, region_map, canonicals):
        """Test that empty/null regions are handled - MIGHT BE FAILING."""
        empty_values = ["", "NULL", "null", None, "   "]
        
        for empty_val in empty_values:
            guess = guess_region(str(empty_val) if empty_val is not None else "", canonicals)
            # Empty values should return None
            assert guess is None, f"Empty value '{empty_val}' should return None, got: {guess}"


if __name__ == "__main__":
    # Run the tests to see which ones fail
    pytest.main([__file__, "-v"])