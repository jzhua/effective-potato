# Product Name Normalization Plan

## Overview

This document outlines the plan to implement product name normalization to fix common data quality issues in product names found in CSV data. Unlike categories and regions, product names have high cardinality (10,000+ variations) making lookup tables impractical. Instead, we use a rule-based algorithmic approach.

## Problem Analysis

### Data Quality Issues Found in CSV

From analysis of `data/input/dirty_1m.csv`, we identified these patterns:

1. **Character Substitutions**: Numbers replacing letters
   - `T0aster` → `Toaster`
   - `Pl1ers` → `Pliers` 
   - `samsung galaxy z fl1p` → `samsung galaxy z flip`

2. **Missing Letters**: Common typos with dropped characters
   - `MacBook Chagrer` → `MacBook Charger` (133 occurrences)
   - `MacBook Aiir` → `MacBook Air`
   - `Wind Chmes` → `Wind Chimes`
   - `MacBoko Air` → `MacBook Air`

3. **Spacing Issues**: Missing spaces around model numbers
   - `iPhone14` → `iPhone 14`
   - `SamsungGalaxyNote` → `Samsung Galaxy Note`
   - `GraphicTShirt` → `Graphic T-Shirt`

4. **Separator Inconsistencies**: Underscores, hyphens, and merged words
   - `External_SSD` → `External SSD`
   - `Nike-Air-Max` → `Nike Air Max`
   - `HighWaistedJeans` → `High Waisted Jeans`

5. **Case Inconsistencies**: Brand names with wrong capitalization
   - `iphone se` → `iPhone SE`
   - `samsung galaxy s22` → `Samsung Galaxy S22`
   - `macbook air` → `MacBook Air`

### Why Rule-Based Approach

**Cardinality Problem:**
- Categories: ~20 values → lookup works
- Regions: ~100 values → lookup works  
- Product Names: 10,000+ values → lookup becomes unwieldy

**Advantages of Rules:**
- Scalable to unlimited variations
- Maintainable through configuration
- Transparent and debuggable
- No storage explosion
- Can handle new products without manual mapping

## Solution Architecture

### 1. Normalization Engine
- **File**: `data_pipeline/cleaning/product_normalizer.py`
- **Class**: `ProductNameNormalizer`
- **Method**: `normalize(product_name: str) -> str`

### 2. Rule Categories
Rules are applied in a specific order to avoid conflicts:

1. **Pre-processing**: Basic cleanup (strip, handle empty)
2. **Character fixes**: Fix obvious character substitutions
3. **Missing letters**: Fix common typos with dropped characters  
4. **Spacing**: Add spaces around model numbers and compound words
5. **Separators**: Normalize underscores/hyphens to spaces
6. **Case standardization**: Fix brand name capitalization
7. **Post-processing**: Clean up multiple spaces, final trim

### 3. Configuration-Driven Design
```yaml
# product_normalization_config.yaml
character_substitutions:
  T0: "To"
  l1: "li"
  O0: "Oo"
  S5: "Ss"

missing_letters:
  Chagrer: "Charger"
  Aiir: "Air"
  Chmes: "Chimes"
  MacBoko: "MacBook"
  Gaame: "Game"
  Kitt: "Kit"
  Bl0cks: "Blocks"

brand_standardization:
  iphone: "iPhone"
  ipad: "iPad"
  macbook: "MacBook"
  samsung galaxy: "Samsung Galaxy"
  google pixel: "Google Pixel"
```

### 4. Integration Points
- **Development**: Standalone function with comprehensive test suite
- **Future Integration**: Add to cleaning pipeline after validation
- **Testing**: Extensive unit tests covering all patterns and edge cases

## Implementation Strategy

### Phase 1: Core Algorithm Implementation ✅
- Implement `normalize_product_name()` function
- Apply rules in correct order to avoid conflicts
- Handle edge cases (empty strings, unicode, special characters)
- Pass comprehensive test suite

### Phase 2: Optimization & Validation
- Performance testing with large datasets
- Validation against real CSV data
- Measure effectiveness (before/after comparison)
- Refine rules based on results

### Phase 3: Configuration System
- Move hardcoded rules to configuration files
- Enable rule customization without code changes
- Add rule versioning and migration support

### Phase 4: Integration with Cleaning Pipeline
- Add to `clean_sales_data.py` after category normalization
- Add configuration option to enable/disable
- Maintain backward compatibility

### Phase 5: Monitoring & Improvement
- Log normalization statistics
- Track common patterns not covered by rules
- Iterative rule improvement based on real data

## Rule Application Order

**Critical**: Rules must be applied in the correct order to avoid conflicts:

```python
def normalize_product_name(product_name: str) -> str:
    # 1. Pre-processing
    normalized = product_name.strip()
    if not normalized:
        return ""
    
    # 2. Character substitutions (T0 → To)
    normalized = apply_character_fixes(normalized)
    
    # 3. Missing letters (Chagrer → Charger)  
    normalized = apply_missing_letter_fixes(normalized)
    
    # 4. Spacing around numbers (iPhone14 → iPhone 14)
    normalized = apply_spacing_fixes(normalized)
    
    # 5. Separator normalization (_ and - to spaces)
    normalized = apply_separator_fixes(normalized)
    
    # 6. Case standardization (iphone → iPhone)
    normalized = apply_case_fixes(normalized)
    
    # 7. Post-processing (clean up multiple spaces)
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    
    return normalized
```

## Expected Outcomes

### Quantitative Improvements
- **Character substitutions**: Fix ~200+ products with T0/l1/O0/S5 patterns
- **Missing letters**: Fix ~150+ products with Chagrer/Aiir/Chmes patterns  
- **Spacing issues**: Fix ~500+ products missing spaces around numbers
- **Separator issues**: Fix ~1000+ products with underscore/hyphen issues
- **Case issues**: Fix ~2000+ products with incorrect brand capitalization

### Quality Metrics
- **Coverage**: % of dirty product names successfully normalized
- **Accuracy**: % of normalizations that are correct
- **Consistency**: Reduction in product name variations for same product
- **Performance**: Processing time per product name

### Success Criteria
- ✅ Pass comprehensive test suite (43 test cases)
- ✅ Handle all patterns identified in CSV analysis
- ✅ Process 1M+ product names in reasonable time (<10 seconds)
- ✅ Reduce product name variations by 30-50%
- ✅ Zero false positive normalizations on clean data

## Risk Mitigation

### Over-normalization Risk
- **Risk**: Rules too aggressive, normalize valid variations
- **Mitigation**: Extensive testing with real data, conservative rules

### Performance Risk  
- **Risk**: Regex processing too slow for large datasets
- **Mitigation**: Caching, compiled regex patterns, chunked processing

### Maintenance Risk
- **Risk**: Rules become complex and hard to maintain
- **Mitigation**: Configuration-driven approach, comprehensive documentation

### Integration Risk
- **Risk**: Breaking existing cleaning pipeline
- **Mitigation**: Standalone implementation first, optional integration

## Testing Strategy

### Unit Tests ✅
- 43 comprehensive test cases covering all patterns
- Edge cases (empty strings, unicode, special characters)
- Performance tests with long strings
- Real-world examples from CSV data

### Integration Tests
- End-to-end testing with full cleaning pipeline
- Before/after comparison on sample datasets
- Performance benchmarking with 1M+ records

### Validation Tests
- Manual review of normalization results
- Statistical analysis of improvement metrics
- User acceptance testing with domain experts

## Deployment Plan

### Development Environment
1. Implement standalone function
2. Pass all unit tests
3. Performance and validation testing

### Staging Environment  
1. Test with full CSV datasets
2. Measure improvement metrics
3. Validate with stakeholders

### Production Environment
1. Deploy as optional feature
2. Monitor effectiveness and performance
3. Gradual rollout with killswitch capability

This approach ensures systematic, maintainable, and effective product name normalization while minimizing risk to existing systems.