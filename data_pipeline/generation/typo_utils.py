"""Utility functions for generating realistic typos and spelling errors."""
import random


def generate_typo(text: str, typo_probability: float = 0.1) -> str:
    """Generate a typo version of the input text using common typing mistakes.
    
    Args:
        text: The original text to introduce typos into
        typo_probability: Probability of applying each type of typo (0.0 to 1.0)
        
    Returns:
        Text with one or more typos applied
    """
    if not text or len(text) < 2:
        return text
        
    # Keyboard layout for fat finger errors (QWERTY)
    keyboard_neighbors = {
        'a': 'sqwz', 'b': 'vghn', 'c': 'xdfv', 'd': 'erfcxs', 'e': 'wsdr',
        'f': 'rtgvcd', 'g': 'tyhbvf', 'h': 'yujnbg', 'i': 'ujko', 'j': 'uikmnh',
        'k': 'iolmj', 'l': 'opmk', 'm': 'njkl', 'n': 'bhjm', 'o': 'iklp',
        'p': 'ol', 'q': 'wa', 'r': 'edfgt', 's': 'awedxz', 't': 'rfgyh',
        'u': 'yhji', 'v': 'cfgb', 'w': 'qase', 'x': 'zasdc', 'y': 'tghu',
        'z': 'asx'
    }
    
    # Common letter substitutions (visual similarity, sound similarity)
    substitutions = {
        'o': '0', '0': 'o', 'i': '1', '1': 'i', 'l': '1', 's': '5', '5': 's',
        'e': '3', '3': 'e', 'a': '@', 'g': '9', '9': 'g', 'b': '6', '6': 'b',
        'c': 'k', 'k': 'c', 'ph': 'f', 'f': 'ph', 'ck': 'k', 'qu': 'kw'
    }
    
    typo_types = [
        'fat_finger',      # Hit adjacent key
        'duplicate',       # Duplicate a letter  
        'omission',        # Skip a letter
        'insertion',       # Insert extra letter
        'transposition',   # Swap adjacent letters
        'case_error',      # Wrong case
        'substitution',    # Common substitutions
        'double_key',      # Press key twice
        'space_error'      # Missing or extra spaces
    ]
    
    result = text
    
    # Apply only one random typo instead of potentially all 9 types
    if random.random() <= typo_probability:
        typo_type = random.choice(typo_types)
        
        if typo_type == 'fat_finger':
            # Hit adjacent key instead of intended key
            pos = random.randint(0, len(result) - 1)
            char = result[pos].lower()
            if char in keyboard_neighbors:
                neighbors = keyboard_neighbors[char]
                new_char = random.choice(neighbors)
                # Preserve original case
                if result[pos].isupper():
                    new_char = new_char.upper()
                result = result[:pos] + new_char + result[pos+1:]
                
        elif typo_type == 'duplicate':
            # Duplicate a random letter
            pos = random.randint(0, len(result) - 1)
            result = result[:pos] + result[pos] + result[pos:]
            
        elif typo_type == 'omission':
            # Skip/delete a letter
            if len(result) > 2:
                pos = random.randint(1, len(result) - 2)  # Don't delete first/last
                result = result[:pos] + result[pos+1:]
                
        elif typo_type == 'insertion':
            # Insert random letter
            pos = random.randint(0, len(result))
            # Insert letter similar to nearby letters or random
            nearby_chars = []
            if pos > 0:
                nearby_chars.append(result[pos-1].lower())
            if pos < len(result):
                nearby_chars.append(result[pos].lower())
            
            if nearby_chars:
                # Insert letter similar to nearby ones
                base_char = random.choice(nearby_chars)
                if base_char in keyboard_neighbors:
                    new_char = random.choice(keyboard_neighbors[base_char])
                else:
                    new_char = random.choice('abcdefghijklmnopqrstuvwxyz')
            else:
                new_char = random.choice('abcdefghijklmnopqrstuvwxyz')
            result = result[:pos] + new_char + result[pos:]
            
        elif typo_type == 'transposition':
            # Swap two adjacent characters
            if len(result) > 1:
                pos = random.randint(0, len(result) - 2)
                result = (result[:pos] + result[pos+1] + result[pos] + result[pos+2:])
                
        elif typo_type == 'case_error':
            # Random case change
            pos = random.randint(0, len(result) - 1)
            char = result[pos]
            if char.isalpha():
                if char.islower():
                    result = result[:pos] + char.upper() + result[pos+1:]
                else:
                    result = result[:pos] + char.lower() + result[pos+1:]
                    
        elif typo_type == 'substitution':
            # Common letter/number substitutions
            for original, replacement in substitutions.items():
                if original in result.lower():
                    # Find position and preserve case
                    pos = result.lower().find(original)
                    if pos != -1:
                        orig_case = result[pos:pos+len(original)]
                        new_replacement = replacement
                        # Try to preserve case for single character substitutions
                        if len(original) == 1 and len(replacement) == 1:
                            if orig_case.isupper():
                                new_replacement = replacement.upper()
                        result = result[:pos] + new_replacement + result[pos+len(original):]
                        break
                        
        elif typo_type == 'double_key':
            # Press same key twice (different from duplicate - affects whole sequence)
            pos = random.randint(0, len(result) - 1)
            char = result[pos]
            result = result[:pos] + char + char + result[pos+1:]
            
        elif typo_type == 'space_error':
            # Missing or extra spaces (if text has spaces)
            if ' ' in result:
                if random.random() < 0.5:
                    # Remove a space
                    space_pos = result.find(' ')
                    if space_pos != -1:
                        result = result[:space_pos] + result[space_pos+1:]
                else:
                    # Add extra space
                    pos = random.randint(0, len(result))
                    result = result[:pos] + ' ' + result[pos:]
    
    return result