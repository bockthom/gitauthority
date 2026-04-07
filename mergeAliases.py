# Parts of the code in this file are taken from https://github.com/bvasiles/ght_unmasking_aliases

import os
from csv import DictReader, writer
import json
import argparse
import unicodedata

import regex

from alias import Alias
from collections import Counter
from itertools import combinations, product

import io

# Debug flag: set to True to enable verbose output
DEBUG = False

DEFAULT_CONFIG = 'merge_config.txt'

def load_config(config_path=DEFAULT_CONFIG):
    """
    Load configuration from a simple text file.
    Format: OPTION_NAME = value  # comment
    """
    config = {}
    if os.path.isabs(config_path):
        full_path = config_path
    else:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(script_dir, config_path)

    # Default values if config file doesn't exist
    defaults = {
        'EMAIL': True,
        'COMP_EMAIL_PREFIX': True,
        'SIMPLE_EMAIL_PREFIX': True,
        'PREFIX_LOGIN': True,
        'PREFIX_NAME': True,
        'LOGIN_NAME': True,
        'FULL_NAME': True,
        'SWITCHED_NAME': True,
        'DOMAIN': True,
        'DOMAIN_MAIN_PART': True,
        'DOMAIN_NAME_MATCH': True,
        'GITHUB_USERNAME_MATCH': True,
        'COMMA_SUFFIX_MATCH': True,
        'ABBREV_FIRST_NAME': True,
        'ABBREV_LAST_NAME': True,
        'ABBREV_MIDDLE_NAME': True,
        'LOCATION': True,
        'THR_MIN': 1,
        'THR_MAX': 40,
        'ENABLE_GERMAN_DETECTION': True,
        'ENABLE_DUAL_NORMALIZATION': True,
        'BLACKLIST_INCLUDE_FIRST_NAMES': True,
        'EXCLUDE_PRIVACY_EMAILS_FROM_MERGING': False,
        # Preprocessing options
        'NORMALIZE_UNICODE_NFC': True,
        'NORMALIZE_LEETSPEAK': True,
        'REMOVE_TIMESTAMPS': True,
        'REMOVE_TIMEZONES': True,
        'REMOVE_YEARS': True,
        'REMOVE_TITLES': True,
        'REMOVE_PUNCTUATION': True,
        'REMOVE_MIDDLE_NAMES': True,
        'DEBUG': False
    }

    if not os.path.exists(full_path):
        print(f"Warning: Config file not found at {full_path}, using defaults")
        return defaults

    with open(full_path, 'r') as f:
        for line in f:
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Parse line: OPTION = value  # comment
            if '=' in line:
                parts = line.split('=', 1)
                key = parts[0].strip()
                value_part = parts[1].split('#')[0].strip()  # Remove inline comments

                # Parse value
                if value_part.lower() == 'true':
                    config[key] = True
                elif value_part.lower() == 'false':
                    config[key] = False
                else:
                    # Try to parse as number
                    try:
                        config[key] = int(value_part)
                    except ValueError:
                        # Check if it's a comma-separated list (for blacklist terms)
                        # Blacklist keys are treated as lists (even single items)
                        if key.startswith('BLACKLIST_') and key not in ['BLACKLIST_INCLUDE_FIRST_NAMES']:
                            if ',' in value_part:
                                # Parse as comma-separated list, strip whitespace from each item
                                config[key] = [item.strip() for item in value_part.split(',')]
                            else:
                                # Single item, still make it a list
                                config[key] = [value_part.strip()]
                        elif ',' in value_part:
                            # Other comma-separated values
                            config[key] = [item.strip() for item in value_part.split(',')]
                        else:
                            config[key] = value_part

    # Merge with defaults (use config values if present, otherwise defaults)
    return {**defaults, **config}

# Global configuration
CONFIG = load_config()

# Update DEBUG from config
DEBUG = CONFIG.get('DEBUG', False)

def get_universal_character_mappings():
    """
    Return comprehensive character mappings for all supported languages.
    Uses simple vowel replacements for ambiguous characters.
    """
    return {
        # German-specific (ß is unambiguous)
        'ß': 'ss',

        # Romance languages
        'ñ': 'n', 'ç': 'c', 'œ': 'oe', 'æ': 'ae',
        'á': 'a', 'à': 'a', 'â': 'a', 'ã': 'a',
        'é': 'e', 'è': 'e', 'ê': 'e', 'ë': 'e',
        'í': 'i', 'ì': 'i', 'î': 'i', 'ï': 'i',
        'ó': 'o', 'ò': 'o', 'ô': 'o', 'õ': 'o',
        'ú': 'u', 'ù': 'u', 'û': 'u',
        'ÿ': 'y', 'ý': 'y',

        # Ambiguous characters - use simple replacements for broader matching
        'ä': 'a', 'ö': 'o', 'ü': 'u',

        # Scandinavian
        'å': 'aa', 'ø': 'oe',

        # Slavic languages
        'ą': 'a', 'ć': 'c', 'č': 'c', 'ď': 'd', 'đ': 'dj', 'ę': 'e', 'ě': 'e',
        'ł': 'l', 'ľ': 'l', 'ĺ': 'l', 'ň': 'n', 'ń': 'n', 'ř': 'r', 'ŕ': 'r',
        'ś': 's', 'š': 's', 'ť': 't', 'ů': 'u', 'ź': 'z', 'ż': 'z', 'ž': 'z',

        # Turkish-specific characters that don't conflict
        'ğ': 'g', 'ı': 'i', 'ş': 's',

        # Hungarian-specific
        'ő': 'o', 'ű': 'u',

        # Baltic languages
        'ā': 'a', 'ē': 'e', 'ī': 'i', 'ū': 'u', 'ģ': 'g', 'ķ': 'k', 'ļ': 'l', 'ņ': 'n',

        # Cyrillic script
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'h', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',

        # Greek script
        'α': 'a', 'β': 'b', 'γ': 'g', 'δ': 'd', 'ε': 'e', 'ζ': 'z', 'η': 'i',
        'θ': 'th', 'ι': 'i', 'κ': 'k', 'λ': 'l', 'μ': 'm', 'ν': 'n', 'ξ': 'x',
        'ο': 'o', 'π': 'p', 'ρ': 'r', 'σ': 's', 'ς': 's', 'τ': 't', 'υ': 'y',
        'φ': 'f', 'χ': 'ch', 'ψ': 'ps', 'ω': 'o',

        # Other specific characters
        'ð': 'd', 'þ': 'th', 'ij': 'ij', 'ġ': 'g', 'ħ': 'h'
    }

def get_german_specific_mappings():
    """Return German-specific character mappings."""
    return {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'ß': 'ss'}

def get_leetspeak_mappings():
    """
    Return leetspeak character mappings.
    Maps numbers and special characters commonly used in leetspeak to their letter equivalents.
    Note: We only map digits and less common special characters to avoid conflicts with
    email addresses and common punctuation.
    """
    return {
        # Digits commonly used in leetspeak
        '0': 'o',
        '1': 'i',
        '3': 'e',
        '4': 'a',
        '5': 's',
        '7': 't',
        '8': 'b',
        '9': 'g',
        # Only uncommon special characters (not @, +, ( which are common in emails/names)
        '$': 's',
        '|': 'i',
        '<': 'c',
        '€': 'e',
        '£': 'l'
    }

def has_ill_encoded_characters(text):
    """
    Check if a name contains ill-encoded characters that should be avoided
    when selecting representatives.

    Args:
        text (str): The text to check for encoding issues

    Returns:
        bool: True if the text contains ill-encoded characters
    """
    if not text or not isinstance(text, str):
        return False

    # Check for Unicode replacement character (U+FFFD)
    if '\ufffd' in text:
        return True

    # Check for null bytes or other control characters that shouldn't be in names
    if '\x00' in text or any(ord(c) < 32 and c not in '\t\n\r' for c in text):
        return True

    # Check for common encoding artifacts - sequences that suggest double encoding
    # Common patterns when UTF-8 is incorrectly decoded as Latin-1 then re-encoded
    encoding_artifacts = [
        'Ã¡', 'Ã©', 'Ã­', 'Ã³', 'Ãº',  # á é í ó ú incorrectly encoded
        'Ã¢', 'Ãª', 'Ã®', 'Ã´', 'Ã»',  # â ê î ô û incorrectly encoded
        'Ã£', 'Ã±', 'Ã§',              # ã ñ ç incorrectly encoded
        'Ã¤', 'Ã¶', 'Ã¼',              # ä ö ü incorrectly encoded
        'ÃŸ',                          # ß incorrectly encoded
        'Â©', 'Â®', 'Â',               # copyright, registered, other artifacts
    ]

    for artifact in encoding_artifacts:
        if artifact in text:
            return True

    # Check for sequences of high-bit characters that might indicate encoding issues
    # Look for patterns like multiple consecutive characters with code points > 127
    # that don't form valid UTF-8 sequences
    try:
        # Try to encode/decode to detect corrupted encodings
        text.encode('utf-8').decode('utf-8')
    except (UnicodeEncodeError, UnicodeDecodeError):
        return True

    # Check for suspicious character combinations that often result from encoding issues
    # Characters that shouldn't normally appear together in names
    suspicious_patterns = [
        'â€™',  # Smart quote encoding artifact
        'â€œ',  # Smart quote encoding artifact
        'â€',   # Common encoding artifact prefix
    ]

    for pattern in suspicious_patterns:
        if pattern in text:
            return True

    # More specific check for Latin-1/UTF-8 confusion - look for Ã followed by specific patterns
    # Only flag if it's clearly an encoding artifact, not legitimate accented characters
    if 'Ã' in text:
        # These are common double-encoding patterns where Ã appears as an artifact
        # Check for patterns that are clearly encoding errors
        double_encoding_patterns = [
            'Ã¡',  # Should be á
            'Ã©',  # Should be é
            'Ã­',  # Should be í
            'Ã³',  # Should be ó
            'Ãº',  # Should be ú
            'Ã¢',  # Should be â
            'Ãª',  # Should be ê
            'Ã®',  # Should be î
            'Ã´',  # Should be ô
            'Ã»',  # Should be û
            'Ã£',  # Should be ã
            'Ã±',  # Should be ñ
            'Ã§',  # Should be ç
            'Ã¤',  # Should be ä
            'Ã¶',  # Should be ö
            'Ã¼',  # Should be ü
            'ÃŸ',  # Should be ß
        ]

        for pattern in double_encoding_patterns:
            if pattern in text:
                return True

    return False

def detect_german_context(text, email_domain=None, location=None):
    """
    Detect if German-specific mappings should be used based on context clues.

    Args:
        text (str): The text to analyze
        email_domain (str, optional): Email domain for additional context
        location (str, optional): Location information for additional context

    Returns:
        bool: True if German-specific mappings should be used
    """
    if not text:
        return False

    # German-speaking country domains
    german_domains = [
        '.de',    # Germany
        '.at',    # Austria
        '.ch',    # Switzerland
        '.lu',    # Luxembourg
        '.li',    # Liechtenstein
        '.be',    # Belgium (German is official language alongside Dutch and French)
        '.it',    # Italy (South Tyrol region)
        '.se',    # Sweden (has similar umlaut usage)
        '.dk',    # Denmark (minor German-speaking population)
    ]

    # Check email domain for German-speaking countries
    if email_domain:
        email_lower = email_domain.lower()
        if any(email_lower.endswith(domain) for domain in german_domains):
            return True

    # Check for ß character (uniquely German)
    if 'ß' in text.lower():
        return True

    return False




def normalize_for_comparison(text, email_domain=None, location=None, force_german=None, force_leetspeak=None):
    """
    Normalize text for comparison with intelligent German detection.

    Args:
        text (str): The text to normalize. If None, returns None.
        email_domain (str, optional): Email domain for German context detection
        location (str, optional): Location for German context detection
        force_german (bool, optional): Force German-specific mappings if True,
                                     disable if False, auto-detect if None
        force_leetspeak (bool, optional): If True, apply leetspeak replacements (3→e, 0→o, etc.),
                                         if False, skip leetspeak (chars removed later by punctuation),
                                         if None, use config default

    Returns:
        str or None: Normalized text, or None if input is None.
    """
    if text is None:
        return None

    text = text.lower()

    import re

    # Normalize Unicode to NFC (composed form) first, before any character replacements
    # This ensures that different Unicode representations of the same character are unified
    # For example: 'ö' can be represented as NFC (single composed character U+00F6)
    # or NFD (decomposed: 'o' + combining diaeresis U+006F + U+0308)
    if CONFIG.get('NORMALIZE_UNICODE_NFC', True):
        text = unicodedata.normalize('NFC', text)

    # Apply leetspeak normalization early in the pipeline
    # Two modes for dual normalization:
    # 1. Replace mode: Convert leetspeak chars to letter equivalents (e.g., "h3ll0" -> "hello")
    # 2. Skip mode: Leave as-is, chars will be removed later by punctuation (e.g., "h3ll0" -> "hllo")
    if force_leetspeak is None:
        apply_leetspeak = CONFIG.get('NORMALIZE_LEETSPEAK', True)
    else:
        apply_leetspeak = force_leetspeak

    if apply_leetspeak:
        leetspeak_mappings = get_leetspeak_mappings()
        for original, replacement in leetspeak_mappings.items():
            text = text.replace(original, replacement)

    # Strip timezone abbreviations, timestamps, and years from beginning and end
    # These are often artifacts from git commit metadata

    # Remove timestamp patterns (e.g., "13:37:28", "10:30:45")
    # Pattern: one or more sequences of digits:digits (with optional leading/trailing spaces)
    if CONFIG.get('REMOVE_TIMESTAMPS', True):
        text = re.sub(r'^\s*\d+:\d+:\d+\s+', '', text)  # Remove from beginning
        text = re.sub(r'\s+\d+:\d+:\d+\s*$', '', text)  # Remove from end

    # Remove timezone abbreviations and years
    if CONFIG.get('REMOVE_TIMEZONES', True) or CONFIG.get('REMOVE_YEARS', True):
        timezone_and_year_patterns = []

        if CONFIG.get('REMOVE_TIMEZONES', True):
            # Common timezone abbreviations
            timezone_and_year_patterns.extend([
                'gmt', 'bst', 'cet', 'cest', 'utc', 'pst', 'pdt', 'est', 'edt',
                'mst', 'mdt', 'cst', 'cdt', 'ist', 'jst', 'aest', 'aedt'
            ])

        if CONFIG.get('REMOVE_YEARS', True):
            # Years (1900-2099) - match as standalone words
            timezone_and_year_patterns.extend([r'\b19\d{2}\b', r'\b20\d{2}\b'])

        # Remove from beginning and end
        for pattern in timezone_and_year_patterns:
            # Remove from beginning (with optional trailing spaces)
            text = re.sub(r'^' + pattern + r'\s+', '', text)
            # Remove from end (with optional leading spaces)
            text = re.sub(r'\s+' + pattern + r'$', '', text)

    # Clean up extra whitespace that may have been created
    text = re.sub(r'\s+', ' ', text).strip()

    # Strip common titles and suffixes by removing them as complete words
    # List of titles/suffixes to remove (with and without periods)
    if CONFIG.get('REMOVE_TITLES', True):
        titles_to_remove = [
            'dr', 'dr.', 'prof', 'prof.', 'phd', 'phd.',
            'mr', 'mr.', 'mrs', 'mrs.', 'ms', 'ms.',
            'jr', 'jr.', 'sr', 'sr.', 'sir',
            'ii', 'iii', 'iv', 'v', 'vi', 'vii', 'viii', 'ix', 'x'
        ]

        # Split text into words, remove titles, rejoin
        words = text.split()
        filtered_words = [word for word in words if word not in titles_to_remove]
        text = ' '.join(filtered_words)

    # Determine whether to use German-specific mappings
    if force_german is None:
        use_german = detect_german_context(text, email_domain, location)
    else:
        use_german = force_german

    # Apply appropriate character mappings
    if use_german:
        # Use German-specific mappings
        german_mappings = get_german_specific_mappings()
        for original, replacement in german_mappings.items():
            text = text.replace(original, replacement)

        # Apply universal mappings for non-German characters
        universal_mappings = get_universal_character_mappings()
        for original, replacement in universal_mappings.items():
            if original not in german_mappings:
                text = text.replace(original, replacement)
    else:
        # Apply universal mappings
        universal_mappings = get_universal_character_mappings()
        for original, replacement in universal_mappings.items():
            text = text.replace(original, replacement)

    # Fallback: normalize remaining Unicode characters
    normalized = unicodedata.normalize('NFD', text)
    ascii_text = ''.join(char for char in normalized if unicodedata.category(char) != 'Mn')

    # Remove punctuation marks
    if CONFIG.get('REMOVE_PUNCTUATION', True):
        text_no_punct = regex.sub(r'[.,:;!?\'"()\[\]{}/@#$%^&*+=|\\<>~`\-_]', '', ascii_text).strip()
    else:
        text_no_punct = ascii_text.strip()

    # Remove middle names in 3-part names if they are single characters or common first names
    # This helps match "Timur I. Bakeyev" with "Timur Bakeyev"
    # But keeps full middle names that might distinguish different people
    words = text_no_punct.split()
    if CONFIG.get('REMOVE_MIDDLE_NAMES', True) and len(words) == 3:
        middle_word = words[1]
        # Remove middle word if it's a single character (initial) or a common first name
        # Note: blacklist contains capitalized names, so we need case-insensitive comparison
        if len(middle_word) == 1 or middle_word.lower() in [n.lower() for n in blacklist_first_names]:
            words = [words[0], words[2]]  # Keep only first and last

    return ' '.join(words)


def normalize_for_comparison_dual(text, email_domain=None, location=None):
    """
    Normalize text and return multiple forms to handle inconsistent transliterations.

    This handles cases where people use different conventions:
    - German umlauts: ü→ue vs ü→u, ä→ae vs ä→a, ö→oe vs ö→o
    - Leetspeak: 3→e,0→o vs digits left as-is (removed later)

    Args:
        text (str): The text to normalize
        email_domain (str, optional): Email domain for context
        location (str, optional): Location for context

    Returns:
        set: Set of normalized forms (1-4 strings depending on presence of umlauts/leetspeak)
             Only returns multiple forms if they're actually different
    """
    if text is None:
        return {None}

    forms = set()

    # Generate all combinations: German (True/False) × Leetspeak (True/False)
    for force_german in [True, False]:
        for force_leetspeak in [True, False]:
            form = normalize_for_comparison(text, email_domain, location,
                                          force_german=force_german,
                                          force_leetspeak=force_leetspeak)
            forms.add(form)

    # The set automatically deduplicates, so we return all unique forms
    # If all forms are the same, the set will contain only 1 element
    return forms


EMAIL                 = 'EMAIL'
COMP_EMAIL_PREFIX     = 'COMP_EMAIL_PREFIX'
SIMPLE_EMAIL_PREFIX   = 'SIMPLE_EMAIL_PREFIX'
PREFIX_LOGIN          = 'PREFIX_LOGIN'
PREFIX_NAME           = 'PREFIX_NAME'
LOGIN_NAME            = 'LOGIN_NAME'
FULL_NAME             = 'FULL_NAME'
SWITCHED_NAME         = 'SWITCHED_NAME'
SIMPLE_NAME           = 'SIMPLE_NAME'
LOCATION              = 'LOCATION'
DOMAIN                = 'EMAIL_DOMAIN'
DOMAIN_MAIN_PART      = 'EMAIL_DOMAIN_MAIN_PART'
DOMAIN_NAME_MATCH     = 'DOMAIN_NAME_MATCH'
GITHUB_USERNAME_MATCH = 'GITHUB_USERNAME_MATCH'
COMMA_SUFFIX_MATCH    = 'COMMA_SUFFIX_MATCH'
ABBREV_FIRST_NAME     = 'ABBREV_FIRST_NAME'
ABBREV_LAST_NAME      = 'ABBREV_LAST_NAME'
ABBREV_MIDDLE_NAME    = 'ABBREV_MIDDLE_NAME'

THR_MIN = CONFIG.get('THR_MIN', 1)
THR_MAX = CONFIG.get('THR_MAX', 40)
THR_MIN_LENGTH = CONFIG.get('THR_MIN_LENGTH', 3)
MIN_NAME_PART_LENGTH = 2  # Minimum length of an individual name component (e.g. last name in abbreviation keys)

unmask = {}

## Set up argument parser
#Parser = argparse.ArgumentParser(description='Process a file from a given path')
#Parser.add_argument('file_path', type=str, help='Path to the file to process')
#
## Parse arguments
#Args = parser.parse_args()
#
## Access the file path
#File_path = args.file_path
#
## Now you can use the file path
#Print(f"Processing directory: {file_path}")

#dataPath = os.path.abspath('../data/')


def get_first_names():
    # data source: https://github.com/solvenium/names-dataset (based on Wiktionary Names Appendix)
    script_dir = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(script_dir, 'Female_given_names.txt'), 'r') as file:
        female_common_first_names = file.read().splitlines()

    with open(os.path.join(script_dir, 'Male_given_names.txt'), 'r') as file:
        male_common_first_names = file.read().splitlines()

    first_names = male_common_first_names + female_common_first_names
    return first_names


# Default blacklist values (used if not specified in config)
default_blacklist_git_hosting = ["git", "github", "bitbucket", "gitlab", "gitbucket", "gitea", "gogs", "gitolite", "gitweb",
                         "gitolite3", "gitolite-admin", "gitolite-repositories", "jira", "bugzilla", "launchpad", "sourceforge", "codeberg", "sr.ht", "pagure", "repo", "repos"] # generated by GitHub Copilot

default_blacklist_email_providers = ["gmail", "yahoo", "hotmail", "outlook", "live", "icloud", "mailinator", "trashmail", "trash-mail"] # generated by GitHub Copilot

default_blacklist_generic_terms = ["admin", "administrator", "root", "support", "info", "contact", "no-reply", "noreply", "webmaster", "mail", "email", "post", "linux", "code", "system admin", "system administrator", "domain",
                           "sysadmin", "team", "staff", "office", "office admin", "office administrator", "bug", "bugs", "error", "errors", # generated by GitHub Copilot
                           "developer", "development", "engineering", "engineer", "it", "it admin", "it administrator", "it support", "tech", "technical", "tech support", "technical support", "dev", # generated by GitHub Copilot
                           "help", "service", "sales", "marketing", "security", "abuse", "devnull", "nullmailer", "mailer-daemon", "daemon", "hostmaster", "host", "ftp", "smtp", "pop", "imap", # generated by GitHub Copilot
                           "user", "test", "example", "demo", "guest", "anonymous", "unknown", "system", "service", "bot", "robot", "agent", # generated by GitHub Copilot
                           "development", "dev", "developer", "maint", "maintainer", "i18n", "spam", "bug", "bugs", "mailing", "list", "contact", "project", # taken from Goeminne and Mens, Science of Computer Programming, 2013
                           "hello", "hi", "hey", "greetings", "welcome", "howdy", "hola", "bonjour", "ciao", "salut", "namaste", # suggested by GitHub Copilot
                           "me", "my", "your", "we", "our", "us", "myself", "yourself", "world", "everyone", "crew", "folks", "people", # suggested by GitHub Copilot
                           "international", "universal", "general", "common", "public", "private", "personal", "business", "professional", "work", "home", "family", "friends", # suggested by GitHub Copilot
                           "colleague", "colleagues", "partner", "partners", "associate", "associates", "human", "users", "member", "members", "customer", "customers", "client", "clients", # suggested by GitHub Copilot
                           "send", "receive", "reply", "message", "messages", "write", "read", "subscribe", "unsubscribe", # suggested by GitHub Copilot
                           "new", "old", "current", "today", "now", "main", "primary", "secondary", "first", "second", "default", # suggested by GitHub Copilot
                           "good", "best", "great", "super", "amazing", "awesome", "nice", "cool", # suggested by GitHub Copilot
                           "testing", "sample", "temp", "temporary", "official", "unofficial", "real", "fake", "virtual", "digital", # suggested by GitHub Copilot
                           "online", "offline", "active", "inactive", "busy", "available", "away", "idle", "open", "closed", # suggested by GitHub Copilot
                           "start", "stop", "begin", "end", "finish", "complete", "incomplete", # suggested by GitHub Copilot
                           "alpha", "beta", "rsvp", "stable", "unstable", "draft", "final", "version", "release", "update", "upgrade", "downgrade", # suggested by GitHub Copilot
                           "install", "uninstall", "configure", "setup", "reset", "restart", "reboot", "shutdown", "poweroff", "sleep", "hibernate", "lock", "unlock", # suggested by GitHub Copilot
                           "package", "packages", "kernel", "webnews", "weboffice", "webmail", "webshield", "website", "devel", "announce", "supervision", "postmaster", # found on the Linux Kernel Mailing List
                           "patches", "patch", "owner", "news", "moderated", "moderator", "moderators", "mailinglist", "mailing-list", "mailing-lists", "mailinglists", # found on the Linux Kernel Mailing List
                           "mailman", "mailsweeper", "mails", "legal", "feedback", "experience", "backport", "backports", "autoreply", "autobot", "autoanswer", "autobuild", # found on the Linux Kernel Mailing List
                           "autopackage", "automated", "auto", "address", "addresses", "account", "accounts"] # found on the Linux Kernel Mailing List

default_blacklist_no_names = ["no name", "no-name", "noname", "none", "null", "n/a", "n.a.", "na", "n.a", "my name", "your name", "our name", "name", # generated by GitHub Copilot
                      "nobody", "no author"]

default_blacklist_privacy_emails = ["users.noreply.github.com"] # Privacy-preserving email domains that don't reveal real identity

default_blacklist_machine_names = ["think.pad", "user-laptop", "macbook-pro.local", "localhost.localdomain", "ubuntu.(none)", "example.com", "imac.local", "ubuntu.localdomain", "ubuntu.ubuntu-domain",
                           "notebook.(none)", "pivotal.io", "pivotal", "pc", "laptop", "desktop", "workstation", "work-station", "work station", "ubuntu@ip", "brun", "bs.local", # taken from Fry et al 2020
                           "ubuntu", "debian", "fedora", "centos", "redhat", "red-hat", "alpine", "arch", "kali", "mint", "manjaro", "opensuse", "suse", "gentoo", # OS and distro names
                           "slackware", "raspbian", "freebsd", "openbsd", "netbsd", "solaris", "windows", "macos", "darwin", "raspberry-pi", "raspberrypi"] # OS and distro names

# Get configurable blacklists from CONFIG, or use defaults
blacklist_git_hosting = CONFIG.get('BLACKLIST_GIT_HOSTING', default_blacklist_git_hosting)
blacklist_email_providers = CONFIG.get('BLACKLIST_EMAIL_PROVIDERS', default_blacklist_email_providers)
blacklist_generic_terms = CONFIG.get('BLACKLIST_GENERIC_TERMS', default_blacklist_generic_terms)
blacklist_no_names = CONFIG.get('BLACKLIST_NO_NAMES', default_blacklist_no_names)
blacklist_privacy_emails = CONFIG.get('BLACKLIST_PRIVACY_EMAILS', default_blacklist_privacy_emails)
blacklist_machine_names = CONFIG.get('BLACKLIST_MACHINE_NAMES', default_blacklist_machine_names)
blacklist_first_names = get_first_names()

def get_blacklist(include_first_names=True):
    """
    Returns a list of common first names and email-related terms to be used as a blacklist.
    This list is used to filter out common names and terms that are unlikely to be associated with real users.
    The blacklist includes common first names, email providers, and generic terms that are often used in email addresses.
    Args:
        include_first_names (bool): If True, includes common first names in the blacklist.
    Returns: a list of strings representing common first names and email-related terms.
    """

    blacklist = []

    blacklist = blacklist + blacklist_git_hosting + blacklist_email_providers + blacklist_generic_terms + blacklist_no_names + blacklist_privacy_emails + blacklist_machine_names

    if (include_first_names):
        blacklist = blacklist + blacklist_first_names

    # convert blacklist to lowercase
    blacklist = [x.lower() for x in blacklist]

    return blacklist


def contains_blacklisted_term(text, blacklist):
    """
    Check if a text (e.g., email prefix) contains any blacklisted term as a substring.

    This prevents merging people based on generic terms in their email addresses,
    like "bugzilla", "gnome", "ubuntu", etc.

    Args:
        text (str): The text to check (typically an email prefix)
        blacklist (list): List of blacklisted terms

    Returns:
        bool: True if text contains any blacklisted term, False otherwise
    """
    if not text:
        return False

    text_lower = text.lower()

    # Check if text exactly matches a blacklisted term
    if text_lower in blacklist:
        return True

    # Check if text contains any blacklisted term as a substring
    # Only check for terms that are 4+ characters to avoid false positives
    # (e.g., we don't want to block "john" in "johnson")
    for term in blacklist:
        if len(term) >= 4 and term in text_lower:
            return True

    return False


def do_merging(data, file_path, config_path=None):
    """
    Merges aliases based on various heuristics and writes the results to CSV files.

    Args:
    data (DataFrame): The input data containing user information.
    file_path (str): The path where the output CSV files will be saved.
    config_path (str): Path to the config file (default: 'merge_config.txt' in script directory).

    Returns: A dictionary mapping user IDs to their representative user information.
    """

    global CONFIG, THR_MIN, THR_MAX, THR_MIN_LENGTH
    global blacklist_git_hosting, blacklist_email_providers, blacklist_generic_terms
    global blacklist_no_names, blacklist_privacy_emails, blacklist_machine_names

    CONFIG = load_config(config_path or DEFAULT_CONFIG)
    THR_MIN = CONFIG.get('THR_MIN', 1)
    THR_MAX = CONFIG.get('THR_MAX', 40)
    THR_MIN_LENGTH = CONFIG.get('THR_MIN_LENGTH', 3)
    blacklist_git_hosting = CONFIG.get('BLACKLIST_GIT_HOSTING', default_blacklist_git_hosting)
    blacklist_email_providers = CONFIG.get('BLACKLIST_EMAIL_PROVIDERS', default_blacklist_email_providers)
    blacklist_generic_terms = CONFIG.get('BLACKLIST_GENERIC_TERMS', default_blacklist_generic_terms)
    blacklist_no_names = CONFIG.get('BLACKLIST_NO_NAMES', default_blacklist_no_names)
    blacklist_privacy_emails = CONFIG.get('BLACKLIST_PRIVACY_EMAILS', default_blacklist_privacy_emails)
    blacklist_machine_names = CONFIG.get('BLACKLIST_MACHINE_NAMES', default_blacklist_machine_names)

    # Clear global unmask dictionary to prevent memory leak across multiple calls
    global unmask
    unmask = {}

    dataPath = os.path.abspath(file_path)

    idx = 0
    step = 100000
    curidx = step

    aliases = {}

    uid_name_mail = data.to_csv(index=False)
    uid_name_mail = io.StringIO(uid_name_mail)
    reader = DictReader(uid_name_mail)

    # Create two blacklists:
    # 1. Full blacklist (with first names) - for name-based checks
    blacklist = get_blacklist(include_first_names=CONFIG.get('BLACKLIST_INCLUDE_FIRST_NAMES', True))

    # 2. Blacklist without first names - for email prefix checks
    # This prevents blocking legitimate personal email prefixes
    # while still blocking generic system and organizational terms
    blacklist_no_first_names = get_blacklist(include_first_names=False)

    # Helper structures
    d_email_uid = {}
    d_uid_email = {}

    d_prefix_uid = {}
    d_uid_prefix = {}

    d_comp_prefix_uid = {}
    d_uid_comp_prefix = {}

    d_uid_domain = {}
    d_domain_uid = {}

    d_uid_domain_main_part = {}
    d_domain_main_part_uid = {}

    d_name_uid = {}
    d_uid_name = {}

    d_login_uid = {}
    d_uid_login = {}

    d_location_uid = {}
    d_uid_location = {}

    d_uid_type = {}

    uid = 0

    raw = {}

    # Cache for normalized forms to avoid redundant computation
    normalization_cache = {}

    def get_normalized_forms(text, email_domain, location):
        """Wrapper for normalize_for_comparison_dual with caching."""
        key = (text, email_domain, location)
        if key not in normalization_cache:
            normalization_cache[key] = normalize_for_comparison_dual(text, email_domain, location)
        return normalization_cache[key]

    for row in reader:
        # line = row[0].decode('utf-8').strip()
        uid = int(row['uid'])
        row["is_weird_id"] = (False, False, False, False, False, False, False, False, False, False, False, False)
        raw[uid] = json.dumps(row)
        login = row['login'] if 'login' in row.keys() else None
        user_type = row['user_type'] if 'user_type' in row.keys() else None
        location = row['location'] if 'location' in row.keys() else None
        is_weird_id = row['is_weird_id'] if 'is_weird_id' in row.keys() else True
        try:
            name = row['name']
            email = row['email']
        except:
            print(row)
            exit()

        a = Alias(uid, login, name, email, location, user_type, None, None, None, is_weird_id)
        #if not is_alias_weird_id(a):
        is_weird = is_alias_weird_id(a)
        a.set_weird(is_weird)
        row['is_weird_id'] = is_weird
        raw[uid] = json.dumps(row)

        unmask[raw[uid]] = raw[uid]

        aliases[uid] = a
        if DEBUG:
            print(row)
            print(row['email'])
            print(a.uid, a.login, a.name, a.email, a.location)


        # - email
        d_uid_email[a.uid] = a.email
        if a.email is not None:
            d_email_uid.setdefault(a.email, set([a.uid]))
            d_email_uid[a.email].add(a.uid)

        # - prefix
        d_uid_prefix[a.uid] = a.email_prefix
        d_uid_comp_prefix[a.uid] = a.email_prefix
        if a.email_prefix is not None and len(a.email_prefix) >= 4:

            # Store normalized version (without punctuation) for broader matching
            normalized_prefixes = get_normalized_forms(a.email_prefix, a.email_domain, a.location)
            for normalized_prefix in normalized_prefixes:
                if normalized_prefix:
                    d_prefix_uid.setdefault(normalized_prefix, set([a.uid]))
                    d_prefix_uid[normalized_prefix].add(a.uid)
                    d_comp_prefix_uid.setdefault(normalized_prefix, set([a.uid]))
                    d_comp_prefix_uid[normalized_prefix].add(a.uid)

            if len(a.email_prefix.split('.')) > 1 or len(a.email_prefix.split('_')) > 1 or len(a.email_prefix.split('-')) > 1 or len(a.email_prefix.split('+')) > 1:
                # Handle compound prefixes with spaces
                comp_prefix_spaces = regex.sub(r'[.\-_\+]', ' ', a.email_prefix.lower()).strip() if a.email_prefix.lower()[0].isdigit() else regex.sub(r'[.\-_\d\+]', ' ', a.email_prefix.lower()).strip()
                d_comp_prefix_uid.setdefault(comp_prefix_spaces, set([a.uid]))
                d_comp_prefix_uid[comp_prefix_spaces].add(a.uid)

                # Handle compound prefixes without spaces
                simple_prefix = regex.sub(r'[.\-_\+]', '', a.email_prefix.lower()).strip() if a.email_prefix.lower()[0].isdigit() else regex.sub(r'[.\-_\d\+]', '', a.email_prefix.lower()).strip()
                d_prefix_uid.setdefault(simple_prefix, set([a.uid]))
                d_prefix_uid[simple_prefix].add(a.uid)

                # Handle reversed order for compound prefixes
                parts = regex.split(r'[ .\-_\+]', a.email_prefix) if a.email_prefix[0].isdigit() else regex.split(r'[ .\-_\d\+]', a.email_prefix)
                if len(parts) >= 2:
                    email_prefix = f"{parts[1].strip()} {parts[0].strip()}"
                    email_prefix2 = f"{parts[1].strip()}{parts[0].strip()}"

                    # Store the reversed versions with normalization
                    comp_rev_norms = get_normalized_forms(email_prefix, a.email_domain, a.location)
                    simple_rev_norms = get_normalized_forms(email_prefix2, a.email_domain, a.location)

                    for comp_rev_norm in comp_rev_norms:
                        if comp_rev_norm:
                            d_comp_prefix_uid.setdefault(comp_rev_norm, set([a.uid]))
                            d_comp_prefix_uid[comp_rev_norm].add(a.uid)

                    for simple_rev_norm in simple_rev_norms:
                        if simple_rev_norm:
                            d_prefix_uid.setdefault(simple_rev_norm, set([a.uid]))
                            d_prefix_uid[simple_rev_norm].add(a.uid)

            else:
                # For simple prefixes, store with space replacement
                simple_with_spaces = regex.sub(r'[.\-_\+]', ' ', a.email_prefix.lower()).strip() if a.email_prefix.lower()[0].isdigit() else regex.sub(r'[.\-_\d\+]', ' ', a.email_prefix.lower()).strip()
                d_prefix_uid.setdefault(simple_with_spaces, set([a.uid]))
                d_prefix_uid[simple_with_spaces].add(a.uid)


        # - domain
        d_uid_domain[a.uid] = a.email_domain
        if a.email_domain is not None:
            d_domain_uid.setdefault(a.email_domain.lower(), set([a.uid]))
            d_domain_uid[a.email_domain.lower()].add(a.uid)

        # - domain main part
        # get second last part of domain (i.e., split domain by dot and take the second last part)
        if a.email_domain is not None and len(a.email_domain.split('.')) > 1:
            d_uid_domain_main_part[a.uid] = a.email_domain.split('.')[-2]
            if a.email_domain is not None:
                d_domain_main_part_uid.setdefault(a.email_domain.split('.')[-2].lower(), set([a.uid]))
                d_domain_main_part_uid[a.email_domain.split('.')[-2].lower()].add(a.uid)

        # - login
        d_uid_login[a.uid] = a.login
        if a.login is not None:
            # Store both original and normalized versions for comparison
            d_login_uid.setdefault(a.login.lower(), set([a.uid]))
            d_login_uid[a.login.lower()].add(a.uid)

            # Store normalized version (without punctuation) for broader matching
            normalized_logins = get_normalized_forms(a.login, a.email_domain, a.location)
            for normalized_login in normalized_logins:
                if normalized_login and normalized_login != a.login.lower():
                    d_login_uid.setdefault(normalized_login, set([a.uid]))
                    d_login_uid[normalized_login].add(a.uid)

        # type
        d_uid_type[a.uid] = a.usr_type

        # - name
        d_uid_name[a.uid] = a.name
        if a.name is not None and len(a.name):
            # Store both original and normalized versions for comparison
            d_name_uid.setdefault(a.name.lower(), set([a.uid]))
            d_name_uid[a.name.lower()].add(a.uid)

            # Store normalized versions (without punctuation) for broader matching
            normalized_names = get_normalized_forms(a.name, a.email_domain, a.location)
            for normalized_name in normalized_names:
                if normalized_name and normalized_name != a.name.lower():
                    d_name_uid.setdefault(normalized_name, set([a.uid]))
                    d_name_uid[normalized_name].add(a.uid)

            if len(a.name.split(' ')) > 1:
                # Remove spaces and other characters for compound name matching
                compound_name = regex.sub(r'[ .\-_\+]', '', a.name.lower()).strip() if a.name.lower()[0].isdigit() else regex.sub(r'[ .\-_\d\+]', '', a.name.lower()).strip()
                d_name_uid.setdefault(compound_name, set([a.uid]))
                d_name_uid[compound_name].add(a.uid)

                # Also store dual-normalized compound forms (for German umlaut variations)
                compound_normalized_forms = get_normalized_forms(compound_name, a.email_domain, a.location)
                for compound_norm in compound_normalized_forms:
                    if compound_norm and compound_norm != compound_name:
                        d_name_uid.setdefault(compound_norm, set([a.uid]))
                        d_name_uid[compound_norm].add(a.uid)
            else:
                # For single names, also remove punctuation and digits
                clean_name = regex.sub(r'[.\-_\+]', '', a.name.lower()).strip() if a.name.lower()[0].isdigit() else regex.sub(r'[.\-_\d\+]', '', a.name.lower()).strip()
                d_name_uid.setdefault(clean_name, set([a.uid]))
                d_name_uid[clean_name].add(a.uid)

        # - location
        d_uid_location[a.uid] = a.location
        if a.location is not None and len(a.location):
            d_location_uid.setdefault(a.location, set([a.uid]))
            d_location_uid[a.location].add(a.uid)

        idx += 1
        if idx >= curidx:
            curidx += step

    if DEBUG:
        print(d_prefix_uid)
        print(d_name_uid)

    # Generate abbreviation patterns for names with 3+ parts
    # This must happen BEFORE clue creation so the patterns are available for matching
    for uid, alias in aliases.items():
        if alias.name:
            name_parts = alias.name.strip().split()
            if len(name_parts) >= 3:
                first_part = name_parts[0]
                last_part = name_parts[-1]
                middle_parts = name_parts[1:-1]

                # Normalize first name
                first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)

                # Generate pattern: first_name + ALL other name initials
                if len(middle_parts) >= 1:
                    # Build all initials from middle names AND last name
                    all_remaining_parts = middle_parts + [last_part]  # All parts except first

                    # Collect all possible initial combinations from dual normalization
                    all_initials_sets = []
                    for part in all_remaining_parts:
                        part_forms = get_normalized_forms(part, alias.email_domain, alias.location)
                        initials = {form[0] for form in part_forms if form}
                        if initials:
                            all_initials_sets.append(initials)

                    # Generate all combinations of first name forms and initial combinations
                    if all_initials_sets:
                        # For simplicity, just join the first initial from each set (they rarely differ)
                        all_remaining_initials = ''.join([sorted(initials)[0] for initials in all_initials_sets])

                        for first_name in first_name_forms:
                            if first_name and all_remaining_initials:
                                # Generate pattern: first_name + all_remaining_initials
                                # Store this pattern so it can match single-word names
                                firstname_all_initials = first_name + all_remaining_initials
                                d_name_uid.setdefault(firstname_all_initials, set()).add(uid)

    # Pre-compute normalized email prefixes
    prefix_normalized_cache = {}
    for uid, alias in aliases.items():
        if alias.email_prefix:
            prefix_lower = alias.email_prefix.lower()
            if prefix_lower[0].isdigit():
                normalized = regex.sub(r'[.\-_\+]', ' ', prefix_lower).strip()
            else:
                normalized = regex.sub(r'[.\-_\d\+]', ' ', prefix_lower).strip()
            prefix_normalized_cache[uid] = normalized

    # If configured, filter out privacy email aliases from all lookups
    # This prevents them from being merged with anything
    if CONFIG.get('EXCLUDE_PRIVACY_EMAILS_FROM_MERGING', False):
        privacy_email_uids = set()
        for uid, alias in aliases.items():
            if hasattr(alias, 'is_weird_id') and isinstance(alias.is_weird_id, (tuple, list)):
                # Index [6] = name_is_weird_id_privacy_email
                # Index [11] = email_is_weird_id_privacy_email
                if len(alias.is_weird_id) > 11 and (alias.is_weird_id[6] or alias.is_weird_id[11]):
                    privacy_email_uids.add(uid)

        # Remove privacy email UIDs from all lookup dictionaries
        for d in [d_email_uid, d_prefix_uid, d_comp_prefix_uid, d_login_uid, d_name_uid,
                  d_domain_uid, d_domain_main_part_uid, d_location_uid]:
            for key in list(d.keys()):
                if isinstance(d[key], set):
                    d[key] = d[key] - privacy_email_uids
                    # Remove empty sets
                    if not d[key]:
                        del d[key]

    clues = {}

    if CONFIG.get('EMAIL', True):
        for email, set_uid in d_email_uid.items():
            if len(set_uid) > THR_MIN and not email in blacklist:
                for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                    clues.setdefault((a, b), [])
                    clues[(a, b)].append(EMAIL)

    if CONFIG.get('COMP_EMAIL_PREFIX', True):
        for prefix, set_uid in d_comp_prefix_uid.items():
            if len(set_uid) > THR_MIN and len(set_uid) < THR_MAX and not (prefix in blacklist or contains_blacklisted_term(prefix, blacklist_no_first_names)):
                if len(prefix) >= THR_MIN_LENGTH:
                    for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(COMP_EMAIL_PREFIX)

    if CONFIG.get('SIMPLE_EMAIL_PREFIX', True):
        for prefix, set_uid in d_prefix_uid.items():
            if len(set_uid) > THR_MIN and len(set_uid) < THR_MAX and not (prefix in blacklist or contains_blacklisted_term(prefix, blacklist_no_first_names)):
                if len(prefix) >= THR_MIN_LENGTH:
                    for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(SIMPLE_EMAIL_PREFIX)

    if CONFIG.get('PREFIX_LOGIN', True):
        for prefix in set(d_prefix_uid.keys()).intersection(set(d_login_uid.keys())):
            if len(prefix) >= THR_MIN_LENGTH and len(d_prefix_uid[prefix]) < THR_MAX and not (prefix in blacklist or contains_blacklisted_term(prefix, blacklist_no_first_names)):
                for a,b in product(sorted(d_login_uid[prefix], key=lambda uid:int(uid)), sorted(d_prefix_uid[prefix], key=lambda uid:int(uid))):
                    if a < b:
                        clues.setdefault((a, b), [])
                        if not SIMPLE_EMAIL_PREFIX in clues[(a, b)]:
                            clues[(a, b)].append(PREFIX_LOGIN)

    if CONFIG.get('PREFIX_NAME', True):
        for prefix in set(d_prefix_uid.keys()).intersection(set(d_name_uid.keys())):
            if len(prefix) >= THR_MIN_LENGTH and len(d_prefix_uid[prefix]) < THR_MAX and len(d_name_uid[prefix]) < THR_MAX and not (prefix in blacklist or contains_blacklisted_term(prefix, blacklist_no_first_names)):
                for a,b in product(sorted(d_name_uid[prefix], key=lambda uid:int(uid)), sorted(d_prefix_uid[prefix], key=lambda uid:int(uid))):
                    if a < b:
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(PREFIX_NAME)
                    elif a > b:
                        clues.setdefault((b, a), [])
                        clues[(b, a)].append(PREFIX_NAME)


    if CONFIG.get('PREFIX_NAME', True):
        for comp_prefix in set(d_comp_prefix_uid.keys()).intersection(set(d_name_uid.keys())):
            if len(comp_prefix) >= THR_MIN_LENGTH and len(d_comp_prefix_uid[comp_prefix]) < THR_MAX and len(d_name_uid[comp_prefix]) < THR_MAX and not (comp_prefix in blacklist or contains_blacklisted_term(comp_prefix, blacklist_no_first_names)):
                for a,b in product(sorted(d_name_uid[comp_prefix], key=lambda uid:int(uid)), sorted(d_comp_prefix_uid[comp_prefix], key=lambda uid:int(uid))):
                    if a < b:
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(PREFIX_NAME)
                    elif a > b:
                        clues.setdefault((b, a), [])
                        clues[(b, a)].append(PREFIX_NAME)

    if CONFIG.get('LOGIN_NAME', True):
        for prefix in set(d_login_uid.keys()).intersection(set(d_name_uid.keys())):
            if len(d_name_uid[prefix]) < THR_MAX and not prefix in blacklist:
                for a,b in product(sorted(d_name_uid[prefix], key=lambda uid:int(uid)), sorted(d_login_uid[prefix], key=lambda uid:int(uid))):
                    if a < b:
                        clues.setdefault((a, b), [])
                        if not SIMPLE_EMAIL_PREFIX in clues[(a, b)]:
                            clues[(a, b)].append(LOGIN_NAME)

    if CONFIG.get('FULL_NAME', True):
        for name, set_uid in d_name_uid.items():
            if len(set_uid) > THR_MIN and len(set_uid) < THR_MAX and len(name) >= THR_MIN_LENGTH:
                name_parts = name.split(' ')
                if len(name_parts) > 1 or name not in blacklist:
                    for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(FULL_NAME)
                #else:
                #    for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                #        clues.setdefault((a, b), [])
                #        clues[(a, b)].append(SIMPLE_NAME)

    if CONFIG.get('DOMAIN', True):
        for domain, set_uid in d_domain_uid.items():
            if len(set_uid) > THR_MIN and len(set_uid) < THR_MAX and not domain in blacklist:
                for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                    clues.setdefault((a, b), [])
                    clues[(a, b)].append(DOMAIN)

    if CONFIG.get('DOMAIN_MAIN_PART', True):
        for domain_main_part, set_uid in d_domain_main_part_uid.items():
            if len(set_uid) > THR_MIN and len(set_uid) < THR_MAX and not domain_main_part in blacklist:
                for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                    a_normalized_prefix = prefix_normalized_cache.get(a, '')
                    b_normalized_prefix = prefix_normalized_cache.get(b, '')
                    if len(a_normalized_prefix) > 0 and len(b_normalized_prefix) > 0 and a_normalized_prefix == b_normalized_prefix:
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(DOMAIN_MAIN_PART)

    if CONFIG.get('DOMAIN_NAME_MATCH', True):
        # Check if domain (excluding TLD) matches normalized name across different aliases
        # First, collect domain main parts and normalized names for all aliases
        domain_to_uid = {}  # domain_main -> set of uids
        name_to_uid = {}    # normalized_name_no_spaces -> set of uids

        # New: Track domain/prefix/name parts for more targeted matching
        # Format: (domain_main, email_prefix) -> uid
        domain_prefix_to_uid = {}  # Track both domain and prefix together
        # Format: (first_name, last_name) -> uid (for email pattern matching)
        first_last_to_uid = {}  # Track first and last names separately
        # Format: (first_name, last_name) -> set of uids (for matching any name with email pattern)
        name_parts_to_uid = {}  # Track all aliases with 2+ part names

        for uid, alias in aliases.items():
            if alias.email_domain and alias.name:
                # Extract domain main part (excluding TLD)
                domain_parts = alias.email_domain.split('.')
                if len(domain_parts) >= 2:
                    domain_main = domain_parts[-2].lower()  # Get second-to-last part (main domain)
                    domain_to_uid.setdefault(domain_main, set()).add(uid)

                    # Normalize the name for comparison (use dual normalization for German umlauts)
                    normalized_names = get_normalized_forms(alias.name, alias.email_domain, alias.location)

                    # Remove spaces from normalized names for domain comparison
                    for normalized_name in normalized_names:
                        normalized_name_no_spaces = normalized_name.replace(' ', '') if normalized_name else None
                        if normalized_name_no_spaces:
                            name_to_uid.setdefault(normalized_name_no_spaces, set()).add(uid)

                        # Store first and last names separately for targeted domain/prefix matching
                        if normalized_name:
                            name_parts = normalized_name.split()
                            if len(name_parts) >= 2:
                                first_name = name_parts[0]
                                last_name = name_parts[-1]

                                # Store name parts for matching with email patterns
                                name_key = (first_name, last_name)
                                name_parts_to_uid.setdefault(name_key, set()).add(uid)

                                # Store email prefix patterns if available
                                if alias.email_prefix:
                                    # Normalize email prefix
                                    prefix_normalized_forms = get_normalized_forms(alias.email_prefix, alias.email_domain, alias.location)
                                    for prefix_normalized in prefix_normalized_forms:
                                        if prefix_normalized:
                                            # Store mapping for this specific alias
                                            key = (domain_main, prefix_normalized, first_name, last_name)
                                            first_last_to_uid[key] = uid

        # Cross-compare: find matches where one alias's normalized name matches another's domain main part
        for normalized_name_no_spaces, name_uids in name_to_uid.items():
            if normalized_name_no_spaces in domain_to_uid and not normalized_name_no_spaces in blacklist:
                domain_uids = domain_to_uid[normalized_name_no_spaces]
                # Create clues between aliases with matching name/domain
                if len(name_uids) + len(domain_uids) > THR_MIN and len(name_uids) + len(domain_uids) < THR_MAX:
                    for name_uid in name_uids:
                        for domain_uid in domain_uids:
                            if name_uid != domain_uid:  # Don't match alias with itself
                                a, b = sorted([name_uid, domain_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(DOMAIN_NAME_MATCH)

        # Targeted domain/prefix matching to handle cases like "firstname@lastname.email"
        # matching "Firstname Lastname" without causing false positives
        # Require BOTH domain and prefix to match name parts (in either direction)
        for (domain_main, prefix_normalized, first_name, last_name), uid1 in first_last_to_uid.items():
            # Case 1: domain matches last_name AND prefix matches first_name
            lookup_key1 = (last_name, first_name, domain_main, prefix_normalized)
            if lookup_key1 in first_last_to_uid:
                uid2 = first_last_to_uid[lookup_key1]
                if uid1 != uid2:
                    a, b = sorted([uid1, uid2])
                    clues.setdefault((a, b), [])
                    clues[(a, b)].append(DOMAIN_NAME_MATCH)

            # Case 2: domain matches first_name AND prefix matches last_name
            lookup_key2 = (first_name, last_name, domain_main, prefix_normalized)
            if lookup_key2 in first_last_to_uid:
                uid2 = first_last_to_uid[lookup_key2]
                if uid1 != uid2:
                    a, b = sorted([uid1, uid2])
                    clues.setdefault((a, b), [])
                    clues[(a, b)].append(DOMAIN_NAME_MATCH)

        # Additional matching: email pattern "firstname@lastname.domain" with name "Firstname Lastname"
        for uid, alias in aliases.items():
            if alias.email_prefix and alias.email_domain:
                domain_parts = alias.email_domain.split('.')
                if len(domain_parts) >= 2:
                    domain_main_raw = domain_parts[-2]

                    # Normalize both email prefix and domain main part
                    prefix_normalized_forms = get_normalized_forms(alias.email_prefix, alias.email_domain, alias.location)
                    domain_normalized_forms = get_normalized_forms(domain_main_raw, alias.email_domain, alias.location)

                    for prefix_normalized in prefix_normalized_forms:
                        if prefix_normalized:
                            for domain_normalized in domain_normalized_forms:
                                if domain_normalized:
                                    # Check if (prefix, domain_main) matches any (first_name, last_name)
                                    lookup_key = (prefix_normalized, domain_normalized)
                                    if lookup_key in name_parts_to_uid:
                                        for matched_uid in name_parts_to_uid[lookup_key]:
                                            if uid != matched_uid:
                                                a, b = sorted([uid, matched_uid])
                                                clues.setdefault((a, b), [])
                                                clues[(a, b)].append(DOMAIN_NAME_MATCH)

    if CONFIG.get('COMMA_SUFFIX_MATCH', True):
        # Comma suffix matching: Match names that are exactly the same (at least two components)
        # but one has a comma-separated suffix (e.g., "John Smith" and "John Smith, PhD")
        name_base_to_uid = {}  # normalized_name_without_suffix -> set of uids

        for uid, alias in aliases.items():
            if alias.name and len(alias.name.split()) >= 2:  # At least two components
                name_lower = alias.name.lower().strip()

                # Check if name has comma suffix
                if ',' in name_lower:
                    # Extract base name (everything before first comma)
                    base_name = name_lower.split(',')[0].strip()
                else:
                    base_name = name_lower

                # Only process if base name has at least 2 components
                if len(base_name.split()) >= 2:
                    name_base_to_uid.setdefault(base_name, set()).add(uid)

        # Find matches where base names are identical
        for base_name, uids in name_base_to_uid.items():
            if len(uids) > THR_MIN and len(uids) < THR_MAX and base_name not in blacklist:
                for a, b in combinations(sorted(uids), 2):
                    alias_a = aliases[a]
                    alias_b = aliases[b]

                    name_a = alias_a.name.lower().strip()
                    name_b = alias_b.name.lower().strip()

                    # Check if one has comma suffix and the other doesn't, or both have different suffixes
                    has_comma_a = ',' in name_a
                    has_comma_b = ',' in name_b

                    if has_comma_a != has_comma_b or (has_comma_a and has_comma_b and name_a != name_b):
                        clues.setdefault((a, b), [])
                        clues[(a, b)].append(COMMA_SUFFIX_MATCH)

    if CONFIG.get('GITHUB_USERNAME_MATCH', True):
        # GitHub username matching: Match emails like "12345678+username@users.noreply.github.com"
        # with names like "username <user@example.com>" based on the GitHub username
        github_username_to_uid = {}  # github_username -> set of uids
        name_to_uid_github = {}      # normalized_name -> set of uids

        for uid, alias in aliases.items():
            # Check for GitHub noreply email pattern
            if (alias.email and 'users.noreply.github.com' in alias.email.lower() and
                alias.email_prefix and '+' in alias.email_prefix):
                # Extract GitHub username from email like "12345678+username@users.noreply.github.com"
                github_username = alias.email_prefix.split('+')[1] if '+' in alias.email_prefix else None
                if github_username:
                    # Normalize the GitHub username
                    github_username_normalized_forms = get_normalized_forms(github_username, alias.email_domain, alias.location)
                    for github_username_normalized in github_username_normalized_forms:
                        if github_username_normalized:
                            github_username_to_uid.setdefault(github_username_normalized, set()).add(uid)

            # Also check if the name itself matches a potential GitHub username pattern
            if alias.name:
                # Normalize name for GitHub username comparison
                normalized_name_forms = get_normalized_forms(alias.name, alias.email_domain, alias.location)
                for normalized_name in normalized_name_forms:
                    normalized_name_no_spaces = normalized_name.replace(' ', '') if normalized_name else None
                    if normalized_name_no_spaces and len(normalized_name_no_spaces) >= THR_MIN_LENGTH:
                        name_to_uid_github.setdefault(normalized_name_no_spaces, set()).add(uid)

        # Cross-compare: find matches between GitHub usernames and names
        for github_username, github_uids in github_username_to_uid.items():
            if github_username in name_to_uid_github:
                name_uids = name_to_uid_github[github_username]
                # Create clues between aliases with matching GitHub username and name
                if len(github_uids) + len(name_uids) > THR_MIN and len(github_uids) + len(name_uids) < THR_MAX and github_username not in blacklist:
                    for github_uid in github_uids:
                        for name_uid in name_uids:
                            if github_uid != name_uid:  # Don't match alias with itself
                                a, b = sorted([github_uid, name_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append('GITHUB_USERNAME_MATCH')

    # Name abbreviation matching (name-to-name): Match names with abbreviated first, middle, or last names
    # Examples:
    # - "J. Smith" matches "John Smith" (abbreviated first name)
    # - "John S." matches "John Smith" (abbreviated last name)
    # - "John M. Smith" matches "John Michael Smith" (abbreviated middle name)

    # Helper functions for abbreviation matching
    def normalize_abbreviation(name_part):
        """Normalize a name part by removing dots and lowercasing."""
        return name_part.replace('.', '').strip().lower()

    def is_abbreviation_match(abbrev, full):
        """Check if abbrev is an abbreviation of full (case-insensitive)."""
        abbrev_norm = normalize_abbreviation(abbrev)
        full_norm = normalize_abbreviation(full)

        # Check if abbreviated part is just one character and matches first letter
        return len(abbrev_norm) == 1 and full_norm.startswith(abbrev_norm)

    # Build lookup dictionaries for name-to-name matching
    # Format: (first_initial, last_name) -> set of uids (for abbreviated first name)
    abbrev_first_to_uid = {}
    full_first_to_uid = {}

    # Format: (first_name, last_initial) -> set of uids (for abbreviated last name)
    abbrev_last_to_uid = {}
    full_last_to_uid = {}

    # Format: (first_name, middle_initial, last_name) -> set of uids (for abbreviated middle name)
    abbrev_middle_to_uid = {}
    full_middle_to_uid = {}

    for uid, alias in aliases.items():
        if alias.name:
            name_parts = alias.name.strip().split()

            # Handle single-word abbreviated names (e.g., "smithj" matching "James Smith" or "jsmith" matching "John Smith")
            if len(name_parts) == 1:
                # Normalize name
                name_normalized_forms = get_normalized_forms(alias.name, alias.email_domain, alias.location)
                for name_normalized in name_normalized_forms:
                    if name_normalized and len(name_normalized) >= THR_MIN_LENGTH:
                        # Store the normalized single-word name in d_name_uid for broader matching
                        # This allows it to match with email prefixes that have the same pattern
                        d_name_uid.setdefault(name_normalized, set()).add(uid)

                    # Pattern 1: Check if it matches pattern last_name + first_initial (e.g., "smithj")
                    # Try splitting at each position to find potential lastname+initial combinations
                    for i in range(1, len(name_normalized)):
                        lastname_part = name_normalized[:i]
                        first_initial_part = name_normalized[i:]

                        # Only consider if the remaining part is a single character (potential initial)
                        if len(first_initial_part) == 1 and len(lastname_part) >= MIN_NAME_PART_LENGTH:
                            key = (first_initial_part, lastname_part)
                            abbrev_first_to_uid.setdefault(key, set()).add(uid)

                    # Pattern 2: Check if it matches pattern first_initial + last_name (e.g., "jsmith")
                    # Try splitting at each position to find potential initial+lastname combinations
                    for i in range(1, len(name_normalized)):
                        first_initial_part = name_normalized[:i]
                        lastname_part = name_normalized[i:]

                        # Only consider if the first part is a single character (potential initial)
                        if len(first_initial_part) == 1 and len(lastname_part) >= MIN_NAME_PART_LENGTH:
                            key = (first_initial_part, lastname_part)
                            abbrev_first_to_uid.setdefault(key, set()).add(uid)

            # Only process names with at least 2 parts
            if len(name_parts) >= 2:
                # Handle first name abbreviation: "J. Smith" or "J Smith"
                first_part = name_parts[0]
                last_part = name_parts[-1]

                # Check if first name looks like an abbreviation (single char or char with dot)
                if len(normalize_abbreviation(first_part)) == 1:
                    # This is abbreviated first name
                    first_initial = normalize_abbreviation(first_part)
                    # Normalize the last name
                    last_name_forms = get_normalized_forms(last_part, alias.email_domain, alias.location)
                    for last_name in last_name_forms:
                        if last_name:
                            key = (first_initial, last_name)
                            abbrev_first_to_uid.setdefault(key, set()).add(uid)
                else:
                    # This could match an abbreviated first name
                    first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)
                    last_name_forms = get_normalized_forms(last_part, alias.email_domain, alias.location)

                    # Generate keys for all combinations of normalized forms.
                    # Require last_name to have ≥2 chars: a singly-abbreviated name like
                    # "Andreas S." has last_name='s', which would land in the same
                    # full_first_to_uid[('a','s')] bucket as "Antoine S.", letting any
                    # doubly-abbreviated "A. S. <>" bridge completely unrelated people.
                    for first_name in first_name_forms:
                        if first_name:
                            for last_name in last_name_forms:
                                if last_name and len(last_name) >= MIN_NAME_PART_LENGTH:
                                    first_initial = first_name[0]
                                    key = (first_initial, last_name)
                                    full_first_to_uid.setdefault(key, set()).add(uid)

                # Handle last name abbreviation: "John S." or "John S"
                if len(normalize_abbreviation(last_part)) == 1:
                    # This is abbreviated last name
                    # Normalize the first name
                    first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)
                    last_initial = normalize_abbreviation(last_part)
                    for first_name in first_name_forms:
                        if first_name:
                            key = (first_name, last_initial)
                            abbrev_last_to_uid.setdefault(key, set()).add(uid)
                else:
                    # This could match an abbreviated last name
                    first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)
                    last_name_forms = get_normalized_forms(last_part, alias.email_domain, alias.location)

                    # Generate keys for all combinations of normalized forms.
                    # Require first_name to have ≥2 chars: a singly-abbreviated name like
                    # "A. Stathopoulos" has first_name='a', which would land in the same
                    # full_last_to_uid[('a','s')] bucket as "A. Stevens", letting any
                    # doubly-abbreviated "A. S. <>" bridge completely unrelated people.
                    for first_name in first_name_forms:
                        if first_name and len(first_name) >= MIN_NAME_PART_LENGTH:
                            for last_name in last_name_forms:
                                if last_name:
                                    last_initial = last_name[0]
                                    key = (first_name, last_initial)
                                    full_last_to_uid.setdefault(key, set()).add(uid)

                # Handle middle name abbreviations for names with 3+ parts
                # Examples: "John M. Smith", "Juan C. García López", "María A. B. Fernández"
                if len(name_parts) >= 3:
                    # For names with multiple middle parts, check each middle part for abbreviations
                    middle_parts = name_parts[1:-1]  # All parts between first and last

                    # Pre-compute normalized forms once for all middle names
                    first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)
                    last_name_forms = get_normalized_forms(last_part, alias.email_domain, alias.location)

                    # Process each middle name position
                    for middle_idx, middle_part in enumerate(middle_parts):
                        if len(normalize_abbreviation(middle_part)) == 1:
                            # This middle name is abbreviated
                            middle_initial = normalize_abbreviation(middle_part)
                            for first_name in first_name_forms:
                                if first_name:
                                    for last_name in last_name_forms:
                                        if last_name:
                                            key = (first_name, middle_initial, last_name)
                                            abbrev_middle_to_uid.setdefault(key, set()).add(uid)
                        else:
                            # This could match an abbreviated middle name
                            middle_name_forms = get_normalized_forms(middle_part, alias.email_domain, alias.location)

                            # Generate keys for all combinations of normalized forms.
                            # Require first_name and last_name to have ≥2 chars for the same
                            # reason as full_first_to_uid and full_last_to_uid: a 1-char
                            # component makes the key too generic and bridges unrelated people.
                            for first_name in first_name_forms:
                                if first_name and len(first_name) >= MIN_NAME_PART_LENGTH:
                                    for middle_name in middle_name_forms:
                                        if middle_name:
                                            for last_name in last_name_forms:
                                                if last_name and len(last_name) >= MIN_NAME_PART_LENGTH:
                                                    middle_initial = middle_name[0]
                                                    key = (first_name, middle_initial, last_name)
                                                    full_middle_to_uid.setdefault(key, set()).add(uid)

    # Match abbreviated first names with full first names
    if CONFIG.get('ABBREV_FIRST_NAME', True):
        for key, abbrev_uids in abbrev_first_to_uid.items():
            if key in full_first_to_uid:
                full_uids = full_first_to_uid[key]
                first_initial, last_name = key
                # Check if the non-abbreviated part (last_name) is a common first name
                # If so, skip matching to avoid false positives like "michaelh" matching both
                # "Michael Hilton" and "Michael Huber" (where "michael" is the common first name)
                # But allow matches like "doej" with "John Doe" (where "doe" is not common)
                skip_all = False
                if last_name and last_name.lower() in [n.lower() for n in blacklist_first_names]:
                    skip_all = True

                if not skip_all and len(abbrev_uids) + len(full_uids) > THR_MIN and len(abbrev_uids) + len(full_uids) < THR_MAX:
                    for abbrev_uid in abbrev_uids:
                        for full_uid in full_uids:
                            if abbrev_uid != full_uid:
                                a, b = sorted([abbrev_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_FIRST_NAME)

    # Match abbreviated last names with full last names
    if CONFIG.get('ABBREV_LAST_NAME', True):
        for key, abbrev_uids in abbrev_last_to_uid.items():
            if key in full_last_to_uid:
                full_uids = full_last_to_uid[key]
                first_name, last_initial = key
                # Check if the non-abbreviated part (first_name) is a common first name
                # If so, skip matching to avoid false positives like "Michael H" matching both
                # "Michael Hilton" and "Michael Huber" (where "michael" is the common first name)
                # But allow matches like "Doe J" with "Doe John" (where "doe" is not common)
                skip_all = False
                if first_name and first_name.lower() in [n.lower() for n in blacklist_first_names]:
                    skip_all = True

                if not skip_all and len(abbrev_uids) + len(full_uids) > THR_MIN and len(abbrev_uids) + len(full_uids) < THR_MAX:
                    for abbrev_uid in abbrev_uids:
                        for full_uid in full_uids:
                            if abbrev_uid != full_uid:
                                a, b = sorted([abbrev_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_LAST_NAME)

    # Match abbreviated middle names with full middle names
    if CONFIG.get('ABBREV_MIDDLE_NAME', True):
        for key, abbrev_uids in abbrev_middle_to_uid.items():
            if key in full_middle_to_uid:
                full_uids = full_middle_to_uid[key]
                if len(abbrev_uids) + len(full_uids) > THR_MIN and len(abbrev_uids) + len(full_uids) < THR_MAX:
                    for abbrev_uid in abbrev_uids:
                        for full_uid in full_uids:
                            if abbrev_uid != full_uid:
                                a, b = sorted([abbrev_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_MIDDLE_NAME)

    # Email prefix abbreviation matching: Match email prefixes with abbreviated patterns
    # Examples:
    # - "jsmith@example.com" matches "John Smith" (first_initial + last_name)
    # - "smithj@example.com" matches "John Smith" (last_name + first_initial)
    # - "johns@example.com" matches "John Smith" (first_name + last_initial)
    # - "jmsmith@example.com" matches "John Michael Smith" (first_initial + middle_initial + last_name)
    # - "smithjm@example.com" matches "John Michael Smith" (last_name + first_initial + middle_initial)

    # Format: (first_initial, last_name) -> set of uids (for email prefix matching like "jsmith")
    email_prefix_abbrev_first_to_uid = {}
    # Format: (first_name, last_initial) -> set of uids (for email prefix matching like "johns")
    email_prefix_abbrev_last_to_uid = {}
    # Format: (first_name, middle_initial, last_name) -> set of uids (for email prefix matching like "jmsmith")
    email_prefix_abbrev_middle_to_uid = {}

    for uid, alias in aliases.items():
        # Check email prefix for abbreviation patterns like "jsmith" or "johns"
        if alias.email_prefix and alias.name:
            name_parts = alias.name.strip().split()
            if len(name_parts) >= 2:
                first_part = name_parts[0]
                last_part = name_parts[-1]

                # Normalize name parts and email prefix
                first_name_forms = get_normalized_forms(first_part, alias.email_domain, alias.location)
                last_name_forms = get_normalized_forms(last_part, alias.email_domain, alias.location)
                email_prefix_norm_forms = get_normalized_forms(alias.email_prefix, alias.email_domain, alias.location)

                # Generate patterns for all combinations of normalized forms
                for first_name in first_name_forms:
                    if first_name:
                        for last_name in last_name_forms:
                            if last_name:
                                for email_prefix_norm in email_prefix_norm_forms:
                                    if email_prefix_norm and len(email_prefix_norm) >= THR_MIN_LENGTH:
                                        # Check if email prefix matches pattern: first_initial + last_name (e.g., "jsmith")
                                        first_initial_lastname = first_name[0] + last_name
                                        if email_prefix_norm == first_initial_lastname:
                                            # Store this email prefix pattern
                                            key = (first_name[0], last_name)
                                            email_prefix_abbrev_first_to_uid.setdefault(key, set()).add(uid)

                                        # Check if email prefix matches pattern: first_name + last_initial (e.g., "johns")
                                        firstname_last_initial = first_name + last_name[0]
                                        if email_prefix_norm == firstname_last_initial:
                                            # Store this email prefix pattern
                                            key = (first_name, last_name[0])
                                            email_prefix_abbrev_last_to_uid.setdefault(key, set()).add(uid)

                                        # Check if email prefix matches pattern: last_name + first_initial (e.g., "smithj")
                                        lastname_first_initial = last_name + first_name[0]
                                        if email_prefix_norm == lastname_first_initial:
                                            # Store this email prefix pattern (using same key format as jsmith)
                                            key = (first_name[0], last_name)
                                            email_prefix_abbrev_first_to_uid.setdefault(key, set()).add(uid)

                # Handle middle name abbreviations in email prefix for names with 3+ parts
                if len(name_parts) >= 3:
                    middle_parts = name_parts[1:-1]  # All parts between first and last

                    # Process each middle name position individually
                    for middle_idx, middle_part in enumerate(middle_parts):
                        # Normalize middle name
                        middle_name_forms = get_normalized_forms(middle_part, alias.email_domain, alias.location)

                        # Generate patterns for all combinations of normalized forms
                        for first_name in first_name_forms:
                            if first_name:
                                for middle_name in middle_name_forms:
                                    if middle_name:
                                        for last_name in last_name_forms:
                                            if last_name:
                                                for email_prefix_norm in email_prefix_norm_forms:
                                                    if email_prefix_norm and len(email_prefix_norm) >= 4:
                                                        # Check if email prefix matches pattern: first_initial + middle_initial + last_name (e.g., "jmsmith")
                                                        first_middle_initial_lastname = first_name[0] + middle_name[0] + last_name
                                                        if email_prefix_norm == first_middle_initial_lastname:
                                                            # Store this email prefix pattern
                                                            key = (first_name[0], middle_name[0], last_name)
                                                            email_prefix_abbrev_middle_to_uid.setdefault(key, set()).add(uid)

                                                        # Check if email prefix matches pattern: last_name + first_initial + middle_initial (e.g., "smithjm")
                                                        lastname_first_middle_initial = last_name + first_name[0] + middle_name[0]
                                                        if email_prefix_norm == lastname_first_middle_initial:
                                                            # Store this email prefix pattern (using same key format as jmsmith)
                                                            key = (first_name[0], middle_name[0], last_name)
                                                            email_prefix_abbrev_middle_to_uid.setdefault(key, set()).add(uid)

    # Match email prefix abbreviations with full names
    # Match "jsmith" and "smithj" patterns with full first names
    if CONFIG.get('ABBREV_FIRST_NAME', True):
        for key, email_prefix_uids in email_prefix_abbrev_first_to_uid.items():
            if key in full_first_to_uid:
                full_uids = full_first_to_uid[key]
                first_initial, last_name = key
                # Check if the non-abbreviated part (last_name) is a common first name
                # If so, skip matching to avoid false positives like "michaelh" matching both
                # "Michael Hilton" and "Michael Huber" (where "michael" is the common first name)
                # But allow matches like "doej" with "John Doe" (where "doe" is not common)
                skip_all = False
                if last_name and last_name.lower() in [n.lower() for n in blacklist_first_names]:
                    skip_all = True

                if not skip_all and len(email_prefix_uids) + len(full_uids) > THR_MIN and len(email_prefix_uids) + len(full_uids) < THR_MAX:
                    for email_uid in email_prefix_uids:
                        for full_uid in full_uids:
                            if email_uid != full_uid:
                                a, b = sorted([email_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_FIRST_NAME)

    # Match "johns" pattern with full last names
    if CONFIG.get('ABBREV_LAST_NAME', True):
        for key, email_prefix_uids in email_prefix_abbrev_last_to_uid.items():
            if key in full_last_to_uid:
                full_uids = full_last_to_uid[key]
                first_name, last_initial = key
                # Check if the non-abbreviated part (first_name) is a common first name
                # If so, skip matching to avoid false positives like "michaelh" matching both
                # "Michael Hilton" and "Michael Huber" (where "michael" is the common first name)
                # But allow matches like "doej" with "Doe John" (where "doe" is not common)
                skip_all = False
                if first_name and first_name.lower() in [n.lower() for n in blacklist_first_names]:
                    skip_all = True

                if not skip_all and len(email_prefix_uids) + len(full_uids) > THR_MIN and len(email_prefix_uids) + len(full_uids) < THR_MAX:
                    for email_uid in email_prefix_uids:
                        for full_uid in full_uids:
                            if email_uid != full_uid:
                                a, b = sorted([email_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_LAST_NAME)

    # Match "jmsmith" pattern with full middle names
    if CONFIG.get('ABBREV_MIDDLE_NAME', True):
        for key, email_prefix_uids in email_prefix_abbrev_middle_to_uid.items():
            if key in full_middle_to_uid:
                full_uids = full_middle_to_uid[key]
                if len(email_prefix_uids) + len(full_uids) > THR_MIN and len(email_prefix_uids) + len(full_uids) < THR_MAX:
                    for email_uid in email_prefix_uids:
                        for full_uid in full_uids:
                            if email_uid != full_uid:
                                a, b = sorted([email_uid, full_uid])
                                clues.setdefault((a, b), [])
                                clues[(a, b)].append(ABBREV_MIDDLE_NAME)

    if CONFIG.get('SWITCHED_NAME', True):
        # Switched name matching: Match aliases where first and last name order is swapped.
        # Handles:
        # (1) "Lastname First"      <-> "First Lastname"      (2-word, no comma)
        # (2) "Lastname, First"     <-> "First Lastname"      (2-word, comma format)
        # (3) "Lastname First M"    <-> "First M Lastname"    (middle initial, no comma)
        # (4) "Lastname, First M"   <-> "First M Lastname"    (comma + middle initial)
        # (5) "Lastname, First Mid" <-> "First Mid Lastname"  (comma + full middle name)
        #
        # Strategy: build two separate 2-word indexes.
        #   normal_idx:     canonical forms from ordinary normalization
        #   rearranged_idx: canonical forms after explicitly rearranging switched names
        #
        # Cross-matching then works two ways:
        #   Type 1 – "A B" in normal_idx vs "B A" in normal_idx  → catches cases (1) & (2)
        #   Type 2 – key in rearranged_idx vs same key in normal_idx → catches cases (3–5)
        #            (both aliases arrive at the same canonical form via different paths)

        normal_idx = {}      # 2-word normalized name -> set of uids (normal path)
        rearranged_idx = {}  # 2-word normalized name -> set of uids (rearrangement path)

        def _qualify(norm):
            """Return True if norm is a usable 2-word canonical name."""
            if not norm:
                return False
            parts = norm.split()
            return (len(parts) == 2
                    and len(parts[0]) >= 2 and len(parts[1]) >= 2
                    and norm not in blacklist)

        for uid, alias in aliases.items():
            if not alias.name:
                continue
            raw_name = alias.name.strip()

            # Normal normalization — correctly handles canonical "First [M] Last" names
            # and 2-word "Last First" / "Last, First" for cases (1) and (2)
            for norm in get_normalized_forms(raw_name, alias.email_domain, alias.location):
                if _qualify(norm):
                    normal_idx.setdefault(norm, set()).add(uid)

            # Rearrangement paths for switched names where normal normalization would
            # strip the wrong word (it assumes First-Middle-Last positional order).
            if ',' in raw_name:
                # Cases (2), (4), (5): "Last, First [Middle...]"
                # The comma is an explicit marker that the last name comes first.
                comma_idx = raw_name.index(',')
                last_word = raw_name[:comma_idx].strip()
                first_and_rest = raw_name[comma_idx + 1:].strip()
                # Guard: last part must be a single token (e.g. not "van der Berg, Jan")
                if last_word and ' ' not in last_word and first_and_rest:
                    rearranged = first_and_rest + ' ' + last_word
                    for norm in get_normalized_forms(rearranged, alias.email_domain, alias.location):
                        if _qualify(norm):
                            rearranged_idx.setdefault(norm, set()).add(uid)
            else:
                # Case (3): "Last First M" — rearrange only when the trailing token is a
                # single-character initial, which strongly signals Last-First-Initial order.
                words = raw_name.split()
                if len(words) == 3 and len(words[2].rstrip('.')) == 1:
                    rearranged = words[1] + ' ' + words[2] + ' ' + words[0]
                    for norm in get_normalized_forms(rearranged, alias.email_domain, alias.location):
                        if _qualify(norm):
                            rearranged_idx.setdefault(norm, set()).add(uid)

        seen_switched_pairs = set()

        def _add_switched_clue(uid_a, uid_b):
            if uid_a == uid_b:
                return
            a, b = min(uid_a, uid_b), max(uid_a, uid_b)
            if (a, b) not in seen_switched_pairs:
                seen_switched_pairs.add((a, b))
                clues.setdefault((a, b), [])
                if SWITCHED_NAME not in clues[(a, b)]:
                    clues[(a, b)].append(SWITCHED_NAME)

        # Type 1: normal "A B"  vs  normal "B A"  — catches cases (1) and (2)
        for name, uids in normal_idx.items():
            parts = name.split()
            reversed_name = parts[1] + ' ' + parts[0]
            if reversed_name in normal_idx and reversed_name != name:
                reversed_uids = normal_idx[reversed_name]
                if len(uids) < THR_MAX and len(reversed_uids) < THR_MAX:
                    for uid_a in uids:
                        for uid_b in reversed_uids:
                            _add_switched_clue(uid_a, uid_b)

        # Type 2: rearranged canonical form  vs  same key in normal index
        # Catches cases (3–5) where both aliases share the same canonical form but
        # one alias arrived there via explicit rearrangement (switched order detected).
        for name, re_uids in rearranged_idx.items():
            if name in normal_idx:
                norm_uids = normal_idx[name]
                if len(re_uids) < THR_MAX and len(norm_uids) < THR_MAX:
                    for uid_a in re_uids:
                        for uid_b in norm_uids:
                            _add_switched_clue(uid_a, uid_b)

    if CONFIG.get('LOCATION', True):
        for location, set_uid in d_location_uid.items():
            if len(set_uid) > THR_MIN:
                for a,b in combinations(sorted(set_uid, key=lambda uid:int(uid)), 2):
                    na = d_uid_name[a]
                    nb = d_uid_name[b]
                    if na is not None and nb is not None and len(na.split()) > 1 and na==nb and na not in blacklist:
                        if len(d_name_uid.get(na, set([]))) < THR_MAX:
                            clues.setdefault((a, b), [])
                            clues[(a, b)].append(LOCATION)

    d_alias_map = {}
    clusters = {}
    labels = {}

    def merge(a,b,rule):
        # Contract: a < b
        assert a<b, "A must be less than B"

        if DEBUG:
            print(f"\n=== MERGE OPERATION ===")
            print(f"Merging: {aliases[a].name} <{aliases[a].email}> (ID: {a})")
            print(f"   with: {aliases[b].name} <{aliases[b].email}> (ID: {b})")
            print(f"   Rule: {rule}")

        if a in d_alias_map.keys():
            if b in d_alias_map.keys():
                if d_alias_map[a] == d_alias_map[b]:
                    if DEBUG:
                        print(f"   Both already in same cluster {d_alias_map[a]}, just adding rule")
                    labels[d_alias_map[a]].append(rule)
                else:
                    lowest = min(d_alias_map[a], d_alias_map[b])
                    highest = max(d_alias_map[a], d_alias_map[b])
                    if DEBUG:
                        print(f"   Merging clusters {d_alias_map[a]} and {d_alias_map[b]} into cluster {lowest}")
                        print(f"   Cluster {lowest} before merge: {[aliases[uid].name for uid in clusters[lowest]]}")
                        print(f"   Cluster {highest} before merge: {[aliases[uid].name for uid in clusters[highest]]}")
                    labels[lowest].extend(labels[highest])
                    labels[lowest].append(rule)
                    clusters[lowest].update(clusters[highest])
                    for x in clusters[highest]: d_alias_map[x] = lowest
                    del labels[highest]
                    del clusters[highest]
                    d_alias_map[a] = lowest
                    d_alias_map[b] = lowest
                    if DEBUG:
                        print(f"   Merged cluster {lowest}: {[aliases[uid].name for uid in clusters[lowest]]}")

            else:
                # a is an alias; first time I see b
                if DEBUG:
                    print(f"   Adding {aliases[b].name} to existing cluster {d_alias_map[a]}")
                    print(f"   Cluster before: {[aliases[uid].name for uid in clusters[d_alias_map[a]]]}")
                d_alias_map[b] = d_alias_map[a]
                clusters[d_alias_map[a]].add(b)
                labels[d_alias_map[a]].append(rule)
                if DEBUG:
                    print(f"   Cluster after: {[aliases[uid].name for uid in clusters[d_alias_map[a]]]}")
        else:
            if b in d_alias_map.keys():
                #b_src = d_alias_map[b] # b_src < a by construction
                if DEBUG:
                    print(f"   Adding {aliases[a].name} to existing cluster {d_alias_map[b]}")
                    print(f"   Cluster before: {[aliases[uid].name for uid in clusters[d_alias_map[b]]]}")
                d_alias_map[a] = d_alias_map[b]
                clusters[d_alias_map[b]].add(a)
                labels[d_alias_map[b]].append(rule)
                if DEBUG:
                    print(f"   Cluster after: {[aliases[uid].name for uid in clusters[d_alias_map[b]]]}")
            else:
                # First time I see this pair (guaranteed sorted)
                if DEBUG:
                    print(f"   Creating new cluster {a}")
                d_alias_map[a] = a
                d_alias_map[b] = a
                clusters[a] = set([a,b])
                labels[a] = [rule]
                if DEBUG:
                    print(f"   New cluster: {[aliases[uid].name for uid in clusters[a]]}")


    if DEBUG:
        print(f"\n=== PROCESSING CLUES ===")
        print(f"Found {len(clues)} potential merges based on various rules")

    for (a,b), list_clues in sorted(clues.items(), key=lambda e:(int(e[0][0]),int(e[0][1]))):
        aa = aliases[a]
        ab = aliases[b]

        # Show clues for names that might interest us
        if DEBUG:
            print(f"\nFound potential match:")
            print(f"  {aa.name} <{aa.email}> (ID: {a})")
            print(f"  {ab.name} <{ab.email}> (ID: {b})")
            print(f"  Clues: {list_clues}")

        if EMAIL in list_clues:
            merge(a,b,EMAIL)
        elif len(set(list_clues)) >= 2:
            effective_clues = set(list_clues)
            # DOMAIN_NAME_MATCH is not independent from EMAIL_DOMAIN / DOMAIN_MAIN_PART:
            # both signals are derived from the same email domain, so they should not
            # count as two independent clues. Discard DOMAIN_NAME_MATCH when a domain
            # clue is already present to prevent a single underlying signal from
            # satisfying the two-clue threshold on its own.
            if DOMAIN_NAME_MATCH in effective_clues and (DOMAIN in effective_clues or DOMAIN_MAIN_PART in effective_clues):
                effective_clues.discard(DOMAIN_NAME_MATCH)
            if len(effective_clues) >= 2:
                for clue in effective_clues:
                    merge(a,b,clue)
        elif FULL_NAME in list_clues:
            merge(a,b,FULL_NAME)
        elif SWITCHED_NAME in list_clues:
            merge(a,b,SWITCHED_NAME)
        elif COMP_EMAIL_PREFIX in list_clues:
            merge(a,b,COMP_EMAIL_PREFIX)
        elif SIMPLE_NAME in list_clues:
            merge(a,b,SIMPLE_NAME)
        elif PREFIX_NAME in list_clues:
            merge(a,b,PREFIX_NAME)
        elif DOMAIN_NAME_MATCH in list_clues:
            merge(a,b,DOMAIN_NAME_MATCH)
        elif GITHUB_USERNAME_MATCH in list_clues:
            merge(a,b,GITHUB_USERNAME_MATCH)
        elif COMMA_SUFFIX_MATCH in list_clues:
            merge(a,b,COMMA_SUFFIX_MATCH)
        elif ABBREV_FIRST_NAME in list_clues:
            merge(a,b,ABBREV_FIRST_NAME)
        elif ABBREV_LAST_NAME in list_clues:
            merge(a,b,ABBREV_LAST_NAME)
        elif ABBREV_MIDDLE_NAME in list_clues:
            merge(a,b,ABBREV_MIDDLE_NAME)


    if DEBUG:
        print('Done: clusters')
        print(f"\n=== FINAL CLUSTERS SUMMARY ===")
        print(f"Total clusters formed: {len(clusters)}")

        for cluster_id, member_uids in clusters.items():
            cluster_members = [aliases[uid] for uid in member_uids]
            print(f"\n--- CLUSTER {cluster_id} ---")
            print(f"Members ({len(cluster_members)}):")
            for member in cluster_members:
                print(f"  - {member.name} <{member.email}> (ID: {member.uid})")
            print(f"Rules that formed this cluster: {labels.get(cluster_id, [])}")

    for uid, member_uids in clusters.items():
        members = [aliases[m] for m in member_uids]

        # Filter members with location
        real = members
        with_location = [m for m in members if m.location is not None]

        # Count rules that fired
        cl = Counter(labels[uid])

        is_valid = False

        if len(members) > THR_MAX:
            continue

        # If all have the same email there is no doubt
        if cl.get(EMAIL,0) >= (len(members)-1):
            is_valid = True
        #elif cl.get(SIMPLE_EMAIL_PREFIX,0) >= 1: # and
        elif cl.get(DOMAIN_MAIN_PART,0) >= 1 and cl.get(SIMPLE_EMAIL_PREFIX,0) >= 1:
            #for m in members:
            #    print(m.uid, m.login, m.name, m.email, m.location)
            is_valid = True
        # If domain matches name
        elif cl.get(DOMAIN_NAME_MATCH,0) >= 1:
            is_valid = True
        # If GitHub username matches name
        elif cl.get(GITHUB_USERNAME_MATCH,0) >= 1:
            is_valid = True
        # If names match with comma suffix difference
        elif cl.get(COMMA_SUFFIX_MATCH,0) >= 1:
            is_valid = True
        # If names match with abbreviation (first, last, or middle name)
        elif cl.get(ABBREV_FIRST_NAME,0) >= 1:
            is_valid = True
        elif cl.get(ABBREV_LAST_NAME,0) >= 1:
            is_valid = True
        elif cl.get(ABBREV_MIDDLE_NAME,0) >= 1:
            is_valid = True
        # If all members have the same email
        elif len(Counter([m.email for m in members]).keys()) == 1:
            is_valid = True
        # If there is at most one member, at least two rules fired, and each rule applied to each pair
        elif len(members) <= 1 and len(cl.keys()) > 1 and min(cl.values()) >= (len(members)-1):
            is_valid = True
        # At most 100 members, the only rule that fired is COMP_EMAIL_PREFIX or FULL_NAME or SIMPLE_EMAIL_PREFIX or SWITCHED_NAME
        elif len(members) <= 100 and \
                (cl.get(COMP_EMAIL_PREFIX,0) or cl.get(FULL_NAME,0) or cl.get(SIMPLE_EMAIL_PREFIX,0) or cl.get(SWITCHED_NAME,0)):
            # Check if this might be a bot using multiple real users' email addresses
            if cl.get(FULL_NAME,0) or cl.get(SWITCHED_NAME,0):
                email_domains = set([m.email_domain for m in members if m.email_domain])
                has_generic_name = any(m.is_weird_id[3] if isinstance(m.is_weird_id, (tuple, list)) and len(m.is_weird_id) > 3 else False for m in members if hasattr(m, 'is_weird_id'))

                # If different domains and generic name, don't merge (likely a bot)
                if len(email_domains) > 1 and has_generic_name:
                    is_valid = False
                else:
                    is_valid = True
            else:
                is_valid = True
        # All with same full name and location / same full name and email domain
        elif cl.get(FULL_NAME,0) >= (len(members)-1) and \
                (cl.get(LOCATION,0) >= (len(members)-1) or cl.get(DOMAIN,0) >= (len(members)-1)):
            is_valid = True
        # All with same full name
        elif cl.get(FULL_NAME,0) >= (len(members)-1):
            # Check if this might be a bot using multiple real users' email addresses
            # Don't merge if: (1) members have different email domains AND (2) name contains generic/bot terms
            email_domains = set([m.email_domain for m in members if m.email_domain])

            # Check if any member has a name flagged as generic (is_weird_id[3])
            has_generic_name = any(m.is_weird_id[3] for m in members if hasattr(m, 'is_weird_id') and len(m.is_weird_id) > 3)

            # If different domains and generic name, don't merge (likely a bot)
            if len(email_domains) > 1 and has_generic_name:
                is_valid = False
            else:
                is_valid = True
        # All with same login
        elif cl.get(LOGIN_NAME,0) >= (len(members)-1):
            is_valid = True
        # The only two rules that fired are full name and email, in some combination
        elif len(cl.keys()) == 2 and cl.get(FULL_NAME,0) > 0 and cl.get(EMAIL,0) > 0:
            is_valid = True
        elif len(cl.keys()) == 3 and cl.get(FULL_NAME,0) > 0 and cl.get(EMAIL,0) > 0 and cl.get(SIMPLE_NAME,0) > 0:
            is_valid = True
        elif len(cl.keys()) == 2 and cl.get(EMAIL,0) > 0 and cl.get(SIMPLE_NAME,0) > 0:
            is_valid = True
        elif cl.get(PREFIX_NAME,0) > 0:
            is_valid = True
        else:
            # Split by email address if at least 2 share one
            if cl.get(EMAIL,0):
                ce = [e for e,c in Counter([m.email for m in members]).items() if c > 1]
                for e in ce:
                    extra_members = [m for m in members if m.email==e]
                    extra_with_location = [m for m in extra_members if m.location is not None]

                    if len(extra_with_location):
                        # Filter out names with ill-encoded characters
                        extra_with_location_clean = [m for m in extra_with_location if not has_ill_encoded_characters(m.name)]
                        if len(extra_with_location_clean) > 0:
                            extra_with_location = extra_with_location_clean
                        # Pick the one with the oldest account with location, if available
                        rep = sorted(extra_with_location, key=lambda m:int(m.uid))[0]
                    else:
                        # Filter out names with ill-encoded characters
                        extra_members_clean = [m for m in extra_members if not has_ill_encoded_characters(m.name)]
                        if len(extra_members_clean) > 0:
                            extra_members = extra_members_clean
                        # Otherwise pick the one with the oldest account
                        rep = sorted(extra_members, key=lambda m:int(m.uid))[0]

                    for a in extra_members:
                        if a.uid != rep.uid:
                            unmask[raw[a.uid]] = raw[rep.uid]


        if is_valid:
            # Determine group representative
            if len(real):
                if len(with_location):
                    # Pick the one with the oldest account with location, if available
                    rep = sorted(with_location, key=lambda m:int(m.uid))[0]
                else:
                    # Pick the one whose name consists of two parts
                    # (e.g. "John Smith")
                    # get all names that have two parts and where both the first and the second parts are larger than 1 character
                    # Filter out names with ill-encoded characters
                    two_parts = [m for m in real if len(m.name.split(' ')) == 2 and len(m.name.split(' ')[0]) > 1 and len(m.name.split(' ')[1]) > 1 and not has_ill_encoded_characters(m.name)]

                    if DEBUG:
                        print(two_parts)
                    if len(two_parts) >= 1:
                        if len(two_parts) > 1:
                            # remove those whose email consists of +
                            two_parts_without_plus = [m for m in two_parts if m.email_prefix and not regex.search(r'\+', m.email_prefix)]
                            if len(two_parts_without_plus) > 0:
                                two_parts = two_parts_without_plus

                        # pick the one with the longest/most complete name
                        # Sort by: 1) total length, 2) minimum part length, 3) proper capitalization, 4) alphabetically
                        def capitalization_score(name):
                            # Prefer proper title case (first letter of each word capitalized)
                            # 0 = proper title case, 1 = mixed case, 2 = all lower, 3 = all upper
                            if name == ' '.join(word.capitalize() for word in name.split()):
                                return 0  # Proper title case
                            elif name.islower():
                                return 2  # All lowercase
                            elif name.isupper():
                                return 3  # All uppercase
                            else:
                                return 1  # Mixed case

                        rep = sorted(two_parts, key=lambda m: (-len(m.name), -min(len(part) for part in m.name.split()), capitalization_score(m.name), m.name))[0]
                    # Otherwise pick the one with the oldest account
                    else:
                        if len(real) > 1:
                            # remove those whose email consists of +
                            real_without_plus = [m for m in real if m.email_prefix and not regex.search(r'\+', m.email_prefix)]
                            if len(real_without_plus) > 0:
                                real = real_without_plus

                            # Filter out names with ill-encoded characters
                            real_without_encoding_issues = [m for m in real if not has_ill_encoded_characters(m.name)]
                            if len(real_without_encoding_issues) > 0:
                                real = real_without_encoding_issues

                        rep = sorted(real, key=lambda m:int(m.uid))[0]
            else:
                if len(members) > 1:
                    # remove those whose email consists of +
                        members_without_plus = [m for m in members if m.email_prefix and not regex.search(r'\+', m.email_prefix)]
                        if len(members_without_plus) > 0:
                            members = members_without_plus

                        # Filter out names with ill-encoded characters
                        members_without_encoding_issues = [m for m in members if not has_ill_encoded_characters(m.name)]
                        if len(members_without_encoding_issues) > 0:
                            members = members_without_encoding_issues

                rep = sorted(members, key=lambda m:int(m.uid))[0]

            for a in members:
                if a.uid != rep.uid:
                    unmask[raw[a.uid]] = raw[rep.uid]

    return(unmask)



def is_alias_weird_id(alias):
    """
    Check if the given user ID is a weird ID based on specific criteria.

    Args:
        alias (Alias): An instance of the Alias class containing user information.
    Returns:
        a touple of bools:
            - name_is_weird_id_git_provider: True if the alias name is associated with a known git hosting provider.
            - name_is_weird_id_email_provider: True if the alias name is associated with a known email provider.
            - name_is_weird_id_first_name: True if the alias name is a common first name.
            - name_is_weird_id_generic: True if the alias name contains generic terms.
            - name_is_weird_id_name_is_missing: True if the alias name is missing or empty.
            - name_is_weird_id_name_is_machine_name: True if the alias name is a known machine name.
            - name_is_weird_id_privacy_email: True if the alias name is a privacy-preserving email domain.
            - email_is_weird_id_git_provider: True if the alias email is associated with a known git hosting provider.
            - email_is_weird_id_generic: True if the alias email contains generic terms.
            - email_is_weird_id_email_is_missing: True if the alias email is missing or empty.
            - email_is_weird_id_email_is_machine_name: True if the alias email is a known machine name.
            - email_is_weird_id_privacy_email: True if the alias email is a privacy-preserving email domain.
    """

    contains_github = (alias.email and 'github' in alias.email.lower()) or (alias.email_domain and 'github' in alias.email_domain.lower())
    name_is_first_name = alias.name.lower() in get_blacklist(include_first_names=True)
    name_is_not_a_single_human = alias.name.lower() in get_blacklist(include_first_names=False)
    name_is_missing = alias.name is None or len(alias.name) == 0


    if len(alias.name) < 3:
        if DEBUG:
            print(f"Name too short: {alias.name}")
        return False

    name_is_weird_id_git_provider = False
    name_is_weird_id_email_provider = False
    name_is_weird_id_first_name = False
    name_is_weird_id_generic = False
    name_is_weird_id_name_is_missing = False
    name_is_weird_id_name_is_machine_name = False
    name_is_weird_id_privacy_email = False

    email_is_weird_id_git_provider = False
    email_is_weird_id_generic = False
    email_is_weird_id_email_is_missing = False
    email_is_weird_id_email_is_machine_name = False
    email_is_weird_id_privacy_email = False


    if normalize_for_comparison(alias.name.lower()) in blacklist_git_hosting:
        if DEBUG:
            print(f"Name is git hosting provider: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_git_provider = True

    if normalize_for_comparison(alias.name.lower()) in blacklist_email_providers:
        if DEBUG:
            print(f"Name is email provider: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_email_provider = True

    if normalize_for_comparison(alias.name.lower()) in blacklist_first_names:
        if DEBUG:
            print(f"Name is first name: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_first_name = True

    if normalize_for_comparison(alias.name.lower()) in blacklist_generic_terms:
        if DEBUG:
            print(f"Name is generic term: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_generic = True
    elif len(alias.name.split(' ')) > 1 or len(alias.name.split('_')) > 1 or len(alias.name.split('.')) > 1 or len(alias.name.split('+')) > 1:
        if any(normalize_for_comparison(part.lower()) in blacklist_generic_terms for part in alias.name.split(' ')) or any(normalize_for_comparison(part.lower()) in blacklist_generic_terms for part in alias.name.split('_')) \
        or any(normalize_for_comparison(part.lower()) in blacklist_generic_terms for part in alias.name.split('.')) or any(normalize_for_comparison(part.lower()) in blacklist_generic_terms for part in alias.name.split('+')):
            if DEBUG:
                print(f"Name is generic term: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
            name_is_weird_id_generic = True

    if normalize_for_comparison(alias.name.lower()) in blacklist_no_names:
        if DEBUG:
            print(f"Name is missing: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_name_is_missing = True
    elif len(alias.name.split(' ')) == 1 and len(alias.name.split('_')) == 1 and len(alias.name.split('.')) == 1 and len(alias.name.split('+')) == 1:
        if any(normalize_for_comparison(part.lower()) in blacklist_no_names for part in alias.name.split(' ')) or any(normalize_for_comparison(part.lower()) in blacklist_no_names for part in alias.name.split('_')) \
        or any(normalize_for_comparison(part.lower()) in blacklist_no_names for part in alias.name.split('.')) or any(normalize_for_comparison(part.lower()) in blacklist_no_names for part in alias.name.split('+')):
            if DEBUG:
                print(f"Name is missing: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
            name_is_weird_id_name_is_missing = True


    if normalize_for_comparison(alias.name.lower()) in blacklist_machine_names:
        if DEBUG:
            print(f"Name is machine name: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_name_is_machine_name = True

    if normalize_for_comparison(alias.name.lower()) in blacklist_privacy_emails:
        if DEBUG:
            print(f"Name is privacy email: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        name_is_weird_id_privacy_email = True


    if (alias.email and alias.email.lower() in blacklist_git_hosting) or (alias.email_domain and alias.email_domain.lower() in blacklist_git_hosting) or (alias.email_prefix and alias.email_prefix.lower() in blacklist_git_hosting):
        if DEBUG:
            print(f"Email is git hosting provider: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        email_is_weird_id_git_provider = True


    if (alias.email and alias.email.lower() in blacklist_generic_terms) or (alias.email_domain and alias.email_domain.lower() in blacklist_generic_terms) or (alias.email_prefix and alias.email_prefix.lower() in blacklist_generic_terms):
        if DEBUG:
            print(f"Email is generic term: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        email_is_weird_id_generic = True
    elif alias.email_prefix and (len(alias.email_prefix.split('.')) > 1 or len(alias.email_prefix.split('_')) > 1):
        if alias.email_prefix and (any(part.lower() in blacklist_generic_terms for part in alias.email_prefix.split('.')) or any(part.lower() in blacklist_generic_terms for part in alias.email_prefix.split('_'))):
            if DEBUG:
                print(f"Email is generic term: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
            email_is_weird_id_generic = True

    if (alias.email and alias.email.lower() in blacklist_machine_names) or (alias.email_domain and alias.email_domain.lower() in blacklist_machine_names) or (alias.email_prefix and alias.email_prefix.lower() in blacklist_machine_names):
        if DEBUG:
            print(f"Email is machine name: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        email_is_weird_id_email_is_machine_name = True

    if (alias.email and alias.email.lower() in blacklist_privacy_emails) or (alias.email_domain and alias.email_domain.lower() in blacklist_privacy_emails) or (alias.email_prefix and alias.email_prefix.lower() in blacklist_privacy_emails):
        if DEBUG:
            print(f"Email is privacy email: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        email_is_weird_id_privacy_email = True

    if (alias.email and alias.email.lower() in blacklist_no_names) or (alias.email_domain and alias.email_domain.lower() in blacklist_no_names) or (alias.email_prefix and alias.email_prefix.lower() in blacklist_no_names):
        if DEBUG:
            print(f"Email is missing: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
        email_is_weird_id_email_is_missing = True
    elif alias.email_prefix and len(alias.email_prefix.split(' ')) == 1 and len(alias.email_prefix.split('_')) == 1:
        if alias.email_prefix and (any(part.lower() in blacklist_no_names for part in alias.email_prefix.split(' ')) or any(part.lower() in blacklist_no_names for part in alias.email_prefix.split('_'))):
            if DEBUG:
                print(f"Email is missing: {alias.name}, {alias.email}, {alias.email_domain}, {alias.email_prefix}")
            email_is_weird_id_email_is_missing = True


    return (name_is_weird_id_git_provider,
            name_is_weird_id_email_provider,
            name_is_weird_id_first_name,
            name_is_weird_id_generic,
            name_is_weird_id_name_is_missing,
            name_is_weird_id_name_is_machine_name,
            name_is_weird_id_privacy_email,
            email_is_weird_id_git_provider,
            email_is_weird_id_generic,
            email_is_weird_id_email_is_missing,
            email_is_weird_id_email_is_machine_name,
            email_is_weird_id_privacy_email)
