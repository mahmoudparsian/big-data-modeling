#!/usr/bin/env python3
"""
word_count_single_file_v2.py

Count word frequencies in a single text file.

The program mirrors a basic MapReduce word count:

1. Map: tokenize each line into normalized words.
2. Reduce: aggregate each word's frequency in a dictionary.

Usage:
    python3 word_count_single_file_v2.py <input_file>
    
Run it with:
    python3 word_count_single_file_v2.py test_file.txt
    
Or sort by descending frequency:
    python3 word_count_single_file_v2.py test_file.txt --by-frequency

Compared with line.split(), the regular-expression 
tokenizer prevents "fox", "fox,", and "fox. from being 
counted as three different words. casefold() also provides 
stronger Unicode-aware normalization than lower().

"""

import argparse
import re
from pathlib import Path


# Words may contain letters, numbers, and internal apostrophes.
# Examples: "fox", "Python3", "don't"
WORD_PATTERN = re.compile(r"[^\W_]+(?:['’][^\W_]+)*", re.UNICODE)


def count_words(file_name: str | Path) -> dict[str, int]:
    """Count the words in a UTF-8 text file.

    Args:
        file_name: Path to the input text file.

    Returns:
        A dictionary mapping each normalized word to its frequency.
    """
    word_counts: dict[str, int] = {}

    with Path(file_name).open(
        mode="r",
        encoding="utf-8",
        errors="replace",
    ) as text_file:
        for line in text_file:
            for word in WORD_PATTERN.findall(line.casefold()):
                word_counts[word] = word_counts.get(word, 0) + 1

    return word_counts


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Count word frequencies in a text file."
    )
    parser.add_argument(
        "input_file",
        type=Path,
        help="path to the input text file",
    )
    parser.add_argument(
        "--by-frequency",
        action="store_true",
        help="sort words by frequency instead of alphabetically",
    )
    return parser.parse_args()


def main() -> int:
    """Run the command-line program."""
    args = parse_arguments()

    try:
        input_file = args.input_file
        print("input_file=", input_file)
        word_counts = count_words(input_file)
    except OSError as error:
        print(f"Error: unable to read {input_file}: {error}")
        return 1

    if args.by_frequency:
        # Highest frequency first; alphabetic order breaks ties.
        results = sorted(
            word_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
    else:
        results = sorted(word_counts.items())

    for word, frequency in results:
        print(f"{word}: {frequency}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
SAMPLE RUN:

% python3 word_count_single_file_v2.py test_file.txt
input_file= test_file.txt
and: 3
cute: 1
fox: 8
gray: 2
is: 1
jumped: 7
over: 3
red: 3
"""