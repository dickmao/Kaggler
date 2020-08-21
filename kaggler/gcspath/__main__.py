"""
Kicks things off via

python -m gcspath

"""

import sys

from .gcspath import gcspath

competition = sys.argv[1] if len(sys.argv) > 1 else None
print(gcspath(competition))
